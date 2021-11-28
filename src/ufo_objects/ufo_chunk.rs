use core::slice;
use std::io::Error;
use std::panic;
use std::sync::{Arc, RwLock, RwLockReadGuard, Weak};

use anyhow::Result;

use libc::c_void;
use log::{debug, trace};

use crate::events::{UfoEvent, UfoUnloadDisposition};
use crate::once_await::OnceAwait;
use crate::once_await::OnceFulfiller;
use crate::sizes::*;
use crate::{UfoEventSender, UfoWriteListenerEvent};

use crate::mmap_wrapers::*;
use crate::return_checks::*;
use crate::ufo_core::*;

use super::*;

pub(crate) struct UfoChunk {
    ufo_id: UfoId,
    has_listener: bool,
    object: Weak<RwLock<UfoObject>>,
    offset: UfoOffset,
    length: Option<Bytes>,
    // hash: Arc<OnceAwait<Option<DataHash>>>,
}

impl UfoChunk {
    pub fn new(
        arc: &WrappedUfoObject,
        object: &RwLockReadGuard<UfoObject>,
        offset: UfoOffset,
        length: Bytes,
    ) -> UfoChunk {
        assert!(length.bytes > 0);
        assert!(
            offset.offset().from_base().bytes + length.bytes <= object.mmap.length(),
            "{} + {} > {}",
            offset.offset().from_base().bytes,
            length.bytes,
            object.mmap.length()
        );
        UfoChunk {
            ufo_id: object.id,
            has_listener: object.config.writeback_listener.is_some(),
            object: Arc::downgrade(arc),
            offset,
            length: Some(length),
            // hash: Arc::new(OnceAwait::new()),
        }
    }

    pub fn offset(&self) -> &UfoOffset {
        &self.offset
    }

    // pub fn hash_fulfiller(&self) -> impl OnceFulfiller<Option<DataHash>> {
    //     self.hash.clone()
    // }

    fn free_fast(
        &self,
        event_queue: &UfoEventSender,
        dispositon: UfoUnloadDisposition,
        obj: &UfoObject,
        length_page_multiple: &PageAlignedBytes,
    ) -> Result<PageAlignedBytes> {
        // Not doing a writeback, punch it out and leave
        unsafe {
            let offset_bytes = self.offset.offset().from_base().bytes;
            let data_ptr = obj.mmap.as_ptr().add(offset_bytes);
            check_return_zero(libc::madvise(
                data_ptr.cast(),
                length_page_multiple.aligned().bytes,
                libc::MADV_DONTNEED,
            ))?;
        }
        event_queue
            .send_event(UfoEvent::UnloadChunk {
                ufo_id: self.ufo_id.0,
                disposition: dispositon,
                memory_freed: length_page_multiple.aligned().bytes,
            })
            .map_err(|_| Error::new(std::io::ErrorKind::Other, "event_queue broken"))?;

        Ok(*length_page_multiple)
    }

    pub fn free_and_writeback_dirty(
        &mut self,
        event_queue: &UfoEventSender,
        pivot: &BaseMmap,
    ) -> Result<PageAlignedBytes> {
        match (self.length, self.object.upgrade()) {
            (Some(length), Some(obj)) => {
                let length_bytes = length.bytes;
                let length_page_multiple = self.size_in_page_bytes();
                let obj = obj
                    .read()
                    .map_err(|_| anyhow::anyhow!("UFO {:?} lock poisoned", self.ufo_id()))?;

                trace!(target: "ufo_object", "free chunk {:?}@{} ({}b / {}pageBytes)",
                    self.ufo_id, self.offset(), length_bytes, length_page_multiple.aligned().bytes
                );

                if !obj.config.should_try_writeback() {
                    trace!(target: "ufo_object", "no writeback {:?}", self.ufo_id);
                    // calls the event handler for us
                    return self.free_fast(
                        event_queue,
                        UfoUnloadDisposition::ReadOnly,
                        &obj,
                        &length_page_multiple,
                    );
                }

                let chunk_number = self.offset.chunk().absolute_offset();
                assert_eq!(
                    chunk_number,
                    obj.config
                        .chunk_size()
                        .align_down(&self.offset().offset().from_header())
                        .as_chunks()
                );

                let chunk_lock = obj
                    .writeback_util
                    .chunk_locks
                    .spinlock(chunk_number)
                    .unwrap();

                let is_dirty = obj.writeback_util.dirty_flags.test(chunk_number.chunks);
                if !is_dirty {
                    trace!(target: "ufo_object", "clean chunk {:?}", self.ufo_id);
                    // calls the event handler for us
                    return self.free_fast(
                        event_queue,
                        UfoUnloadDisposition::Clean,
                        &obj,
                        &length_page_multiple,
                    );
                }

                // Dirty, need to write back

                // performs the writeback with an mremap, which is an atomic move
                // even in the face of racing writers this will read SOMETHING cleanly
                // a writer that sees this chunk disappear will simply trigger another fault
                // and round and round we go
                // we hold the chunk lock while this is happening so we won't be interrupted by a racing popualte or write fault
                let action = obj.writeback_util.mremap_writeback(
                    &chunk_lock,
                    &length_page_multiple,
                    &self.offset,
                    &obj,
                )?;

                let unload_disposition = match action {
                    UfoWritebackAction::NewWriteback => UfoUnloadDisposition::NewlyDirty,
                    UfoWritebackAction::UpdateWriteback => UfoUnloadDisposition::ExistingDirty,
                };

                event_queue
                    .send_event(UfoEvent::UnloadChunk {
                        ufo_id: self.ufo_id.0,
                        disposition: unload_disposition,
                        memory_freed: length_page_multiple.aligned().bytes,
                    })
                    .map_err(|_| Error::new(std::io::ErrorKind::Other, "event_queue broken"))?;

                trace!("unlock free {:?}@{}", obj.id, self.offset());
                chunk_lock.droplockster();
                // return page multiple of bytes since memory is consumed by the page
                Ok(length_page_multiple)
            }
            _ => Ok(ToPage.align_down(&Bytes::from(0))),
        }
    }

    pub fn mark_freed_notify_listener(
        &mut self,
        ufo: &UfoObject,
    ) -> std::result::Result<(), UfoInternalErr> {
        let length;
        if let Some(l) = self.length {
            length = l;
        } else {
            debug!(
                "Chunk already free {:?}@{}",
                self.ufo_id(),
                self.offset().chunk().absolute_offset().chunks
            );
            return Ok(()); // Already freed
        }

        if !self.has_listener {
            // No listener, just drop it
            self.length = None;
            return Ok(());
        }

        let writeback_listener = ufo.config.writeback_listener.as_ref().ok_or_else(|| {
            UfoInternalErr::UfoStateError("cannot has_listener without a listener!".to_string())
        })?;

        let chunk_number = self.offset.chunk().absolute_offset();
        assert_eq!(
            chunk_number,
            ufo.config
                .chunk_size()
                .align_down(&self.offset().offset().from_header())
                .as_chunks()
        );

        let chunk_lock = ufo
            .writeback_util
            .chunk_locks
            .spinlock(chunk_number)
            .unwrap();
        
        if !ufo.writeback_util.dirty_flags.test(chunk_number.chunks) {
            // not dirty
            return Ok(());
        }

        let chunk_ptr = self.offset().offset().absolute_offset().bytes as *const u8;

        let start = self.offset.as_index_floor().absolute_offset().aligned();
        let elements = (length.bytes / ufo.config.stride().alignment_quantum().bytes).into();
        let end = ufo.config.element_ct.bound(start.add(&elements)).bounded();
        
        (writeback_listener)(UfoWriteListenerEvent::Writeback {
            start_idx: start.elements,
            end_idx: end.elements,
            data: chunk_ptr,
        });

        self.length = None;
        Ok(())
    }

    pub fn ufo_id(&self) -> UfoId {
        self.ufo_id
    }

    pub fn size(&self) -> Bytes {
        self.length.unwrap_or(Bytes::from(0))
    }

    pub(crate) fn size_in_page_bytes(&self) -> PageAlignedBytes {
        ToPage.align_up(&self.size())
    }
}
