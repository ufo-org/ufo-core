use core::slice;
use std::io::Error;
use std::num::NonZeroUsize;
use std::panic;
use std::sync::{Arc, RwLock, RwLockReadGuard, Weak};

use anyhow::Result;
use num::Integer;

use log::{debug, trace};

use crate::events::{UfoEvent, UfoUnloadDisposition};
use crate::once_await::OnceAwait;
use crate::once_await::OnceFulfiller;
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
    length: Option<NonZeroUsize>,
    hash: Arc<OnceAwait<Option<DataHash>>>,
}

impl UfoChunk {
    pub fn new(
        arc: &WrappedUfoObject,
        object: &RwLockReadGuard<UfoObject>,
        offset: UfoOffset,
        length: usize,
    ) -> UfoChunk {
        assert!(length > 0);
        assert!(
            offset.absolute_offset() + length <= object.mmap.length(),
            "{} + {} > {}",
            offset.absolute_offset(),
            length,
            object.mmap.length()
        );
        UfoChunk {
            ufo_id: object.id,
            has_listener: object.config.writeback_listener.is_some(),
            object: Arc::downgrade(arc),
            offset,
            length: NonZeroUsize::new(length),
            hash: Arc::new(OnceAwait::new()),
        }
    }

    pub fn offset(&self) -> &UfoOffset {
        &self.offset
    }

    pub fn hash_fulfiller(&self) -> impl OnceFulfiller<Option<DataHash>> {
        self.hash.clone()
    }

    pub fn free_and_writeback_dirty(
        &mut self,
        event_queue: &UfoEventSender,
        pivot: &BaseMmap,
    ) -> Result<usize> {
        match (self.length, self.object.upgrade()) {
            (Some(length), Some(obj)) => {
                let length_bytes = length.get();
                let length_page_multiple = self.size_in_page_bytes();
                let obj = obj
                    .read()
                    .map_err(|_| anyhow::anyhow!("UFO {:?} lock poisoned", self.ufo_id()))?;

                trace!(target: "ufo_object", "free chunk {:?}@{} ({}b / {}pageBytes)",
                    self.ufo_id, self.offset(), length_bytes, length_page_multiple
                );

                if !obj.config.should_try_writeback() {
                    trace!(target: "ufo_object", "no writeback {:?}", self.ufo_id);
                    // Not doing writebacks, punch it out and leave
                    unsafe {
                        let data_ptr = obj.mmap.as_ptr().add(self.offset.absolute_offset());
                        check_return_zero(libc::madvise(
                            data_ptr.cast(),
                            length_page_multiple,
                            libc::MADV_DONTNEED,
                        ))?;
                    }
                    event_queue
                        .send_event(UfoEvent::UnloadChunk {
                            ufo_id: self.ufo_id.0,
                            disposition: UfoUnloadDisposition::ReadOnly,
                            memory_freed: length_page_multiple,
                        })
                        .map_err(|_| Error::new(std::io::ErrorKind::Other, "event_queue broken"))?;
                    return Ok(length_page_multiple);
                }

                // must get the hash before we try to lock the chunk
                // this guarantees that the lock on the chunk from populate will be clear
                debug!(target: "ufo_object", "{:?}@{} retrieve hash", self.ufo_id, self.offset());
                let stored_hash = self.hash.get();
                trace!(target: "ufo_object", "{:?}@{} hash retrieved", self.ufo_id, self.offset());

                // We first remap the pages from the UFO into the file backing
                // we check the value of the page after the remap because the remap
                //  is atomic and will let us read cleanly in the face of racing writers
                let chunk_number = self.offset.chunk_number();
                debug!("try to uncontended-lock {:?}.{}", obj.id, self.offset());
                let chunk_lock = obj
                    .writeback_util
                    .chunk_locks
                    .lock_uncontended(chunk_number)
                    .unwrap();
                trace!("locked {:?}@{}", obj.id, self.offset());
                unsafe {
                    anyhow::ensure!(length_page_multiple <= pivot.length(), "Pivot too small");
                    let data_ptr = obj.mmap.as_ptr().add(self.offset.absolute_offset());
                    let pivot_ptr = pivot.as_ptr();
                    check_ptr_nonneg(libc::mremap(
                        data_ptr.cast(),
                        length_page_multiple,
                        length_page_multiple,
                        libc::MREMAP_FIXED | libc::MREMAP_MAYMOVE | libc::MREMAP_DONTUNMAP,
                        pivot_ptr,
                    ))?;
                    trace!(target: "ufo_object", "{:?}@{} mremaped data to pivot", self.ufo_id, self.offset());
                }

                let mut was_on_disk = false;
                let mut written_to_disk = false;

                if let Some(hash) = stored_hash {
                    let calculated_hash = pivot.with_slice(0, length_bytes, hash_function);
                    trace!(target: "ufo_object", "{:?}@{} writeback hash matches {}", self.ufo_id, self.offset(), hash == &calculated_hash);
                    if hash != &calculated_hash {
                        let (_, rb): ((), Result<()>) = rayon::join(
                            || {
                                let start = self.offset.as_index_floor();
                                let end = (obj.config.elements_loaded_at_once() + start)
                                    .min(obj.config.element_ct());
                                let ptr = pivot.as_ptr();
                                if let Some(listener) = &obj.config.writeback_listener {
                                    listener(UfoWriteListenerEvent::Writeback {
                                        start_idx: start,
                                        end_idx: end,
                                        data: ptr,
                                    });
                                }
                            },
                            || {
                                let writeback_action_taken =
                                    pivot.with_slice(0, length_bytes, |data| {
                                        obj.writeback_util.writeback(&self.offset, data)
                                    })?;
                                was_on_disk = writeback_action_taken.was_on_disk();
                                written_to_disk = true;
                                Ok(())
                            },
                        );
                        rb?;
                    }
                }

                let unload_disposition = match (was_on_disk, written_to_disk) {
                    (false, true) => UfoUnloadDisposition::NewlyDirty,
                    (true, true) => UfoUnloadDisposition::ExistingDirty,
                    (_, false) => UfoUnloadDisposition::Clean,
                };

                event_queue
                    .send_event(UfoEvent::UnloadChunk {
                        ufo_id: self.ufo_id.0,
                        disposition: unload_disposition,
                        memory_freed: length_page_multiple,
                    })
                    .map_err(|_| Error::new(std::io::ErrorKind::Other, "event_queue broken"))?;

                self.length = None;
                trace!("unlock free {:?}@{}", obj.id, self.offset());
                chunk_lock.unlock();
                // return page multiple of bytes since memory is consumed by the page
                Ok(length_page_multiple)
            }
            _ => Ok(0),
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
                self.offset().chunk_number()
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

        let known_hash = self
            .hash
            .get()
            .ok_or(UfoInternalErr::UfoStateError("no chunk hash".to_string()))?;

        let chunk_slice = unsafe {
            let chunk_ptr: *const u8 = ufo.mmap.as_ptr().add(self.offset.absolute_offset());
            let chunk_length = length.get();
            slice::from_raw_parts(chunk_ptr, chunk_length)
        };

        println!("hashing {}", self.offset().chunk_number());
        let calculated_hash = hash_function(chunk_slice);
        println!("hashed {}", self.offset().chunk_number());

        if known_hash != calculated_hash {
            let start = self.offset.as_index_floor();
            let end = ufo.config.element_ct.min(start + self.size());
            (writeback_listener)(UfoWriteListenerEvent::Writeback {
                start_idx: start,
                end_idx: end,
                data: chunk_slice.as_ptr(),
            });
        }

        self.length = None;
        Ok(())
    }

    pub fn ufo_id(&self) -> UfoId {
        self.ufo_id
    }

    pub fn size(&self) -> usize {
        self.length.map(NonZeroUsize::get).unwrap_or(0)
    }

    pub(crate) fn size_in_page_bytes(&self) -> usize {
        self.size().next_multiple_of(&crate::get_page_size())
    }
}
