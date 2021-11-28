use std::io::Error;

use anyhow::Result;

use libc::c_void;
use log::{debug, trace};

use crate::bitset::AtomicBitset;
use crate::bitwise_spinlock::{BitGuard, Bitlock};

use crate::mmap_wrapers::*;
use crate::return_checks::*;
use crate::sizes::*;
use crate::ufo_core::*;
use crate::{math::*, UfoWriteListenerEvent};

use super::*;

pub(crate) enum UfoWritebackAction {
    NewWriteback,
    UpdateWriteback,
}

impl UfoWritebackAction {
    pub fn was_on_disk(&self) -> bool {
        match self {
            UfoWritebackAction::NewWriteback => false,
            _ => true,
        }
    }
}

pub(crate) struct UfoFileWriteback {
    ufo_id: UfoId,
    mmap: MmapFd,
    chunk_ct: Total<Chunks>,
    final_chunk_size: Bytes,
    pub(crate) chunk_locks: Bitlock,
    pub(crate) dirty_flags: AtomicBitset,
    pub(crate) written_flags: AtomicBitset,
    chunk_size: ToChunk<Bytes>,
    total_bytes: Total<PageAlignedBytes>,
    body_bytes: Total<Bytes>,
    header_offset: FromHeader<Bytes, FromBase<Bytes>>,
    // bitlock_bytes: usize,
    // bitmap_bytes: usize,
}

impl UfoFileWriteback {
    pub fn new(
        ufo_id: UfoId,
        cfg: &UfoObjectConfig,
        core: &Arc<UfoCore>,
    ) -> Result<UfoFileWriteback, Error> {
        let chunk_ct: Total<Chunks> = cfg
            .elements_loaded_at_once
            .align_up(cfg.element_ct().total())
            .as_chunks()
            .as_total();
        assert!(
            chunk_ct.total().chunks * cfg.elements_loaded_at_once.alignment_quantum().elements
                >= cfg.element_ct.total().elements
        );

        let elements_at_once = cfg.elements_loaded_at_once;
        let stride_bytes = cfg.stride.alignment_quantum();

        let chunk_size: ToChunk<Bytes> =
            (elements_at_once.alignment_quantum().elements * stride_bytes.bytes).into();
        assert_eq!(
            chunk_size.alignment_quantum().bytes,
            ToPage
                .align_up(&chunk_size.alignment_quantum())
                .aligned()
                .bytes
        );

        let trailing_elements = cfg.element_ct().total().sub(
            &elements_at_once
                .align_down(cfg.element_ct().total())
                .aligned(),
        );

        let final_chunk_size = if 0 == trailing_elements.elements {
            cfg.chunk_size().alignment_quantum()
        } else {
            (trailing_elements.elements * stride_bytes.bytes).into()
        };

        let bitset_bytes = div_ceil(chunk_ct.total().chunks, 8).into(); /*8 bits per byte*/
        // Now we want to get the bitmap bytes up to the next multiple of the page size
        let bitset_bytes = ToPage.align_up(&bitset_bytes);
        assert!(bitset_bytes.aligned().bytes * 8 >= chunk_ct.total().chunks);

        let header_bytes = ToPage.align_up(&Bytes::from(bitset_bytes.aligned().bytes * 3));

        // round the mmap up to the nearest chunk size
        // when loading we need to give back chunks this large so even though no useful user data may
        // be in the last chunk we still need to have this available for in the readback chunk

        let data_bytes_with_padding = cfg.aligned_body_size();
        let total_bytes = header_bytes.add(&data_bytes_with_padding);

        let body_bytes = cfg.stride().as_bytes(cfg.element_ct().total());

        let temp_file = unsafe {
            OpenFile::temp(
                core.config.writeback_temp_path.as_str(),
                total_bytes.aligned().bytes,
            )
        }?;

        let mmap = MmapFd::new(
            total_bytes.aligned().bytes,
            &[MemoryProtectionFlag::Read, MemoryProtectionFlag::Write],
            &[MmapFlag::Shared],
            None,
            temp_file,
            0,
        )?;

        //TODO: do these with the mmap base
        let mmap_basis = Absolute::with_base(Bytes::from(mmap.as_ptr() as usize));
        let locks_offset = mmap_basis.relative(0.into());

        let dirty_offset = locks_offset.add(&bitset_bytes.aligned());
        assert!(locks_offset.absolute_offset().bytes < dirty_offset.absolute_offset().bytes);

        let written_offset = dirty_offset.add(&bitset_bytes.aligned());
        assert!(dirty_offset.absolute_offset().bytes < written_offset.absolute_offset().bytes);

        assert!(written_offset.from_base().bytes < header_bytes.aligned().bytes);

        let header_offset = mmap_basis.with_header(header_bytes.aligned());

        let lock_ptr = locks_offset.absolute_offset().bytes as *mut u8;
        let dirty_ptr = dirty_offset.absolute_offset().bytes as *mut u8;
        let written_ptr = written_offset.absolute_offset().bytes as *mut u8;

        let chunk_locks = Bitlock::new(lock_ptr, chunk_ct.total().chunks);
        let dirty_flags = AtomicBitset::new(dirty_ptr, chunk_ct.total().chunks);
        let written_flags = AtomicBitset::new(written_ptr, chunk_ct.total().chunks);

        Ok(UfoFileWriteback {
            ufo_id,
            chunk_ct,
            chunk_size,
            chunk_locks,
            dirty_flags,
            written_flags,
            final_chunk_size,
            mmap,
            body_bytes: body_bytes.as_total(),
            total_bytes: total_bytes.as_total(),
            header_offset,
        })
    }

    pub fn used_bytes(&self) -> PageAlignedBytes {
        let chunk_ct = &self.chunk_ct;
        let last_chunk = chunk_ct.total().chunks - 1;

        let mut sum: PageAlignedBytes = ToPage.align_down(&0.into());
        for x in 0..chunk_ct.total().chunks {
            let size = if x == last_chunk {
                ToPage.align_up(&self.final_chunk_size)
            } else {
                ToPage.align_up(&self.chunk_size.alignment_quantum())
            };

            if self.written_flags.test(x) {
                sum = sum.add(&size);
            }
        }
        sum
    }

    fn body_bytes(&self) -> &Total<Bytes> {
        &self.body_bytes
    }

    pub(super) fn mremap_writeback(
        &self,
        _lock: &BitGuard,
        length: &PageAlignedBytes,
        offset: &UfoOffset,
        ufo: &UfoObject,
    ) -> Result<UfoWritebackAction> {
        unsafe {
            let data_ptr = offset.offset().absolute_offset().bytes as *mut u8;
            let writeback_ptr = self
                .header_offset
                .relative(offset.offset().from_header())
                .absolute_offset()
                .bytes as *mut u8;

            check_ptr_nonneg(libc::mremap(
                data_ptr.cast(),
                length.aligned().bytes,
                length.aligned().bytes,
                libc::MREMAP_FIXED | libc::MREMAP_MAYMOVE | libc::MREMAP_DONTUNMAP,
                writeback_ptr.cast::<c_void>(),
            ))?;
            trace!(target: "ufo_object", "{:?}@{} mremaped data to writeback", ufo.id, offset);

            if let Some(wb) = &ufo.config.writeback_listener {
                let start = offset.as_index_floor().absolute_offset().aligned();
                let ct = ufo
                    .config
                    .stride()
                    .align_down(&length.aligned())
                    .as_elements();
                let end = start.add(&ct);

                wb(UfoWriteListenerEvent::Writeback {
                    start_idx: start.elements,
                    end_idx: end.elements,
                    data: writeback_ptr as *const u8,
                });
            }

            let was_written = self
                .written_flags
                .set(offset.chunk().absolute_offset().chunks);
            if was_written {
                Ok(UfoWritebackAction::UpdateWriteback)
            } else {
                Ok(UfoWritebackAction::NewWriteback)
            }
        }
    }

    pub(super) fn writeback(
        &self,
        offset: &UfoOffset,
        _chunk_lock: &BitGuard,
        data: &[u8],
    ) -> Result<UfoWritebackAction> {
        let ufo_body_offset = offset.offset().from_header();
        anyhow::ensure!(
            ufo_body_offset.bytes < self.body_bytes().total().bytes,
            "{} outside of range",
            ufo_body_offset.bytes
        );
        anyhow::ensure!(
            ufo_body_offset.bytes + data.len() <= self.body_bytes().total().bytes,
            "{} + {} outside of range",
            ufo_body_offset.bytes,
            data.len()
        );

        let chunk_number = offset.chunk().absolute_offset();
        assert!(chunk_number.chunks < self.chunk_ct.total().chunks);
        assert_eq!(
            self.chunk_size.align_down(&ufo_body_offset).as_chunks(),
            chunk_number
        );
        let file_writeback_offset: Bytes = self
            .header_offset
            .relative(ufo_body_offset)
            .absolute_offset();

        debug!(target: "ufo_object", "writeback offset {:#x}", file_writeback_offset.bytes);

        let expected_size: Bytes = std::cmp::min(
            self.chunk_size.alignment_quantum().bytes,
            self.body_bytes().total().bytes - ufo_body_offset.bytes,
        )
        .into();

        anyhow::ensure!(
            data.len() == expected_size.bytes,
            "given data does not match the expected size given {} vs expected {}",
            data.len(),
            expected_size.bytes
        );

        // writebacks only occur with lock held
        let writeback_arr: &mut [u8] = unsafe {
            std::slice::from_raw_parts_mut(
                self.mmap.as_ptr().add(file_writeback_offset.bytes),
                expected_size.bytes,
            )
        };

        writeback_arr.copy_from_slice(data);
        let was_set = self.written_flags.set(chunk_number.chunks);
        if was_set {
            Ok(UfoWritebackAction::UpdateWriteback)
        } else {
            Ok(UfoWritebackAction::NewWriteback)
        }
    }

    pub fn try_readback<'a>(
        &'a self,
        _chunk_lock: &'a BitGuard,
        offset: &UfoOffset,
    ) -> Result<Option<&'a [u8]>, UfoInternalErr> {
        let off_head = offset.offset().from_header();
        trace!(target: "ufo_object", "try readback {:?}@{:#x}", self.ufo_id, off_head.bytes);

        let chunk_number = self.chunk_size.align_down(&off_head).as_chunks();
        let readback_offset = self.header_offset.relative(off_head).absolute_offset();

        if self.written_flags.test(chunk_number.chunks) {
            trace!(target: "ufo_object", "allow readback {:?}@{:#x}", self.ufo_id, off_head.bytes);
            let arr: &[u8] = unsafe {
                std::slice::from_raw_parts(
                    self.mmap.as_ptr().add(readback_offset.bytes),
                    self.chunk_size.alignment_quantum().bytes,
                )
            };
            Ok(Some(arr))
        } else {
            Ok(None)
        }
    }

    pub fn reset(&self) -> Result<PageAlignedBytes> {
        let used_disk = self.used_bytes();
        let ptr = self.mmap.as_ptr();
        unsafe {
            check_return_zero(libc::madvise(
                ptr.cast(),
                self.total_bytes.total().aligned().bytes,
                // punch a hole in the file
                libc::MADV_REMOVE,
            ))?;
        }
        Ok(used_disk)
    }
}
