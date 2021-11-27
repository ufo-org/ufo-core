use std::io::Error;

use anyhow::Result;

use log::{debug, trace};

use crate::bitwise_spinlock::{BitGuard, Bitlock};

use crate::math::*;
use crate::mmap_wrapers::*;
use crate::return_checks::*;
use crate::sizes::*;
use crate::ufo_core::*;

use super::*;

// someday make this atomic_from_mut
fn atomic_bitset(target: &mut u8, mask: u8) -> u8 {
    unsafe {
        let t = &mut *(target as *mut u8 as *mut AtomicU8);
        t.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| Some(x | mask))
            .unwrap()
    }
}

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

        let trailing_elements = elements_at_once
            .align_down(cfg.element_ct().total())
            .aligned()
            .sub(cfg.element_ct().total());

        let final_chunk_size = if 0 == trailing_elements.elements {
            cfg.chunk_size().alignment_quantum()
        } else {
            (trailing_elements.elements * stride_bytes.bytes).into()
        };

        let bitmap_bytes = div_ceil(chunk_ct.total().chunks, 8).into(); /*8 bits per byte*/
        // Now we want to get the bitmap bytes up to the next multiple of the page size
        let bitmap_bytes = ToPage.align_up(&bitmap_bytes);
        assert!(bitmap_bytes.aligned().bytes * 8 >= chunk_ct.total().chunks);

        // the bitlock uses the same math as the bitmap
        let bitlock_bytes = bitmap_bytes;

        // round the mmap up to the nearest chunk size
        // when loading we need to give back chunks this large so even though no useful user data may
        // be in the last chunk we still need to have this available for in the readback chunk

        let data_bytes_with_padding = cfg.aligned_body_size();
        let total_bytes = bitmap_bytes
            .add(&bitlock_bytes)
            .add(&data_bytes_with_padding);

        let body_bytes: Bytes =
            (cfg.stride().alignment_quantum().bytes * cfg.element_ct().total().elements).into();

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

        let lock_ptr = unsafe { mmap.as_ptr().add(bitmap_bytes.aligned().bytes) };
        let chunk_locks = Bitlock::new(lock_ptr, chunk_ct.total().chunks);

        let header_bytes = bitmap_bytes.add(&bitlock_bytes).aligned();
        let header_offset = Absolute::with_base(0.into()).with_header(header_bytes);
        // Offset::absolute(header_bytes)
        //     .with_base(0.into())
        //     .with_header(header_bytes);

        Ok(UfoFileWriteback {
            ufo_id,
            chunk_ct,
            chunk_size,
            chunk_locks,
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

        let bitmap_ptr = self.mmap.as_ptr();
        let mut sum: PageAlignedBytes = ToPage.align_down(&0.into());
        for x in 0..chunk_ct.total().chunks {
            let byte = x >> 3;
            let bit = x & 0b111;
            let mask = 1 << bit;

            let size = if x == last_chunk {
                ToPage.align_up(&self.final_chunk_size)
            } else {
                ToPage.align_up(&self.chunk_size.alignment_quantum())
            };

            let is_set = unsafe { *bitmap_ptr.add(byte) & mask } > 0;
            if is_set {
                sum = sum.add(&size);
            }
        }
        sum
    }

    fn body_bytes(&self) -> &Total<Bytes> {
        &self.body_bytes
    }

    pub(super) fn writeback(&self, offset: &UfoOffset, data: &[u8]) -> Result<UfoWritebackAction> {
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
        let writeback_offset: Bytes = self.header_offset.relative(ufo_body_offset).from_header();
        let chunk_byte = chunk_number.chunks >> 3;
        let chunk_bit = 1u8 << (chunk_number.chunks & 0b111);
        assert!(chunk_byte < (self.header_offset.header_size().bytes >> 1));

        debug!(target: "ufo_object", "writeback offset {:#x}", writeback_offset.bytes);

        let bitmap_ptr: &mut u8 = unsafe { self.mmap.as_ptr().add(chunk_byte).as_mut().unwrap() };
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

        // TODO: blocks CAN be loaded with the UFO lock held!! FIXME
        // We aren't a mutable copy but writebacks never overlap and we hold the UFO read lock so a chunk cannot be loaded
        let writeback_arr: &mut [u8] = unsafe {
            std::slice::from_raw_parts_mut(
                self.mmap.as_ptr().add(writeback_offset.bytes),
                expected_size.bytes,
            )
        };

        writeback_arr.copy_from_slice(data);
        let old_bits = atomic_bitset(bitmap_ptr, chunk_bit);
        if 0 == chunk_bit & old_bits {
            Ok(UfoWritebackAction::NewWriteback)
        } else {
            Ok(UfoWritebackAction::UpdateWriteback)
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
        let readback_offset = self.header_offset.relative(off_head).from_header();

        let chunk_byte = chunk_number.chunks >> 3;
        let chunk_bit = 1u8 << (chunk_number.chunks & 0b111);

        let bitmap_ptr: &u8 = unsafe { self.mmap.as_ptr().add(chunk_byte).as_ref().unwrap() };
        let is_written = *bitmap_ptr & chunk_bit != 0;

        if is_written {
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
