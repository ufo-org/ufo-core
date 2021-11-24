use std::io::Error;

use anyhow::Result;
use num::Integer;

use log::{debug, trace};

use crate::bitwise_spinlock::{BitGuard, Bitlock};

use crate::math::*;
use crate::mmap_wrapers::*;
use crate::return_checks::*;
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
    chunk_ct: usize,
    final_chunk_size_page_aligned: usize,
    pub(crate) chunk_locks: Bitlock,
    chunk_size: usize,
    total_bytes_with_padding: usize,
    body_bytes: usize,
    header_bytes: usize,
    // bitlock_bytes: usize,
    // bitmap_bytes: usize,
}

impl UfoFileWriteback {
    pub fn new(
        ufo_id: UfoId,
        cfg: &UfoObjectConfig,
        core: &Arc<UfoCore>,
    ) -> Result<UfoFileWriteback, Error> {
        let page_size = crate::get_page_size();

        let chunk_ct = div_ceil(cfg.element_ct, cfg.elements_loaded_at_once);
        assert!(chunk_ct * cfg.elements_loaded_at_once >= cfg.element_ct);

        let chunk_size = cfg.elements_loaded_at_once * cfg.stride;
        let final_chunk_size_page_aligned =
            (match (cfg.true_size_with_padding - cfg.header_size_with_padding) % chunk_size {
                0 => chunk_size,
                x => x,
            })
            .next_multiple_of(&page_size);

        let bitmap_bytes = div_ceil(chunk_ct, 8); /*8 bits per byte*/
        // Now we want to get the bitmap bytes up to the next multiple of the page size
        let bitmap_bytes = bitmap_bytes.next_multiple_of(&page_size);
        assert!(bitmap_bytes * 8 >= chunk_ct);
        assert!(bitmap_bytes.trailing_zeros() >= page_size.trailing_zeros());

        // the bitlock uses the same math as the bitmap
        let bitlock_bytes = bitmap_bytes;

        // round the mmap up to the nearest chunk size
        // when loading we need to give back chunks this large so even though no useful user data may
        // be in the last chunk we still need to have this available for in the readback chunk
        let data_bytes_with_padding = (cfg.element_ct * cfg.stride).next_multiple_of(&chunk_size);
        let total_bytes_with_padding = bitmap_bytes + bitlock_bytes + data_bytes_with_padding;

        let temp_file = unsafe {
            OpenFile::temp(
                core.config.writeback_temp_path.as_str(),
                total_bytes_with_padding,
            )
        }?;

        let mmap = MmapFd::new(
            total_bytes_with_padding,
            &[MemoryProtectionFlag::Read, MemoryProtectionFlag::Write],
            &[MmapFlag::Shared],
            None,
            temp_file,
            0,
        )?;

        let chunk_locks = Bitlock::new(unsafe { mmap.as_ptr().add(bitmap_bytes) }, chunk_ct);

        Ok(UfoFileWriteback {
            ufo_id,
            chunk_ct,
            chunk_size,
            chunk_locks,
            final_chunk_size_page_aligned,
            mmap,
            total_bytes_with_padding,
            body_bytes: cfg.element_ct * cfg.stride,
            header_bytes: bitmap_bytes + bitlock_bytes,
        })
    }

    pub fn used_bytes(&self) -> usize {
        let chunk_ct = self.chunk_ct;
        let last_chunk = chunk_ct - 1;

        let bitmap_ptr = self.mmap.as_ptr();
        let mut sum = 0;
        for x in 0..chunk_ct {
            let byte = x >> 3;
            let bit = x & 0b111;
            let mask = 1 << bit;

            let size = if x == last_chunk {
                self.final_chunk_size_page_aligned
            } else {
                self.chunk_size
            };

            let is_set = unsafe { *bitmap_ptr.add(byte) & mask } > 0;
            if is_set {
                sum += size;
            }
        }
        sum
    }

    fn body_bytes(&self) -> usize {
        self.body_bytes
    }

    pub(super) fn writeback(&self, offset: &UfoOffset, data: &[u8]) -> Result<UfoWritebackAction> {
        let ufo_body_offset = offset.body_offset();
        anyhow::ensure!(
            ufo_body_offset < self.body_bytes(),
            "{} outside of range",
            ufo_body_offset
        );
        anyhow::ensure!(
            ufo_body_offset + data.len() <= self.body_bytes(),
            "{} + {} outside of range",
            ufo_body_offset,
            data.len()
        );

        let chunk_number = offset.chunk_number();
        assert!(chunk_number < self.chunk_ct);
        assert_eq!(div_floor(ufo_body_offset, self.chunk_size), chunk_number);
        let writeback_offset = self.header_bytes + ufo_body_offset;

        let chunk_byte = chunk_number >> 3;
        let chunk_bit = 1u8 << (chunk_number & 0b111);
        assert!(chunk_byte < (self.header_bytes >> 1));

        debug!(target: "ufo_object", "writeback offset {:#x}", writeback_offset);

        let bitmap_ptr: &mut u8 = unsafe { self.mmap.as_ptr().add(chunk_byte).as_mut().unwrap() };
        let expected_size = std::cmp::min(self.chunk_size, self.body_bytes() - ufo_body_offset);

        anyhow::ensure!(
            data.len() == expected_size,
            "given data does not match the expected size given {} vs expected {}",
            data.len(),
            expected_size
        );

        // TODO: blocks CAN be loaded with the UFO lock held!! FIXME
        // We aren't a mutable copy but writebacks never overlap and we hold the UFO read lock so a chunk cannot be loaded
        let writeback_arr: &mut [u8] = unsafe {
            std::slice::from_raw_parts_mut(self.mmap.as_ptr().add(writeback_offset), expected_size)
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
        let off_head = offset.body_offset();
        trace!(target: "ufo_object", "try readback {:?}@{:#x}", self.ufo_id, off_head);

        let chunk_number = div_floor(off_head, self.chunk_size);
        let readback_offset = self.header_bytes + off_head;

        let chunk_byte = chunk_number >> 3;
        let chunk_bit = 1u8 << (chunk_number & 0b111);

        let bitmap_ptr: &u8 = unsafe { self.mmap.as_ptr().add(chunk_byte).as_ref().unwrap() };
        let is_written = *bitmap_ptr & chunk_bit != 0;

        if is_written {
            trace!(target: "ufo_object", "allow readback {:?}@{:#x}", self.ufo_id, off_head);
            let arr: &[u8] = unsafe {
                std::slice::from_raw_parts(self.mmap.as_ptr().add(readback_offset), self.chunk_size)
            };
            Ok(Some(arr))
        } else {
            Ok(None)
        }
    }

    pub fn reset(&self) -> Result<usize> {
        let used_disk = self.used_bytes();
        let ptr = self.mmap.as_ptr();
        unsafe {
            check_return_zero(libc::madvise(
                ptr.cast(),
                self.total_bytes_with_padding,
                // punch a hole in the file
                libc::MADV_REMOVE,
            ))?;
        }
        Ok(used_disk)
    }
}
