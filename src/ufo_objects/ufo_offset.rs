use num::Integer;

use crate::{math::div_floor, UfoObject};

pub(crate) struct UfoOffset {
    base_addr: usize,
    chunk_number: usize,
    stride: usize,
    header_bytes_with_padding: usize,
    absolute_offset_bytes: usize,
}

impl UfoOffset {
    pub fn from_addr(ufo: &UfoObject, addr: *const libc::c_void) -> UfoOffset {
        let addr = addr as usize;
        let base_addr = ufo.mmap.as_ptr() as usize;
        let absolute_offset_bytes = addr
            .checked_sub(base_addr)
            .unwrap_or_else(|| panic!("Addr less than base {} < {}", addr, base_addr));
        let header_bytes_with_padding = ufo.config.header_size_with_padding;

        assert!(
            header_bytes_with_padding <= absolute_offset_bytes,
            "Cannot offset into the header"
        );

        let offset_from_header = absolute_offset_bytes - header_bytes_with_padding;
        let bytes_loaded_at_once = ufo.config.elements_loaded_at_once * ufo.config.stride;
        let chunk_number = div_floor(offset_from_header, bytes_loaded_at_once);
        assert!(chunk_number * bytes_loaded_at_once <= offset_from_header);
        assert!((chunk_number + 1) * bytes_loaded_at_once > offset_from_header);

        UfoOffset {
            base_addr,
            chunk_number,
            stride: ufo.config.stride,
            header_bytes_with_padding,
            absolute_offset_bytes,
        }
    }

    pub fn absolute_offset(&self) -> usize {
        self.absolute_offset_bytes
    }

    pub fn body_offset(&self) -> usize {
        self.absolute_offset_bytes - self.header_bytes_with_padding
    }

    pub fn as_ptr_int(&self) -> usize {
        self.base_addr + self.absolute_offset()
    }

    pub fn as_index_floor(&self) -> usize {
        div_floor(self.body_offset(), self.stride)
    }

    pub fn down_to_nearest_n_relative_to_body(&self, nearest: usize) -> UfoOffset {
        let offset = self.body_offset().prev_multiple_of(&nearest);
        let absolute_offset_bytes = self.header_bytes_with_padding + offset;

        UfoOffset {
            absolute_offset_bytes,
            ..*self
        }
    }

    pub fn chunk_number(&self) -> usize {
        self.chunk_number
    }
}

impl std::fmt::Display for UfoOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(UfoOffset {}@{})",
            self.chunk_number(),
            self.absolute_offset(),
        )
    }
}
