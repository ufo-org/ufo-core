use crate::{sizes::*, UfoObject};

pub(crate) struct UfoOffset {
    chunk_number: ChunkOffset,
    index_floor: ChunkAlignedElements,
    // stride: ToStride<Bytes>,
    offset: BodyOffsetBytes,
}

impl UfoOffset {
    pub fn from_addr(ufo: &UfoObject, addr: *const libc::c_void) -> UfoOffset {
        let base_addr = FromBase::new((ufo.mmap.as_ptr() as usize).into());
        let header_base = base_addr.with_header(ufo.config.header_size_with_padding.aligned());

        let offset: BodyOffsetBytes = header_base.with_absolute((addr as usize).into());

        assert!(
            offset.absolute_offset().bytes
                >= header_base.relative(0.into()).absolute_offset().bytes,
            "Cannot offset into the header"
        );

        assert!(
            ufo.config.body_size().total().bytes > offset.from_header().bytes,
            "address past the end of the UFO"
        );

        let chunk_size = ufo.config.chunk_size();
        let chunk_number = chunk_size.align_down(&offset.from_header()).as_chunks();

        let index = ufo
            .config
            .stride()
            .align_down(&offset.from_header())
            .as_elements();
        let index_floor = ufo.config.elements_loaded_at_once().align_down(&index);

        let chunk_number = Offset::absolute(chunk_number.into());
        let index_floor = Offset::absolute(index_floor);

        UfoOffset {
            chunk_number,
            index_floor,
            // stride: ufo.config.stride,
            offset,
        }
    }

    pub fn offset(&self) -> &BodyOffsetBytes {
        &self.offset
    }

    pub fn as_index_floor(&self) -> &ChunkAlignedElements {
        &self.index_floor
    }

    pub fn chunk(&self) -> &ChunkOffset {
        &self.chunk_number
    }
}

impl std::fmt::Display for UfoOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(UfoOffset {}@{})",
            self.chunk().absolute_offset().chunks,
            self.offset().from_header().bytes,
        )
    }
}
