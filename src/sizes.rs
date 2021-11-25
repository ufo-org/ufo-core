use std::marker::PhantomData;

use num::Integer;

/* Units */

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Copy, Clone)]
pub(crate) struct Bytes {
    bytes: usize,
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Copy, Clone, Hash)]
pub(crate) struct Chunks {
    chunks: usize,
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Copy, Clone, Hash)]
pub(crate) struct Elements {
    elements: usize,
}

impl From<usize> for Bytes {
    fn from(u: usize) -> Self {
        Bytes { bytes: u}
    }
}

impl From<usize> for Chunks {
    fn from(u: usize) -> Self {
        Chunks { chunks: u }
    }
}

impl From<usize> for Elements {
    fn from(u: usize) -> Self {
        Elements { elements: u }
    }
}

pub(crate) trait ReadableUnit where
    Self: From<usize>
{
    fn read_raw_unit(&self) -> usize;
}

impl ReadableUnit for Bytes {
    fn read_raw_unit(&self) -> usize {
        self.bytes
    }
}

impl ReadableUnit for Chunks {
    fn read_raw_unit(&self) -> usize {
        self.chunks
    }
}

impl ReadableUnit for Elements {
    fn read_raw_unit(&self) -> usize {
        self.elements
    }
}

/* Alignment */
pub(crate) struct ToChunkBytes{
    chunk_size: Bytes,
    total_bytes: Bytes,
}

pub(crate) struct ToChunkElts{
    chunk_size: Elements,
    total_chunks: Elements,
}

pub(crate) struct ToPage;


pub(crate) trait Alignable<U> where
    Self: Sized,
    U: ReadableUnit,
{
    fn align_quantum(&self) -> usize;

    fn align_down(&self, unit: &U) -> Aligned<U, Self>{
        let unit = unit.read_raw_unit()
            .prev_multiple_of(&self.align_quantum())
            .into();
        Aligned {
            unit,
            _alignment: PhantomData,
        }
    }

    fn align_up(&self, unit: &U) -> Aligned<U, Self>{
        let unit = unit.read_raw_unit()
            .next_multiple_of(&self.align_quantum())
            .into();
        Aligned {
            unit,
            _alignment: PhantomData,
        }
    }
}

impl Alignable<Bytes> for ToPage {
    fn align_quantum(&self) -> usize {
        crate::get_page_size()
    }
}

impl Alignable<Bytes> for ToChunkBytes {
    fn align_quantum(&self) -> usize {
        self.chunk_size.bytes
    }
}

impl Alignable<Elements> for ToChunkElts {
    fn align_quantum(&self) -> usize {
        self.chunk_size.elements
    }
}

pub(crate) struct Aligned<U, A> {
    unit: U,
    _alignment: PhantomData<A>,
}

impl<U, A> Aligned<U, A> {
    pub fn aligned(&self) -> &U {
        &self.unit
    }
}

pub(crate) type PageAlignedBytes = Aligned<Bytes, ToPage>;
pub(crate) type ChunkAlignedBytes = Aligned<Bytes, ToChunkBytes>;

/* unit bearing traits */

pub(crate) trait ByteOffset {
    fn absolute_offset(&self) -> Bytes;

    fn body_offset(&self) -> Bytes;
}

pub(crate) trait ByteSized {
    fn byte_size(&self) -> Bytes;
}

pub(crate) trait ChunkOffset {
    fn chunk_number(&self) -> Chunks;
}

pub(crate) trait EltIdxed {
    fn idx(&self) -> Elements;
}