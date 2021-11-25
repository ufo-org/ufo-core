use std::{marker::PhantomData, ptr::read};

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

/* From usize */

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

/* unified function to read the value */

pub(crate) trait ReadableUnit {
    type U: From<usize>; 
    fn read_raw_unit(&self) -> usize;
}

impl ReadableUnit for Bytes {
    type U = Self;
    fn read_raw_unit(&self) -> usize {
        self.bytes
    }
}

impl ReadableUnit for Chunks {
    type U = Self;
    fn read_raw_unit(&self) -> usize {
        self.chunks
    }
}

impl ReadableUnit for Elements {
    type U = Self;
    fn read_raw_unit(&self) -> usize {
        self.elements
    }
}

/* Alignment */
pub(crate) struct ToChunk<U>{
    chunk_size: U,
}

pub(crate) struct ToPage;


pub(crate) trait Alignable<R, U> where
    R: ReadableUnit<U=U>,
    U: From<usize>,
{
    fn align_quantum(&self) -> usize;

    fn align_down(&self, from: &R) -> Aligned<U, Self>{
        let unit = from.read_raw_unit()
            .prev_multiple_of(&self.align_quantum())
            .into();
        Aligned {
            unit,
            _alignment: PhantomData,
        }
    }

    fn align_up(&self, unit: &R) -> Aligned<U, Self>{
        let unit = unit.read_raw_unit()
            .next_multiple_of(&self.align_quantum())
            .into();
        Aligned {
            unit,
            _alignment: PhantomData,
        }
    }
}

impl Alignable<Bytes, Bytes> for ToPage {
    fn align_quantum(&self) -> usize {
        crate::get_page_size()
    }
}

impl Alignable<Bytes, Bytes> for ToChunk<Bytes> {
    fn align_quantum(&self) -> usize {
        self.chunk_size.bytes
    }
}

impl Alignable<Elements, Elements> for ToChunk<Elements> {
    fn align_quantum(&self) -> usize {
        self.chunk_size.elements
    }
}

pub(crate) struct Aligned<U, A> where
    A: ?Sized,
    U: From<usize>,
{
    unit: U,
    _alignment: PhantomData<A>,
}

impl<U, A> Aligned<U, A> where 
    A: ?Sized,
    U: From<usize>,
{
    pub fn aligned(&self) -> &U {
        &self.unit
    }
}

impl<U,A> ReadableUnit for Aligned<U, A>where
    A: ?Sized,
    U: ReadableUnit + From<usize>,
{
    type U = U;
    fn read_raw_unit(&self) -> usize {
        self.unit.read_raw_unit()
    }
}

pub(crate) type PageAlignedBytes = Aligned<Bytes, ToPage>;
pub(crate) type ChunkAlignedBytes = Aligned<Bytes, ToChunk<Bytes>>;

/* Limits */

pub(crate) trait Bound<U> where U: ReadableUnit<U=U> + From<usize> {
    fn min(&self) -> &U;

    fn max(&self) -> &U;

    fn bound(&self, u: U) -> Bounded<U, Self>{
        let u = u.read_raw_unit();

        let bounded = u
            .min(self.min().read_raw_unit())
            .max(self.max().read_raw_unit())
            .into();
        Bounded { unit: bounded, _bound: PhantomData }
    }
}

struct ByTotal<U> where U: From<usize>{
    limits: (U, U),
}

impl<U> Bound<U> for ByTotal<U> where U: ReadableUnit<U=U> + From<usize>{
    fn min(&self) -> &U{
        &self.limits.0
    }

    fn max(&self) -> &U{
        &self.limits.1
    }
}

pub(crate) struct Bounded<U, B> where
    B: ?Sized
{
    unit: U,
    _bound: PhantomData<B>,
}

impl<U, B> Bounded<U, B>{
    pub fn bounded(&self) -> &U{
        &self.unit
    }
}

impl<U, B> ReadableUnit for Bounded<U, B> where
    B: ?Sized,
    U: ReadableUnit + From<usize>
{
    type U = U;

    fn read_raw_unit(&self) -> usize {
        self.unit.read_raw_unit()
    }
}


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