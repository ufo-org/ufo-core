use std::marker::PhantomData;

use num::Integer;

/* Units */

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Copy, Clone)]
pub struct Bytes {
    pub bytes: usize,
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Copy, Clone, Hash)]
pub struct Chunks {
    pub chunks: usize,
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Copy, Clone, Hash)]
pub struct Elements {
    pub elements: usize,
}

/* From usize */

impl From<usize> for Bytes {
    fn from(u: usize) -> Self {
        Bytes { bytes: u }
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

pub trait ReadableUnit {
    type Unit: From<usize>;
    fn read_raw_unit(&self) -> usize;
}

impl ReadableUnit for Bytes {
    type Unit = Self;
    fn read_raw_unit(&self) -> usize {
        self.bytes
    }
}

impl ReadableUnit for Chunks {
    type Unit = Self;
    fn read_raw_unit(&self) -> usize {
        self.chunks
    }
}

impl ReadableUnit for Elements {
    type Unit = Self;
    fn read_raw_unit(&self) -> usize {
        self.elements
    }
}

pub trait AddReadable
where
    Self: ReadableUnit<Unit = Self> + From<usize>,
{
    fn add(&self, other: &Self) -> Self {
        (self.read_raw_unit() + other.read_raw_unit()).into()
    }

    fn sub(&self, other: &Self) -> Self {
        (self.read_raw_unit() - other.read_raw_unit()).into()
    }
}

impl<T> AddReadable for T where T: ReadableUnit<Unit = Self> + From<usize> {}

/* Alignment */

#[derive(Copy, Clone)]
pub struct ToStride<U> {
    chunk_size: U,
}

impl<U> From<usize> for ToStride<U>
where
    U: From<usize>,
{
    fn from(u: usize) -> Self {
        ToStride {
            chunk_size: u.into(),
        }
    }
}

impl ToStride<Bytes> {
    pub fn as_bytes(&self, elements: &Elements) -> Bytes {
        (self.alignment_quantum().bytes * elements.elements).into()
    }
}

#[derive(Clone, Copy)]
pub struct ToChunk<U> {
    chunk_size: U,
}

impl ToChunk<Bytes> {
    pub fn as_bytes(&self, chunks: &Chunks) -> Bytes {
        (self.alignment_quantum().bytes * chunks.chunks).into()
    }
}

impl ToChunk<Elements> {
    pub fn as_elements(&self, chunks: &Chunks) -> Elements {
        (self.alignment_quantum().elements * chunks.chunks).into()
    }
}

impl<U> From<usize> for ToChunk<U>
where
    U: From<usize>,
{
    fn from(u: usize) -> Self {
        ToChunk {
            chunk_size: u.into(),
        }
    }
}

#[derive(Copy, Clone)]
pub struct ToPage;

pub trait Alignable<R, U = R>
where
    Self: Sized + Copy,
    R: ReadableUnit<Unit = U>,
    U: From<usize> + ReadableUnit<Unit = U>,
{
    fn alignment_quantum(&self) -> U;

    fn align_down(&self, from: &R) -> Aligned<U, Self> {
        let unit = from
            .read_raw_unit()
            .prev_multiple_of(&self.alignment_quantum().read_raw_unit())
            .into();
        Aligned {
            unit,
            alignment: *self,
        }
    }

    fn align_up(&self, unit: &R) -> Aligned<U, Self> {
        let unit = unit
            .read_raw_unit()
            .next_multiple_of(&self.alignment_quantum().read_raw_unit())
            .into();
        Aligned {
            unit,
            alignment: *self,
        }
    }
}

impl Alignable<Bytes> for ToPage {
    fn alignment_quantum(&self) -> Bytes {
        crate::get_page_size().into()
    }
}

impl Alignable<Bytes> for ToChunk<Bytes> {
    fn alignment_quantum(&self) -> Bytes {
        self.chunk_size
    }
}

impl Alignable<Bytes> for ToStride<Bytes> {
    fn alignment_quantum(&self) -> Bytes {
        self.chunk_size
    }
}

impl Alignable<Elements> for ToChunk<Elements> {
    fn alignment_quantum(&self) -> Elements {
        self.chunk_size
    }
}

pub struct Aligned<U, A>
where
    U: From<usize>,
{
    unit: U,
    alignment: A,
}

impl<U, A> Aligned<U, A>
where
    U: From<usize> + ReadableUnit,
{
    pub fn aligned(&self) -> U {
        self.unit
    }

    pub fn add(&self, other: &Self) -> Self {
        let total = self.aligned().read_raw_unit() + other.aligned().read_raw_unit();
        Aligned {
            unit: total.into(),
            alignment: self.alignment,
        }
    }
}

impl Aligned<Bytes, ToStride<Bytes>> {
    pub fn as_elements(&self) -> Elements {
        (self.aligned().bytes / self.alignment.alignment_quantum().bytes).into()
    }
}

impl Aligned<Bytes, ToChunk<Bytes>> {
    pub fn as_chunks(&self) -> Chunks {
        (self.aligned().bytes / self.alignment.alignment_quantum().bytes).into()
    }
}

impl Aligned<Elements, ToChunk<Elements>> {
    pub fn as_chunks(&self) -> Chunks {
        (self.aligned().elements / self.alignment.alignment_quantum().elements).into()
    }
}

impl<U, A> ReadableUnit for Aligned<U, A>
where
    U: ReadableUnit + From<usize>,
{
    type Unit = U;
    fn read_raw_unit(&self) -> usize {
        self.unit.read_raw_unit()
    }
}

pub type PageAlignedBytes = Aligned<Bytes, ToPage>;
pub type ChunkAlignedBytes = Aligned<Bytes, ToChunk<Bytes>>;

/* Bounds */

pub trait Bound<R, U>
where
    R: ReadableUnit<Unit = U>,
    U: ReadableUnit<Unit = U> + From<usize>,
{
    fn total(&self) -> &R;

    fn bound(&self, u: R) -> Bounded<U, Self> {
        let u = u.read_raw_unit();

        let bounded = u.max(self.total().read_raw_unit()).into();
        Bounded {
            unit: bounded,
            _bound: PhantomData,
        }
    }
}

pub struct Total<R>
where
    R: ReadableUnit,
{
    total: R,
}

pub trait AsTotal
where
    Self: Sized + ReadableUnit,
{
    fn as_total(self) -> Total<Self> {
        Total { total: self }
    }
}

impl<R> AsTotal for R where R: ReadableUnit {}

impl<R, U> Bound<R, U> for Total<R>
where
    R: ReadableUnit<Unit = U>,
    U: ReadableUnit<Unit = U> + From<usize>,
{
    fn total(&self) -> &R {
        &self.total
    }
}

pub struct Bounded<U, B>
where
    B: ?Sized,
{
    unit: U,
    _bound: PhantomData<B>,
}

impl<U, B> Bounded<U, B> {
    pub fn bounded(&self) -> U {
        self.unit
    }
}

impl<U, B> ReadableUnit for Bounded<U, B>
where
    B: ?Sized,
    U: ReadableUnit + From<usize>,
{
    type Unit = U;

    fn read_raw_unit(&self) -> usize {
        self.unit.read_raw_unit()
    }
}

#[derive(Copy, Clone)]
pub struct Offset<U, B> {
    basis: B,
    absolute_offset: U,
}

impl<U, B> Offset<U, B> {
    pub fn basis(&self) -> &B {
        &self.basis
    }
}

/*
   Absolute (Offset from 0)
*/
pub trait AbsoluteOffset<U> {
    fn absolute_offset(&self) -> U;
}

pub struct Absolute;

impl Absolute {
    pub fn with_base<U>(base: U) -> FromBase<U> {
        FromBase { base }
    }
}

pub trait HasAbsolute {}

impl HasAbsolute for Absolute {}

impl<U, B> AbsoluteOffset<U> for Offset<U, B>
where
    B: HasAbsolute,
{
    fn absolute_offset(&self) -> U {
        self.absolute_offset
    }
}

impl<U> Offset<U, Absolute> {
    pub fn absolute(offset: U) -> Self {
        Offset {
            basis: Absolute,
            absolute_offset: offset,
        }
    }
}

/*
   From Base [Address]
*/
#[derive(Copy, Clone)]
pub struct FromBase<U> {
    base: U,
}

pub trait OffsetFromBase<U>
where
    U: ReadableUnit + From<usize>,
{
    fn from_base(&self) -> U;
}

pub trait HasBase<U>
where
    Self: HasAbsolute,
{
    fn base(&self) -> U;
}

impl<U> HasAbsolute for FromBase<U> {}
impl<U> HasBase<U> for FromBase<U> {
    fn base(&self) -> U {
        self.base
    }
}

impl<U, B> OffsetFromBase<U> for Offset<U, B>
where
    U: ReadableUnit + From<usize>,
    B: HasBase<U>,
{
    fn from_base(&self) -> U {
        let base = self.basis.base().read_raw_unit();
        let abs_offset = self.absolute_offset.read_raw_unit();
        (abs_offset - base).into()
    }
}

impl<U> Offset<U, Absolute> {
    pub fn with_base(&self, base: U) -> Offset<U, FromBase<U>> {
        Offset {
            basis: FromBase { base },
            absolute_offset: self.absolute_offset(),
        }
    }
}

impl<U> FromBase<U> {
    pub fn new(base: U) -> Self {
        FromBase { base }
    }

    pub fn with_header(&self, header_size: U) -> FromHeader<U, Self> {
        let base = *self.clone();
        FromHeader { base, header_size }
    }

    pub fn with_absolute(&self, absolute_offset: U) -> Offset<U, Self> {
        let basis = *self.clone();
        Offset {
            basis,
            absolute_offset,
        }
    }
}

impl<U> FromBase<U>
where
    U: ReadableUnit + From<usize>,
{
    pub fn relative(&self, relative_offset: U) -> Offset<U, Self> {
        let relative_offset = relative_offset.read_raw_unit();
        let base = self.base().read_raw_unit();
        let absolute_offset = (relative_offset + base).into();
        let basis = *self.clone();
        Offset {
            basis,
            absolute_offset,
        }
    }
}

/*
   From Header (Body)
*/

#[derive(Copy, Clone)]
pub struct FromHeader<U, B>
where
    B: HasBase<U>,
{
    base: B,
    header_size: U,
}

pub trait OffsetFromHeader<U>
where
    U: ReadableUnit + From<usize>,
{
    fn from_header(&self) -> U;
}

pub trait HasHeader<U>
where
    Self: HasBase<U>,
{
    fn header_size(&self) -> U;
}

impl<U, B> HasAbsolute for FromHeader<U, B> where B: HasBase<U> {}
impl<U, B> HasBase<U> for FromHeader<U, B>
where
    B: HasBase<U>,
{
    fn base(&self) -> U {
        self.base.base()
    }
}
impl<U, B> HasHeader<U> for FromHeader<U, B>
where
    B: HasBase<U>,
{
    fn header_size(&self) -> U {
        self.header_size
    }
}

impl<U, H> OffsetFromHeader<U> for Offset<U, H>
where
    U: ReadableUnit + From<usize>,
    H: HasHeader<U>,
{
    fn from_header(&self) -> U {
        let base = self.basis.base().read_raw_unit();
        let header_size = self.basis.header_size().read_raw_unit();
        let abs_offset = self.absolute_offset.read_raw_unit();

        ((abs_offset - header_size) - base).into()
    }
}

impl<U, B> Offset<U, B>
where
    B: HasBase<U> + HasAbsolute,
{
    pub fn with_header(&self, header_size: U) -> Offset<U, FromHeader<U, B>> {
        Offset {
            basis: FromHeader {
                base: self.basis,
                header_size,
            },
            absolute_offset: self.absolute_offset(),
        }
    }
}

impl<U, B> Offset<U, B>
where
    U: ReadableUnit + From<usize>,
{
    pub fn relative(&self, offset: U) -> Offset<U, B> {
        let absolute_offset =
            (self.absolute_offset.read_raw_unit() + offset.read_raw_unit()).into();
        Offset {
            basis: self.basis,
            absolute_offset,
        }
    }
}

impl<U, B> FromHeader<U, B>
where
    B: HasBase<U>,
{
    pub fn with_absolute(&self, absolute_offset: U) -> Offset<U, Self> {
        let basis = *self.clone();
        Offset {
            basis,
            absolute_offset,
        }
    }
}

impl<U, B> FromHeader<U, B>
where
    B: HasBase<U>,
    U: ReadableUnit + From<usize>,
{
    pub fn relative(&self, relative_offset: U) -> Offset<U, Self> {
        let relative_offset = relative_offset.read_raw_unit();
        let base = self.base().read_raw_unit();
        let header_size = self.header_size().read_raw_unit();
        let absolute_offset = (relative_offset + header_size + base).into();
        let basis = *self.clone();
        Offset {
            basis,
            absolute_offset,
        }
    }
}

/* types */

pub type AbsoluteOffsetBytes = Offset<Bytes, Absolute>;
pub type BaseOffsetBytes = Offset<Bytes, FromBase<Bytes>>;
pub type BodyOffsetBytes = Offset<Bytes, FromHeader<Bytes, FromBase<Bytes>>>;
pub type ChunkAlignedElements = Offset<Aligned<Elements, ToChunk<Elements>>, Absolute>;

pub type ChunkOffset = Offset<Chunks, Absolute>;
pub type ElementOffset = Offset<Elements, Absolute>;

pub type BaseAddress = FromBase<Bytes>;
