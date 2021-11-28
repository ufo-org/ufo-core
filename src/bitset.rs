use std::sync::atomic::{AtomicU8, Ordering};

pub(crate) struct AtomicBitset {
    base: usize,
    size_bits: usize,
}

impl AtomicBitset {
    pub fn new(ptr: *mut u8, ct: usize) -> Self {
        AtomicBitset {
            base: ptr as usize,
            size_bits: ct,
        }
    }

    fn atomic_byte<'a>(&'a self, byte: usize) -> &'a mut AtomicU8 {
        let ptr = self.base as *mut u8;
        unsafe { &mut *(ptr.add(byte) as *mut u8 as *mut AtomicU8) }
    }

    fn byte_and_bit(&self, idx: usize) -> (usize, u8) {
        assert!(idx < self.size_bits);
        (idx >> 3, 1 << (idx & 0b111))
    }

    pub fn set(&self, idx: usize) -> bool {
        let (byte, bit) = self.byte_and_bit(idx);
        let atomic = self.atomic_byte(byte);
        let prev = atomic.fetch_or(bit, Ordering::Relaxed);
        prev | bit == 0
    }

    pub fn clear(&self, idx: usize) -> bool {
        let (byte, bit) = self.byte_and_bit(idx);
        let inv = !bit;
        assert_eq!(inv | bit, 0xff);
        let atomic = self.atomic_byte(byte);
        let prev = atomic.fetch_and(inv, Ordering::Relaxed);
        prev | bit != 0
    }

    pub fn test(&self, idx: usize) -> bool {
        let (byte, bit) = self.byte_and_bit(idx);
        let atomic = self.atomic_byte(byte);
        atomic.load(Ordering::Relaxed) & bit != 0
    }
}
