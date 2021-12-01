use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use xorshift::{Rng, SeedableRng, SplitMix64};

use num::Integer;

use std::fmt::Display;
use thiserror::Error;

use crate::experimental_compat::thread_id_u64;
use crate::sizes::*;

pub(crate) struct BitGuard<'a> {
    idx_chunks: Chunks,
    parent: &'a Bitlock,
}

impl Drop for BitGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.parent.poisoned.store(true, Ordering::Release);
        }

        self.parent.unlock(self);
    }
}

impl BitGuard<'_> {
    pub fn droplockster(self) {
        std::mem::drop(self);
    }
}

#[derive(Error, Debug)]
pub enum BitlockErr {
    OutOfBounds(usize, usize),
    Contended(usize),
    PoisonErr(usize),
}

impl Display for BitlockErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

pub(crate) struct Bitlock {
    base: *mut u8,
    size_bits: usize,
    coprime_multiplier: usize,
    poisoned: AtomicBool,
}

unsafe impl Send for Bitlock {}
unsafe impl Sync for Bitlock {}

struct MappedIdx {
    byte_idx: usize,
    bit: u8,
}

impl Bitlock {
    pub fn new(base: *mut u8, ct: usize) -> Self {
        let mut coprime_multiplier = 8 * 64;
        while coprime_multiplier.gcd(&ct) > 1 {
            coprime_multiplier += 1;
        }

        Bitlock {
            base,
            size_bits: ct,
            coprime_multiplier,
            poisoned: AtomicBool::new(false),
        }
    }

    fn atomic_byte<'a>(&'a self, idx: &MappedIdx) -> &'a mut AtomicU8 {
        unsafe { &mut *(self.base.add(idx.byte_idx) as *mut u8 as *mut AtomicU8) }
    }

    fn map_index(&self, idx: Chunks) -> Result<MappedIdx, BitlockErr> {
        if idx.chunks >= self.size_bits {
            Err(BitlockErr::OutOfBounds(idx.chunks, self.size_bits))
        } else {
            let mapped = idx.chunks.wrapping_mul(self.coprime_multiplier) % self.size_bits;

            let byte_idx = mapped >> 3;
            assert!(byte_idx < self.size_bits);

            let bit = 1 << (mapped & 0b111);
            assert!(1 == u8::count_ones(bit));

            Ok(MappedIdx { byte_idx, bit })
        }
    }

    fn unlock(&self, guard: &BitGuard) {
        let mapped_idx = self
            .map_index(guard.idx_chunks)
            .expect("locked an invlid bit?");
        let target = self.atomic_byte(&mapped_idx);

        let inv_bit = !mapped_idx.bit;
        debug_assert_eq!(inv_bit | mapped_idx.bit, 0xff);
        let prev = target.fetch_and(inv_bit, Ordering::Release);
        debug_assert!(
            prev & mapped_idx.bit != 0,
            "{} wasn't locked {:08b} & {:08b} → {:08b}\nThis should be impossible",
            guard.idx_chunks.chunks,
            prev,
            mapped_idx.bit,
            prev & mapped_idx.bit
        );
    }

    fn try_lock(target: &mut AtomicU8, idx: &MappedIdx) -> bool {
        let previous = target.fetch_or(idx.bit, Ordering::Acquire);
        0 == (idx.bit & previous)
    }

    pub fn lock_uncontended(&self, idx_chunks: Chunks) -> Result<BitGuard, BitlockErr> {
        let idx = idx_chunks.chunks;
        let mapped_idx = self.map_index(idx_chunks)?;
        let target = self.atomic_byte(&mapped_idx);

        if self.poisoned.load(Ordering::Relaxed) {
            return Err(BitlockErr::PoisonErr(idx));
        }

        if Self::try_lock(target, &mapped_idx) {
            Ok(BitGuard {
                idx_chunks,
                parent: self,
            })
        } else {
            Err(BitlockErr::Contended(idx))
        }
    }

    pub fn spinlock(&self, idx_chunks: Chunks) -> Result<BitGuard, BitlockErr> {
        let idx = idx_chunks.chunks;
        let mapped_idx = self.map_index(idx_chunks)?;
        let target = self.atomic_byte(&mapped_idx);

        let seed = (idx as u64).wrapping_add(thread_id_u64(std::thread::current().id()));
        let mut xor_rnd = SplitMix64::from_seed(seed);
        let mut ctr = 0;

        loop {
            if self.poisoned.load(Ordering::Relaxed) {
                return Err(BitlockErr::PoisonErr(idx));
            }

            if Self::try_lock(target, &mapped_idx) {
                return Ok(BitGuard {
                    idx_chunks,
                    parent: self,
                });
            }

            loop {
                match (xor_rnd.next_u32() & 0x0f, ctr) {
                    (0, x) if x > 16 => std::thread::yield_now(),
                    (0, _) => {
                        core::hint::spin_loop();
                        ctr += 1;
                    }
                    (x, _) if x > 12 => break,
                    _ => {}
                }
            }
        }
    }
}
