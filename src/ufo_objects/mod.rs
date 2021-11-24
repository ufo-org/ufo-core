use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Weak,
};

use anyhow::Result;
use crossbeam::sync::WaitGroup;

use crate::UfoWriteListenerEvent;

use super::errors::*;
use super::mmap_wrapers::*;
use super::return_checks::*;
use super::ufo_core::*;

mod ufo_chunk;
mod ufo_config;
mod ufo_file_writeback;
mod ufo_id;
mod ufo_offset;

pub(crate) use ufo_chunk::*;
pub use ufo_config::*;
pub(crate) use ufo_file_writeback::*;
pub use ufo_id::*;
pub(crate) use ufo_offset::*;

type DataHash = blake3::Hash;

pub fn hash_function(data: &[u8]) -> DataHash {
    #[cfg(feature = "parallel_hashing")]
    {
        if data.len() > 128 * 1024 {
            // On large blocks we can get significant gains from parallelism
            blake3::Hasher::new().update_rayon(data).finalize()
        } else {
            blake3::hash(data)
        }
    }

    #[cfg(not(feature = "parallel_hashing"))]
    {
        blake3::hash(data)
    }
}

pub struct UfoObject {
    pub id: UfoId,
    pub core: Weak<UfoCore>,
    pub config: UfoObjectConfig,
    pub mmap: BaseMmap,
    pub(crate) writeback_util: UfoFileWriteback,
}

impl std::cmp::PartialEq for UfoObject {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl std::cmp::Eq for UfoObject {}

impl UfoObject {
    /// returns the number of DISK bytes freed (from the writeback utility)
    pub(crate) fn reset_internal(&mut self) -> anyhow::Result<usize> {
        let length = self.config.true_size_with_padding - self.config.header_size_with_padding;
        unsafe {
            check_return_zero(libc::madvise(
                self.mmap
                    .as_ptr()
                    .add(self.config.header_size_with_padding)
                    .cast(),
                length,
                libc::MADV_DONTNEED,
            ))?;
        }
        let writeback_bytes_freed = self.writeback_util.reset()?;
        if let Some(listener) = &self.config.writeback_listener {
            listener(UfoWriteListenerEvent::Reset);
        }
        Ok(writeback_bytes_freed)
    }

    pub fn header_ptr(&self) -> *mut std::ffi::c_void {
        let header_offset = self.config.header_size_with_padding - self.config.header_size;
        unsafe { self.mmap.as_ptr().add(header_offset).cast() }
    }

    pub fn body_ptr(&self) -> *mut std::ffi::c_void {
        unsafe {
            self.mmap
                .as_ptr()
                .add(self.config.header_size_with_padding)
                .cast()
        }
    }

    pub fn reset(&mut self) -> Result<WaitGroup, UfoInternalErr> {
        let wait_group = crossbeam::sync::WaitGroup::new();
        let core = match self.core.upgrade() {
            None => return Err(UfoInternalErr::CoreShutdown),
            Some(x) => x,
        };

        core.msg_send
            .send(UfoInstanceMsg::Reset(wait_group.clone(), self.id))?;

        Ok(wait_group)
    }

    pub fn free(&mut self) -> Result<WaitGroup, UfoInternalErr> {
        let wait_group = crossbeam::sync::WaitGroup::new();
        let core = match self.core.upgrade() {
            None => return Err(UfoInternalErr::CoreShutdown),
            Some(x) => x,
        };

        core.msg_send
            .send(UfoInstanceMsg::Free(wait_group.clone(), self.id))?;

        Ok(wait_group)
    }
}
