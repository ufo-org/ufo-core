use std::sync::Weak;

use anyhow::Result;
use crossbeam::sync::WaitGroup;

use crate::UfoWriteListenerEvent;

use crate::mmap_wrapers::*;
use crate::return_checks::*;
use crate::sizes::*;
use crate::ufo_core::*;

use super::*;
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
    pub(crate) fn reset_internal(&mut self) -> anyhow::Result<PageAlignedBytes> {
        let length = self.config.aligned_body_size().aligned().bytes;
        unsafe {
            check_return_zero(libc::madvise(
                self.mmap
                    .as_ptr()
                    .add(self.config.header_size_with_padding.aligned().bytes)
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
        let header_offset = self
            .config
            .header_size_with_padding
            .aligned()
            .sub(&self.config.header_size)
            .bytes;
        unsafe { self.mmap.as_ptr().add(header_offset).cast() }
    }

    pub fn body_ptr(&self) -> *mut std::ffi::c_void {
        unsafe {
            self.mmap
                .as_ptr()
                .add(self.config.header_size_with_padding.aligned().bytes)
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
