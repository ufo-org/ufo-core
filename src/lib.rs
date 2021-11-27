// #![feature(
//     ptr_internals,
//     once_cell,
//     slice_ptr_get,
//     mutex_unlock,
//     thread_id_value,
//     int_roundings
// )]

mod bitwise_spinlock;
mod errors;
mod events;
mod experimental_compat;
mod math;
mod mmap_wrapers;
mod once_await;
mod populate_workers;
// mod ptr_hiding;
mod return_checks;
pub mod sizes;
mod ufo_core;
mod ufo_objects;
mod write_buffer;

pub use crate::errors::*;
pub use crate::events::*;
pub use crate::ufo_core::*;
pub use crate::ufo_objects::*;

pub type UfoPopulateFn =
    dyn Fn(usize, usize, *mut u8) -> Result<(), UfoPopulateError> + Sync + Send;

#[repr(C)]
pub enum UfoWriteListenerEvent {
    Writeback {
        start_idx: usize,
        end_idx: usize,
        data: *const u8,
    },
    Reset,
}

pub type UfoWritebackListenerFn = dyn Fn(UfoWriteListenerEvent) + Sync + Send;

static PAGE_SIZE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

pub(crate) fn get_page_size() -> usize {
    let sz = PAGE_SIZE.load(std::sync::atomic::Ordering::Relaxed);
    if 0 != sz {
        return sz;
    }
    let ps = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    assert!(ps > 0);
    PAGE_SIZE.store(ps as usize, std::sync::atomic::Ordering::Relaxed);
    ps as usize
}
