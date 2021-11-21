use std::{sync::MutexGuard, thread::ThreadId};

pub(crate) fn thread_id_u64(tid: ThreadId) -> u64 {
    unsafe { *(&tid as *const ThreadId).cast() }
}

pub(crate) fn droplockster<T>(_: MutexGuard<T>) {}
