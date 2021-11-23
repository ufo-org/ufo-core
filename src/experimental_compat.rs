use std::{sync::{MutexGuard, RwLockReadGuard, RwLockWriteGuard}, thread::ThreadId};

pub(crate) fn thread_id_u64(tid: ThreadId) -> u64 {
    unsafe { *(&tid as *const ThreadId).cast() }
}

pub(crate) trait Droplockster{
    fn droplockster(self);
}

impl<T> Droplockster for MutexGuard<'_, T>{
    fn droplockster(self) {}
}

impl<T> Droplockster for RwLockReadGuard<'_, T>{
    fn droplockster(self) {}
}

impl<T> Droplockster for RwLockWriteGuard<'_, T>{
    fn droplockster(self) {}
}