use std::{ops::Deref, sync::{
    atomic::{AtomicPtr, Ordering},
    Condvar, Mutex,
}};

use log::debug;

use crate::experimental_compat::{Droplockster, thread_id_u64};

pub(crate) struct OnceAwait<T> {
    value: AtomicPtr<T>,
    mutex: Mutex<()>,
    condition: Condvar,
}

pub(crate) trait OnceFulfiller<T> {
    fn try_init(&self, value: T);
}

impl<T> OnceFulfiller<T> for OnceAwait<T> {
    fn try_init(&self, value: T) {
        let current = self.value.load(Ordering::Acquire);
        if !current.is_null() {
            debug!(target: "ufo_core", "Already initialized");
            return; // already initialized
        }

        // needs initialization
        let boxed = Box::new(value);
        let as_ptr = Box::into_raw(boxed);
        let res = self.value.compare_exchange(
            std::ptr::null_mut(),
            as_ptr,
            Ordering::Release,
            Ordering::Relaxed,
        );
        match res {
            Ok(_) => {
                debug!(target: "ufo_core", "Initialized, wake everyone up");
                // on success we wake everyone up!
                let guard = self.mutex.lock().unwrap();
                self.condition.notify_all();
                guard.droplockster();
            }
            Err(_) => {
                debug!(target: "ufo_core", "Lost the race");
                // On an error we did not initialize the value, reconstruct and drop the box
                unsafe { Box::from_raw(as_ptr) };
            }
        }
    }
}

impl<D, T> OnceFulfiller<T> for D where
    D: Deref<Target=OnceAwait<T>>
{
    fn try_init(&self, value: T) {
        let s = &**self;
        s.try_init(value);
    }
}

impl<T> OnceAwait<T> {
    pub fn new() -> Self {
        OnceAwait {
            value: AtomicPtr::new(std::ptr::null_mut()),
            mutex: Mutex::new(()),
            condition: Condvar::new(),
        }
    }

    pub fn get(&self) -> &T {
        let ptr = self.value.load(Ordering::Acquire);
        if std::ptr::null_mut() != ptr {
            debug!(target: "ufo_core", "No Waiting");
            return unsafe { &*ptr };
        }

        let tid = thread_id_u64(std::thread::current().id());
        let guard = self.mutex.lock().unwrap();
        let _guard = self.condition
            .wait_while(guard, |_| {
                debug!(target: "ufo_core", "Waiting for promised pointer {}", tid);
                self.value.load(Ordering::Acquire).is_null()
            })
            .unwrap();
        debug!(target: "ufo_core", "Got promised pointer {}", tid);
        unsafe { &*self.value.load(Ordering::Acquire) }
    }
}
