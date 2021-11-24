use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicPtr, Ordering},
        Condvar, Mutex,
    },
};

use crate::experimental_compat::Droplockster;

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
                // on success we wake everyone up!
                let guard = self.mutex.lock().unwrap();
                self.condition.notify_all();
                guard.droplockster();
            }
            Err(_) => {
                // On an error we did not initialize the value, reconstruct and drop the box
                unsafe { Box::from_raw(as_ptr) };
            }
        }
    }
}

impl<D, T> OnceFulfiller<T> for D
where
    D: Deref<Target = OnceAwait<T>>,
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
            return unsafe { &*ptr };
        }

        let guard = self.mutex.lock().unwrap();
        let _guard = self
            .condition
            .wait_while(guard, |_| self.value.load(Ordering::Acquire).is_null())
            .unwrap();
        unsafe { &*self.value.load(Ordering::Acquire) }
    }
}
