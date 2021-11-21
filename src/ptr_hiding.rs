// pub fn as_c_void<T>(value: T) -> *mut libc::c_void {
//     Box::into_raw(Box::new(value)).cast()
// }

// pub fn as_ref<T>(ptr: &*mut libc::c_void) -> &T {
//     let ptr = *ptr;
//     let ptr = ptr.cast::<T>();
//     unsafe { ptr.as_ref().unwrap() }
// }

// pub fn free_c_void<T>(ptr: *mut libc::c_void) {
//     unsafe { Box::from_raw(ptr.cast::<T>()) };
// }
