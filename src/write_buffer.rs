use std::alloc;

pub(crate) struct UfoWriteBuffer {
    pub ptr: *mut u8,
    pub size: usize,
}

/// To avoid allocating memory over and over again for population functions keep around an
/// allocated block and just grow it to the needed capacity
impl UfoWriteBuffer {
    pub(crate) fn new() -> UfoWriteBuffer {
        UfoWriteBuffer {
            size: 0,
            ptr: std::ptr::null_mut(),
        }
    }

    pub(crate) unsafe fn ensure_capcity(&mut self, capacity: usize) -> *mut u8 {
        if self.size < capacity {
            let layout = alloc::Layout::from_size_align(self.size, crate::get_page_size()).unwrap();
            let new_ptr = alloc::realloc(self.ptr, layout, capacity);

            if new_ptr.is_null() {
                alloc::handle_alloc_error(layout);
            } else {
                self.ptr = new_ptr;
                self.size = capacity;
            }
        }
        self.ptr
    }

    pub(crate) unsafe fn slice(&self) -> &[u8] {
        std::slice::from_raw_parts(self.ptr, self.size)
    }
    // unsafe fn slice_mut(&self) -> &mut [u8] {
    //     std::slice::from_raw_parts_mut(self.ptr, self.size)
    // }
}

impl Drop for UfoWriteBuffer {
    fn drop(&mut self) {
        if self.size > 0 {
            let layout = alloc::Layout::from_size_align(self.size, crate::get_page_size()).unwrap();
            unsafe {
                alloc::dealloc(self.ptr, layout);
            }
        }
    }
}
