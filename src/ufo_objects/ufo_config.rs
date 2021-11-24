use crate::{UfoPopulateFn, UfoWritebackListenerFn};
use num::Integer;

pub struct UfoObjectParams {
    pub header_size: usize,
    pub stride: usize,
    pub min_load_ct: Option<usize>,
    pub read_only: bool,
    pub populate: Box<UfoPopulateFn>,
    pub writeback_listener: Option<Box<UfoWritebackListenerFn>>,
    pub element_ct: usize,
}

impl UfoObjectParams {
    pub fn new_config(self) -> UfoObjectConfig {
        UfoObjectConfig::new_config(self)
    }
}

pub struct UfoObjectConfig {
    pub(crate) populate: Box<UfoPopulateFn>,
    pub(crate) writeback_listener: Option<Box<UfoWritebackListenerFn>>,

    pub(crate) header_size_with_padding: usize,
    pub(crate) header_size: usize,

    pub(crate) stride: usize,
    pub(crate) elements_loaded_at_once: usize,
    pub(crate) element_ct: usize,
    pub(crate) true_size_with_padding: usize,
    pub(crate) read_only: bool,
}

// Getters
impl UfoObjectConfig {
    pub fn header_size(&self) -> usize {
        self.header_size
    }
    pub fn stride(&self) -> usize {
        self.stride
    }
    pub fn elements_loaded_at_once(&self) -> usize {
        self.elements_loaded_at_once
    }
    pub fn element_ct(&self) -> usize {
        self.element_ct
    }
    pub fn read_only(&self) -> bool {
        self.read_only
    }
    pub fn body_size(&self) -> usize {
        self.element_ct * self.stride
    }
}

impl UfoObjectConfig {
    pub(crate) fn new_config(params: UfoObjectParams) -> UfoObjectConfig {
        let min_load_ct = params.min_load_ct.unwrap_or(1);
        let page_size = crate::get_page_size();

        /* Headers and size */
        let header_size_with_padding = (params.header_size as usize).next_multiple_of(&page_size);
        let body_size_with_padding =
            (params.stride * params.element_ct).next_multiple_of(&page_size);
        let true_size_with_padding = header_size_with_padding + body_size_with_padding;

        /* loading quanta */
        let min_load_bytes = num::integer::lcm(page_size, params.stride * min_load_ct);
        let elements_loaded_at_once = min_load_bytes / params.stride;
        assert!(elements_loaded_at_once * params.stride == min_load_bytes);

        UfoObjectConfig {
            header_size: params.header_size,
            stride: params.stride,
            read_only: params.read_only,

            header_size_with_padding,
            true_size_with_padding,

            elements_loaded_at_once,
            element_ct: params.element_ct,

            populate: params.populate,
            writeback_listener: params.writeback_listener,
        }
    }

    pub(crate) fn should_try_writeback(&self) -> bool {
        // this may get more complex in the future, for example we may implement ALWAYS writeback
        !self.read_only
    }
}
