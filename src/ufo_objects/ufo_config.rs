use crate::{sizes::*, UfoPopulateFn, UfoWritebackListenerFn};

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

    pub(crate) header_size_with_padding: PageAlignedBytes,
    pub(crate) header_size: Bytes,

    pub(crate) stride: ToStride<Bytes>,
    pub(crate) elements_loaded_at_once: ToChunk<Elements>,
    pub(crate) element_ct: Total<Elements>,
    pub(crate) true_size_with_padding: Total<PageAlignedBytes>,
    pub(crate) read_only: bool,
}

// Getters
impl UfoObjectConfig {
    pub fn header_size(&self) -> Bytes {
        self.header_size
    }
    pub fn stride(&self) -> ToStride<Bytes> {
        self.stride
    }
    pub fn chunk_size(&self) -> ToChunk<Bytes> {
        let stride = self.stride.alignment_quantum().read_raw_unit();
        (stride * self.elements_loaded_at_once.alignment_quantum().elements).into()
    }
    pub fn elements_loaded_at_once(&self) -> ToChunk<Elements> {
        self.elements_loaded_at_once
    }
    pub fn element_ct(&self) -> &Total<Elements> {
        &self.element_ct
    }

    pub fn read_only(&self) -> bool {
        self.read_only
    }
    pub fn body_size(&self) -> Total<Bytes> {
        self.stride.as_bytes(self.element_ct.total()).as_total()
    }

    pub fn aligned_body_size(&self) -> PageAlignedBytes {
        let bytes = self.element_ct.total().elements * self.stride.alignment_quantum().bytes;
        ToPage.align_up(&bytes.into())
    }
}

impl UfoObjectConfig {
    pub(crate) fn new_config(params: UfoObjectParams) -> UfoObjectConfig {
        let min_load_ct = params.min_load_ct.unwrap_or(1);
        let page_size = crate::get_page_size();

        /* Headers and size */
        let header_size_with_padding: PageAlignedBytes =
            ToPage.align_up(&(params.header_size as usize).into());
        let body_size_with_padding = ToPage.align_up(&(params.stride * params.element_ct).into());
        let true_size_with_padding = header_size_with_padding
            .add(&body_size_with_padding)
            .as_total();

        /* loading quanta */
        let min_load_bytes = num::integer::lcm(page_size, params.stride * min_load_ct);
        let elements_loaded_at_once = min_load_bytes / params.stride;
        assert!(elements_loaded_at_once * params.stride == min_load_bytes);

        UfoObjectConfig {
            header_size: params.header_size.into(),
            stride: params.stride.into(),
            read_only: params.read_only,

            header_size_with_padding,
            true_size_with_padding,

            elements_loaded_at_once: elements_loaded_at_once.into(),
            element_ct: Elements::from(params.element_ct).as_total(),

            populate: params.populate,
            writeback_listener: params.writeback_listener,
        }
    }

    pub(crate) fn should_try_writeback(&self) -> bool {
        // this may get more complex in the future, for example we may implement ALWAYS writeback
        !self.read_only
    }
}
