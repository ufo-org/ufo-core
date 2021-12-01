use anyhow::Result;
use log::trace;

use crate::{
    mmap_wrapers::{BaseMmap, MemoryProtectionFlag, Mmap, MmapFlag},
    sizes::{PageAlignedBytes, ToPage},
    UfoChunk, UfoEventSender,
};

pub(crate) struct ChunkFreer {
    event_sender: UfoEventSender,
    pivot: Option<BaseMmap>,
}

impl ChunkFreer {
    pub fn new(event_sender: UfoEventSender) -> Self {
        ChunkFreer {
            event_sender,
            pivot: None,
        }
    }

    fn ensure_capcity(&mut self, to_fit: &UfoChunk) -> Result<()> {
        let required_size = to_fit.size_in_page_bytes();
        trace!(target: "ufo_object", "ensuring pivot capacity {}", required_size.aligned().bytes);
        if let None = self.pivot {
            trace!(target: "ufo_object", "init pivot {}", required_size.aligned().bytes);
            self.pivot = Some(BaseMmap::new(
                required_size.aligned().bytes,
                &[MemoryProtectionFlag::Read, MemoryProtectionFlag::Write],
                &[MmapFlag::Anonymous, MmapFlag::Private],
                None,
            )?);
        }

        let current_size = self.pivot.as_ref().expect("just checked").length();
        if current_size < required_size.aligned().bytes {
            trace!(target: "ufo_object", "grow pivot from {} to {}", current_size, required_size.aligned().bytes);
            let mut old_pivot = None;
            std::mem::swap(&mut old_pivot, &mut self.pivot);
            let pivot = old_pivot.unwrap();
            self.pivot = Some(pivot.resize(required_size.aligned().bytes)?);
        }

        Ok(())
    }

    pub fn free_chunk(&mut self, chunk: &mut UfoChunk) -> Result<PageAlignedBytes> {
        if 0 == chunk.size().bytes {
            return Ok(ToPage::zero());
        }
        self.ensure_capcity(chunk)?;
        let pivot = self.pivot.as_ref().expect("just checked");
        chunk.free_and_writeback_dirty(&self.event_sender, pivot)
    }
}
