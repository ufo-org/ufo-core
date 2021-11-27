use std::{collections::VecDeque, sync::Arc};

use itertools::Itertools;
use log::debug;
use rayon::iter::{IntoParallelIterator, IntoParallelRefMutIterator, ParallelIterator};

use crate::{sizes::AbsoluteOffset, ufo_core::chunk_freer::ChunkFreer, UfoEventSender};

use super::*;

pub(super) struct UfoChunks {
    loaded_chunks: VecDeque<UfoChunk>,
    used_memory: usize,
    config: Arc<UfoCoreConfig>,
}

impl UfoChunks {
    pub fn new(config: Arc<UfoCoreConfig>) -> UfoChunks {
        UfoChunks {
            loaded_chunks: VecDeque::new(),
            used_memory: 0,
            config,
        }
    }

    pub fn used_memory(&self) -> usize {
        self.used_memory
    }

    pub fn add(&mut self, chunk: UfoChunk) {
        self.used_memory += chunk.size_in_page_bytes();
        self.loaded_chunks.push_back(chunk);
    }

    pub fn drop_ufo_chunks(&mut self, ufo: &UfoObject) -> Result<(usize, usize), UfoInternalErr> {
        let before = self.used_memory;
        let chunks = &mut self.loaded_chunks;

        let ct = chunks
            .par_iter_mut()
            .filter(|c| c.ufo_id() == ufo.id)
            .map(|chunk| chunk.mark_freed_notify_listener(ufo))
            .map(|r| r.and(Ok(1)))
            .reduce(|| Ok(0), |a, b| Ok(a? + b?))?;
        self.used_memory = chunks.iter().map(UfoChunk::size_in_page_bytes).sum();

        Ok((before - self.used_memory, ct))
    }

    pub fn free_until_low_water_mark(
        &mut self,
        event_sender: &UfoEventSender,
    ) -> anyhow::Result<usize> {
        debug!(target: "ufo_core", "Freeing memory");
        event_sender.send_event(UfoEvent::GcCycleStart)?;

        let low_water_mark = self.config.low_watermark;

        let mut to_free = Vec::new();
        let mut will_free_bytes = 0;

        while self.used_memory - will_free_bytes > low_water_mark {
            match self.loaded_chunks.pop_front() {
                None => anyhow::bail!("nothing to free"),
                Some(chunk) => {
                    let size = chunk.size_in_page_bytes(); // chunk.free_and_writeback_dirty()?;
                    will_free_bytes += size;
                    to_free.push(chunk);
                    // self.used_memory -= size;
                }
            }
        }

        debug!(target: "ufo_core", "Freeing chunks {}", {
            let names  = to_free.iter()
                .map(|c| format!("{:?}@{}", c.ufo_id(), c.offset().chunk().absolute_offset().chunks));
            // currently an unstable feature in the std lib, use itertools
            Itertools::intersperse(names, ", ".to_string())
                .collect::<String>()
        });

        let freed_memory = to_free
            .into_par_iter()
            .map_init(
                || ChunkFreer::new(event_sender.clone()),
                |f, mut c| f.free_chunk(&mut c),
            )
            .reduce(|| Ok(0), |a, b| Ok(a? + b?))?;
        assert_eq!(will_free_bytes, freed_memory);
        debug!(target: "ufo_core", "Done freeing memory");
        event_sender.send_event(UfoEvent::GcCycleEnd)?;

        self.used_memory -= freed_memory;
        assert!(self.used_memory <= low_water_mark);

        Ok(self.used_memory)
    }
}
