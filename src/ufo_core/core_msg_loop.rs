use std::sync::{Arc, RwLock};
use std::{ops::Range, vec::Vec};

use log::{debug, info, trace};

use crossbeam::channel::Receiver;

use crate::events::UfoEvent;
use crate::populate_workers::PopulateWorkers;
use crate::UfoEventSender;

use crate::mmap_wrapers::*;
use crate::ufo_objects::*;

use super::*;

fn allocate_impl(
    this: &Arc<UfoCore>,
    event_sender: &UfoEventSender,
    config: UfoObjectConfig,
) -> anyhow::Result<WrappedUfoObject> {
    info!(target: "ufo_object", "new Ufo {{
        header_size: {},
        stride: {},
        header_size_with_padding: {},
        true_size: {},

        elements_loaded_at_once: {},
        element_ct: {},
     }}",
        config.header_size,
        config.stride,

        config.header_size_with_padding,
        config.true_size_with_padding,

        config.elements_loaded_at_once,
        config.element_ct,
    );

    let state = &mut *this.get_locked_state()?;

    let id_map = &state.objects_by_id;
    let id_gen = &mut state.object_id_gen;

    let id = id_gen.next(|k| {
        trace!(target: "ufo_core", "testing id {:?}", k);
        !k.is_sentinel() && !id_map.contains_key(k)
    });

    debug!(target: "ufo_core", "allocate {:?}: {} elements with stride {} [pad|header⋮body] [{}|{}⋮{}]",
        id,
        config.element_ct,
        config.stride,
        config.header_size_with_padding - config.header_size,
        config.header_size,
        config.stride * config.element_ct,
    );

    let mmap = BaseMmap::new(
        config.true_size_with_padding,
        &[MemoryProtectionFlag::Read, MemoryProtectionFlag::Write],
        &[MmapFlag::Anonymous, MmapFlag::Private, MmapFlag::NoReserve],
        None,
    )
    .expect("Mmap Error");

    let mmap_ptr = mmap.as_ptr();
    let true_size = config.true_size_with_padding;
    let mmap_base = mmap_ptr as usize;
    let segment = Range {
        start: mmap_base,
        end: mmap_base + true_size,
    };

    debug!(target: "ufo_core", "mmapped {:#x} - {:#x}", mmap_base, mmap_base + true_size);

    let writeback = UfoFileWriteback::new(id, &config, this)?;
    this.uffd.register(mmap_ptr.cast(), true_size)?;

    //Pre-zero the header, that isn't part of our populate duties
    if config.header_size_with_padding > 0 {
        unsafe {
            this.uffd
                .zeropage(mmap_ptr.cast(), config.header_size_with_padding, true)
        }?;
    }

    let ufo_event = UfoEvent::AllocateUfo {
        ufo_id: id.0,

        intended_body_size: config.element_ct() * config.stride(),
        intended_header_size: config.header_size(),

        header_size_with_padding: config.header_size_with_padding,
        body_size_with_padding: config.true_size_with_padding - config.header_size_with_padding,
        total_size_with_padding: config.true_size_with_padding,

        read_only: config.read_only(),
    };

    // let header_offset = config.header_size_with_padding - config.header_size;
    // let body_offset = config.header_size_with_padding;
    let ufo = UfoObject {
        id,
        core: Arc::downgrade(this),
        config,
        mmap,
        writeback_util: writeback,
    };

    let ufo = Arc::new(RwLock::new(ufo));

    state.objects_by_id.insert(id, ufo.clone());
    state
        .objects_by_segment
        .insert(segment, ufo.clone())
        .expect("ufos must not overlap");

    event_sender.send_event(ufo_event)?;

    Ok(ufo)
}

fn reset_impl(
    this: &Arc<UfoCore>,
    event_sender: &UfoEventSender,
    ufo_id: UfoId,
) -> anyhow::Result<()> {
    let state = &mut *this.get_locked_state()?;

    let ufo = &mut *(state
        .objects_by_id
        .get(&ufo_id)
        .map(Ok)
        .unwrap_or_else(|| Err(anyhow::anyhow!("unknown ufo")))?
        .write()
        .map_err(|_| anyhow::anyhow!("lock poisoned"))?);

    debug!(target: "ufo_core", "resetting {:?}", ufo.id);

    let disk_freed = ufo.reset_internal()?;
    let (memory_freed, chunks_freed) = state.loaded_chunks.drop_ufo_chunks(ufo)?;

    event_sender.send_event(UfoEvent::UfoReset {
        ufo_id: ufo_id.0,
        disk_freed,
        memory_freed,
        chunks_freed,
    })?;

    Ok(())
}

fn free_impl(
    this: &Arc<UfoCore>,
    event_sender: &UfoEventSender,
    ufo_id: UfoId,
) -> anyhow::Result<()> {
    debug!(target: "ufo_core", "Seeking UFO for free {:?}", ufo_id);
    //TODO: When freeing we need to check all chunks and call the writeback listener as needed
    // but only if there is a writeback function
    let state = &mut *this.get_locked_state()?;
    let ufo = state
        .objects_by_id
        .remove(&ufo_id)
        .map(Ok)
        .unwrap_or_else(|| Err(anyhow::anyhow!("No such Ufo {}", ufo_id.0)))?;
    let ufo = ufo
        .write()
        .map_err(|_| anyhow::anyhow!("Broken Ufo Lock"))?;

    debug!(target: "ufo_core", "freeing {:?} @ {:?}", ufo.id, ufo.mmap.as_ptr());

    let mmap_base = ufo.mmap.as_ptr() as usize;
    let segment = state
        .objects_by_segment
        .get_entry(&mmap_base)
        .map(Ok)
        .unwrap_or_else(|| Err(anyhow::anyhow!("memory segment missing")))?;

    debug_assert_eq!(
        mmap_base, *segment.start,
        "mmap lower bound not equal to segment lower bound"
    );
    debug_assert_eq!(
        mmap_base + ufo.mmap.length(),
        *segment.end,
        "mmap upper bound not equal to segment upper bound"
    );

    let config = &ufo.config;

    let (memory_freed, chunks_freed) = state.loaded_chunks.drop_ufo_chunks(&ufo)?;
    let disk_freed = ufo.writeback_util.used_bytes();
    debug!(target: "ufo_core", "chunks dropped {:?}", ufo.id);

    this.uffd
        .unregister(ufo.mmap.as_ptr().cast(), config.true_size_with_padding)?;
    debug!(target: "ufo_core", "unregistered from uffd {:?}", ufo.id);

    let start_addr = segment.start.clone();
    state.objects_by_segment.remove_by_start(&start_addr);
    debug!(target: "ufo_core", "removed from segment map {:?}", ufo.id);

    event_sender.send_event(UfoEvent::FreeUfo {
        ufo_id: ufo_id.0,

        intended_body_size: config.element_ct() * config.stride(),
        intended_header_size: config.header_size(),

        header_size_with_padding: config.header_size_with_padding,
        body_size_with_padding: config.true_size_with_padding - config.header_size_with_padding,
        total_size_with_padding: config.true_size_with_padding,

        memory_freed,
        chunks_freed,
        disk_freed,
    })?;
    debug!(target: "ufo_core", "sent free event {:?}", ufo.id);

    info!(target: "ufo_core", "UFO Successfully Freed {:?}", ufo.id);

    Ok(())
}

fn shutdown_impl<F>(
    this: &Arc<UfoCore>,
    event_sender: &UfoEventSender,
    populate_pool: &Arc<PopulateWorkers<F>>,
) {
    info!(target: "ufo_core", "shutting down");
    let keys: Vec<UfoId> = {
        let state = &mut *this.get_locked_state().expect("err on shutdown");
        state.objects_by_id.keys().cloned().collect()
    };

    keys.iter()
        .for_each(|k| free_impl(this, event_sender, *k).expect("err on free"));
    populate_pool.shutdown();
    event_sender
        .send_event(UfoEvent::Shutdown)
        .expect("event queue broken at shutdown");

    if let Some(sync) = this
        .event_qeueue_shutdown_sync
        .lock()
        .expect("event queue sync lock broken")
        .take()
    {
        sync.wait(); // wait for the shutdown event to be processed
    }
}

impl UfoCore {
    #[inline]
    pub(super) fn msg_loop_inner<F>(
        this: &Arc<UfoCore>,
        event_sender: &UfoEventSender,
        populate_pool: &Arc<PopulateWorkers<F>>,
        recv: &Receiver<UfoInstanceMsg>,
    ) -> bool {
        match recv.recv() {
            Ok(m) => match m {
                UfoInstanceMsg::Allocate(fulfiller, cfg) => {
                    fulfiller
                        .fulfill(allocate_impl(&this, &event_sender, cfg).expect("Allocate Error"))
                        .unwrap_or(());
                }
                UfoInstanceMsg::Reset(_, ufo_id) => {
                    reset_impl(&this, &event_sender, ufo_id).expect("Reset Error");
                }
                UfoInstanceMsg::Free(_, ufo_id) => {
                    free_impl(&this, &event_sender, ufo_id).expect("Free Error");
                }
                UfoInstanceMsg::Shutdown(_) => {
                    shutdown_impl(&this, &event_sender, populate_pool);
                    drop(recv);
                    info!(target: "ufo_core", "closing msg loop");
                    return false; /*done*/
                }
            },
            err => {
                err.expect("recv error");
            }
        }
        return true;
    }

    pub(super) fn msg_loop<F>(
        this: Arc<UfoCore>,
        event_sender: UfoEventSender,
        populate_pool: Arc<PopulateWorkers<F>>,
        recv: Receiver<UfoInstanceMsg>,
    ) {
        trace!(target: "ufo_core", "Started msg loop");
        loop {
            if UfoCore::msg_loop_inner(&this, &event_sender, &populate_pool, &recv) {
                return;
            }
        }
    }
}
