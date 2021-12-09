use std::sync::{Arc, RwLock};
use std::{ops::Range, vec::Vec};

use crossbeam::channel::Receiver;
use log::{debug, info, trace};

use crate::events::UfoEvent;
use crate::experimental_compat::Droplockster;
use crate::populate_workers::PopulateWorkers;
use crate::{UfoEventSender, UfoWriteListenerEvent};

use crate::mmap_wrapers::*;
use crate::sizes::*;

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
        config.header_size.bytes,
        config.stride.alignment_quantum().bytes,

        config.header_size_with_padding.aligned().bytes,
        config.true_size_with_padding.total().aligned().bytes,

        config.elements_loaded_at_once.alignment_quantum().elements,
        config.element_ct.total().elements,
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
        config.element_ct.total().elements,
        config.stride.alignment_quantum().bytes,
        config.header_size_with_padding.aligned().bytes - config.header_size.bytes,
        config.header_size.bytes,
        config.body_size().total().bytes,
    );

    let mmap = BaseMmap::new(
        config.true_size_with_padding.total().aligned().bytes,
        &[MemoryProtectionFlag::Read, MemoryProtectionFlag::Write],
        &[MmapFlag::Anonymous, MmapFlag::Private, MmapFlag::NoReserve],
        None,
    )
    .expect("Mmap Error");

    let mmap_ptr = mmap.as_ptr();
    let true_size = &config.true_size_with_padding;
    let mmap_base = mmap_ptr as usize;
    let segment = Range {
        start: mmap_base,
        end: mmap_base + true_size.total().aligned().bytes,
    };

    debug!(target: "ufo_core", "mmapped {:#x} - {:#x}", 
        mmap_base, mmap_base + true_size.total().aligned().bytes);

    let writeback = UfoFileWriteback::new(id, &config, this)?;
    this.uffd
        .register(mmap_ptr.cast(), true_size.total().aligned().bytes)?;

    //Pre-zero the header, that isn't part of our populate duties
    if config.header_size_with_padding.aligned().bytes > 0 {
        unsafe {
            this.uffd.zeropage(
                mmap_ptr.cast(),
                config.header_size_with_padding.aligned().bytes,
                true,
            )
        }?;
    }

    let ufo_event = UfoEvent::AllocateUfo {
        ufo_id: id.0,

        intended_body_size: config.element_ct().total().elements
            * config.stride().alignment_quantum().bytes,
        intended_header_size: config.header_size().bytes,

        header_size_with_padding: config.header_size_with_padding.aligned().bytes,
        body_size_with_padding: config.aligned_body_size().aligned().bytes,
        total_size_with_padding: config.true_size_with_padding.total().aligned().bytes,

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
    let state = this.get_locked_state()?;

    let ufo = state
        .objects_by_id
        .get(&ufo_id)
        .map(Ok)
        .unwrap_or_else(|| Err(anyhow::anyhow!("unknown ufo")))?
        .clone();
    // we cannot lock the UFO for write while the core state is held, this can rarely cause a deadlock
    state.droplockster();

    let mut ufo = ufo.write().map_err(|_| anyhow::anyhow!("lock poisoned"))?;

    debug!(target: "ufo_core", "resetting {:?}", ufo.id);

    let disk_freed = ufo.reset_internal()?;

    let mut state = this.get_locked_state()?;
    let (memory_freed, chunks_freed) = state.loaded_chunks.drop_ufo_chunks(&ufo)?;
    state.droplockster();

    if let Some(wb_listener) = &ufo.config.writeback_listener {
        wb_listener(UfoWriteListenerEvent::Reset);
    }

    event_sender.send_event(UfoEvent::UfoReset {
        ufo_id: ufo_id.0,
        disk_freed: disk_freed.aligned().bytes,
        memory_freed: memory_freed.aligned().bytes,
        chunks_freed: chunks_freed.elements,
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
    let mut state = this.get_locked_state()?;
    let ufo = state
        .objects_by_id
        .remove(&ufo_id)
        .map(Ok)
        .unwrap_or_else(|| Err(anyhow::anyhow!("No such Ufo {}", ufo_id.0)))?
        .clone();
    debug!(target: "ufo_core", "Found UFO {:?}, performing write lock", ufo_id);
    // we cannot lock the UFO for write while the core state is held, this can rarely cause a deadlock
    state.droplockster();

    let ufo = ufo
        .write()
        .map_err(|_| anyhow::anyhow!("Broken Ufo Lock"))?;

    debug!(target: "ufo_core", "Locked, freeing {:?} @ {:?}", ufo.id, ufo.mmap.as_ptr());

    let mmap_base = ufo.mmap.as_ptr() as usize;
    let mut state = this.get_locked_state()?;
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

    let start_addr = segment.start.clone();
    let config = &ufo.config;

    let (memory_freed, chunks_freed) = state.loaded_chunks.drop_ufo_chunks(&ufo)?;
    let disk_freed = ufo.writeback_util.used_bytes();
    debug!(target: "ufo_core", "chunks dropped {:?}", ufo.id);

    this.uffd.unregister(
        ufo.mmap.as_ptr().cast(),
        config.true_size_with_padding.total().aligned().bytes,
    )?;
    debug!(target: "ufo_core", "unregistered from uffd {:?}", ufo.id);

    state.objects_by_segment.remove_by_start(&start_addr);
    debug!(target: "ufo_core", "removed from segment map {:?}", ufo.id);

    state.droplockster();

    if let Some(wb_listener) = &ufo.config.writeback_listener {
        wb_listener(UfoWriteListenerEvent::UfoWBDestroy);
    }

    event_sender.send_event(UfoEvent::FreeUfo {
        ufo_id: ufo_id.0,

        intended_body_size: config.body_size().total().bytes,
        intended_header_size: config.header_size().bytes,

        header_size_with_padding: config.header_size_with_padding.aligned().bytes,
        body_size_with_padding: config.aligned_body_size().aligned().bytes,
        total_size_with_padding: config.true_size_with_padding.total().aligned().bytes,

        memory_freed: memory_freed.aligned().bytes,
        chunks_freed: chunks_freed.elements,
        disk_freed: disk_freed.aligned().bytes,
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
    info!(target: "ufo_core", "core shutting down");
    let keys: Vec<UfoId> = {
        let state = &mut *this.get_locked_state().expect("err on shutdown");
        state.objects_by_id.keys().cloned().collect()
    };

    debug!(target: "ufo_core", "freeing {} UFOs", keys.len());
    keys.iter()
        .for_each(|k| free_impl(this, event_sender, *k).expect("err on free"));
    debug!(target: "ufo_core", "UFOs free, shutting down populate workers");
    populate_pool.shutdown();
    debug!(target: "ufo_core", "populate workers signaled, sending shutdown event");
    event_sender
        .send_event(UfoEvent::Shutdown)
        .expect("event queue broken at shutdown");
    debug!(target: "ufo_core", "signaled event sender");

    if let Some(sync) = this
        .event_qeueue_shutdown_sync
        .lock()
        .expect("event queue sync lock broken")
        .take()
    {
        debug!(target: "ufo_core", "event sender sync found, waiting");
        sync.wait(); // wait for the shutdown event to be processed
        debug!(target: "ufo_core", "event sender synced");
    }
    info!(target: "ufo_core", "core shut down");
}

impl UfoCore {
    pub(super) fn msg_loop<F>(
        this: Arc<UfoCore>,
        event_sender: UfoEventSender,
        populate_pool: Arc<PopulateWorkers<F>>,
        recv: Receiver<UfoInstanceMsg>,
    ) {
        trace!(target: "ufo_core", "Started msg loop");
        loop {
            match recv.recv() {
                Ok(m) => match m {
                    UfoInstanceMsg::Allocate(fulfiller, cfg) => {
                        fulfiller
                            .fulfill(
                                allocate_impl(&this, &event_sender, cfg).expect("Allocate Error"),
                            )
                            .unwrap_or(());
                    }
                    UfoInstanceMsg::Reset(_, ufo_id) => {
                        reset_impl(&this, &event_sender, ufo_id).expect("Reset Error");
                    }
                    UfoInstanceMsg::Free(_, ufo_id) => {
                        free_impl(&this, &event_sender, ufo_id).expect("Free Error");
                    }
                    UfoInstanceMsg::Shutdown(_) => {
                        info!(target: "ufo_core", "begining shutdown");
                        shutdown_impl(&this, &event_sender, &populate_pool);
                        info!(target: "ufo_core", "closing msg loop");
                        return; /*done*/
                    }
                },
                err => {
                    err.expect("recv error");
                }
            }
        }
    }
}
