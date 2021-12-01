use std::result::Result;
use std::sync::Arc;
use std::{cmp::min, ffi::c_void, ops::Deref};

use log::{debug, info, trace, warn};

use crate::events::UfoEvent;
use crate::experimental_compat::Droplockster;
use crate::once_await::OnceFulfiller;
use crate::populate_workers::{RequestWorker, ShouldRun};

use crate::sizes::*;
use crate::ufo_objects::*;
use crate::write_buffer::*;

use super::*;

fn populate_impl(
    core: &UfoCore,
    buffer: &mut UfoWriteBuffer,
    addr: *mut c_void,
) -> Result<(), UfoPopulateError> {
    let event_queue = { core.ufo_event_sender.clone() };

    // fn droplockster<T>(_: T){}
    let mut state = core.get_locked_state().unwrap();

    let ptr_int = Bytes::from(addr as usize);

    // blindly unwrap here because if we get a message for an address we don't have then it is explodey time
    // clone the arc so we aren't borrowing the state
    let ufo_arc = state
        .objects_by_segment
        .get(&ptr_int.bytes)
        .unwrap()
        .clone();
    let ufo = ufo_arc.read().unwrap();

    let ufo_offset = UfoOffset::from_addr(ufo.deref(), addr);
    let config = &ufo.config;

    let full_chunk_load_size = config.chunk_size().alignment_quantum();
    let populate_offset = config
        .chunk_size()
        .align_down(&ufo_offset.offset().from_header());
    assert!(
        ufo_offset.offset().from_header().bytes - populate_offset.aligned().bytes
            < full_chunk_load_size.bytes,
        "incorrect chunk calculated for populate"
    );

    let offset_basis = ufo.offset_basis();
    let raw_offset = offset_basis.with_absolute(ptr_int);
    let aligned_offset = ufo
        .config
        .chunk_size()
        .align_down(&raw_offset.from_header());
    assert!(aligned_offset.aligned().bytes <= raw_offset.from_header().bytes);
    assert!(aligned_offset.aligned().bytes < ufo.config.body_size().total().bytes);
    let populate_offset = offset_basis.relative(aligned_offset.aligned());

    let start = config
        .stride()
        .align_down(&populate_offset.from_header())
        .as_elements();
    let end = start.add(&config.elements_loaded_at_once.alignment_quantum());
    let pop_end = config.element_ct().bound(end).bounded();
    let pop_ct = pop_end.sub(&start);

    let populate_size = config.stride.as_bytes(&pop_ct);
    assert_eq!(
        min(
            full_chunk_load_size.bytes,
            config
                .body_size()
                .total()
                .sub(&populate_offset.from_header())
                .bytes,
        ),
        populate_size.bytes
    );
    let populate_size_padded = ToPage.align_up(&populate_size);

    debug!(target: "ufo_core", "fault at {}, populate {} bytes at {:#x}",
        start.elements, config.stride.as_bytes(&pop_ct).bytes, populate_offset.absolute_offset().bytes );

    assert_eq!(
        populate_offset.from_header(),
        ufo.config.stride().as_bytes(&start),
        "inequal offsets calculated {:x} != {:x}",
        populate_offset.from_header().bytes,
        ufo.config.stride().as_bytes(&start).bytes,
    );

    assert_eq!(
        populate_offset.absolute_offset(),
        ufo_offset.offset().absolute_offset(),
        "inequal offsets calculated {:x} != {:x}",
        populate_offset.absolute_offset().bytes,
        ufo_offset.offset().absolute_offset().bytes,
    );

    let chunk = UfoChunk::new(&ufo_arc, &ufo, ufo_offset, populate_size);
    // shouldn't need to drop this since read is a shared lock
    // ufo.droplockster();

    // Before we perform the load ensure that there is capacity
    UfoCore::ensure_capcity(
        &event_queue,
        &core.config,
        &mut *state,
        chunk.size_in_page_bytes().aligned(),
    );

    // drop the lock before loading so that UFOs can be recursive
    state.droplockster();

    // let ufo = ufo_arc.read().unwrap();
    let config = &ufo.config;

    let chunk_number = chunk.offset().chunk().absolute_offset();
    trace!("spin locking {:?}@{}", ufo.id, chunk.offset());
    let chunk_lock = ufo
        .writeback_util
        .chunk_locks
        .spinlock(chunk_number)
        .map_err(|_| UfoPopulateError)?;
    debug!("locked {:?}@{}", ufo.id, chunk.offset());

    let mut from_writeback = true;
    let raw_data = ufo
        .writeback_util
        .try_readback(&chunk_lock, &chunk.offset())?
        .map(|v| Ok(v) as Result<&[u8], UfoPopulateError>)
        .unwrap_or_else(|| {
            trace!(target: "ufo_core", "calculate");
            from_writeback = false;
            unsafe {
                buffer.ensure_capcity(full_chunk_load_size.bytes);
                (config.populate)(start.elements, pop_end.elements, buffer.ptr)?;
                Ok(&buffer.slice()[0..full_chunk_load_size.bytes])
            }
        })?;
    assert!(raw_data.len() >= populate_size_padded.aligned().bytes);
    trace!(target: "ufo_core", "data ready");

    unsafe {
        core.uffd
            .copy(
                raw_data.as_ptr().cast(),
                chunk.offset().offset().absolute_offset().bytes as *mut c_void,
                populate_size_padded.aligned().bytes,
                true,
            )
            .expect("unable to populate range");
    }
    trace!("populated {:?}.{}", ufo.id, chunk.offset());

    event_queue.send_event(UfoEvent::PopulateChunk {
        memory_used: chunk.size_in_page_bytes().aligned().bytes,
        ufo_id: ufo.id.0,
        loaded_from_writeback: from_writeback,
    })?;

    let hash_fulfiller = chunk.hash_fulfiller();

    let mut state = core.get_locked_state().unwrap();
    let unlock_str = format!("unlocked {:?}@{}", ufo.id, chunk.offset());
    state.loaded_chunks.add(chunk);
    trace!(target: "ufo_core", "chunk saved");

    // release the lock before calculating the hash so other workers can proceed
    state.droplockster();

    if config.should_try_writeback() {
        // Make sure to take a slice of the raw data. the kernel operates in page sized chunks but the UFO ends where it ends
        let mut calculated_hash = None;
        core.rayon_pool.in_place_scope(|s| {
            // do this work in a dedicated thread pool so things waiting can't block the work
            s.spawn(|_| {
                trace!(target: "ufo_core", "calculating hash");
                calculated_hash = Some(hash_function(&raw_data[0..populate_size.bytes]));
                trace!(target: "ufo_core", "calculated hash");
            });
        });
        assert!(calculated_hash.is_some());
        chunk_lock.droplockster(); // must drop this lock before the hash is released
        trace!("{}", unlock_str);
        hash_fulfiller.try_init(calculated_hash);
    } else {
        chunk_lock.droplockster(); // must drop this lock before the hash is released
        trace!("{}", unlock_str);
        hash_fulfiller.try_init(None);
    }

    Ok(())
}

impl UfoCore {
    fn ensure_capcity(
        event_queue: &UfoEventSender,
        config: &UfoCoreConfig,
        state: &mut UfoCoreState,
        to_load: Bytes,
    ) {
        assert!(to_load.bytes + config.low_watermark < config.high_watermark);
        if to_load.bytes + state.loaded_chunks.used_memory().aligned().bytes > config.high_watermark
        {
            state
                .loaded_chunks
                .free_until_low_water_mark(event_queue)
                .unwrap();
        }
    }

    pub(super) fn populate_loop(this: Arc<UfoCore>, request_worker: &dyn RequestWorker) {
        trace!(target: "ufo_core", "Started pop loop");
        let uffd = &this.uffd;
        // Per-worker buffer
        let mut buffer = UfoWriteBuffer::new();

        loop {
            if ShouldRun::Shutdown == request_worker.await_work() {
                return;
            }
            match uffd.read_event() {
                Ok(Some(event)) => match event {
                    userfaultfd::Event::Pagefault {
                        rw: _,
                        addr,
                        kind: _,
                    } => {
                        request_worker.request_worker(); // while we work someone else waits
                        populate_impl(&*this, &mut buffer, addr).expect("Error during populate");
                    }
                    e => panic!("Recieved an event we did not register for {:?}", e),
                },
                Ok(None) => {
                    /*huh*/
                    warn!(target: "ufo_core", "huh")
                }
                Err(userfaultfd::Error::SystemError(e)) if e == nix::errno::Errno::EBADF => {
                    info!(target: "ufo_core", "closing uffd loop on ebadf");
                    return /*done*/;
                }
                Err(userfaultfd::Error::ReadEof) => {
                    info!(target: "ufo_core", "closing uffd loop");
                    return /*done*/;
                }
                err => {
                    err.expect("uffd read error");
                }
            }
        }
    }
}
