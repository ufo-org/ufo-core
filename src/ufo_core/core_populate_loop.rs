use std::result::Result;
use std::sync::Arc;
use std::{cmp::min, ffi::c_void, ops::Deref};

use log::{debug, info, trace, warn};

use num::Integer;

use crate::events::UfoEvent;
use crate::experimental_compat::Droplockster;
use crate::once_await::OnceFulfiller;
use crate::populate_workers::{RequestWorker, ShouldRun};

use crate::ufo_objects::*;
use crate::write_buffer::*;

use super::*;

impl UfoCore {
    fn ensure_capcity(
        event_queue: &UfoEventSender,
        config: &UfoCoreConfig,
        state: &mut UfoCoreState,
        to_load: usize,
    ) {
        assert!(to_load + config.low_watermark < config.high_watermark);
        if to_load + state.loaded_chunks.used_memory() > config.high_watermark {
            state
                .loaded_chunks
                .free_until_low_water_mark(event_queue)
                .unwrap();
        }
    }

    pub(super) fn populate_loop(this: Arc<UfoCore>, request_worker: &dyn RequestWorker) {
        trace!(target: "ufo_core", "Started pop loop");
        fn populate_impl(
            core: &UfoCore,
            buffer: &mut UfoWriteBuffer,
            addr: *mut c_void,
        ) -> Result<(), UfoPopulateError> {
            let event_queue = { core.ufo_event_sender.clone() };

            // fn droplockster<T>(_: T){}
            let mut state = core.get_locked_state().unwrap();

            let ptr_int = addr as usize;

            // blindly unwrap here because if we get a message for an address we don't have then it is explodey time
            // clone the arc so we aren't borrowing the state
            let ufo_arc = state.objects_by_segment.get(&ptr_int).unwrap().clone();
            let ufo = ufo_arc.read().unwrap();

            let fault_offset = UfoOffset::from_addr(ufo.deref(), addr);

            let config = &ufo.config;

            let load_size = config.elements_loaded_at_once * config.stride;
            let populate_offset = fault_offset.down_to_nearest_n_relative_to_body(load_size);
            assert!(fault_offset.body_offset() - populate_offset.body_offset() < load_size);

            let start = populate_offset.as_index_floor();
            let end = start + config.elements_loaded_at_once;
            let pop_end = min(end, config.element_ct);

            let populate_size = min(
                load_size,
                config.body_size() - populate_offset.body_offset(),
            );
            assert_eq!((pop_end - start) * config.stride(), populate_size);
            let populate_size_padded = populate_size.next_multiple_of(&crate::get_page_size());

            debug!(target: "ufo_core", "fault at {}, populate {} bytes at {:#x}",
                start, (pop_end-start) * config.stride, populate_offset.as_ptr_int());

            let chunk = UfoChunk::new(&ufo_arc, &ufo, populate_offset, populate_size);
            // shouldn't need to drop this since read is a shared lock
            // ufo.droplockster();

            // Before we perform the load ensure that there is capacity
            UfoCore::ensure_capcity(
                &event_queue,
                &core.config,
                &mut *state,
                chunk.size_in_page_bytes(),
            );

            // drop the lock before loading so that UFOs can be recursive
            state.droplockster();

            // let ufo = ufo_arc.read().unwrap();
            let config = &ufo.config;

            trace!("spin locking {:?}.{}", ufo.id, chunk.offset());
            let chunk_lock = ufo
                .writeback_util
                .chunk_locks
                .spinlock(chunk.offset().chunk_number())
                .map_err(|_| UfoPopulateError)?;

            let mut from_writeback = true;
            let raw_data = ufo
                .writeback_util
                .try_readback(&chunk_lock, &chunk.offset())?
                .map(|v| Ok(v) as Result<&[u8], UfoPopulateError>)
                .unwrap_or_else(|| {
                    trace!(target: "ufo_core", "calculate");
                    from_writeback = false;
                    unsafe {
                        buffer.ensure_capcity(load_size);
                        (config.populate)(start, pop_end, buffer.ptr)?;
                        Ok(&buffer.slice()[0..load_size])
                    }
                })?;
            trace!(target: "ufo_core", "data ready");

            unsafe {
                core.uffd
                    .copy(
                        raw_data.as_ptr().cast(),
                        chunk.offset().as_ptr_int() as *mut c_void,
                        populate_size_padded,
                        true,
                    )
                    .expect("unable to populate range");
            }
            trace!("unlock, populated {:?}.{}", ufo.id, chunk.offset());

            event_queue.send_event(UfoEvent::PopulateChunk {
                memory_used: chunk.size_in_page_bytes(),
                ufo_id: ufo.id.0,
                loaded_from_writeback: from_writeback,
            })?;

            assert!(raw_data.len() == load_size);
            let hash_fulfiller = chunk.hash_fulfiller();

            let mut state = core.get_locked_state().unwrap();
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
                        calculated_hash = Some(hash_function(&raw_data[0..populate_size]));
                    });
                });
                assert!(calculated_hash.is_some());
                chunk_lock.unlock(); // must drop this lock before the hash is released
                hash_fulfiller.try_init(calculated_hash);
            } else {
                chunk_lock.unlock(); // must drop this lock before the hash is released
                hash_fulfiller.try_init(None);
            }

            Ok(())
        }

        let uffd = &this.uffd;
        // Per-worker buffer
        let mut buffer = UfoWriteBuffer::new();

        loop {
            if ShouldRun::Shutdown == request_worker.await_work() {
                return;
            }
            match uffd.read_event() {
                Ok(Some(event)) => match event {
                    userfaultfd::Event::Pagefault { rw: _, addr } => {
                        request_worker.request_worker(); // while we work someone else waits
                        populate_impl(&*this, &mut buffer, addr).expect("Error during populate");
                    }
                    e => panic!("Recieved an event we did not register for {:?}", e),
                },
                Ok(None) => {
                    /*huh*/
                    warn!(target: "ufo_core", "huh")
                }
                Err(userfaultfd::Error::SystemError(e))
                    if e.as_errno() == Some(nix::errno::Errno::EBADF) =>
                {
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
