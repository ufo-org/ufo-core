use std::result::Result;
use std::sync::Arc;
use std::{cmp::min, ffi::c_void, ops::Deref};

use log::{debug, info, trace, warn};
use nix::sys::mman::{ProtFlags, mprotect};
use userfaultfd::{FaultKind, ReadWrite};

use crate::events::UfoEvent;
use crate::experimental_compat::Droplockster;
use crate::once_await::OnceFulfiller;
use crate::populate_workers::{RequestWorker, ShouldRun};

use crate::sizes::*;
use crate::ufo_objects::*;
use crate::write_buffer::*;

use super::*;

fn unprotect_impl(
    core: &UfoCore,
    addr: *mut c_void,
) -> Result<(), UfoPopulateError> {
    let state = core.get_locked_state().unwrap();

    let ptr_int = Bytes::from(addr as usize);

    debug!(target: "ufo_core", "write fault at {:x}", addr as usize);

    // blindly unwrap here because if we get a message for an address we don't have then it is explodey time
    // clone the arc so we aren't borrowing the state
    let ufo_arc = state
        .objects_by_segment
        .get(&ptr_int.bytes)
        .unwrap()
        .clone();
    trace!(target: "ufo_core", "locking ufo for {:x}", addr as usize);
    let ufo = ufo_arc.read().unwrap();
    debug!(target: "ufo_core", "locked ufo for {:x}: {:?}", addr as usize, ufo.id);

    let aligned_addr = ufo.config.chunk_size().align_down(&Bytes::from(addr as usize));
    let offset = ufo.offset_basis()
        .with_absolute(aligned_addr.aligned());
    let chunk_number = ufo.config.chunk_size().align_down(&offset.from_header()).as_chunks();

    let end = offset.add(&ufo.config.chunk_size().alignment_quantum());
    let end = ufo.config.body_size().bound(end.from_header());
    let end = ToPage.align_up(&end.bounded());

    let length = end.aligned().sub(&offset.from_header());

    debug!(target: "ufo_core", "locking to unprotect page {:?}@{}", ufo.id, chunk_number.chunks);
    let chunk_lock = ufo.writeback_util.chunk_locks
        .spinlock(chunk_number)?;

    trace!(target: "ufo_core", "Marking dirty and unprotecting page {:?}@{}", ufo.id, chunk_number.chunks);
    ufo.writeback_util.dirty_flags.set(chunk_number.chunks);
    core.uffd.remove_write_protection(offset.absolute_offset().bytes as *mut c_void, length.bytes, true)?;

    chunk_lock.droplockster();
    
    Ok(())
}

fn populate_impl(
    core: &UfoCore,
    buffer: &mut UfoWriteBuffer,
    rw: ReadWrite,
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

    let target_ptr = chunk.offset().offset().absolute_offset().bytes as *mut c_void;
    // if we get in a read fault then we optimistically mark the chunk as read-only
    // if it is a write then we know it will be dirtied instantly and just mark it as writeable right away
    // we set the flags in the writeback utility to match the incoming event
    // if it is a write then mark it down, clearing it on a read
    match &rw {
        ReadWrite::Read => {
            core.uffd.write_protect(target_ptr, populate_size_padded.aligned().bytes)?;
            ufo.writeback_util.dirty_flags.clear(chunk_number.chunks);
        },
        ReadWrite::Write => {
            //TODO: makle sure that wake false is approporiate here, seems to be since we need to populate still
            core.uffd.remove_write_protection(target_ptr, populate_size_padded.aligned().bytes, false)?;
            ufo.writeback_util.dirty_flags.set(chunk_number.chunks);
        },
    };

    unsafe {
        core.uffd
            .copy(
                raw_data.as_ptr().cast(),
                target_ptr as *mut c_void,
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

    chunk_lock.droplockster();

    let mut state = core.get_locked_state().unwrap();
    state.loaded_chunks.add(chunk);
    trace!(target: "ufo_core", "chunk saved");

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
                    userfaultfd::Event::Pagefault { rw, addr, kind: FaultKind::Missing } => {
                        request_worker.request_worker(); // while we work someone else waits
                        populate_impl(&this, &mut buffer, rw, addr)
                            .expect("Error during populate");
                    },
                    userfaultfd::Event::Pagefault { rw: _, addr, kind: FaultKind::WriteProtected } => {
                        request_worker.request_worker(); // while we work someone else waits
                        unprotect_impl(&this, addr).expect("Error during unprotect");
                    },
                    e => panic!("Recieved an event we did not register for {:?}", e),
                },
                Ok(None) => {
                    /*huh*/
                    warn!(target: "ufo_core", "huh")
                }
                Err(userfaultfd::Error::SystemError(e))
                    if e == nix::errno::Errno::EBADF =>
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
