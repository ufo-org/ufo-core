use std::result::Result;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use std::{
    cmp::min,
    ffi::c_void,
    ops::{Deref, Range},
    vec::Vec,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::MutexGuard,
};

use log::{debug, info, trace, warn};

use btree_interval_map::IntervalMap;
use crossbeam::channel::{Receiver, Sender};
use crossbeam::sync::WaitGroup;
use itertools::Itertools;
use num::Integer;
use rayon::iter::{IntoParallelIterator, IntoParallelRefMutIterator, ParallelIterator};
use rayon::{ThreadPool, ThreadPoolBuilder};
use semver::Version;
use uname::uname;
use userfaultfd::Uffd;

use crate::events::{start_qeueue_runner, UfoEvent, UfoEventandTimestamp};
use crate::experimental_compat::Droplockster;
use crate::once_await::OnceFulfiller;
use crate::populate_workers::{PopulateWorkers, RequestWorker, ShouldRun};
use crate::{UfoEventConsumer, UfoEventResult};

use super::errors::*;
use super::mmap_wrapers::*;
use super::ufo_objects::*;
use super::write_buffer::*;

pub enum UfoInstanceMsg {
    Shutdown(WaitGroup),
    Allocate(promissory::Fulfiller<WrappedUfoObject>, UfoObjectConfig),
    Reset(WaitGroup, UfoId),
    Free(WaitGroup, UfoId),
}

struct UfoChunks {
    loaded_chunks: VecDeque<UfoChunk>,
    used_memory: usize,
    config: Arc<UfoCoreConfig>,
}

impl UfoChunks {
    fn new(config: Arc<UfoCoreConfig>) -> UfoChunks {
        UfoChunks {
            loaded_chunks: VecDeque::new(),
            used_memory: 0,
            config,
        }
    }

    fn add(&mut self, chunk: UfoChunk) {
        self.used_memory += chunk.size_in_page_bytes();
        self.loaded_chunks.push_back(chunk);
    }

    fn drop_ufo_chunks(&mut self, ufo: &UfoObject) -> Result<(usize, usize), UfoInternalErr> {
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

    fn free_until_low_water_mark(
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
                .map(|c| format!("{:?}@{}", c.ufo_id(), c.offset().chunk_number()));
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

pub struct UfoCoreConfig {
    pub writeback_temp_path: String,
    pub high_watermark: usize,
    pub low_watermark: usize,
}

pub type WrappedUfoObject = Arc<RwLock<UfoObject>>;

pub struct UfoCoreState {
    object_id_gen: UfoIdGen,

    objects_by_id: HashMap<UfoId, WrappedUfoObject>,
    objects_by_segment: IntervalMap<usize, WrappedUfoObject>,

    loaded_chunks: UfoChunks,
}

#[derive(Clone)]
pub(crate) struct UfoEventSender {
    zero_time: Instant,
    sender: Sender<UfoEventResult>,
}

impl UfoEventSender {
    pub fn new_callback(
        &self,
        callback: Option<Box<UfoEventConsumer>>,
    ) -> Result<(), UfoInternalErr> {
        Ok(self.sender.send(UfoEventResult::NewCallback {
            callback,
            timestamp_nanos: self.zero_time.elapsed().as_nanos() as u64,
        })?)
    }

    pub fn send_event(&self, event: UfoEvent) -> Result<(), UfoInternalErr> {
        Ok(self
            .sender
            .send(UfoEventResult::Event(UfoEventandTimestamp {
                timestamp_nanos: self.zero_time.elapsed().as_nanos() as u64,
                event,
            }))?)
    }
}

pub struct UfoCore {
    uffd: Uffd,
    state: Mutex<UfoCoreState>,
    rayon_pool: ThreadPool,

    pub config: Arc<UfoCoreConfig>,
    pub msg_send: Sender<UfoInstanceMsg>,

    ufo_event_sender: Mutex<UfoEventSender>,
    event_qeueue_shutdown_sync: Mutex<Option<WaitGroup>>,
}

impl UfoCore {
    pub fn new(config: UfoCoreConfig) -> Result<Arc<UfoCore>, std::io::Error> {
        let kernel_version = uname()?;
        let kernel_version = Version::parse(&kernel_version.release).unwrap();
        assert!(
            kernel_version.major > 5 || (kernel_version.major == 5 && kernel_version.minor >= 7),
            "Minimum kernel version 5.7, found {}",
            kernel_version
        );

        // If this fails then there is nothing we should even try to do about it honestly
        let uffd = userfaultfd::UffdBuilder::new()
            .close_on_exec(true)
            .non_blocking(false)
            .create()
            .unwrap();

        let config = Arc::new(config);
        // We want zero capacity so that when we shut down there isn't a chance of any messages being lost
        // TODO CMYK 2021.03.04: find a way to close the channel but still clear the queue
        let (send, recv) = crossbeam::channel::bounded(0);

        let state = Mutex::new(UfoCoreState {
            object_id_gen: UfoIdGen::new(),

            loaded_chunks: UfoChunks::new(Arc::clone(&config)),
            objects_by_id: HashMap::new(),
            objects_by_segment: IntervalMap::new(),
        });

        let (sender, reciever) = crossbeam::channel::unbounded();

        let sender = UfoEventSender {
            zero_time: std::time::Instant::now(),
            sender,
        };
        let msg_thread_sender = sender.clone();

        let event_qeueue_shutdown_sync = WaitGroup::new();
        start_qeueue_runner(
            move || reciever.recv().unwrap_or(UfoEventResult::RecvErr),
            event_qeueue_shutdown_sync.clone(),
        )?;

        let rayon_pool = ThreadPoolBuilder::new()
            .thread_name(|x| format!("UFO Core hasher {}", x))
            .build()
            .unwrap();

        let core = Arc::new(UfoCore {
            uffd,
            config,
            rayon_pool,
            msg_send: send,
            state,
            ufo_event_sender: Mutex::new(sender),
            event_qeueue_shutdown_sync: Mutex::new(Some(event_qeueue_shutdown_sync)),
        });

        trace!(target: "ufo_core", "starting threads");
        let pop_core = core.clone();
        let pop_workers = PopulateWorkers::new("Ufo Core", move |request_worker| {
            UfoCore::populate_loop(pop_core.clone(), request_worker)
        });
        pop_workers.request_worker();
        PopulateWorkers::spawn_worker(pop_workers.clone());

        let msg_core = core.clone();
        std::thread::Builder::new()
            .name("Ufo Msg".to_string())
            .spawn(move || UfoCore::msg_loop(msg_core, msg_thread_sender, pop_workers, recv))?;

        Ok(core)
    }

    pub fn allocate_ufo(
        &self,
        object_config: UfoObjectConfig,
    ) -> Result<WrappedUfoObject, UfoAllocateErr> {
        let (fulfiller, awaiter) = promissory::promissory();
        self.msg_send
            .send(UfoInstanceMsg::Allocate(fulfiller, object_config))
            .expect("Messages pipe broken");

        Ok(awaiter.await_value()?)
    }

    fn get_locked_state(&self) -> anyhow::Result<MutexGuard<UfoCoreState>> {
        match self.state.lock() {
            Err(_) => Err(anyhow::Error::msg("broken core lock")),
            Ok(l) => Ok(l),
        }
    }

    fn ensure_capcity(
        event_queue: &UfoEventSender,
        config: &UfoCoreConfig,
        state: &mut UfoCoreState,
        to_load: usize,
    ) {
        assert!(to_load + config.low_watermark < config.high_watermark);
        if to_load + state.loaded_chunks.used_memory > config.high_watermark {
            state
                .loaded_chunks
                .free_until_low_water_mark(event_queue)
                .unwrap();
        }
    }

    pub fn get_ufo_by_id(&self, id: UfoId) -> Result<WrappedUfoObject, UfoInternalErr> {
        self.get_locked_state()
            .map_err(|e| UfoInternalErr::CoreBroken(format!("{:?}", e)))?
            .objects_by_id
            .get(&id)
            .cloned()
            .map(Ok)
            .unwrap_or_else(|| Err(UfoInternalErr::UfoNotFound))
    }

    pub fn get_ufo_by_address(&self, ptr: usize) -> Result<WrappedUfoObject, UfoInternalErr> {
        self.get_locked_state()
            .map_err(|e| UfoInternalErr::CoreBroken(format!("{:?}", e)))?
            .objects_by_segment
            .get(&ptr)
            .cloned()
            .map(Ok)
            .unwrap_or_else(|| Err(UfoInternalErr::UfoNotFound))
    }

    pub fn new_event_callback(
        &self,
        callback: Option<Box<UfoEventConsumer>>,
    ) -> Result<(), UfoInternalErr> {
        self.ufo_event_sender.lock()?.new_callback(callback)
    }

    fn populate_loop(this: Arc<UfoCore>, request_worker: &dyn RequestWorker) {
        trace!(target: "ufo_core", "Started pop loop");
        fn populate_impl(
            core: &UfoCore,
            buffer: &mut UfoWriteBuffer,
            addr: *mut c_void,
        ) -> Result<(), UfoPopulateError> {
            let event_queue = { core.ufo_event_sender.lock()?.clone() };

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

    fn msg_loop<F>(
        this: Arc<UfoCore>,
        event_sender: UfoEventSender,
        populate_pool: Arc<PopulateWorkers<F>>,
        recv: Receiver<UfoInstanceMsg>,
    ) {
        trace!(target: "ufo_core", "Started msg loop");
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
                body_size_with_padding: config.true_size_with_padding
                    - config.header_size_with_padding,
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
                body_size_with_padding: config.true_size_with_padding
                    - config.header_size_with_padding,
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
            populate_pool: Arc<PopulateWorkers<F>>,
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
                        reset_impl(&this, &event_sender, ufo_id).expect("Reset Error")
                    }
                    UfoInstanceMsg::Free(_, ufo_id) => {
                        free_impl(&this, &event_sender, ufo_id).expect("Free Error")
                    }
                    UfoInstanceMsg::Shutdown(_) => {
                        shutdown_impl(&this, &event_sender, populate_pool);
                        drop(recv);
                        info!(target: "ufo_core", "closing msg loop");
                        return /*done*/;
                    }
                },
                err => {
                    err.expect("recv error");
                }
            }
        }
    }

    pub fn shutdown(&self) {
        let sync = WaitGroup::new();
        trace!(target: "ufo_core", "sending shutdown msg");
        self.msg_send
            .send(UfoInstanceMsg::Shutdown(sync.clone()))
            .expect("Can't send shutdown signal");
        trace!(target: "ufo_core", "awaiting shutdown sync");
        sync.wait();
        trace!(target: "ufo_core", "sync, closing uffd filehandle");

        let fd = std::os::unix::prelude::AsRawFd::as_raw_fd(&self.uffd);
        // this will signal to the populate loop that it is time to close down
        let close_result = unsafe { libc::close(fd) };
        match close_result {
            0 => {}
            _ => {
                panic!(
                    "clouldn't close uffd handle {}",
                    nix::errno::Errno::last().desc()
                );
            }
        }

        trace!(target: "ufo_core", "close uffd handle: {}", close_result);
    }
}
