use std::result::Result;
use std::sync::{Arc, Mutex, RwLock};
use std::{collections::HashMap, sync::MutexGuard};

use crossbeam::channel::Sender;
use log::trace;

use btree_interval_map::IntervalMap;
use crossbeam::sync::WaitGroup;
use rayon::{ThreadPool, ThreadPoolBuilder};
use semver::Version;
use uname::uname;
use userfaultfd::{FeatureFlags, IoctlFlags, Uffd};

use crate::populate_workers::{PopulateWorkers, RequestWorker};
use crate::{UfoEventConsumer, UfoEventSender};

use crate::errors::*;
use crate::ufo_objects::*;

use super::ufo_chunks::UfoChunks;

pub enum UfoInstanceMsg {
    Shutdown(WaitGroup),
    Allocate(promissory::Fulfiller<WrappedUfoObject>, UfoObjectConfig),
    Reset(WaitGroup, UfoId),
    Free(WaitGroup, UfoId),
}

pub struct UfoCoreConfig {
    pub writeback_temp_path: String,
    pub high_watermark: usize,
    pub low_watermark: usize,
}

pub type WrappedUfoObject = Arc<RwLock<UfoObject>>;

pub(super) struct UfoCoreState {
    pub(super) object_id_gen: UfoIdGen,

    pub(super) objects_by_id: HashMap<UfoId, WrappedUfoObject>,
    pub(super) objects_by_segment: IntervalMap<usize, WrappedUfoObject>,

    pub(super) loaded_chunks: UfoChunks,
}

pub struct UfoCore {
    pub(super) uffd: Uffd,
    pub(super) state: Mutex<UfoCoreState>,
    pub(super) rayon_pool: ThreadPool,

    pub(crate) config: Arc<UfoCoreConfig>,
    pub(crate) msg_send: Sender<UfoInstanceMsg>,

    pub(super) ufo_event_sender: UfoEventSender,
    pub(super) event_qeueue_shutdown_sync: Mutex<Option<WaitGroup>>,
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
            .require_ioctls(IoctlFlags::WRITE_PROTECT)
            .require_features(FeatureFlags::PAGEFAULT_FLAG_WP)
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

        let (ufo_event_sender, event_qeueue_shutdown_sync) = UfoEventSender::new()?;
        let msg_thread_event_sender = ufo_event_sender.clone();

        let rayon_pool = ThreadPoolBuilder::new()
            .thread_name(|x| format!("UFO hasher No. {}", x))
            .build()
            .unwrap();

        let core = Arc::new(UfoCore {
            uffd,
            config,
            rayon_pool,
            msg_send: send,
            state,
            ufo_event_sender,
            event_qeueue_shutdown_sync: Mutex::new(Some(event_qeueue_shutdown_sync)),
        });

        trace!(target: "ufo_core", "starting threads");
        let pop_core = core.clone();
        let pop_workers = PopulateWorkers::new("Ufo pop worker", move |request_worker| {
            UfoCore::populate_loop(pop_core.clone(), request_worker)
        });
        pop_workers.request_worker();
        PopulateWorkers::spawn_worker(pop_workers.clone());

        let msg_core = core.clone();
        std::thread::Builder::new()
            .name("Ufo Msg_Loop".to_string())
            .spawn(move || {
                UfoCore::msg_loop(msg_core, msg_thread_event_sender, pop_workers, recv)
            })?;

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

    pub(super) fn get_locked_state(&self) -> anyhow::Result<MutexGuard<UfoCoreState>> {
        match self.state.lock() {
            Err(_) => Err(anyhow::Error::msg("broken core lock")),
            Ok(l) => Ok(l),
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
        self.ufo_event_sender.new_callback(callback)
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
