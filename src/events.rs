use std::io::Error;

use crossbeam::sync::WaitGroup;
use log::{info, trace};

pub type UfoEventConsumer = dyn Fn(&UfoEventandTimestamp) + Send;

#[repr(C)]
pub enum UfoUnloadDisposition {
    /// Read only UFO
    ReadOnly,
    /// RW UFO but memory was clean
    Clean,
    /// RW UFO and dirty chunk will be written to disk, using more disk
    NewlyDirty,
    /// Chunk was already from disk, and was updated. No new disk useage
    ExistingDirty,
}

pub(crate) enum UfoEventResult {
    RecvErr,
    NewCallback {
        callback: Option<Box<UfoEventConsumer>>,
        timestamp_nanos: u64,
    },
    Event(UfoEventandTimestamp),
}

#[repr(C)]
pub struct UfoEventandTimestamp {
    pub timestamp_nanos: u64,
    pub event: UfoEvent,
}

#[repr(C)]
pub enum UfoEvent {
    NewCallbackAck,

    /// Allocation uses memory equal to header_size_with_padding
    AllocateUfo {
        ufo_id: u64,

        header_size_with_padding: usize,
        body_size_with_padding: usize,
        total_size_with_padding: usize,

        intended_header_size: usize,
        intended_body_size: usize,

        read_only: bool,
    },

    /// Popuate a chunk, using memory equal to memory_used
    /// memory_used might be larger than expected in some situations
    ///  because of rounding up to the page boundary
    PopulateChunk {
        ufo_id: u64,

        loaded_from_writeback: bool,
        memory_used: usize,
    },

    GcCycleStart,

    /// Unload the chunk, freeing RAM
    /// the amount of disk used on a UfoUnloadDisposition::NewlyDirty equals memory_freed
    UnloadChunk {
        ufo_id: u64,

        disposition: UfoUnloadDisposition,
        memory_freed: usize,
    },

    UfoReset {
        ufo_id: u64,
        memory_freed: usize,
        chunks_freed: usize,
        disk_freed: usize,
    },

    GcCycleEnd,

    /// Free the UFO, removing all resource use
    FreeUfo {
        ufo_id: u64,

        header_size_with_padding: usize,
        body_size_with_padding: usize,
        total_size_with_padding: usize,

        intended_header_size: usize,
        intended_body_size: usize,

        memory_freed: usize,
        chunks_freed: usize,
        disk_freed: usize,
    },

    Shutdown,
}

pub(crate) fn start_qeueue_runner<Recv>(
    reciever: Recv,
    shutdown_sync: WaitGroup,
) -> Result<(), Error>
where
    Recv: 'static + Send + Fn() -> UfoEventResult,
{
    std::thread::Builder::new()
        .name("Message Queue Runner".to_string())
        .spawn(move || {
            let mut callback = None;
            loop {
                let recv_msg = reciever();
                match (recv_msg, &callback) {
                    // new callback function
                    (
                        UfoEventResult::NewCallback {
                            callback: cb,
                            timestamp_nanos,
                        },
                        _,
                    ) => {
                        if let Some(the_cb) = &cb {
                            the_cb(&UfoEventandTimestamp {
                                timestamp_nanos,
                                event: UfoEvent::NewCallbackAck,
                            });
                            trace!(target: "ufo_core", "New ufo event listener");
                        } else {
                            trace!(target: "ufo_core", "Null ufo event listener");
                        }
                        callback = cb;
                    }
                    // Shutdown with an active callback
                    (
                        UfoEventResult::Event(
                            msg
                            @
                            UfoEventandTimestamp {
                                event: UfoEvent::Shutdown,
                                ..
                            },
                        ),
                        Some(cb),
                    ) => {
                        trace!(target: "ufo_core", "Sending shutdown event to listener");
                        cb(&msg);
                        info!(target: "ufo_core", "Ufo event listener loop shutting down");
                        break;
                    }
                    // Shutdown with no active callback
                    (
                        UfoEventResult::Event(UfoEventandTimestamp {
                            event: UfoEvent::Shutdown,
                            ..
                        }),
                        _,
                    ) => {
                        info!(target: "ufo_core", "Ufo event lsitener loop shutting down");
                        break;
                    }
                    // Normal event
                    (UfoEventResult::Event(msg), Some(cb)) => cb(&msg),

                    (UfoEventResult::RecvErr, _) => break, // hard shutdown
                    _ => { /* NOP */ }
                }
            }
            std::mem::drop(shutdown_sync);
        })?;
    Ok(())
}
