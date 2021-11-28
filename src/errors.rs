use thiserror::Error;

use crate::bitwise_spinlock::BitlockErr;

#[derive(Error, Debug)]
pub enum UfoInternalErr {
    #[error("Core shutdown")]
    CoreShutdown,
    #[error("Core non functional, {0}")]
    CoreBroken(String),
    #[error("Ufo Lock is broken")]
    UfoLockBroken,
    #[error("Ufo writeback bitlock error {0}")]
    UfoBitlockError(BitlockErr),
    #[error("Ufo not found")]
    UfoNotFound,
    #[error("Ufo State Error: {0}")]
    UfoStateError(String),
    #[error("Ufo Allocate Error: {0}")]
    UfoAllocateError(UfoAllocateErr),
}

impl<T> From<std::sync::PoisonError<T>> for UfoInternalErr {
    fn from(_: std::sync::PoisonError<T>) -> Self {
        UfoInternalErr::UfoLockBroken
    }
}

impl<T> From<std::sync::mpsc::SendError<T>> for UfoInternalErr {
    fn from(_e: std::sync::mpsc::SendError<T>) -> Self {
        UfoInternalErr::CoreBroken("Error when sending messsge to the core".into())
    }
}

impl<T> From<crossbeam::channel::SendError<T>> for UfoInternalErr {
    fn from(_e: crossbeam::channel::SendError<T>) -> Self {
        UfoInternalErr::CoreBroken("Error when sending messsge to the core".into())
    }
}

impl From<UfoAllocateErr> for UfoInternalErr {
    fn from(e: UfoAllocateErr) -> Self {
        UfoInternalErr::UfoAllocateError(e)
    }
}

#[derive(Error, Debug)]
pub enum UfoAllocateErr {
    #[error("Could not send message, messaage channel broken")]
    MessageSendError,
    #[error("Could not recieve message")]
    MessageRecvError,
}

impl<T> From<std::sync::mpsc::SendError<T>> for UfoAllocateErr {
    fn from(_e: std::sync::mpsc::SendError<T>) -> Self {
        UfoAllocateErr::MessageSendError
    }
}

impl From<std::sync::mpsc::RecvError> for UfoAllocateErr {
    fn from(_e: std::sync::mpsc::RecvError) -> Self {
        UfoAllocateErr::MessageSendError
    }
}

impl<T> From<crossbeam::channel::SendError<T>> for UfoAllocateErr {
    fn from(_e: crossbeam::channel::SendError<T>) -> Self {
        UfoAllocateErr::MessageSendError
    }
}

impl From<crossbeam::channel::RecvError> for UfoAllocateErr {
    fn from(_e: crossbeam::channel::RecvError) -> Self {
        UfoAllocateErr::MessageSendError
    }
}

#[derive(Error, Debug)]
#[error("Internal Ufo Error when populating")]
pub struct UfoPopulateError;

impl<T> From<std::sync::PoisonError<T>> for UfoPopulateError {
    fn from(_: std::sync::PoisonError<T>) -> Self {
        UfoPopulateError
    }
}

impl From<UfoInternalErr> for UfoPopulateError {
    fn from(_e: UfoInternalErr) -> Self {
        UfoPopulateError
    }
}

impl From<BitlockErr> for UfoPopulateError {
    fn from(_e: BitlockErr) -> Self {
        UfoPopulateError
    }
}

impl From<userfaultfd::Error> for UfoPopulateError {
    fn from(e: userfaultfd::Error) -> Self {
        UfoPopulateError
    }
}
