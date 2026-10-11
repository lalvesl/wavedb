//! Boot the ONE manager task per process, lazily, on first use.
//!
//! Native: a `OnceLock` handle whose task runs on a carrier thread of its
//! own (one Embassy executor, RFC 0066 — so the actor world is
//! single-threaded and may hold `Rc`/`RefCell` state). Browser: a `thread_local` handle whose task is a
//! detached `spawn_local` — the page world is already single-threaded.
//!
//! If the carrier thread cannot spawn (environment-catastrophic), the
//! installed handle's channel has no receiver, so every later use fails
//! loudly with [`ManagerUnavailable`](crate::Error::ManagerUnavailable)
//! rather than hanging.

use futures::channel::mpsc;

use super::{Cmd, Handle, tasks};
use crate::error::Result;

/// The process-wide manager handle, booting the task on first call.
#[cfg(not(target_arch = "wasm32"))]
pub(super) fn handle() -> Result<Handle> {
    static HANDLE: std::sync::OnceLock<Handle> = std::sync::OnceLock::new();
    if let Some(handle) = HANDLE.get() {
        return Ok(handle.clone());
    }
    let (tx, rx) = mpsc::unbounded::<Cmd>();
    let handle = Handle { tx };
    if HANDLE.set(handle.clone()).is_err() {
        // Lost the install race: the winner's task is the manager; this
        // `rx` drops here, unused.
        if let Some(winner) = HANDLE.get() {
            return Ok(winner.clone());
        }
    }
    tasks::start(rx)?;
    Ok(handle)
}

/// The process-wide manager handle, booting the task on first call.
#[cfg(target_arch = "wasm32")]
pub(super) fn handle() -> Result<Handle> {
    use std::cell::RefCell;
    thread_local! {
        static HANDLE: RefCell<Option<Handle>> = const { RefCell::new(None) };
    }
    HANDLE.with(|slot| {
        if let Some(handle) = slot.borrow().as_ref() {
            return Ok(handle.clone());
        }
        let (tx, rx) = mpsc::unbounded::<Cmd>();
        let handle = Handle { tx };
        tasks::start(rx)?;
        *slot.borrow_mut() = Some(handle.clone());
        Ok(handle)
    })
}
