//! Background tasks in the browser — the seam the client's connection
//! manager runs on there.
//!
//! - [`spawn_detached`] boots a **never-ending** task;
//! - [`spawn_local`] spawns a subtask inside it (per-request work,
//!   per-connection actors).
//!
//! Both are `wasm_bindgen_futures::spawn_local`: the page has one thread, so
//! subtask futures may hold `Rc`/`RefCell` state. Natively the same work runs
//! on [`carrier`](crate::carrier) threads — Embassy executors whose tasks come
//! from static pools (RFC 0066) — which is why this module is browser-only:
//! a generic `spawn` cannot exist over static pools.

use std::future::Future;

use crate::error::Result;

/// Boot a detached, long-lived task; returns once it is scheduled.
///
/// The task ends only when its future completes — a connection manager's
/// never does. `name` names a native thread and is unused here.
///
/// # Errors
/// None in the browser; the `Result` keeps the native call shape.
pub fn spawn_detached<F, Fut>(name: &str, factory: F) -> Result<()>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    let _ = name;
    wasm_bindgen_futures::spawn_local(factory());
    Ok(())
}

/// Spawn a subtask on the page's one thread.
pub fn spawn_local<Fut: Future<Output = ()> + 'static>(future: Fut) {
    wasm_bindgen_futures::spawn_local(future);
}
