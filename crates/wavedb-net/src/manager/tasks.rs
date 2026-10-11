//! The manager's tasks, one spawn function per kind, cfg-switched like the
//! platform seam ([RFC 0066] §4).
//!
//! Native: Embassy tasks from **static pools** on the manager's carrier
//! thread. A full pool is a refusal — [`Error::Saturated`] — never a queue;
//! the caller decides whether to try again. Browser: `spawn_local`, as
//! before (the page has one thread and no pools), behind the same functions,
//! so the manager's code does not branch.
//!
//! [RFC 0066]: ../../../../rfcs/0066-embassy-executor-and-async-io-WIP.md

use std::cell::Cell;
use std::rc::Rc;

use futures::channel::{mpsc, oneshot};

use super::{PostFrames, poll, ws_conn};
use crate::error::Result;
use crate::frame::{Auth, CommandFrame};

/// POST exchanges in flight at once, per process. One more is refused.
///
/// Set at **build** time with `WAVEDB_CLIENT_MAX_POSTS` (default 1024): the
/// pool is a `static`, so its size is a compile-time constant.
pub const MAX_POSTS: usize = match option_env!("WAVEDB_CLIENT_MAX_POSTS") {
    Some(digits) => parse(digits),
    None => 1024,
};

/// A build-time count, in decimal. A malformed value fails the build rather
/// than shipping a surprise pool size.
const fn parse(digits: &str) -> usize {
    let bytes = digits.as_bytes();
    assert!(!bytes.is_empty(), "WAVEDB_CLIENT_MAX_POSTS is empty");
    let mut value = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        let digit = bytes[i];
        assert!(
            digit.is_ascii_digit(),
            "WAVEDB_CLIENT_MAX_POSTS is not a decimal count"
        );
        value = value * 10 + (digit - b'0') as usize;
        i += 1;
    }
    assert!(value > 0, "WAVEDB_CLIENT_MAX_POSTS must be at least 1");
    value
}

/// Arguments of one POST exchange, as its task receives them.
pub(super) struct Post {
    pub addr: String,
    pub auth: Auth,
    pub frame: CommandFrame,
    pub connected: oneshot::Sender<Result<PostFrames>>,
    pub poll: Option<mpsc::UnboundedSender<poll::PollCmd>>,
    /// This exchange's place in the in-flight count, released when the
    /// exchange ends.
    pub slot: InFlight,
}

/// One counted POST. Dropping it decrements the manager's in-flight count.
pub(super) struct InFlight(Rc<Cell<usize>>);

impl InFlight {
    /// Count one more exchange in flight.
    pub fn take(count: &Rc<Cell<usize>>) -> Self {
        count.set(count.get() + 1);
        Self(Rc::clone(count))
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.set(self.0.get().saturating_sub(1));
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) use native::*;
#[cfg(target_arch = "wasm32")]
pub(super) use wasm::*;

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::time::Duration;

    use futures::channel::mpsc;
    use wavedb_platform::ws::{Received, RecvHalf};

    use super::{MAX_POSTS, Post};
    use crate::manager::{Cmd, ConnKey, actor, poll, ws_conn, ws_dial};

    pub use wavedb_platform::carrier::Here;

    /// Watched `(addr, identity)` connections at once, per kind (WebSocket,
    /// HTTP poll). A client normally has one or two; the browser has no
    /// pools, so this is native only.
    pub const MAX_WATCH_CONNS: usize = 64;

    /// One manager per process; the headroom is for nothing in particular —
    /// a pool of one would do — but costs nothing either.
    const MANAGERS: usize = 1;

    #[wavedb_platform::carrier::rt::task(
        pool_size = MANAGERS,
        embassy_executor = wavedb_platform::carrier::rt
    )]
    async fn manager(rx: mpsc::UnboundedReceiver<Cmd>, here: Here) {
        actor::run(rx, here).await;
    }

    #[wavedb_platform::carrier::rt::task(
        pool_size = MAX_POSTS,
        embassy_executor = wavedb_platform::carrier::rt
    )]
    async fn post(args: Post, here: Here) {
        actor::run_post(args).await;
        drop(here); // held to the end: the carrier waits for this task
    }

    #[wavedb_platform::carrier::rt::task(
        pool_size = MAX_WATCH_CONNS,
        embassy_executor = wavedb_platform::carrier::rt
    )]
    async fn ws_actor(
        key: ConnKey,
        cmds: mpsc::UnboundedReceiver<ws_conn::ConnCmd>,
        here: Here,
    ) {
        ws_conn::run(key, cmds, here).await;
    }

    #[wavedb_platform::carrier::rt::task(
        pool_size = MAX_WATCH_CONNS,
        embassy_executor = wavedb_platform::carrier::rt
    )]
    async fn poll_actor(
        key: ConnKey,
        every: Duration,
        cmds: mpsc::UnboundedReceiver<poll::PollCmd>,
        here: Here,
    ) {
        poll::run(key, every, cmds).await;
        drop(here);
    }

    /// Twice the connections: a reconnect's new reader can start while the
    /// old one is still waiting for its socket to close.
    const READERS: usize = 2 * MAX_WATCH_CONNS;

    #[wavedb_platform::carrier::rt::task(
        pool_size = READERS,
        embassy_executor = wavedb_platform::carrier::rt
    )]
    async fn ws_reader(
        recv: RecvHalf,
        msgs: mpsc::UnboundedSender<Received>,
        here: Here,
    ) {
        ws_dial::read_into(recv, msgs).await;
        drop(here);
    }

    /// Start the manager on a carrier thread of its own.
    pub fn start(rx: mpsc::UnboundedReceiver<Cmd>) -> crate::Result<()> {
        // The thread ends only if the manager does, which it never does: the
        // process-wide handle holds the sender.
        drop(wavedb_platform::carrier::spawn(
            "wavedb-conn-manager",
            move |here| {
                // Refused: `rx` drops, and every send reports the manager
                // unavailable — loudly, not as a hang.
                let _ = here.spawn(manager(rx, here.clone()));
            },
        )?);
        Ok(())
    }

    pub fn spawn_post(here: &Here, args: Post) -> bool {
        here.spawn(post(args, here.clone())).is_ok()
    }

    pub fn spawn_ws(
        here: &Here,
        key: ConnKey,
        cmds: mpsc::UnboundedReceiver<ws_conn::ConnCmd>,
    ) -> bool {
        here.spawn(ws_actor(key, cmds, here.clone())).is_ok()
    }

    pub fn spawn_poll(
        here: &Here,
        key: ConnKey,
        every: Duration,
        cmds: mpsc::UnboundedReceiver<poll::PollCmd>,
    ) -> bool {
        here.spawn(poll_actor(key, every, cmds, here.clone()))
            .is_ok()
    }

    pub fn spawn_reader(
        here: &Here,
        recv: RecvHalf,
        msgs: mpsc::UnboundedSender<Received>,
    ) -> bool {
        here.spawn(ws_reader(recv, msgs, here.clone())).is_ok()
    }
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    use std::time::Duration;

    use futures::channel::mpsc;
    use wavedb_platform::task::{spawn_detached, spawn_local};
    use wavedb_platform::ws::{Received, RecvHalf};

    use super::Post;
    use crate::manager::{Cmd, ConnKey, actor, poll, ws_conn, ws_dial};

    /// The page's one thread: nothing to carry, so nothing to hold.
    #[derive(Debug, Clone)]
    pub struct Here;

    pub fn start(rx: mpsc::UnboundedReceiver<Cmd>) -> crate::Result<()> {
        spawn_detached("wavedb-conn-manager", move || actor::run(rx, Here))?;
        Ok(())
    }

    pub fn spawn_post(_: &Here, args: Post) -> bool {
        spawn_local(actor::run_post(args));
        true
    }

    pub fn spawn_ws(
        _: &Here,
        key: ConnKey,
        cmds: mpsc::UnboundedReceiver<ws_conn::ConnCmd>,
    ) -> bool {
        spawn_local(ws_conn::run(key, cmds, Here));
        true
    }

    pub fn spawn_poll(
        _: &Here,
        key: ConnKey,
        every: Duration,
        cmds: mpsc::UnboundedReceiver<poll::PollCmd>,
    ) -> bool {
        spawn_local(poll::run(key, every, cmds));
        true
    }

    pub fn spawn_reader(
        _: &Here,
        recv: RecvHalf,
        msgs: mpsc::UnboundedSender<Received>,
    ) -> bool {
        spawn_local(ws_dial::read_into(recv, msgs));
        true
    }
}

/// What the manager refuses when a pool is full: the ack or answer each
/// command carries hears [`Error::Saturated`], so a refused caller gets a
/// cause instead of a closed channel.
pub(super) trait Refuse {
    fn refuse(self);
}

impl Refuse for ws_conn::ConnCmd {
    fn refuse(self) {
        if let Self::Subscribe { ack, .. } = self {
            let _ = ack.send(Err(saturated("websocket watch connections")));
        }
    }
}

impl Refuse for poll::PollCmd {
    fn refuse(self) {
        if let Self::Subscribe { ack, .. } = self {
            let _ = ack.send(Err(saturated("poll watch connections")));
        }
    }
}

pub(super) const fn saturated(what: &'static str) -> crate::Error {
    crate::Error::Saturated(what)
}

#[cfg(test)]
mod tests {
    use futures::channel::{mpsc, oneshot};

    use super::{Refuse, parse};
    use crate::Error;
    use crate::manager::ws_conn::ConnCmd;
    use crate::ws::Topic;

    #[test]
    fn a_build_time_count_parses_as_decimal() {
        assert_eq!(parse("1024"), 1024);
        assert_eq!(parse("7"), 7);
    }

    /// A watch refused for a full pool hears why, rather than seeing its ack
    /// channel close with no cause.
    #[test]
    fn a_refused_watch_is_told_the_pool_is_saturated() {
        let (events, _rx) = mpsc::unbounded();
        let (ack, answer) = oneshot::channel();
        ConnCmd::Subscribe {
            topic: Topic {
                struct_hash: 1,
                pivot: None,
            },
            watch: 0,
            events,
            ack,
        }
        .refuse();
        let refused = futures::executor::block_on(answer).expect("answered");
        assert!(matches!(refused, Err(Error::Saturated(_))));
    }
}
