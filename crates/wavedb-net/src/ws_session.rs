//! [`WsSession`] — the client half of the WebSocket transport (both
//! targets): open a connection, present the identity **once**, declare
//! subscriptions, receive pushed [`RecordEvent`]s.
//!
//! The session is watch-shaped (M7 W5): it carries no `Call`s — commands
//! keep riding HTTP POST, one identity-checked exchange each, while this
//! connection exists to be pushed to. `subscribe` waits for the node's
//! [`TopicOk`](ServerMsg::TopicOk) ack (messages are FIFO on the
//! connection), so when it returns the watch is live and cannot miss a
//! later mutation; events of already-live subscriptions arriving while an
//! ack is awaited are buffered, never dropped.
//!
//! A refused identity has no error envelope — the node answers a bad
//! `Hello` by closing — so it surfaces as a transport-shaped
//! [`Error::Http`]. An **anonymous** session must not subscribe: the node
//! treats that as a protocol violation and closes the connection.

use std::collections::VecDeque;

use wavedb_wire::{from_wire, to_wire};

use crate::error::{Error, Result};
use crate::frame::Auth;
use crate::ws::{ClientMsg, RecordEvent, ServerMsg, Topic};

/// An identity-bound WebSocket session receiving subscription events.
#[derive(Debug)]
pub struct WsSession {
    conn: wavedb_platform::ws::Conn,
    /// Events that arrived while waiting for a subscription ack.
    pending: VecDeque<RecordEvent>,
}

impl WsSession {
    /// Connect to `addr`, present `auth`, and wait for the binding ack.
    ///
    /// # Errors
    /// A socket/handshake fault, or [`Error::Http`] when the node refuses
    /// the identity (it closes instead of answering `HelloOk`).
    pub async fn open(addr: &str, auth: Auth) -> Result<Self> {
        let conn = wavedb_platform::ws::connect(addr).await?;
        let mut session = Self {
            conn,
            pending: VecDeque::new(),
        };
        session.send(&ClientMsg::Hello(auth)).await?;
        match session.next_msg().await? {
            Some(ServerMsg::HelloOk) => Ok(session),
            Some(_) => {
                Err(Error::Http("websocket hello answered off-protocol"))
            }
            None => Err(Error::Http("websocket hello refused")),
        }
    }

    /// Declare interest in `topic` under the session's bound tenant.
    /// Returns once the subscription is **live** (the node acked it), so a
    /// mutation committed after this call cannot be missed.
    ///
    /// # Errors
    /// A socket fault, or a close before the ack — which is also how the
    /// node refuses an anonymous session's subscribe.
    pub async fn subscribe(&mut self, topic: Topic) -> Result<()> {
        self.topic_request(&ClientMsg::Subscribe(topic), topic)
            .await
    }

    /// Stop watching `topic`; returns once the node acked, after which no
    /// further event for it will arrive.
    ///
    /// # Errors
    /// A socket fault or a close before the ack.
    pub async fn unsubscribe(&mut self, topic: Topic) -> Result<()> {
        self.topic_request(&ClientMsg::Unsubscribe(topic), topic)
            .await
    }

    /// The next pushed event; `None` = the node closed the connection.
    ///
    /// # Errors
    /// A socket fault, an undecodable envelope, or a non-event message (a
    /// watch session has no call in flight to be answered).
    pub async fn next_event(&mut self) -> Result<Option<RecordEvent>> {
        if let Some(event) = self.pending.pop_front() {
            return Ok(Some(event));
        }
        match self.next_msg().await? {
            None => Ok(None),
            Some(ServerMsg::Event(event)) => Ok(Some(event)),
            Some(_) => Err(Error::Http("unexpected websocket message")),
        }
    }

    /// Close the session (best-effort close frame; the node unregisters
    /// every subscription on disconnect either way).
    ///
    /// # Errors
    /// A socket fault sending the close frame.
    pub async fn close(&mut self) -> Result<()> {
        self.conn.close().await?;
        Ok(())
    }

    /// Send a subscription mutation and wait for its ack, buffering events
    /// of already-live subscriptions that land in between.
    async fn topic_request(
        &mut self,
        msg: &ClientMsg,
        topic: Topic,
    ) -> Result<()> {
        self.send(msg).await?;
        loop {
            match self.next_msg().await? {
                Some(ServerMsg::TopicOk(acked, _)) if acked == topic => {
                    return Ok(());
                }
                Some(ServerMsg::Event(event)) => self.pending.push_back(event),
                Some(_) => {
                    return Err(Error::Http("unexpected websocket message"));
                }
                None => {
                    return Err(Error::Http(
                        "websocket closed before the subscription ack",
                    ));
                }
            }
        }
    }

    async fn send(&mut self, msg: &ClientMsg) -> Result<()> {
        self.conn.send(&to_wire(msg)).await?;
        Ok(())
    }

    async fn next_msg(&mut self) -> Result<Option<ServerMsg>> {
        match self.conn.recv().await? {
            None => Ok(None),
            Some(bytes) => Ok(Some(from_wire::<ServerMsg>(&bytes)?)),
        }
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use wavedb_core::{Id, U48};
    use wavedb_platform::io::{Listener, ReadHalf, WriteHalf};
    use wavedb_platform::ws::codec::{
        self, Messages, Msg, OP_BINARY, OP_CLOSE,
    };
    use wavedb_wire::{from_wire, to_wire};

    use super::WsSession;
    use crate::frame::Auth;
    use crate::ws::{ClientMsg, EventKind, RecordEvent, ServerMsg, Topic};
    use crate::{Error, http};

    const TOPIC: Topic = Topic {
        struct_hash: 0xFEED,
        pivot: None,
    };

    fn event(n: u64) -> RecordEvent {
        RecordEvent {
            topic: TOPIC,
            id: Id::new(n, U48::from(1u32), false, 0),
            kind: EventKind::Saved,
            meta: None,
            body: vec![7],
        }
    }

    /// Accept one connection and run the RFC 6455 upgrade — the node's
    /// server half, miniaturised for the loopback.
    async fn upgraded(listener: Listener) -> (Messages<ReadHalf>, WriteHalf) {
        let (sock, _) = listener.accept().await.expect("accept");
        let (mut r, mut w) = sock.split();
        let Some(http::Incoming::Upgrade { key }) =
            http::read_request(&mut r).await.expect("read upgrade")
        else {
            panic!("must be an upgrade");
        };
        http::write_switching_head(&mut w, &codec::accept_key(&key))
            .await
            .expect("write 101");
        (Messages::new(r), w)
    }

    async fn expect_client_msg(msgs: &mut Messages<ReadHalf>) -> ClientMsg {
        let Ok(Some(Msg::Binary(bytes))) = msgs.next(true).await else {
            panic!("expected a client message");
        };
        from_wire::<ClientMsg>(&bytes).expect("decode client msg")
    }

    async fn send_server(w: &mut WriteHalf, msg: &ServerMsg) {
        codec::write_message(w, OP_BINARY, &to_wire(msg), false)
            .await
            .expect("server send");
    }

    #[test]
    fn subscribe_acks_and_buffers_events_arriving_early() {
        wavedb_platform::carrier::block_on(async {
            let listener = Listener::bind("127.0.0.1:0").expect("bind");
            let addr = listener.local_addr().expect("addr").to_string();
            let server = std::thread::spawn(move || {
                wavedb_platform::carrier::block_on(async move {
                    let (mut msgs, mut w) = upgraded(listener).await;
                    let ClientMsg::Hello(Auth::Anonymous { tenant }) =
                        expect_client_msg(&mut msgs).await
                    else {
                        panic!("first message must be the hello");
                    };
                    assert_eq!(tenant, U48::from(9u32));
                    send_server(&mut w, &ServerMsg::HelloOk).await;
                    assert_eq!(
                        expect_client_msg(&mut msgs).await,
                        ClientMsg::Subscribe(TOPIC)
                    );
                    // An event lands BEFORE the ack — the session must buffer it.
                    send_server(&mut w, &ServerMsg::Event(event(1))).await;
                    send_server(&mut w, &ServerMsg::TopicOk(TOPIC, 0)).await;
                    send_server(&mut w, &ServerMsg::Event(event(2))).await;
                    let _ = codec::write_message(&mut w, OP_CLOSE, &[], false)
                        .await;
                });
            });

            let mut session = WsSession::open(
                &addr,
                Auth::Anonymous {
                    tenant: U48::from(9u32),
                },
            )
            .await
            .expect("hello ok");
            session.subscribe(TOPIC).await.expect("acked");
            // The buffered pre-ack event first, then the live one, then the close.
            assert_eq!(
                session.next_event().await.expect("event"),
                Some(event(1))
            );
            assert_eq!(
                session.next_event().await.expect("event"),
                Some(event(2))
            );
            assert_eq!(session.next_event().await.expect("closed"), None);
            server.join().expect("server task");
        });
    }

    #[test]
    fn refused_hello_is_an_error_not_a_hang() {
        wavedb_platform::carrier::block_on(async {
            let listener = Listener::bind("127.0.0.1:0").expect("bind");
            let addr = listener.local_addr().expect("addr").to_string();
            let server = std::thread::spawn(move || {
                wavedb_platform::carrier::block_on(async move {
                    let (mut msgs, mut w) = upgraded(listener).await;
                    let _ = expect_client_msg(&mut msgs).await;
                    // The node's refusal: close without a word.
                    let _ = codec::write_message(&mut w, OP_CLOSE, &[], false)
                        .await;
                });
            });

            let refused = WsSession::open(
                &addr,
                Auth::Anonymous {
                    tenant: U48::from(9u32),
                },
            )
            .await;
            assert!(matches!(
                refused,
                Err(Error::Http("websocket hello refused"))
            ));
            server.join().expect("server task");
        });
    }
}
