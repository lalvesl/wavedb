//! The node-side gate + dispatch: turn one decoded [`Request`] into a
//! [`Response`].
//!
//! The gates run **before** the engine, in the order
//! [`wavedb-quick-node` README] documents:
//!
//! 1. **Identity** — [`Auth::Token`] verifies against the node secret
//!    (HMAC, expiry, `Access` purpose); `user`/`tenant` come from the
//!    claims, never from a client field. [`Auth::Anonymous`] becomes the
//!    unauthenticated tier (`user = U48::MAX`) under the claimed tenant —
//!    only `#[server(public)]` functions accept it (the generated guard
//!    refuses everything else, and struct commands refuse it here).
//! 2. **Header** — the `struct_hash` must be listed in the registry
//!    ([`Exposure::knows`]); unlisted / excluded / unknown all refuse
//!    uniformly as [`UnknownStructHash`](wavedb_core::Error::UnknownStructHash).
//! 3. **Decode + engine** — [`Exposure::execute`] decodes the payload for the
//!    command (a `Get`/`Remove` payload is an `Id`, a `Save`/`Insert`/`Update`
//!    payload a body) and drives the storage engine.
//!
//! Gates 5–6 (`validate` / `preprocess`) are still a later seam; gate 4's
//! record-level `Metadata.permission` grants ride with the cross-tenant
//! read path (deferred) — tenant isolation is enforced by the token
//! binding itself.
//!
//! [`wavedb-quick-node` README]: https://docs.rs/wavedb-quick-node

use wavedb_core::expose::{Caller, Exposure, Reply};
use wavedb_core::{Error, Store};
use wavedb_net::auth::{self, TokenPurpose};
use wavedb_net::frame::{
    Auth, CommandFrame, NodeError, NodeErrorKind, Request, Response,
};
use wavedb_net::sync::TopicCursor;

/// The uniform identity refusal — which check failed stays server-side.
fn unauthorized(struct_hash: u64) -> Response {
    Response::Err(NodeError {
        kind: NodeErrorKind::Unauthorized,
        struct_hash,
        message: "unauthorized".into(),
    })
}

/// Gate 1: resolve the request's identity claim into the [`Caller`] the
/// engine executes as. `Err(())` = refuse (bad/expired/foreign token, or a
/// token before the node has a secret). Shared by the HTTP path
/// ([`handle`]) and the WebSocket `Hello` (`serve_ws`), which binds it
/// once for the connection.
pub(crate) fn identify(auth: &Auth, secret: &[u8; 32]) -> Result<Caller, ()> {
    match auth {
        Auth::Anonymous { tenant } => Ok(Caller::anonymous(*tenant)),
        Auth::Token(token) => {
            let claims = auth::verify(
                secret,
                token,
                auth::unix_now(),
                TokenPurpose::Access,
            )
            .map_err(|_| ())?;
            Ok(Caller {
                user: claims.user,
                tenant: claims.tenant,
            })
        }
    }
}

/// One request's full answer: the command's [`Response`] plus an optional
/// piggyback delta.
///
/// The delta is a wire-encoded [`SyncReply`](wavedb_net::sync::SyncReply) for
/// the request's declared poll topics (W7), which the HTTP writer ships as a
/// [`StreamFrame::Sync`](wavedb_net::frame::StreamFrame::Sync) riding the reply.
pub struct Answer {
    /// The executed command's response.
    pub response: Response,
    /// The piggyback sync delta, or `None` when the request declared no poll
    /// topics (or the navigation refused — the command reply still stands).
    pub sync: Option<Vec<u8>>,
}

/// Run the gates and the engine for one request, producing the wire answer.
///
/// Never returns a transport error — a refusal or engine fault is the
/// [`Response::Err`] arm, so the caller always has bytes to send back.
pub async fn handle<E, S>(
    registry: &E,
    store: &S,
    locks: &crate::shard::OwnerLocks,
    secret: &[u8; 32],
    request: Request,
) -> Answer
where
    E: Exposure,
    S: Store,
{
    let Request { auth, frame, sync } = request;

    // Gate 1 — identity.
    let Ok(caller) = identify(&auth, secret) else {
        return Answer {
            response: unauthorized(frame.struct_hash),
            sync: None,
        };
    };

    // The reserved sync exchange (HTTP-poll watch) routes before the
    // registry — no schema hash can reach it, and it can shadow none. It
    // carries its own topics, so it never also piggybacks.
    if frame.struct_hash == wavedb_net::sync::SYNC_STRUCT_HASH {
        let response = sync_poll(registry, store, caller, &frame.payload).await;
        return Answer {
            response,
            sync: None,
        };
    }

    // Gate 1.5 — serialise the operation against others on the same owner.
    //
    // Required, not defensive: a collection op is a read-modify-write across
    // several structures, and it is atomic today only because `PageStore`
    // never suspends. Under any store that does — `ShardStore`, IndexedDB,
    // anything network-backed — two interleaved ops silently lose an index
    // update, leaving a record live at its anchor and reachable from nothing.
    // See `CONCURRENCY_BRAKE.md` and `wavedb-core/tests/concurrent_node_clobber.rs`.
    //
    // The guard is dropped before the piggyback: that navigation is
    // best-effort and read-only, so holding the owner through it would
    // lengthen the critical section for nothing.
    let response = {
        let _owner = locks
            .acquire(crate::shard::OwnerKey::of(
                caller.tenant,
                frame.struct_hash,
            ))
            .await;
        execute(registry, store, caller, frame).await
    };
    let delta = piggyback(registry, store, caller, &sync).await;
    Answer {
        response,
        sync: delta,
    }
}

/// Navigate the W7 piggyback: when the request declared poll topics, run the
/// same stateless `Changes` navigation as [`sync_poll`] and wire-encode the
/// [`SyncReply`](wavedb_net::sync::SyncReply) for a `StreamFrame::Sync` riding
/// the command reply. `None` when none were declared — or when the navigation
/// refused: the delta is best-effort (the command's own reply is
/// authoritative, and a skipped delta is picked up by the next ordinary poll).
async fn piggyback<E, S>(
    registry: &E,
    store: &S,
    caller: Caller,
    topics: &[TopicCursor],
) -> Option<Vec<u8>>
where
    E: Exposure,
    S: Store,
{
    if topics.is_empty() {
        return None;
    }
    match navigate(registry, store, caller, topics).await {
        Response::Ok(Reply::Returned(bytes)) => Some(bytes),
        _ => None,
    }
}

/// The "anything new?" answer — **stateless**: each declared topic rides
/// the caller's cursor, and the node answers by navigating the disk (the
/// type's `Changes` step through the registry match), so nothing is
/// buffered node-side and nothing can be missed or expire. Requires an
/// authenticated identity, matching the WebSocket subscribe; an unlisted
/// topic hash refuses the whole sync exactly like a command would.
async fn sync_poll<E, S>(
    registry: &E,
    store: &S,
    caller: Caller,
    payload: &[u8],
) -> Response
where
    E: Exposure,
    S: Store,
{
    use wavedb_core::wire::from_wire;
    use wavedb_net::sync::{SYNC_STRUCT_HASH, SyncRequest};

    if caller.is_anonymous() {
        return unauthorized(SYNC_STRUCT_HASH);
    }
    let request: SyncRequest = match from_wire(payload) {
        Ok(request) => request,
        Err(e) => {
            return Response::Err(NodeError::from_core(
                SYNC_STRUCT_HASH,
                &Error::from(e),
            ));
        }
    };
    navigate(registry, store, caller, &request.topics).await
}

/// The stateless "what changed?" navigation shared by the HTTP-poll exchange
/// ([`sync_poll`]) and the W7 command piggyback ([`piggyback`]): each declared
/// topic rides the caller's cursor and the node answers by navigating the disk
/// (the type's `Changes` step through the registry match), holding no session
/// state. The wire-`SyncReply` comes back inside `Reply::Returned`, or a
/// `Response::Err` if a topic's navigation refused.
async fn navigate<E, S>(
    registry: &E,
    store: &S,
    caller: Caller,
    topics: &[TopicCursor],
) -> Response
where
    E: Exposure,
    S: Store,
{
    use wavedb_core::expose::{Change, Command};
    use wavedb_core::wire::{from_wire, to_wire};
    use wavedb_net::sync::SyncReply;
    use wavedb_net::ws::{EventKind, RecordEvent};

    let mut reply = SyncReply {
        events: Vec::new(),
        cursors: Vec::new(),
    };
    for declared in topics {
        let topic = declared.topic;
        let changes = to_wire(&(topic.pivot, declared.since));
        let outcome = registry
            .execute(
                store,
                caller,
                topic.struct_hash,
                Command::Changes,
                &changes,
            )
            .await;
        let entries = match outcome {
            Ok(Reply::Values(entries)) => entries,
            Ok(_) => {
                return Response::Err(NodeError::from_core(
                    topic.struct_hash,
                    &Error::Backend(
                        "changes answered with a non-values reply".into(),
                    ),
                ));
            }
            Err(err) => {
                return Response::Err(NodeError::from_core(
                    topic.struct_hash,
                    &err,
                ));
            }
        };
        for entry in entries {
            match from_wire::<Change>(&entry) {
                Ok(Change::Cursor(cursor)) => {
                    reply.cursors.push((topic, cursor));
                }
                Ok(Change::Saved(id, meta, body)) => {
                    reply.events.push(RecordEvent {
                        topic,
                        id,
                        kind: EventKind::Saved,
                        meta: Some(meta),
                        body,
                    });
                }
                Ok(Change::Removed(id, at)) => {
                    reply.events.push(RecordEvent {
                        topic,
                        id,
                        kind: EventKind::Removed(at),
                        meta: None,
                        body: Vec::new(),
                    });
                }
                Err(e) => {
                    return Response::Err(NodeError::from_core(
                        topic.struct_hash,
                        &Error::from(e),
                    ));
                }
            }
        }
    }
    Response::Ok(Reply::Returned(to_wire(&reply)))
}

/// Gates 2–3 for an already-identified [`Caller`].
///
/// The shared tail of the HTTP [`handle`] and the WebSocket session's `Call`
/// (whose caller was bound once at `Hello`, so gate 1 does not repeat per
/// message). Never returns a transport error: a refusal or engine fault is
/// the [`Response::Err`] arm.
pub async fn execute<E, S>(
    registry: &E,
    store: &S,
    caller: Caller,
    frame: CommandFrame,
) -> Response
where
    E: Exposure,
    S: Store,
{
    // Gate 2 — header check. `execute` would also refuse an unlisted hash,
    // but the explicit gate short-circuits and keeps the refusal uniform.
    if !registry.knows(frame.struct_hash) {
        return Response::Err(NodeError::from_core(
            frame.struct_hash,
            &Error::UnknownStructHash(frame.struct_hash),
        ));
    }

    // Gate 3 — decode (inside the generated step) + engine dispatch.
    match registry
        .execute(
            store,
            caller,
            frame.struct_hash,
            frame.command,
            &frame.payload,
        )
        .await
    {
        Ok(reply) => Response::Ok(reply),
        Err(err) => {
            Response::Err(NodeError::from_core(frame.struct_hash, &err))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use wavedb_core::expose::{Caller, Command, Exposure, Reply};
    use wavedb_core::{Error, Id, Result, Store, U48, Write};
    use wavedb_net::auth::{AccessClaims, TokenPurpose, sign, unix_now};
    use wavedb_net::frame::{
        Auth, CommandFrame, NodeErrorKind, Request, Response,
    };

    /// Test shim: the HTTP [`handle`](super::handle) reduced to its command
    /// [`Response`], so the existing gate tests read unchanged. The W7
    /// piggyback delta is asserted through `super::handle` directly (see
    /// `a_command_piggybacks_the_sync_delta_for_declared_topics`).
    async fn handle<E: Exposure, S: Store>(
        registry: &E,
        store: &S,
        secret: &[u8; 32],
        request: Request,
    ) -> Response {
        super::handle(
            registry,
            store,
            &crate::shard::OwnerLocks::new(),
            secret,
            request,
        )
        .await
        .response
    }

    const SECRET: [u8; 32] = [9; 32];

    #[derive(Default)]
    struct MemStore(Mutex<BTreeMap<u128, Vec<u8>>>);

    impl Store for MemStore {
        async fn get(&self, id: Id) -> Result<Option<Vec<u8>>> {
            Ok(self.0.lock().unwrap().get(&id.raw()).cloned())
        }
        async fn apply(&self, batch: &[Write]) -> Result<()> {
            let mut m = self.0.lock().unwrap();
            for w in batch {
                if let Write::Expect(_, id, expected) = w
                    && m.get(&id.raw()) != expected.as_ref()
                {
                    return Err(wavedb_core::Error::Conflict(*id));
                }
            }
            for w in batch {
                match w {
                    Write::Put(id, b) => {
                        m.insert(id.raw(), b.clone());
                    }
                    Write::Remove(_, id) => {
                        m.remove(&id.raw());
                    }
                    Write::Expect(..) => {}
                }
            }
            drop(m);
            Ok(())
        }
    }

    /// A registry that knows exactly one hash and echoes a fixed reply;
    /// its `Changes` arm answers a canned navigation (what a generated
    /// `__wavedb_changes` step would), so the sync translation is testable
    /// without an engine.
    #[derive(Clone, Copy)]
    struct OneHash;

    impl Exposure for OneHash {
        fn knows(&self, struct_hash: u64) -> bool {
            struct_hash == 0x1234
        }
        fn decode_check(&self, struct_hash: u64, _: &[u8]) -> Result<()> {
            if struct_hash == 0x1234 {
                Ok(())
            } else {
                Err(Error::UnknownStructHash(struct_hash))
            }
        }
        async fn execute<S: Store>(
            &self,
            _: &S,
            caller: Caller,
            struct_hash: u64,
            command: Command,
            payload: &[u8],
        ) -> Result<Reply> {
            if struct_hash != 0x1234 {
                return Err(Error::UnknownStructHash(struct_hash));
            }
            if command == Command::Changes {
                use wavedb_core::expose::Change;
                use wavedb_core::wire::{from_wire, to_wire};
                let (_pivot, since): (
                    Option<wavedb_core::LocalId>,
                    Option<u64>,
                ) = from_wire(payload)?;
                // Registration answers the tail alone; a cursored call
                // answers one saved + one removed change past it.
                let entries = since.map_or_else(
                    || vec![to_wire(&Change::Cursor(40))],
                    |c| {
                        let id = Id::new(9, caller.tenant, false, 0);
                        vec![
                            to_wire(&Change::Cursor(c + 2)),
                            to_wire(&Change::Saved(
                                id,
                                wavedb_core::Metadata::default(),
                                vec![7],
                            )),
                            to_wire(&Change::Removed(id, c + 2)),
                        ]
                    },
                );
                return Ok(Reply::Values(entries));
            }
            // Echo the resolved user so tests can see gate 1's output.
            Ok(Reply::Value(Some(vec![
                u8::try_from(caller.user.get() & 0xFF).unwrap(),
            ])))
        }
    }

    fn request(auth: Auth, struct_hash: u64) -> Request {
        Request {
            auth,
            frame: CommandFrame {
                struct_hash,
                command: Command::Get,
                payload: Vec::new(),
            },
            sync: Vec::new(),
        }
    }

    fn token(user: u32, expires_at: u64, purpose: TokenPurpose) -> Vec<u8> {
        sign(
            &SECRET,
            &AccessClaims {
                user: U48::from(user),
                tenant: U48::from(user),
                expires_at,
                purpose,
                session: 0,
                nonce: 0,
            },
        )
    }

    fn anon() -> Auth {
        Auth::Anonymous {
            tenant: U48::from(1u32),
        }
    }

    /// A sync frame declaring one watched topic at `since`.
    fn sync_request(auth: Auth, since: Option<u64>) -> Request {
        use wavedb_net::sync::{SYNC_STRUCT_HASH, SyncRequest, TopicCursor};
        use wavedb_net::ws::Topic;
        Request {
            auth,
            frame: CommandFrame {
                struct_hash: SYNC_STRUCT_HASH,
                command: Command::Get,
                payload: wavedb_core::wire::to_wire(&SyncRequest {
                    topics: vec![TopicCursor {
                        topic: Topic {
                            struct_hash: 0x1234,
                            pivot: None,
                        },
                        since,
                    }],
                }),
            },
            sync: Vec::new(),
        }
    }

    #[test]
    fn sync_navigates_each_topic_from_its_cursor() {
        wavedb_platform::carrier::block_on(async {
            use wavedb_net::sync::SyncReply;
            use wavedb_net::ws::EventKind;

            let store = MemStore::default();
            let auth =
                Auth::Token(token(42, unix_now() + 60, TokenPurpose::Access));
            // A `None` cursor registers: the tail comes back, no events.
            let Response::Ok(Reply::Returned(bytes)) = handle(
                &OneHash,
                &store,
                &SECRET,
                sync_request(auth.clone(), None),
            )
            .await
            else {
                panic!("sync must answer with a return");
            };
            let reply: SyncReply =
                wavedb_core::wire::from_wire(&bytes).unwrap();
            assert!(reply.events.is_empty());
            assert_eq!(reply.cursors.first().map(|(_, c)| *c), Some(40));

            // A cursored sync navigates: the canned step answers one saved and
            // one removed change past it, and the cursor advances.
            let Response::Ok(Reply::Returned(bytes)) =
                handle(&OneHash, &store, &SECRET, sync_request(auth, Some(40)))
                    .await
            else {
                panic!("sync must answer with a return");
            };
            let reply: SyncReply =
                wavedb_core::wire::from_wire(&bytes).unwrap();
            assert_eq!(reply.events.len(), 2);
            assert_eq!(reply.events[0].kind, EventKind::Saved);
            assert!(
                reply.events[0].meta.is_some(),
                "navigated saves carry the node metadata for verbatim adoption"
            );
            assert_eq!(reply.events[1].kind, EventKind::Removed(42));
            assert_eq!(reply.cursors.first().map(|(_, c)| *c), Some(42));
        });
    }

    #[test]
    fn a_command_piggybacks_the_sync_delta_for_declared_topics() {
        wavedb_platform::carrier::block_on(async {
            use wavedb_net::sync::{SyncReply, TopicCursor};
            use wavedb_net::ws::{EventKind, Topic};

            let store = MemStore::default();
            let auth =
                Auth::Token(token(42, unix_now() + 60, TokenPurpose::Access));

            // A plain command with no declared topics: the reply alone, no delta.
            let plain = super::handle(
                &OneHash,
                &store,
                &crate::shard::OwnerLocks::new(),
                &SECRET,
                request(auth.clone(), 0x1234),
            )
            .await;
            assert_eq!(
                plain.response,
                Response::Ok(Reply::Value(Some(vec![42])))
            );
            assert!(plain.sync.is_none(), "no topics declared → no piggyback");

            // The same command now declares a watched topic at cursor 40: the
            // command still answers, and the navigation past 40 rides alongside.
            let mut req = request(auth, 0x1234);
            req.sync = vec![TopicCursor {
                topic: Topic {
                    struct_hash: 0x1234,
                    pivot: None,
                },
                since: Some(40),
            }];
            let answer = super::handle(
                &OneHash,
                &store,
                &crate::shard::OwnerLocks::new(),
                &SECRET,
                req,
            )
            .await;
            assert_eq!(
                answer.response,
                Response::Ok(Reply::Value(Some(vec![42]))),
                "the command's own reply is unchanged by the piggyback"
            );
            let delta: SyncReply = wavedb_core::wire::from_wire(
                &answer.sync.expect("a sync delta rides the reply"),
            )
            .unwrap();
            assert_eq!(
                delta.events.len(),
                2,
                "the canned saved + removed changes"
            );
            assert_eq!(delta.events[0].kind, EventKind::Saved);
            assert_eq!(delta.events[1].kind, EventKind::Removed(42));
            assert_eq!(delta.cursors.first().map(|(_, c)| *c), Some(42));
        });
    }

    #[test]
    fn anonymous_sync_is_unauthorized() {
        wavedb_platform::carrier::block_on(async {
            let store = MemStore::default();
            let Response::Err(e) =
                handle(&OneHash, &store, &SECRET, sync_request(anon(), None))
                    .await
            else {
                panic!("must refuse");
            };
            assert_eq!(e.kind, NodeErrorKind::Unauthorized);
        });
    }

    #[test]
    fn verified_token_reaches_the_engine_as_its_user() {
        wavedb_platform::carrier::block_on(async {
            let store = MemStore::default();
            let auth =
                Auth::Token(token(42, unix_now() + 60, TokenPurpose::Access));
            let resp =
                handle(&OneHash, &store, &SECRET, request(auth, 0x1234)).await;
            assert_eq!(resp, Response::Ok(Reply::Value(Some(vec![42]))));
        });
    }

    #[test]
    fn anonymous_reaches_the_engine_as_the_max_user() {
        wavedb_platform::carrier::block_on(async {
            // The tier itself passes gate 1; refusing non-public work is the
            // engine arms' guard (proven in the macro/e2e layers).
            let store = MemStore::default();
            let resp =
                handle(&OneHash, &store, &SECRET, request(anon(), 0x1234))
                    .await;
            assert_eq!(resp, Response::Ok(Reply::Value(Some(vec![0xFF]))));
        });
    }

    #[test]
    fn expired_token_is_unauthorized() {
        wavedb_platform::carrier::block_on(async {
            let store = MemStore::default();
            let auth =
                Auth::Token(token(42, unix_now() - 1, TokenPurpose::Access));
            let Response::Err(e) =
                handle(&OneHash, &store, &SECRET, request(auth, 0x1234)).await
            else {
                panic!("must refuse");
            };
            assert_eq!(e.kind, NodeErrorKind::Unauthorized);
        });
    }

    #[test]
    fn refresh_token_is_not_an_access_token() {
        wavedb_platform::carrier::block_on(async {
            let store = MemStore::default();
            let auth =
                Auth::Token(token(42, unix_now() + 60, TokenPurpose::Refresh));
            let Response::Err(e) =
                handle(&OneHash, &store, &SECRET, request(auth, 0x1234)).await
            else {
                panic!("must refuse");
            };
            assert_eq!(e.kind, NodeErrorKind::Unauthorized);
        });
    }

    #[test]
    fn foreign_secret_token_is_unauthorized() {
        wavedb_platform::carrier::block_on(async {
            let store = MemStore::default();
            let forged = sign(
                &[1; 32],
                &AccessClaims {
                    user: U48::from(42u32),
                    tenant: U48::from(42u32),
                    expires_at: unix_now() + 60,
                    purpose: TokenPurpose::Access,
                    session: 0,
                    nonce: 0,
                },
            );
            let Response::Err(e) = handle(
                &OneHash,
                &store,
                &SECRET,
                request(Auth::Token(forged), 0x1234),
            )
            .await
            else {
                panic!("must refuse");
            };
            assert_eq!(e.kind, NodeErrorKind::Unauthorized);
        });
    }

    #[test]
    fn unknown_hash_refused_at_the_header_gate() {
        wavedb_platform::carrier::block_on(async {
            let store = MemStore::default();
            let Response::Err(e) =
                handle(&OneHash, &store, &SECRET, request(anon(), 0x9999))
                    .await
            else {
                panic!("must refuse");
            };
            assert_eq!(e.kind, NodeErrorKind::UnknownStructHash);
            assert_eq!(e.struct_hash, 0x9999);
        });
    }
}
