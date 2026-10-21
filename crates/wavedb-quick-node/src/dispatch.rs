//! The node-side gates: turn one identified command into a [`Response`].
//!
//! The gates run **before** the engine, in the order
//! [`wavedb-quick-node` README] documents. Gate 1 runs at the edge, on the
//! request's own thread ([`crate::edge`]); gates 2–3 run on the owner's shard,
//! under the owner's lock ([`call`]):
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
//!    command (a collection command opens with its Pivot) and drives the
//!    storage engine.
//!
//! Gates 5–6 (`validate` / `preprocess`) are still a later seam; gate 4's
//! record-level `Metadata.permission` grants ride with the cross-tenant
//! read path (deferred) — tenant isolation is enforced by the token
//! binding itself.
//!
//! [`wavedb-quick-node` README]: https://docs.rs/wavedb-quick-node

use wavedb_core::expose::{Caller, Exposure};
use wavedb_core::{Error, Store};
use wavedb_net::auth::{self, TokenPurpose};
use wavedb_net::frame::{
    Auth, CommandFrame, NodeError, NodeErrorKind, Response,
};

/// The uniform identity refusal — which check failed stays server-side.
pub(crate) fn unauthorized(struct_hash: u64) -> Response {
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

/// The owner of `frame` reached under `tenant`: what it is placed, locked and
/// stored by.
pub fn owner_of<E: Exposure>(
    registry: &E,
    tenant: wavedb_core::U48,
    frame: &CommandFrame,
) -> crate::shard::Owner {
    crate::shard::Owner::of_command(
        registry.addressing(frame.struct_hash),
        tenant,
        frame.struct_hash,
        frame.command,
        &frame.payload,
    )
}

/// Gates 2–3 for an identified [`Caller`], under its owner's lock.
///
/// Shared by a POST ([`crate::edge::handle`], after gate 1) and a WebSocket `Call` (caller
/// bound at `Hello`): one lock on one key, whichever way a write arrives.
/// The key is the command's `owner` ([`owner_of`]), which the worker derives
/// itself, as the router placed it — and picks `store` by.
pub async fn call<E, S>(
    registry: &E,
    store: &S,
    locks: &crate::shard::OwnerLocks,
    caller: Caller,
    owner: crate::shard::Owner,
    frame: CommandFrame,
) -> Response
where
    E: Exposure,
    S: Store,
{
    let _held = locks
        .acquire(crate::shard::OwnerKey::new(caller.tenant, owner))
        .await;
    execute(registry, store, caller, frame).await
}

/// Gates 2–3 — [`call`]'s body, once the owner is held. Never returns a
/// transport error: a refusal or engine fault is the [`Response::Err`] arm.
async fn execute<E, S>(
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

    /// The edge's whole request path over one store in hand.
    async fn answer<E: Exposure, S: Store>(
        registry: &E,
        store: &S,
        secret: &[u8; 32],
        request: Request,
    ) -> crate::dispatch::Answer {
        let exec = crate::edge::Direct {
            registry,
            store,
            locks: crate::shard::OwnerLocks::new(),
        };
        crate::edge::handle(registry, &exec, secret, request).await
    }

    /// Test shim: [`answer`] reduced to its command [`Response`], so the gate
    /// tests read unchanged. The W7 piggyback delta is asserted through
    /// [`answer`] (see `a_command_piggybacks_the_sync_delta_for_declared_topics`).
    async fn handle<E: Exposure, S: Store>(
        registry: &E,
        store: &S,
        secret: &[u8; 32],
        request: Request,
    ) -> Response {
        answer(registry, store, secret, request).await.response
    }

    const SECRET: [u8; 32] = [9; 32];

    #[derive(Default)]
    struct MemStore(Mutex<BTreeMap<u128, Vec<u8>>>);

    // A test double, written in the trait's own (async) shape.
    #[allow(clippy::unused_async_trait_impl)]
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

    // A test double, written in the trait's own (async) shape.
    #[allow(clippy::unused_async_trait_impl)]
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
            assert!(reply.events.is_empty(), "{:?}", reply.events);
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
            let plain = answer(
                &OneHash,
                &store,
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
            let answer = answer(&OneHash, &store, &SECRET, req).await;
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
