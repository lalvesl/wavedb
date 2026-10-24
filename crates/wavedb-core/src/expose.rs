//! The exposure contract — what `expose_server!` / `expose_client!`
//! expansions implement, and the helpers the generated per-op execution
//! steps call.
//!
//! The registry is **declared, not discovered**: each side lists, in an
//! explicit module, exactly which items it serves or calls, and the macro
//! expands the list into a `match` on the 64-bit `STRUCT_HASH` per operation
//! — concrete, monomorphized arms. No `dyn`, no fn-pointer tables, no runtime
//! registration; an override substitutes a path inside its arm at expansion
//! time.
//!
//! **Every refusal is [`Error::UnknownStructHash`]** — an unlisted type, an
//! excluded (`never`) op, and a command a shape doesn't support are
//! deliberately indistinguishable from a type that never existed (the
//! security surface leaks nothing about what storage holds).

use crate::error::{Error, Result};
use crate::id::Id;
use crate::node_op::{NodeOp, NodeReply};
use crate::record;
use crate::route::Route;
use crate::store::Store;
use crate::traits::WaveDbStruct;
use crate::u48::U48;
use crate::wire::{WaveWire, from_wire, to_wire};

// The 15-bit salt guard is part of this module's surface — the `expose_*`
// macros emit `::wavedb_core::expose::SaltGuard` — but lives in its own file
// for the budget, mirroring the macro side's `expose_collision`.
pub use crate::expose_salt::{SaltGuard, salts_distinct, salts_self_distinct};

// The catch-up navigation (the `Changes` command's engine) lives in its own
// module; re-exported so the wire vocabulary reads from one place.
pub use crate::expose_changes::{Change, collection_changes, unique_changes};

// Likewise the declared-list reads (`Listed` / `ListLen`).
pub use crate::expose_lists::{list_len_value, listed_values};

// And collection membership (the `Get` / `Update` / `Remove` guard).
pub use crate::expose_member::{check_member, member_value};

// And the buffered walks (`All` / `History`).
pub use crate::expose_walks::{all_values, unique_history_values};

/// The wire command set: `Get`/`Save` for a `Unique` type,
/// `Insert`/`Update`/`Remove`/`Get` for a NonUnique one. A `#[server]`
/// function (M4) ignores it — its hash *is* the operation.
///
/// Every NonUnique command's payload **opens with the collection's Pivot**
/// (`Changes` with an `Option` of it): the Pivot owns the operation, so the
/// node places and serialises it by the Pivot before it knows the type
/// ([`crate::placement::Owner::of_command`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, WaveWire)]
pub enum Command {
    /// Unique: the anchor record (empty payload). NonUnique: the record at
    /// `id` in the collection (payload = `(LocalId pivot, Id)`); a record of
    /// another collection answers as absent.
    Get,
    /// Unique upsert (payload = the record body).
    Save,
    /// NonUnique insert (payload = `(PivotId's LocalId, body)`).
    Insert,
    /// NonUnique update at a stable `Id` (payload = `(LocalId pivot, Id,
    /// body)`); the record must belong to that collection.
    Update,
    /// NonUnique move to the dead tree (payload = `(LocalId pivot, Id)`);
    /// the record must belong to that collection.
    Remove,
    /// NonUnique collection walk, most recently written first (payload = the
    /// collection's `Pivot` `LocalId`). Buffered for now — streaming frames
    /// are a later transport refinement.
    All,
    /// Unique version-chain walk, newest-first (empty payload → the tenant's
    /// anchor). Buffered like `All`.
    History,
    /// Everything that changed since a cursor — the reconnect catch-up
    /// (payload = `(Option<LocalId> pivot, Option<u64> since)`; `None`
    /// since = register: answer the current tail, no events). Answered as
    /// `Values` of [`Change`] entries, `Cursor` first.
    Changes,
    /// One page of a NonUnique **declared list**, in its declared order
    /// (payload = `(LocalId pivot, u32 index, u64 offset, u32 limit)`).
    ///
    /// Unlike `All`, this one is **bounded by construction** — the caller's
    /// `limit` is the page it renders — which is why it needs none of the
    /// streaming-frame work `search_by` is waiting on. A reply holding
    /// exactly `limit` entries means "there may be more"; a shorter one is
    /// the end of the list.
    Listed,
    /// How many living records a declared list holds — the pager's "of M"
    /// (payload = `(LocalId pivot, u32 index)`). Answered as `Count`.
    ListLen,
}

/// What an executed command yields. Derives [`WaveWire`] so the transport
/// layer ships it verbatim — the node's reply envelope wraps this value.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub enum Reply {
    /// A `Get`'s result: the record's body wire bytes (`None` = absent).
    Value(Option<Vec<u8>>),
    /// An `Insert`'s minted record identity.
    Inserted(Id),
    /// A `Remove`'s outcome (`false` = was not in the living set).
    Removed(bool),
    /// A `Save`/`Update` completed.
    Done,
    /// An `All` walk's results: each record's body wire bytes, most
    /// recently written first.
    Values(Vec<Vec<u8>>),
    /// A `#[server]` function's wire-encoded return value.
    Returned(Vec<u8>),
    /// A `ListLen`'s answer: how many living records the list holds.
    Count(u64),
}

/// The verified identity a command executes as — gate 1's output.
///
/// `tenant` partitions the data; `user` is authorship (`Metadata.user`).
/// For a B2C session they are equal. The unauthenticated tier is
/// `user == U48::MAX`: only `#[server(public)]` functions accept it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Caller {
    /// The authenticated user (`U48::MAX` = anonymous).
    pub user: U48,
    /// The tenant the command executes under.
    pub tenant: U48,
}

impl Caller {
    /// A caller whose user *is* the tenant — the engine-local/B2C identity.
    #[must_use]
    pub const fn tenant_owned(tenant: U48) -> Self {
        Self {
            user: tenant,
            tenant,
        }
    }

    /// The unauthenticated tier under a claimed tenant.
    #[must_use]
    pub const fn anonymous(tenant: U48) -> Self {
        Self {
            user: U48::MAX,
            tenant,
        }
    }

    /// Is this the unauthenticated tier?
    #[must_use]
    pub const fn is_anonymous(&self) -> bool {
        self.user.get() == U48::MAX.get()
    }
}

/// How a registry addresses a hash it serves — what an operation on it
/// belongs to, which is where it executes ([`crate::placement::Owner`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Addressing {
    /// A `Unique` type: owned by the type under the caller's tenant.
    Unique,
    /// A NonUnique type: owned by the collection its commands name.
    Collection,
    /// A `#[server]` function: owned by nothing; it coordinates operations
    /// that each have an owner of their own.
    Function,
}

/// The declared registry surface.
///
/// Implemented by the zero-sized types `expose_server!` / `expose_client!`
/// emit, and consumed by the node builder (`.registry(REGISTRY)`) via a
/// plain generic bound: static dispatch end to end.
pub trait Exposure {
    /// Is `struct_hash` a declared item on this side? Unlisted ⇒ the wire
    /// cannot name it.
    fn knows(&self, struct_hash: u64) -> bool;

    /// How `struct_hash` is addressed, or `None` when this side does not
    /// serve it (the client registry serves nothing).
    fn addressing(&self, struct_hash: u64) -> Option<Addressing> {
        let _ = struct_hash;
        None
    }

    /// Decode-check `bytes` as the declared type's body — the wire gate that
    /// runs before any engine work.
    ///
    /// # Errors
    /// [`Error::UnknownStructHash`] for an unlisted hash; [`Error::Wire`] on
    /// an undecodable body.
    fn decode_check(&self, struct_hash: u64, bytes: &[u8]) -> Result<()>;

    /// Execute `command` for the struct `struct_hash` over `store` — the
    /// engine dispatch (server side only; the client default refuses). A
    /// function is not executed here but called: [`call_fn`](Self::call_fn).
    ///
    /// # Errors
    /// [`Error::UnknownStructHash`] for anything not declared (or excluded);
    /// otherwise whatever the executed op raises.
    async fn execute<S: Store>(
        &self,
        store: &S,
        caller: Caller,
        struct_hash: u64,
        command: Command,
        payload: &[u8],
    ) -> Result<Reply> {
        let _ = (store, caller, command, payload);
        Err(Error::UnknownStructHash(struct_hash))
    }

    /// Run a node operation on `struct_hash` — the trusted path a
    /// `#[server]` body's operations take (server side only). Serves every
    /// declared struct, `store` entries included, and runs no client gate.
    ///
    /// # Errors
    /// [`Error::UnknownStructHash`] for anything not declared; otherwise
    /// whatever the operation raises.
    async fn run_node<S: Store>(
        &self,
        store: &S,
        tenant: U48,
        struct_hash: u64,
        op: NodeOp,
    ) -> Result<NodeReply> {
        let _ = (store, tenant, op);
        Err(Error::UnknownStructHash(struct_hash))
    }

    /// Call the `#[server]` function `struct_hash`, its operations running
    /// through `route` (server side only).
    ///
    /// # Errors
    /// [`Error::UnknownStructHash`] for anything that is not a declared
    /// function; otherwise whatever the body returns.
    async fn call_fn<R: Route>(
        &self,
        route: &R,
        caller: Caller,
        struct_hash: u64,
        payload: &[u8],
    ) -> Result<Reply> {
        let _ = (route, caller, payload);
        Err(Error::UnknownStructHash(struct_hash))
    }
}

/// Decode-check `bytes` as a `T` body (the generated `decode_check` arms).
///
/// # Errors
/// [`Error::Wire`] when the bytes are not a valid `T`.
pub fn decode_check<T: WaveDbStruct>(bytes: &[u8]) -> Result<()> {
    from_wire::<T>(bytes)?;
    Ok(())
}

/// Fetch the record at `id` as its body wire bytes — the shared tail of both
/// shapes' `Get` steps.
///
/// # Errors
/// Propagates a [`Store`] failure or a decode fault.
pub async fn get_value<T, S>(store: &S, id: Id) -> Result<Reply>
where
    T: WaveDbStruct,
    S: Store,
{
    match store.get_of(T::STRUCT_HASH, id).await? {
        Some(bytes) => {
            let (_, value) =
                record::decode_record::<T>(T::STRUCT_HASH, &bytes)?;
            Ok(Reply::Value(Some(to_wire(&value))))
        }
        None => Ok(Reply::Value(None)),
    }
}

#[cfg(test)]
mod tests {
    use super::{Command, decode_check};
    use crate::wire::{from_wire, to_wire};

    #[test]
    fn command_roundtrips_on_the_wire() {
        for c in [
            Command::Get,
            Command::Save,
            Command::Insert,
            Command::Update,
            Command::Remove,
        ] {
            assert_eq!(from_wire::<Command>(&to_wire(&c)).unwrap(), c);
        }
    }

    #[test]
    fn reply_roundtrips_on_the_wire() {
        use super::Reply;
        use crate::id::Id;
        use crate::u48::U48;

        for r in [
            Reply::Value(None),
            Reply::Value(Some(vec![1, 2, 3])),
            Reply::Inserted(Id::new(7, U48::from(9u32), false, 3)),
            Reply::Removed(true),
            Reply::Done,
        ] {
            assert_eq!(from_wire::<Reply>(&to_wire(&r)).unwrap(), r);
        }
    }

    #[test]
    fn decode_check_gates_bodies() {
        // `u64` isn't a WaveDbStruct; use a unit fixture instead.
        use crate::traits::{Shape, WaveDbStruct};
        use crate::wire::WaveWire;

        #[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
        struct Probe {
            n: u64,
        }
        impl WaveDbStruct for Probe {
            const STRUCT_HASH: u64 = 0xBEEF;
            const SHAPE: Shape = Shape::Unique;
            type PivotId = ();
        }

        assert!(decode_check::<Probe>(&to_wire(&Probe { n: 4 })).is_ok());
        assert!(decode_check::<Probe>(&[1, 2]).is_err());
    }
}
