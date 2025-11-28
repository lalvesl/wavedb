//! [`IdbStore`] — the browser [`Store`]: IndexedDB as a flat
//! `Id → wire bytes` map.
//!
//! **No pages, no journal.** The native engine owns a physical disk layout,
//! so it needs both; IndexedDB is already a durable ordered key→value store
//! doing its own block management and crash safety — stacking WaveDB pages
//! on top would run two block managers and buy nothing. The `Store` trait
//! absorbs the difference: `Chain` / `Collection` / `LocalHandle` run
//! unchanged over this backend (serverless mode falls out).
//!
//! - **Key** = the 128-bit [`Id`] as 16 big-endian bytes — IndexedDB orders
//!   binary keys bytewise, so key order matches numeric `Id` order.
//! - **Value** = the record's wire bytes, exactly as the native engine
//!   stores them (`[STRUCT_HASH][…]`, decode-verified above this layer).
//! - **Ordered lanes** = a second object store keyed `[lane][key]`
//!   ([`idb_lanes`](super::idb_lanes)).
//! - **`apply`** = one `readwrite` transaction over both: IndexedDB commits
//!   it whole or rolls it back whole, which is precisely the atomic-batch
//!   contract.

use js_sys::Uint8Array;
use web_sys::{
    IdbDatabase, IdbObjectStore, IdbTransaction, IdbTransactionMode,
};

use wavedb_core::store::{Entry, Span};
use wavedb_core::{Id, Result, Store, Write};

use crate::cache::idb::{self, LANES_NAME, STORE_NAME, backend};
use crate::cache::idb_lanes::{self, lane_key};

/// The browser store — one IndexedDB database holding one `kv` object
/// store. Cheap handle; transactions are per call.
#[derive(Debug)]
pub struct IdbStore {
    db: IdbDatabase,
}

impl IdbStore {
    /// Open (creating on first use) the database `name`.
    ///
    /// Unlike the native engine there is no one-store-per-process rule:
    /// IndexedDB serialises conflicting transactions itself.
    ///
    /// # Errors
    /// [`Backend`](wavedb_core::Error::Backend) when IndexedDB is
    /// unavailable (no `window`) or refuses to open.
    pub async fn open(name: &str) -> Result<Self> {
        Ok(Self {
            db: idb::open_database(name).await?,
        })
    }

    /// An `Id`'s IndexedDB key: 16 big-endian bytes (bytewise key order ==
    /// numeric `Id` order).
    fn key(id: Id) -> Uint8Array {
        Uint8Array::from(id.raw().to_be_bytes().as_slice())
    }

    /// One transaction over the `kv` store in `mode`.
    fn transaction(
        &self,
        mode: IdbTransactionMode,
    ) -> Result<(IdbTransaction, IdbObjectStore)> {
        self.transaction_over(STORE_NAME, mode)
    }

    /// One transaction over the object store `name` in `mode`.
    fn transaction_over(
        &self,
        name: &str,
        mode: IdbTransactionMode,
    ) -> Result<(IdbTransaction, IdbObjectStore)> {
        let tx = self
            .db
            .transaction_with_str_and_mode(name, mode)
            .map_err(|e| backend("transaction", &e))?;
        let store = tx
            .object_store(name)
            .map_err(|e| backend("object store", &e))?;
        Ok((tx, store))
    }

    /// One `readwrite` transaction over both object stores.
    fn writing(
        &self,
    ) -> Result<(IdbTransaction, IdbObjectStore, IdbObjectStore)> {
        let names = js_sys::Array::of2(&STORE_NAME.into(), &LANES_NAME.into());
        let tx = self
            .db
            .transaction_with_str_sequence_and_mode(
                &names,
                IdbTransactionMode::Readwrite,
            )
            .map_err(|e| backend("transaction", &e))?;
        let store = |name| {
            tx.object_store(name)
                .map_err(|e| backend("object store", &e))
        };
        let (kv, lanes) = (store(STORE_NAME)?, store(LANES_NAME)?);
        Ok((tx, kv, lanes))
    }

    /// Queue every write of `batch` into `store`, or `lanes` for a lane's
    /// entry; a synchronous refusal reports which call threw.
    fn queue(
        store: &IdbObjectStore,
        lanes: &IdbObjectStore,
        batch: &[Write],
    ) -> Result<()> {
        for write in batch {
            match write {
                Write::Put(id, bytes) => {
                    store
                        .put_with_key(
                            &Uint8Array::from(bytes.as_slice()).into(),
                            &Self::key(*id).into(),
                        )
                        .map_err(|e| backend("put", &e))?;
                }
                Write::Remove(_, id) => {
                    store
                        .delete(&Self::key(*id).into())
                        .map_err(|e| backend("delete", &e))?;
                }
                Write::Set(lane, key, value) => {
                    lanes
                        .put_with_key(
                            &Uint8Array::from(value.as_slice()).into(),
                            &lane_key(*lane, key).into(),
                        )
                        .map_err(|e| backend("put", &e))?;
                }
                Write::Unset(lane, key) => {
                    lanes
                        .delete(&lane_key(*lane, key).into())
                        .map_err(|e| backend("delete", &e))?;
                }
                // Validated in `apply` before anything is queued.
                Write::Expect(..) => {}
            }
        }
        Ok(())
    }
}

impl Store for IdbStore {
    async fn get(&self, id: Id) -> Result<Option<Vec<u8>>> {
        let (_tx, store) = self.transaction(IdbTransactionMode::Readonly)?;
        let request = store
            .get(&Self::key(id).into())
            .map_err(|e| backend("get", &e))?;
        let value = idb::settled(&request)
            .await
            .map_err(|e| backend("get", &e))?;
        if value.is_undefined() || value.is_null() {
            return Ok(None);
        }
        Ok(Some(Uint8Array::new(&value).to_vec()))
    }

    // `get_of` keeps its default (falls back to `get`): the keyspace is one
    // flat map, there is no per-type slot to route to.

    async fn scan(&self, lane: u64, span: &Span) -> Result<Vec<Entry>> {
        let (_tx, lanes) =
            self.transaction_over(LANES_NAME, IdbTransactionMode::Readonly)?;
        idb_lanes::scan(&lanes, lane, span).await
    }

    async fn count(
        &self,
        lane: u64,
        lo: &[u8],
        hi: Option<&[u8]>,
    ) -> Result<u64> {
        let (_tx, lanes) =
            self.transaction_over(LANES_NAME, IdbTransactionMode::Readonly)?;
        idb_lanes::count(&lanes, lane, lo, hi).await
    }

    async fn apply(&self, batch: &[Write]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let (tx, store, lanes) = self.writing()?;
        // Guards validate inside the same readwrite transaction that will
        // apply the writes — the transaction holds the store, so
        // check-then-write is atomic.
        for w in batch {
            if let Write::Expect(_, id, expected) = w {
                let request = store
                    .get(&Self::key(*id).into())
                    .map_err(|e| backend("get", &e))?;
                let value = idb::settled(&request)
                    .await
                    .map_err(|e| backend("get", &e))?;
                let current = if value.is_undefined() || value.is_null() {
                    None
                } else {
                    Some(Uint8Array::new(&value).to_vec())
                };
                if current != *expected {
                    let _ = tx.abort();
                    return Err(wavedb_core::Error::Conflict(*id));
                }
            }
        }
        if let Err(refused) = Self::queue(&store, &lanes, batch) {
            // Never leave already-queued writes to auto-commit: the batch
            // is all-or-nothing.
            let _ = tx.abort();
            return Err(refused);
        }
        idb::committed(&tx).await.map_err(|e| backend("apply", &e))
    }
}
