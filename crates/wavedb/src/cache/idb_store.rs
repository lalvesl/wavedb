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
//! - **`apply`** = one `readwrite` transaction: IndexedDB commits it whole
//!   or rolls it back whole, which is precisely the atomic-batch contract.

use js_sys::Uint8Array;
use web_sys::{
    IdbDatabase, IdbObjectStore, IdbTransaction, IdbTransactionMode,
};

use wavedb_core::{Id, Result, Store, Write};

use crate::cache::idb::{self, STORE_NAME, backend};

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
        let tx = self
            .db
            .transaction_with_str_and_mode(STORE_NAME, mode)
            .map_err(|e| backend("transaction", &e))?;
        let store = tx
            .object_store(STORE_NAME)
            .map_err(|e| backend("object store", &e))?;
        Ok((tx, store))
    }

    /// Queue every write of `batch` into `store`; a synchronous refusal
    /// reports which call threw.
    fn queue(store: &IdbObjectStore, batch: &[Write]) -> Result<()> {
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

    async fn apply(&self, batch: &[Write]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let (tx, store) = self.transaction(IdbTransactionMode::Readwrite)?;
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
        if let Err(refused) = Self::queue(&store, batch) {
            // Never leave already-queued writes to auto-commit: the batch
            // is all-or-nothing.
            let _ = tx.abort();
            return Err(refused);
        }
        idb::committed(&tx).await.map_err(|e| backend("apply", &e))
    }
}
