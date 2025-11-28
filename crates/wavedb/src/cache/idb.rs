//! IndexedDB plumbing: open a database and await its event-driven
//! requests as futures.
//!
//! IndexedDB speaks in DOM events (`onsuccess` / `onerror` on a request,
//! `oncomplete` / `onerror` / `onabort` on a transaction). Each helper
//! wires those handlers to a oneshot channel held in Rust locals — the
//! closures drop when the await returns, so nothing leaks per operation.
//!
//! Faults convert to `wavedb_core::Error::Backend` right here — the same
//! seam the native engine uses (`StorageError` → `Backend`); everything
//! above sees one typed error language.

use std::cell::RefCell;
use std::rc::Rc;

use futures::channel::oneshot;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{IdbDatabase, IdbRequest, IdbTransaction};

use wavedb_core::Error;

/// The object store of records: `Id` (16 B big-endian) → wire bytes.
pub const STORE_NAME: &str = "kv";

/// The object store of every ordered lane's entries ([RFC 0078] §7):
/// `[lane hash BE][key]` → value, so a lane is a key range of it.
///
/// [RFC 0078]: https://github.com/wavedb/wavedb/blob/main/rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md
pub const LANES_NAME: &str = "lanes";

/// Schema version — the two object stores, created at first open. Neither
/// shape is a layout, so nothing migrates: a database of version 1 gains
/// the lanes' store, and an older layout is unsupported pre-release.
const DB_VERSION: u32 = 2;

/// Wrap a thrown `JsValue` as the backend fault it is.
pub fn backend(context: &'static str, value: &JsValue) -> Error {
    Error::Backend(format!("indexeddb {context}: {value:?}"))
}

/// One event's outcome, waiting to be sent to the awaiting future exactly
/// once (whichever wired handler fires first takes the sender).
type SignalSlot = Rc<RefCell<Option<oneshot::Sender<Result<(), JsValue>>>>>;

/// A `()`-shaped event handler sending `msg` through the shared slot once.
/// JS ignores the missing event argument, so `FnMut()` handles any event.
fn notify(
    slot: &SignalSlot,
    msg: Result<(), &'static str>,
) -> Closure<dyn FnMut()> {
    let slot = Rc::clone(slot);
    Closure::new(move || {
        if let Some(sender) = slot.borrow_mut().take() {
            let _ = sender.send(msg.map_err(JsValue::from_str));
        }
    })
}

/// Await one request's `success`, returning its `result` value.
pub async fn settled(req: &IdbRequest) -> Result<JsValue, JsValue> {
    let (sender, receiver) = oneshot::channel();
    let slot = Rc::new(RefCell::new(Some(sender)));
    let on_success = notify(&slot, Ok(()));
    let on_error = notify(&slot, Err("request failed"));
    req.set_onsuccess(Some(on_success.as_ref().unchecked_ref()));
    req.set_onerror(Some(on_error.as_ref().unchecked_ref()));
    let outcome = receiver
        .await
        .map_err(|_| JsValue::from_str("request callback dropped"));
    req.set_onsuccess(None);
    req.set_onerror(None);
    match outcome? {
        Ok(()) => Ok(req.result().unwrap_or(JsValue::UNDEFINED)),
        // Prefer the request's own DOMException over the generic message.
        Err(generic) => {
            Err(req.error().ok().flatten().map_or(generic, JsValue::from))
        }
    }
}

/// Await a transaction's terminal event: `complete` = the whole batch is
/// durable; `error`/`abort` = none of it happened (IndexedDB rolls the
/// transaction back whole — that is the atomicity `Store::apply` needs).
pub async fn committed(tx: &IdbTransaction) -> Result<(), JsValue> {
    let (sender, receiver) = oneshot::channel();
    let slot = Rc::new(RefCell::new(Some(sender)));
    let on_complete = notify(&slot, Ok(()));
    let on_error = notify(&slot, Err("transaction failed"));
    let on_abort = notify(&slot, Err("transaction aborted"));
    tx.set_oncomplete(Some(on_complete.as_ref().unchecked_ref()));
    tx.set_onerror(Some(on_error.as_ref().unchecked_ref()));
    tx.set_onabort(Some(on_abort.as_ref().unchecked_ref()));
    let outcome = receiver
        .await
        .map_err(|_| JsValue::from_str("transaction callback dropped"));
    tx.set_oncomplete(None);
    tx.set_onerror(None);
    tx.set_onabort(None);
    outcome?
}

/// Open (creating on first use) the named database with its `kv` and
/// `lanes` object stores.
pub async fn open_database(name: &str) -> wavedb_core::Result<IdbDatabase> {
    let factory = web_sys::window()
        .ok_or_else(|| Error::Backend(String::from("indexeddb: no window")))?
        .indexed_db()
        .map_err(|e| backend("factory", &e))?
        .ok_or_else(|| Error::Backend(String::from("indexeddb unavailable")))?;
    let open = factory
        .open_with_u32(name, DB_VERSION)
        .map_err(|e| backend("open", &e))?;
    // An open below this version: create the stores it lacks (creating one
    // that exists throws, and is let be). A failure in here can't
    // propagate out of the DOM callback; it surfaces as a missing store on
    // the first transaction instead.
    let request = open.clone();
    let on_upgrade = Closure::<dyn FnMut()>::new(move || {
        if let Some(db) = request
            .result()
            .ok()
            .and_then(|db| db.dyn_into::<IdbDatabase>().ok())
        {
            let _ = db.create_object_store(STORE_NAME);
            let _ = db.create_object_store(LANES_NAME);
        }
    });
    open.set_onupgradeneeded(Some(on_upgrade.as_ref().unchecked_ref()));
    let result = settled(&open).await.map_err(|e| backend("open", &e));
    open.set_onupgradeneeded(None);
    result?
        .dyn_into::<IdbDatabase>()
        .map_err(|v| backend("open result", &v))
}
