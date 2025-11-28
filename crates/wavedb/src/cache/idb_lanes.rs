//! The ordered lanes of the browser store ([RFC 0078] §7): one `lanes`
//! object store keyed `[lane hash BE][key]`, so a lane is a key range of it
//! and a span one cursor over that range — IndexedDB orders binary keys
//! bytewise, which is the lanes' order.
//!
//! [RFC 0078]: https://github.com/wavedb/wavedb/blob/main/rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use js_sys::Uint8Array;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{
    IdbCursorDirection, IdbCursorWithValue, IdbKeyRange, IdbObjectStore,
};

use wavedb_core::Result;
use wavedb_core::store::{Entry, Span, successor};

use crate::cache::idb::{self, backend};

/// The IndexedDB key of `key` in `lane`.
pub fn lane_key(lane: u64, key: &[u8]) -> Uint8Array {
    let mut out = lane.to_be_bytes().to_vec();
    out.extend_from_slice(key);
    Uint8Array::from(out.as_slice())
}

/// The key range `[lo, hi)` of `lane` — `hi` `None` to the lane's end.
fn range(lane: u64, lo: &[u8], hi: Option<&[u8]>) -> Result<IdbKeyRange> {
    let lower = lane_key(lane, lo);
    let upper = hi.map_or_else(
        || {
            successor(&lane.to_be_bytes())
                .map(|next| Uint8Array::from(next.as_slice()))
        },
        |hi| Some(lane_key(lane, hi)),
    );
    upper
        .map_or_else(
            || IdbKeyRange::lower_bound(&lower),
            |upper| {
                IdbKeyRange::bound_with_lower_open_and_upper_open(
                    &lower, &upper, false, true,
                )
            },
        )
        .map_err(|e| backend("key range", &e))
}

/// `span`'s entries of `lane` in `store`, by one cursor.
pub async fn scan(
    store: &IdbObjectStore,
    lane: u64,
    span: &Span,
) -> Result<Vec<Entry>> {
    let mut out = Vec::new();
    if span.limit == 0
        || span
            .hi
            .as_deref()
            .is_some_and(|hi| hi <= span.lo.as_slice())
    {
        return Ok(out);
    }
    let direction = if span.rev {
        IdbCursorDirection::Prev
    } else {
        IdbCursorDirection::Next
    };
    let range = range(lane, &span.lo, span.hi.as_deref())?;
    let request = store
        .open_cursor_with_range_and_direction(&range, direction)
        .map_err(|e| backend("cursor", &e))?;
    let mut skip = span.skip;
    loop {
        let at = idb::settled(&request)
            .await
            .map_err(|e| backend("cursor", &e))?;
        if at.is_null() || at.is_undefined() {
            return Ok(out);
        }
        let cursor: IdbCursorWithValue =
            at.dyn_into().map_err(|v| backend("cursor", &v))?;
        if skip > 0 {
            // `advance` takes a `u32`: a larger skip is taken in steps.
            let step = u32::try_from(skip).unwrap_or(u32::MAX);
            skip -= u64::from(step);
            cursor.advance(step).map_err(|e| backend("advance", &e))?;
            continue;
        }
        let key = cursor.key().map_err(|e| backend("cursor key", &e))?;
        let value = cursor.value().map_err(|e| backend("cursor value", &e))?;
        let key = Uint8Array::new(&key).to_vec();
        out.push((key.get(8..).unwrap_or_default().to_vec(), bytes_of(&value)));
        if out.len() >= span.limit {
            return Ok(out);
        }
        cursor.continue_().map_err(|e| backend("continue", &e))?;
    }
}

/// How many entries `lane` holds in `[lo, hi)`.
pub async fn count(
    store: &IdbObjectStore,
    lane: u64,
    lo: &[u8],
    hi: Option<&[u8]>,
) -> Result<u64> {
    if hi.is_some_and(|hi| hi <= lo) {
        return Ok(0);
    }
    let request = store
        .count_with_key(&range(lane, lo, hi)?.into())
        .map_err(|e| backend("count", &e))?;
    let n = idb::settled(&request)
        .await
        .map_err(|e| backend("count", &e))?;
    // A count is a JS number, exact for any count a browser can hold.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Ok(n.as_f64().unwrap_or(0.0) as u64)
}

/// A stored value's bytes.
fn bytes_of(value: &JsValue) -> Vec<u8> {
    Uint8Array::new(value).to_vec()
}
