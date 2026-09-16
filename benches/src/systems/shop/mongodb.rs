//! The MongoDB shop row's **setup**: what is left after [RFC 0065] took the
//! measuring away.
//!
//! The preload is bulk, untimed and single-threaded, and the row
//! (`row::shop`) runs it before the harness starts. Everything that used
//! to time a phase here now lives in `drivers::shop`.
//!
//! The one-node replica set the checkout transaction needs is started by
//! the row, not here: a preload does not need a transaction.
//!
//! [RFC 0065]: ../../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use mongodb::bson::{Document, doc};
use mongodb::sync::{Collection, Database};

use super::ShopCfg;
use crate::shop::{product_row, shopping_row, user_row};

/// Fill the three collections. Batched, but flushed every `CHUNK`: the
/// preload is never timed, and one unbounded `insert_many` of a large tier
/// would be the memory experiment rather than the fill.
///
/// # Errors
/// The server refused an insert.
pub fn preload(cfg: &ShopCfg, db: &Database) -> Result<(), String> {
    const CHUNK: usize = 1000;
    let mut order_id = 0u64;
    let mut item_id = 0u64;
    let mut users = Vec::with_capacity(CHUNK);
    let mut os = Vec::with_capacity(CHUNK);
    let mut is = Vec::with_capacity(CHUNK);
    for u in 0..cfg.users {
        users.push(user_doc(u, cfg));
        for s in 0..crate::shop::shopping_count(u, cfg.seed, cfg.orders_max) {
            order_id += 1;
            os.push(order_doc(order_id, u, s, cfg));
            for p in
                0..crate::shop::product_count(u, s, cfg.seed, cfg.items_max)
            {
                item_id += 1;
                is.push(item_doc(item_id, order_id, u, s, p, cfg));
            }
        }
        flush(profiles(db), &mut users, CHUNK)?;
        flush(orders(db), &mut os, CHUNK)?;
        flush(items(db), &mut is, CHUNK)?;
    }
    flush(profiles(db), &mut users, 0)?;
    flush(orders(db), &mut os, 0)?;
    flush(items(db), &mut is, 0)
}

/// Send `batch` once it has reached `at`, and clear it. `at = 0` means "send
/// whatever is left".
fn flush(
    col: Collection<Document>,
    batch: &mut Vec<Document>,
    at: usize,
) -> Result<(), String> {
    if batch.is_empty() || batch.len() < at {
        return Ok(());
    }
    col.insert_many(&*batch)
        .run()
        .map_err(|e| format!("mongodb: preload: {e}"))?;
    batch.clear();
    Ok(())
}

fn user_doc(u: u64, cfg: &ShopCfg) -> Document {
    let r = user_row(u, cfg.seed);
    doc! {
        "_id": u as i64, "name": r.name, "address": r.address,
        "city": r.city, "email": r.email,
    }
}

pub fn order_doc(id: u64, u: u64, s: u64, cfg: &ShopCfg) -> Document {
    let r = shopping_row(u, s, cfg.seed);
    doc! {
        "_id": id as i64, "user_id": u as i64,
        "bought_at": r.bought_at as i64,
        "discount_cents": r.discount_cents as i64,
        "transport_cents": r.transport_cents as i64,
    }
}

pub fn item_doc(
    id: u64,
    order: u64,
    u: u64,
    s: u64,
    p: u64,
    cfg: &ShopCfg,
) -> Document {
    let r = product_row(u, s, p, cfg.seed);
    doc! {
        "_id": id as i64, "shopping_id": order as i64, "name": r.name,
        "quantity": i64::from(r.quantity), "unit_cents": r.unit_cents as i64,
    }
}

pub fn profiles(db: &Database) -> Collection<Document> {
    db.collection("users")
}

pub fn orders(db: &Database) -> Collection<Document> {
    db.collection("shopping")
}

pub fn items(db: &Database) -> Collection<Document> {
    db.collection("product")
}
