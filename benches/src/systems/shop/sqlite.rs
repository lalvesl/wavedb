//! The SQLite shop row's **setup**: the schema and the preload.
//!
//! What is left after [RFC 0065] took the measuring away. The preload is
//! bulk, untimed and single-threaded — one transaction at `synchronous = OFF`
//! — and the row (`row::shop`) runs it before the harness starts. Everything
//! that used to time a phase here now lives in
//! [`drivers::shop::sqlite`](crate::systems::drivers::shop::sqlite).
//!
//! [RFC 0065]: ../../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use rusqlite::{Connection, params};

use super::ShopCfg;
use crate::shop::{
    product_count, product_row, shopping_count, shopping_row, user_row,
};

pub const DDL: &str = "
CREATE TABLE users (
  id      INTEGER PRIMARY KEY,
  name    TEXT NOT NULL,
  address TEXT NOT NULL,
  city    TEXT NOT NULL,
  email   TEXT NOT NULL
);
CREATE TABLE shopping (
  id              INTEGER PRIMARY KEY,
  user_id         INTEGER NOT NULL,
  bought_at       INTEGER NOT NULL,
  discount_cents  INTEGER NOT NULL,
  transport_cents INTEGER NOT NULL
);
CREATE INDEX idx_shopping_user ON shopping(user_id, bought_at);
CREATE TABLE product (
  id          INTEGER PRIMARY KEY,
  shopping_id INTEGER NOT NULL,
  name        TEXT NOT NULL,
  quantity    INTEGER NOT NULL,
  unit_cents  INTEGER NOT NULL
);
CREATE INDEX idx_product_shopping ON product(shopping_id, name);
";

pub fn preload(cfg: &ShopCfg, conn: &mut Connection) -> Result<(), String> {
    let tx = conn.transaction().map_err(sql)?;
    let mut order_id = 0u64;
    let mut item_id = 0u64;
    for u in 0..cfg.users {
        insert_user(&tx, u, cfg)?;
        for s in 0..shopping_count(u, cfg.seed, cfg.orders_max) {
            order_id += 1;
            insert_order(&tx, order_id, u, s, cfg)?;
            for p in 0..product_count(u, s, cfg.seed, cfg.items_max) {
                item_id += 1;
                insert_item(&tx, item_id, order_id, u, s, p, cfg)?;
            }
        }
    }
    tx.commit().map_err(sql)
}

fn insert_user(conn: &Connection, u: u64, cfg: &ShopCfg) -> Result<(), String> {
    let r = user_row(u, cfg.seed);
    conn.execute(
        "INSERT INTO users (id, name, address, city, email) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![u as i64, r.name, r.address, r.city, r.email],
    )
    .map(|_| ())
    .map_err(sql)
}

fn insert_order(
    conn: &Connection,
    id: u64,
    u: u64,
    s: u64,
    cfg: &ShopCfg,
) -> Result<(), String> {
    let r = shopping_row(u, s, cfg.seed);
    conn.execute(
        "INSERT INTO shopping (id, user_id, bought_at, discount_cents, \
         transport_cents) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            id as i64,
            u as i64,
            r.bought_at as i64,
            r.discount_cents as i64,
            r.transport_cents as i64
        ],
    )
    .map(|_| ())
    .map_err(sql)
}

fn insert_item(
    conn: &Connection,
    id: u64,
    order: u64,
    u: u64,
    s: u64,
    p: u64,
    cfg: &ShopCfg,
) -> Result<(), String> {
    let r = product_row(u, s, p, cfg.seed);
    conn.execute(
        "INSERT INTO product (id, shopping_id, name, quantity, unit_cents) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            id as i64,
            order as i64,
            r.name,
            r.quantity,
            r.unit_cents as i64
        ],
    )
    .map(|_| ())
    .map_err(sql)
}

fn sql(e: rusqlite::Error) -> String {
    format!("sqlite: {e}")
}
