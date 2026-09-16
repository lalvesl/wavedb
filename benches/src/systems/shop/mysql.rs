//! The MySQL shop row's **setup**: what is left after [RFC 0065] took the
//! measuring away.
//!
//! The preload is bulk, untimed and single-threaded, and the row
//! (`row::shop`) runs it before the harness starts. Everything that used
//! to time a phase here now lives in `drivers::shop`.
//!
//! [RFC 0065]: ../../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use mysql::prelude::Queryable;
use mysql::{Conn, TxOpts, params};

use super::ShopCfg;
use crate::shop::{
    product_count, product_row, shopping_count, shopping_row, user_row,
};

const INS_USER: &str = "INSERT INTO users (id, name, address, city, email) \
                        VALUES (:id, :name, :address, :city, :email)";
const INS_ORDER: &str = "INSERT INTO shopping (id, user_id, bought_at, \
                         discount_cents, transport_cents) \
                         VALUES (:id, :user_id, :bought_at, :discount, :transport)";
const INS_ITEM: &str = "INSERT INTO product (id, shopping_id, name, quantity, \
                        unit_cents) VALUES (:id, :shopping_id, :name, :qty, :unit)";

pub fn preload(cfg: &ShopCfg, conn: &mut Conn) -> Result<(), String> {
    let mut tx = conn.start_transaction(TxOpts::default()).map_err(sql)?;
    let mut order_id = 0u64;
    let mut item_id = 0u64;
    for u in 0..cfg.users {
        insert_user(&mut tx, u, cfg)?;
        for s in 0..shopping_count(u, cfg.seed, cfg.orders_max) {
            order_id += 1;
            insert_order(&mut tx, order_id, u, s, cfg)?;
            for p in 0..product_count(u, s, cfg.seed, cfg.items_max) {
                item_id += 1;
                insert_item(&mut tx, item_id, order_id, u, s, p, cfg)?;
            }
        }
    }
    tx.commit().map_err(sql)
}

pub fn insert_user(
    q: &mut impl Queryable,
    u: u64,
    cfg: &ShopCfg,
) -> Result<(), String> {
    let r = user_row(u, cfg.seed);
    q.exec_drop(
        INS_USER,
        params! {
            "id" => u, "name" => &r.name, "address" => &r.address,
            "city" => &r.city, "email" => &r.email,
        },
    )
    .map_err(sql)
}

pub fn insert_order(
    q: &mut impl Queryable,
    id: u64,
    u: u64,
    s: u64,
    cfg: &ShopCfg,
) -> Result<(), String> {
    let r = shopping_row(u, s, cfg.seed);
    q.exec_drop(
        INS_ORDER,
        params! {
            "id" => id, "user_id" => u, "bought_at" => r.bought_at,
            "discount" => r.discount_cents, "transport" => r.transport_cents,
        },
    )
    .map_err(sql)
}

pub fn insert_item(
    q: &mut impl Queryable,
    id: u64,
    order: u64,
    u: u64,
    s: u64,
    p: u64,
    cfg: &ShopCfg,
) -> Result<(), String> {
    let r = product_row(u, s, p, cfg.seed);
    q.exec_drop(
        INS_ITEM,
        params! {
            "id" => id, "shopping_id" => order, "name" => &r.name,
            "qty" => r.quantity, "unit" => r.unit_cents,
        },
    )
    .map_err(sql)
}

pub fn next_id(conn: &mut Conn, table: &str) -> Result<u64, String> {
    conn.query_first(format!(
        "SELECT COALESCE(MAX(id), 0) + 1 FROM bench.{table}"
    ))
    .map_err(sql)
    .map(|v: Option<u64>| v.unwrap_or(1))
}

pub fn sql(e: mysql::Error) -> String {
    format!("mysql: {e}")
}
