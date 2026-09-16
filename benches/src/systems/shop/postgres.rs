//! The PostgreSQL shop row's **setup**: what is left after [RFC 0065] took
//! the measuring away.
//!
//! The preload is bulk, untimed and single-threaded, and the row
//! (`row::shop`) runs it before the harness starts. Everything that used
//! to time a phase here now lives in `drivers::shop`, including the DDL —
//! it is the driver that has to agree with the statements it prepares.
//!
//! [RFC 0065]: ../../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use postgres::Client;

use super::ShopCfg;
use crate::shop::{
    product_count, product_row, shopping_count, shopping_row, user_row,
};

const INS_USER: &str = "INSERT INTO users (id, name, address, city, email) \
                        VALUES ($1, $2, $3, $4, $5)";
const INS_ORDER: &str = "INSERT INTO shopping (id, user_id, bought_at, \
                         discount_cents, transport_cents) \
                         VALUES ($1, $2, $3, $4, $5)";
const INS_ITEM: &str = "INSERT INTO product (id, shopping_id, name, quantity, \
                        unit_cents) VALUES ($1, $2, $3, $4, $5)";

pub fn preload(cfg: &ShopCfg, client: &mut Client) -> Result<(), String> {
    let mut tx = client.transaction().map_err(sql)?;
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
    c: &mut impl postgres::GenericClient,
    u: u64,
    cfg: &ShopCfg,
) -> Result<(), String> {
    let r = user_row(u, cfg.seed);
    c.execute(
        INS_USER,
        &[&(u as i64), &r.name, &r.address, &r.city, &r.email],
    )
    .map(|_| ())
    .map_err(sql)
}

pub fn insert_order(
    c: &mut impl postgres::GenericClient,
    id: u64,
    u: u64,
    s: u64,
    cfg: &ShopCfg,
) -> Result<(), String> {
    let r = shopping_row(u, s, cfg.seed);
    c.execute(
        INS_ORDER,
        &[
            &(id as i64),
            &(u as i64),
            &(r.bought_at as i64),
            &(r.discount_cents as i64),
            &(r.transport_cents as i64),
        ],
    )
    .map(|_| ())
    .map_err(sql)
}

pub fn insert_item(
    c: &mut impl postgres::GenericClient,
    id: u64,
    order: u64,
    u: u64,
    s: u64,
    p: u64,
    cfg: &ShopCfg,
) -> Result<(), String> {
    let r = product_row(u, s, p, cfg.seed);
    c.execute(
        INS_ITEM,
        &[
            &(id as i64),
            &(order as i64),
            &r.name,
            &(r.quantity as i32),
            &(r.unit_cents as i64),
        ],
    )
    .map(|_| ())
    .map_err(sql)
}

pub fn sql(e: postgres::Error) -> String {
    format!("postgres: {e}")
}
