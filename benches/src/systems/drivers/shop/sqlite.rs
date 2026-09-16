//! SQLite in the shop workload.
//!
//! Three tables and two foreign keys, which is where the model differs:
//! WaveDB puts the user in the `Id` and the relationship in a held `PivotId`,
//! and the SQL schema puts both in columns with an index on each. The order
//! page is `ORDER BY bought_at LIMIT 10 OFFSET n` against a declared list's
//! page descent — that comparison is the reason this workload exists.

use std::path::PathBuf;

use rusqlite::{Connection, params};

use crate::harness::{Driver, DriverFactory};
use crate::plan::op::ShopOp;
use crate::shop::PAGE;

use super::SETTLE_AFTER;

const INSERT_USER: &str = "INSERT INTO users (id, name, address, city, email) \
                           VALUES (?1, ?2, ?3, ?4, ?5)";
const INSERT_ORDER: &str = "INSERT INTO shopping (id, user_id, bought_at, discount_cents, \
     transport_cents) VALUES (?1, ?2, ?3, ?4, ?5)";
const INSERT_ITEM: &str = "INSERT INTO product (id, shopping_id, name, quantity, unit_cents) \
     VALUES (?1, ?2, ?3, ?4, ?5)";
const SELECT_USER: &str = "SELECT name FROM users WHERE id = ?1";
const SELECT_PROFILE: &str =
    "SELECT name, address, city, email FROM users WHERE id = ?1";
const SELECT_PAGE: &str = "SELECT id, bought_at, discount_cents, transport_cents FROM shopping \
     WHERE user_id = ?1 ORDER BY bought_at LIMIT ?2 OFFSET ?3";
const SELECT_FIRST: &str =
    "SELECT id FROM shopping WHERE user_id = ?1 ORDER BY bought_at LIMIT 1";
const SELECT_ITEMS: &str = "SELECT name, quantity, unit_cents FROM product WHERE shopping_id = ?1 \
     ORDER BY name LIMIT ?2";

pub struct Factory {
    pub path: PathBuf,
    /// `FULL` or `NORMAL` — the durability row in SQLite's own spelling.
    pub sync: &'static str,
}

pub struct ShopSqlite {
    conn: Connection,
    path: PathBuf,
    sync: &'static str,
}

impl DriverFactory for Factory {
    type Driver = ShopSqlite;

    /// One. SQLite has no shard model, and a second connection would measure
    /// its locking rather than its storage.
    fn consumers(&self) -> usize {
        1
    }

    fn build(&self, _shard: usize) -> Result<ShopSqlite, String> {
        Ok(ShopSqlite {
            conn: connect(&self.path, self.sync)?,
            path: self.path.clone(),
            sync: self.sync,
        })
    }

    fn route(&self, _op: &ShopOp) -> usize {
        0
    }
}

impl Driver for ShopSqlite {
    type Op = ShopOp;

    fn execute(&mut self, op: ShopOp) -> Result<(), String> {
        match op {
            ShopOp::Signup { u, row } => self
                .conn
                .prepare_cached(INSERT_USER)
                .map_err(sql)?
                .execute(params![
                    u as i64,
                    row.name,
                    row.address,
                    row.city,
                    row.email
                ])
                .map(|_| ())
                .map_err(sql),
            ShopOp::Checkout {
                u,
                order,
                first_item,
                row,
                items,
                ..
            } => {
                // One transaction: the order and its line items commit
                // together or not at all, which is how an application writes
                // a checkout and costs one barrier for the whole thing.
                let tx = self.conn.transaction().map_err(sql)?;
                tx.prepare_cached(INSERT_ORDER)
                    .map_err(sql)?
                    .execute(params![
                        order as i64,
                        u as i64,
                        row.bought_at as i64,
                        row.discount_cents as i64,
                        row.transport_cents as i64
                    ])
                    .map_err(sql)?;
                for (k, item) in items.iter().enumerate() {
                    tx.prepare_cached(INSERT_ITEM)
                        .map_err(sql)?
                        .execute(params![
                            (first_item + k as u64) as i64,
                            order as i64,
                            item.name,
                            item.quantity,
                            item.unit_cents as i64
                        ])
                        .map_err(sql)?;
                }
                tx.commit().map_err(sql)
            }
            ShopOp::Profile { u } => {
                let name: String = self
                    .conn
                    .prepare_cached(SELECT_PROFILE)
                    .map_err(sql)?
                    .query_row(params![u as i64], |r| r.get(0))
                    .map_err(sql)?;
                empty_check(name.is_empty(), "profile", u)
            }
            ShopOp::OrderPage { u, page } => self.order_page(u, page),
            ShopOp::OrderDetail { u } => self.order_detail(u),
        }
    }

    /// Quiesce after the writes and reopen, so the three read phases measure a
    /// database nobody has just written to — the counterpart of the WaveDB
    /// row's evict and the servers' restart.
    fn between_phases(
        &mut self,
        done: &str,
        _next: &str,
    ) -> Result<(), String> {
        if done != SETTLE_AFTER {
            return Ok(());
        }
        checkpoint(&self.conn)?;
        self.conn = connect(&self.path, self.sync)?;
        Ok(())
    }

    fn close(self) -> Result<(), String> {
        checkpoint(&self.conn)
    }
}

impl ShopSqlite {
    /// A user, then one page of their order history.
    fn order_page(&self, u: u64, page: usize) -> Result<(), String> {
        let name: String = self
            .conn
            .prepare_cached(SELECT_USER)
            .map_err(sql)?
            .query_row(params![u as i64], |r| r.get(0))
            .map_err(sql)?;
        let mut stmt = self.conn.prepare_cached(SELECT_PAGE).map_err(sql)?;
        let rows = stmt
            .query_map(
                params![u as i64, PAGE as i64, (page * PAGE) as i64],
                |r| r.get::<_, i64>(0),
            )
            .map_err(sql)?
            .count();
        // A later page may legitimately be empty; the first may not.
        empty_check(name.is_empty() || (rows == 0 && page == 0), "page", u)
    }

    /// A user, their first order, and that order's line items.
    fn order_detail(&self, u: u64) -> Result<(), String> {
        let name: String = self
            .conn
            .prepare_cached(SELECT_USER)
            .map_err(sql)?
            .query_row(params![u as i64], |r| r.get(0))
            .map_err(sql)?;
        let order: i64 = self
            .conn
            .prepare_cached(SELECT_FIRST)
            .map_err(sql)?
            .query_row(params![u as i64], |r| r.get(0))
            .map_err(sql)?;
        let mut stmt = self.conn.prepare_cached(SELECT_ITEMS).map_err(sql)?;
        let items = stmt
            .query_map(params![order, PAGE as i64], |r| r.get::<_, String>(0))
            .map_err(sql)?
            .count();
        empty_check(name.is_empty() || items == 0, "detail", u)
    }
}

/// A read that returned nothing is a fast operation and a wrong one.
fn empty_check(empty: bool, what: &str, u: u64) -> Result<(), String> {
    if empty {
        return Err(format!("{what}: user {u} came back empty"));
    }
    Ok(())
}

fn connect(path: &std::path::Path, sync: &str) -> Result<Connection, String> {
    let conn = Connection::open(path).map_err(sql)?;
    conn.query_row("PRAGMA journal_mode = WAL", [], |r| r.get::<_, String>(0))
        .map_err(sql)?;
    conn.pragma_update(None, "synchronous", sync).map_err(sql)?;
    Ok(conn)
}

fn checkpoint(conn: &Connection) -> Result<(), String> {
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
        .map_err(sql)
}

fn sql(e: rusqlite::Error) -> String {
    format!("sqlite: {e}")
}
