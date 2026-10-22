//! Valkey in the shop workload.
//!
//! The same three record kinds as every peer, as hashes, and the two
//! secondary indexes the SQL trio declares as sorted sets the client keeps:
//!
//! | key | holds |
//! |---|---|
//! | `user:<u>` | the profile |
//! | `order:<id>` | one order, with its `user_id` |
//! | `orders:<u>` | the user's order ids, scored by `bought_at` — the `(user_id, bought_at)` index |
//! | `item:<id>` | one line item, with its `shopping_id` |
//! | `items:<order>` | `name \0 id` at score 0, so lexical order is `name` order — the `(shopping_id, name)` index |
//!
//! A **checkout** is one `MULTI … EXEC`: the order, its index entry and every
//! line item with theirs, pipelined — one round trip and one AOF write, the
//! counterpart of the others' one transaction. The read phases fetch the way
//! a Valkey application would: the index first, then the records it names,
//! each step one pipelined round trip.

use std::path::PathBuf;
use std::sync::Arc;

use crate::harness::{Driver, DriverFactory};
use crate::plan::op::ShopOp;
use crate::shop::{PAGE, ProductRow, ShoppingRow, UserRow};
use crate::systems::Durability;
use crate::systems::drivers::resp::{Reply, Resp};
use crate::systems::drivers::valkey_server::{
    Shared, meter, restart_server, sock,
};

use super::SETTLE_AFTER;

pub struct Factory {
    pub dir: PathBuf,
    pub durability: Durability,
    pub server: Shared,
}

pub struct ShopValkey {
    conn: Resp,
    dir: PathBuf,
    durability: Durability,
    server: Shared,
}

impl DriverFactory for Factory {
    type Driver = ShopValkey;

    fn consumers(&self) -> usize {
        1
    }

    fn build(&self, _shard: usize) -> Result<ShopValkey, String> {
        Ok(ShopValkey {
            conn: Resp::connect(&sock(&self.dir))?,
            dir: self.dir.clone(),
            durability: self.durability,
            server: Arc::clone(&self.server),
        })
    }

    fn route(&self, _op: &ShopOp) -> usize {
        0
    }

    fn meter(&self) -> crate::io_counters::Meter {
        meter(&self.server)
    }
}

/// Queue the user's profile. One command.
///
/// # Errors
/// The write buffer could not drain into the socket.
pub fn queue_user(conn: &mut Resp, u: u64, r: &UserRow) -> Result<(), String> {
    conn.send(&[
        b"HSET",
        format!("user:{u}").as_bytes(),
        b"name",
        r.name.as_bytes(),
        b"address",
        r.address.as_bytes(),
        b"city",
        r.city.as_bytes(),
        b"email",
        r.email.as_bytes(),
    ])
}

/// Queue one order and its entry in the user's index. Two commands.
///
/// # Errors
/// The write buffer could not drain into the socket.
pub fn queue_order(
    conn: &mut Resp,
    id: u64,
    u: u64,
    r: &ShoppingRow,
) -> Result<(), String> {
    let (id, u, at) = (id.to_string(), u.to_string(), r.bought_at.to_string());
    conn.send(&[
        b"HSET",
        format!("order:{id}").as_bytes(),
        b"user_id",
        u.as_bytes(),
        b"bought_at",
        at.as_bytes(),
        b"discount_cents",
        r.discount_cents.to_string().as_bytes(),
        b"transport_cents",
        r.transport_cents.to_string().as_bytes(),
    ])?;
    conn.send(&[
        b"ZADD",
        format!("orders:{u}").as_bytes(),
        at.as_bytes(),
        id.as_bytes(),
    ])
}

/// Queue one line item and its entry in the order's index. Two commands.
///
/// # Errors
/// The write buffer could not drain into the socket.
pub fn queue_item(
    conn: &mut Resp,
    id: u64,
    order: u64,
    r: &ProductRow,
) -> Result<(), String> {
    let id = id.to_string();
    conn.send(&[
        b"HSET",
        format!("item:{id}").as_bytes(),
        b"shopping_id",
        order.to_string().as_bytes(),
        b"name",
        r.name.as_bytes(),
        b"quantity",
        r.quantity.to_string().as_bytes(),
        b"unit_cents",
        r.unit_cents.to_string().as_bytes(),
    ])?;
    let member = [r.name.as_bytes(), b"\0", id.as_bytes()].concat();
    conn.send(&[b"ZADD", format!("items:{order}").as_bytes(), b"0", &member])
}

impl Driver for ShopValkey {
    type Op = ShopOp;

    fn execute(&mut self, op: ShopOp) -> Result<(), String> {
        match op {
            ShopOp::Signup { u, row } => {
                queue_user(&mut self.conn, u, &row)?;
                let added = self.conn.replies(1)?.swap_remove(0).int()?;
                if added != 4 {
                    return Err(format!("signup {u} added {added} fields"));
                }
                Ok(())
            }
            ShopOp::Checkout {
                u,
                order,
                first_item,
                row,
                items,
                ..
            } => {
                self.conn.send(&[b"MULTI"])?;
                queue_order(&mut self.conn, order, u, &row)?;
                for (k, it) in items.iter().enumerate() {
                    queue_item(
                        &mut self.conn,
                        first_item + k as u64,
                        order,
                        it,
                    )?;
                }
                self.conn.send(&[b"EXEC"])?;
                self.conn.exec(2 + 2 * items.len()).map(|_| ())
            }
            ShopOp::Profile { u } => {
                let key = format!("user:{u}");
                let got = self.conn.call(&[b"HGETALL", key.as_bytes()])?;
                empty(got.array()?.is_empty(), "profile", u)
            }
            ShopOp::OrderPage { u, page } => self.order_page(u, page),
            ShopOp::OrderDetail { u } => self.order_detail(u),
        }
    }

    /// Restart after the write phases, like every server row. Valkey replays
    /// its AOF into memory on the way back, so the read phases are resident.
    fn between_phases(
        &mut self,
        done: &str,
        _next: &str,
    ) -> Result<(), String> {
        if done != SETTLE_AFTER {
            return Ok(());
        }
        restart_server(&self.server, &self.dir, self.durability)?;
        self.conn = Resp::connect(&sock(&self.dir))?;
        Ok(())
    }
}

impl ShopValkey {
    /// The user and the page's order ids, then the orders: two round trips.
    fn order_page(&mut self, u: u64, page: usize) -> Result<(), String> {
        let (first, last) = (page * PAGE, page * PAGE + PAGE - 1);
        let (user, ids) = self.user_and_range(u, first, last)?;
        let orders = self.hgetall_each("order", &ids)?;
        empty(user || (orders == 0 && page == 0), "order_page", u)
    }

    /// The user and the first order's id, then that order's item ids, then
    /// the items: three round trips.
    fn order_detail(&mut self, u: u64) -> Result<(), String> {
        let (user, ids) = self.user_and_range(u, 0, 0)?;
        let order = ids
            .first()
            .ok_or_else(|| format!("order_detail: user {u} has no order"))?;
        let key = [b"items:".as_slice(), order].concat();
        let members = self
            .conn
            .call(&[b"ZRANGE", &key, b"0", (PAGE - 1).to_string().as_bytes()])?
            .array()?
            .into_iter()
            .map(|m| {
                let m = m.bulk()?;
                let at =
                    m.iter().rposition(|&b| b == 0).ok_or("item member")?;
                Ok(m[at + 1..].to_vec())
            })
            .collect::<Result<Vec<_>, String>>()?;
        let items = self.hgetall_each("item", &members)?;
        empty(user || items == 0, "order_detail", u)
    }

    /// One round trip: is user `u` missing, and the ids at `first..=last` of
    /// their order index.
    fn user_and_range(
        &mut self,
        u: u64,
        first: usize,
        last: usize,
    ) -> Result<(bool, Vec<Vec<u8>>), String> {
        self.conn
            .send(&[b"HGETALL", format!("user:{u}").as_bytes()])?;
        self.conn.send(&[
            b"ZRANGE",
            format!("orders:{u}").as_bytes(),
            first.to_string().as_bytes(),
            last.to_string().as_bytes(),
        ])?;
        let mut got = self.conn.replies(2)?.into_iter();
        let missing = got.next().ok_or("no HGETALL reply")?.array()?.is_empty();
        let ids = got
            .next()
            .ok_or("no ZRANGE reply")?
            .array()?
            .into_iter()
            .map(Reply::bulk)
            .collect::<Result<_, _>>()?;
        Ok((missing, ids))
    }

    /// One round trip: `HGETALL <kind>:<id>` for every id. Answers how many
    /// came back non-empty.
    fn hgetall_each(
        &mut self,
        kind: &str,
        ids: &[Vec<u8>],
    ) -> Result<usize, String> {
        for id in ids {
            let key = [kind.as_bytes(), b":", id].concat();
            self.conn.send(&[b"HGETALL", &key])?;
        }
        let mut found = 0;
        for r in self.conn.replies(ids.len())? {
            found += usize::from(!r.array()?.is_empty());
        }
        Ok(found)
    }
}

fn empty(missing: bool, what: &str, u: u64) -> Result<(), String> {
    if missing {
        return Err(format!("{what}: user {u} came back empty"));
    }
    Ok(())
}
