//! Valkey on the [`Driver`] seam.
//!
//! A row is a hash, `thing:<n>`, holding the five fields — the record whole,
//! addressed by the dataset id like every other peer's primary key. The
//! secondary index on `tag` that the SQL trio declares and MongoDB builds is
//! a set per tag value, `tag:<tag>`, maintained by the client: Valkey has no
//! index of its own, so leaving it out would make Valkey the one row not
//! paying for one on every write, which flatters rather than breaks.
//!
//! - **insert** is `MULTI · HSET · SADD · EXEC`, pipelined: one round trip,
//!   one AOF write for record and index together.
//! - **read** is `HGET thing:<n> name`, the counterpart of `SELECT name`.
//! - **update** rewrites all five fields (whole, never a patch) and moves the
//!   row between tag sets. The old tag has to be read first — the price of an
//!   index the client keeps — so it is two round trips.

use std::path::PathBuf;
use std::sync::Arc;

use super::resp::{Reply, Resp};
use super::valkey_server::{Shared, meter, restart_server, sock};
use crate::harness::{Driver, DriverFactory};
use crate::plan::op::MicroOp;
use crate::schema::Thing;
use crate::systems::Durability;

/// The directory of an already-running server.
pub struct Factory {
    pub dir: PathBuf,
    /// Kept so a restart brings the server back under the durability it was
    /// measured with.
    pub durability: Durability,
    pub server: Shared,
}

pub struct ValkeyDriver {
    conn: Resp,
    dir: PathBuf,
    durability: Durability,
    server: Shared,
}

impl DriverFactory for Factory {
    type Driver = ValkeyDriver;

    fn consumers(&self) -> usize {
        1
    }

    fn build(&self, _shard: usize) -> Result<ValkeyDriver, String> {
        Ok(ValkeyDriver {
            conn: Resp::connect(&sock(&self.dir))?,
            dir: self.dir.clone(),
            durability: self.durability,
            server: Arc::clone(&self.server),
        })
    }

    fn route(&self, _op: &MicroOp) -> usize {
        0
    }

    fn meter(&self) -> crate::io_counters::Meter {
        meter(&self.server)
    }
}

/// Queue `HSET <key>` with the record's five fields.
pub fn hset_thing(
    conn: &mut Resp,
    key: &[u8],
    t: &Thing,
) -> Result<(), String> {
    let (kind, score) = (t.kind.to_string(), t.score.to_string());
    conn.send(&[
        b"HSET",
        key,
        b"kind",
        kind.as_bytes(),
        b"score",
        score.as_bytes(),
        b"name",
        t.name.as_bytes(),
        b"tag",
        t.tag.as_bytes(),
        b"body",
        t.body.as_bytes(),
    ])
}

fn tag_key(tag: &[u8]) -> Vec<u8> {
    [b"tag:".as_slice(), tag].concat()
}

impl Driver for ValkeyDriver {
    type Op = MicroOp;

    fn execute(&mut self, op: MicroOp) -> Result<(), String> {
        match op {
            MicroOp::Insert { n, row } => {
                let (key, id) = (format!("thing:{n}"), n.to_string());
                self.conn.send(&[b"MULTI"])?;
                hset_thing(&mut self.conn, key.as_bytes(), &row)?;
                self.conn.send(&[
                    b"SADD",
                    &tag_key(row.tag.as_bytes()),
                    id.as_bytes(),
                ])?;
                self.conn.send(&[b"EXEC"])?;
                let added = self.conn.exec(2)?.swap_remove(0).int()?;
                if added != 5 {
                    return Err(format!("insert {n} added {added} fields"));
                }
            }
            MicroOp::Read { n } => {
                let key = format!("thing:{n}");
                match self.conn.call(&[b"HGET", key.as_bytes(), b"name"])? {
                    Reply::Bulk(Some(_)) => {}
                    _ => return Err(format!("row {n} came back empty")),
                }
            }
            MicroOp::Update { n, row } => {
                let (key, id) = (format!("thing:{n}"), n.to_string());
                let old = self
                    .conn
                    .call(&[b"HGET", key.as_bytes(), b"tag"])?
                    .bulk()
                    .map_err(|e| format!("update {n}: {e}"))?;
                self.conn.send(&[b"MULTI"])?;
                hset_thing(&mut self.conn, key.as_bytes(), &row)?;
                self.conn.send(&[b"SREM", &tag_key(&old), id.as_bytes()])?;
                self.conn.send(&[
                    b"SADD",
                    &tag_key(row.tag.as_bytes()),
                    id.as_bytes(),
                ])?;
                self.conn.send(&[b"EXEC"])?;
                // `HSET` counts the fields it *created*: zero is a rewrite of
                // a record that was there, anything else wrote a new one.
                let created = self.conn.exec(3)?.swap_remove(0).int()?;
                if created != 0 {
                    return Err(format!("update {n} created {created} fields"));
                }
            }
        }
        Ok(())
    }

    /// The same boundary as every server row: a restart after `read_hot`. On
    /// Valkey it replays the AOF into memory rather than emptying a cache, so
    /// `read_cold` reads a resident dataset — the row's notes say so.
    fn between_phases(
        &mut self,
        done: &str,
        _next: &str,
    ) -> Result<(), String> {
        if done != "read_hot" {
            return Ok(());
        }
        restart_server(&self.server, &self.dir, self.durability)?;
        self.conn = Resp::connect(&sock(&self.dir))?;
        Ok(())
    }
}
