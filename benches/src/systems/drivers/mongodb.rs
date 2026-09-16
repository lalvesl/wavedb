//! MongoDB on the [`Driver`] seam — the reference peer.
//!
//! It is the closest thing to WaveDB's model in the comparison: whole
//! documents addressed by `_id`, no join, no query planner in the path this
//! workload exercises. So it is the row where a WaveDB loss is least
//! explainable by "the other system is doing something different".
//!
//! Unlike the SQL pair, the durability knob is the **write concern's `j`
//! flag** — per operation rather than per server. It travels with the write,
//! so the durable row really does wait for the journal on every single insert,
//! and the driver is where it lives.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use mongodb::IndexModel;
use mongodb::bson::{Document, doc};
use mongodb::options::{Acknowledgment, ClientOptions, WriteConcern};
use mongodb::sync::{Client, Collection};

use crate::harness::{Driver, DriverFactory};
use crate::plan::op::MicroOp;
use crate::schema::Thing;
use crate::systems::server::{self, Server};

/// A running server, shared so a phase boundary can restart it.
pub type Shared = Arc<Mutex<Option<Server>>>;

/// A running server, and the durability its writes will carry.
pub struct Factory {
    pub dir: PathBuf,
    pub port: u16,
    pub journal: bool,
    /// True when the secondary index still has to be built. A seeded row
    /// arrives with it already in place.
    pub create: bool,
    pub server: Shared,
}

pub struct MongoDriver {
    col: Collection<Document>,
    /// Held so the connection outlives the collection handle taken from it.
    _client: Client,
    dir: PathBuf,
    port: u16,
    journal: bool,
    server: Shared,
}

/// The document form of a record. `_id` is the dataset id on every system, so
/// the point lookup is the same key everywhere.
fn document(n: u64, t: &Thing) -> Document {
    doc! {
        "_id": n as i64,
        "kind": i64::from(t.kind),
        "score": t.score as i64,
        "name": t.name.clone(),
        "tag": t.tag.clone(),
        "body": t.body.clone(),
    }
}

impl DriverFactory for Factory {
    type Driver = MongoDriver;

    fn consumers(&self) -> usize {
        1
    }

    fn build(&self, _shard: usize) -> Result<MongoDriver, String> {
        let mut opts =
            ClientOptions::parse(format!("mongodb://127.0.0.1:{}", self.port))
                .run()
                .map_err(drv)?;
        opts.write_concern = Some(
            WriteConcern::builder()
                .w(Acknowledgment::Nodes(1))
                .journal(self.journal)
                .build(),
        );
        // One connection, like every other row: the workload is sequential and
        // a pool would quietly measure concurrency the others do not have.
        opts.max_pool_size = Some(1);
        let client = Client::with_options(opts).map_err(drv)?;
        let col: Collection<Document> =
            client.database("bench").collection("thing");
        // The `tag` index, which every SQL peer declares in its DDL. Missing
        // it would not fail a single operation — it would make MongoDB the
        // only row not paying for a secondary index on every write, which is
        // a flattering measurement rather than a broken one.
        if self.create {
            col.create_index(
                IndexModel::builder().keys(doc! { "tag": 1 }).build(),
            )
            .run()
            .map_err(drv)?;
        }
        Ok(MongoDriver {
            col,
            _client: client,
            dir: self.dir.clone(),
            port: self.port,
            journal: self.journal,
            server: Arc::clone(&self.server),
        })
    }

    fn route(&self, _op: &MicroOp) -> usize {
        0
    }

    /// The **server's** disk writes, not this process's. Read from the shared
    /// handle at the start of each phase, so a restart at a phase boundary
    /// hands the next phase the pid that is actually running.
    fn writer(&self) -> crate::metrics::Writer {
        match self.server.lock() {
            Ok(g) => g.as_ref().map_or(crate::metrics::Writer::Current, |s| {
                crate::metrics::Writer::Pid(s.pid)
            }),
            // A poisoned mutex means a consumer panicked mid-restart; the row
            // is already failing, and a wrong pid would not make it clearer.
            Err(_) => crate::metrics::Writer::Current,
        }
    }
}

impl Driver for MongoDriver {
    type Op = MicroOp;

    fn execute(&mut self, op: MicroOp) -> Result<(), String> {
        match op {
            MicroOp::Insert { n, row } => {
                self.col.insert_one(document(n, &row)).run().map_err(drv)?;
            }
            MicroOp::Read { n } => {
                let got = self
                    .col
                    .find_one(doc! { "_id": n as i64 })
                    .run()
                    .map_err(drv)?;
                if got.is_none() {
                    return Err(format!("row {n} came back empty"));
                }
            }
            // Whole-document replace, not `$set`: WaveDB writes whole records,
            // and a field patch would flatter the document store for free.
            MicroOp::Update { n, row } => {
                let replaced = self
                    .col
                    .replace_one(doc! { "_id": n as i64 }, document(n, &row))
                    .run()
                    .map_err(drv)?;
                if replaced.matched_count != 1 {
                    return Err(format!(
                        "update {n} matched {} documents",
                        replaced.matched_count
                    ));
                }
            }
        }
        Ok(())
    }

    /// Restart empties the WiredTiger cache, the counterpart of reopening the
    /// WaveDB store. The OS page cache stays warm on both sides, which is why
    /// this is `read_cold` and not `read_from_disk`.
    fn between_phases(
        &mut self,
        done: &str,
        _next: &str,
    ) -> Result<(), String> {
        if done == "read_hot" {
            return self.restart();
        }
        Ok(())
    }
}

fn drv(e: mongodb::error::Error) -> String {
    format!("mongodb: {e}")
}

/// A port derived from this process, so two runs on one machine do not fight
/// over 27017 — and neither touches a `mongod` the user is running.
#[must_use]
pub fn port() -> u16 {
    27_100 + u16::try_from(std::process::id() % 400).unwrap_or(0)
}

/// Start a server on `dir` and wait until it answers a ping.
///
/// # Errors
/// The process failing to spawn, or never becoming connectable.
pub fn start(dir: &Path, port: u16) -> Result<Server, String> {
    let log = dir.join("mongod.log");
    // No `--fork`: the forked daemon would leave us holding the pid of a
    // process that exits immediately, and the write-bytes column would read
    // zero for every phase.
    let mongo = Server::spawn(
        "mongod",
        &[
            "--dbpath",
            &dir.join("data").display().to_string(),
            "--bind_ip",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            // Pinned, not inferred: WiredTiger sizes its cache from the
            // HOST's RAM, not the cgroup's, so under the 500 MB cage an
            // unpinned mongod asks for gigabytes it cannot have and is
            // OOM-killed.
            "--wiredTigerCacheSizeGB",
            server::CACHE_GB,
            "--logpath",
            &log.display().to_string(),
        ],
        &dir.join("mongod.out"),
    )?;
    server::wait_for("mongod", server::STARTUP_SECS, || {
        ClientOptions::parse(format!("mongodb://127.0.0.1:{port}"))
            .run()
            .ok()
            .and_then(|o| Client::with_options(o).ok())
            .is_some_and(|c| {
                c.database("admin")
                    .run_command(doc! { "ping": 1 })
                    .run()
                    .is_ok()
            })
    })
    .map_err(|e| format!("{e}\n{}", server::log_tail(&log, 10)))?;
    Ok(mongo)
}

/// Stop with `mongod --shutdown`, not a signal: a clean shutdown checkpoints
/// WiredTiger and closes the journal, which is as quiesced as this server gets.
///
/// # Errors
/// The shutdown command refusing, or the process failing to reap.
pub fn stop(mongo: Server, dir: &Path) -> Result<(), String> {
    mongo.stop(
        "mongod",
        &[
            "--dbpath",
            &dir.join("data").display().to_string(),
            "--shutdown",
        ],
    )
}

impl MongoDriver {
    /// Stop the server, start it again, and reconnect.
    fn restart(&mut self) -> Result<(), String> {
        let mut slot = self
            .server
            .lock()
            .map_err(|_| "the server mutex was poisoned".to_string())?;
        let old = slot
            .take()
            .ok_or_else(|| "the server is already gone".to_string())?;
        stop(old, &self.dir)?;
        *slot = Some(start(&self.dir, self.port)?);
        drop(slot);

        let rebuilt = Factory {
            dir: self.dir.clone(),
            port: self.port,
            journal: self.journal,
            // The index survives the restart; rebuilding it here would be
            // work done twice and, on a large tier, minutes of it.
            create: false,
            server: Arc::clone(&self.server),
        }
        .build(0)?;
        *self = rebuilt;
        Ok(())
    }
}

/// WiredTiger's `compact` — release the free blocks a collection is holding,
/// which is the floor a `compacted` footprint is asking for.
///
/// # Errors
/// The server refused the connection or the command.
pub fn compact(port: u16) -> Result<(), String> {
    let opts = ClientOptions::parse(format!("mongodb://127.0.0.1:{port}"))
        .run()
        .map_err(drv)?;
    let client = Client::with_options(opts).map_err(drv)?;
    client
        .database("bench")
        .run_command(doc! { "compact": "thing" })
        .run()
        .map(|_| ())
        .map_err(drv)
}

/// Connect to an already-running server on `port`, with the write concern the
/// row was configured for.
///
/// # Errors
/// The connection options were rejected.
pub fn connect_at(port: u16, journal: bool) -> Result<Client, String> {
    let mut opts = ClientOptions::parse(format!("mongodb://127.0.0.1:{port}"))
        .run()
        .map_err(drv)?;
    opts.write_concern = Some(
        WriteConcern::builder()
            .w(Acknowledgment::Nodes(1))
            .journal(journal)
            .build(),
    );
    // One connection, like every other row: a pool would quietly measure
    // concurrency the other adapters do not have.
    opts.max_pool_size = Some(1);
    Client::with_options(opts).map_err(drv)
}

/// Stop whatever is in `held` and start a replacement, which is how this
/// system empties the WiredTiger cache.
///
/// # Errors
/// The slot was empty or poisoned, or the server would not come back.
pub fn restart_server(
    held: &Shared,
    dir: &Path,
    port: u16,
) -> Result<(), String> {
    let mut slot = held
        .lock()
        .map_err(|_| "the server mutex was poisoned".to_string())?;
    let old = slot
        .take()
        .ok_or_else(|| "the server is already gone".to_string())?;
    stop(old, dir)?;
    *slot = Some(start(dir, port)?);
    Ok(())
}
