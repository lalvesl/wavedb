//! The node's tasks, and the contract a schema's registry fulfils to run
//! them ([RFC 0066] §4).
//!
//! Embassy keeps tasks in **static pools**, and a `static` needs a concrete
//! type. The node's accept, shard and job tasks are generic over the
//! registry `E`, which only the schema crate knows — and so is a connection's,
//! because routing asks the registry how each hash is addressed. So the
//! schema crate declares them: `expose_server!` emits
//! [`node_tasks!`](crate::node_tasks)`(ServerRegistry)`, which defines four
//! concrete `#[task]` functions and implements
//! [`NodeTasks`] for the registry by calling them. The node only ever calls
//! `E::spawn_*` — resolved at compile time, no `dyn`, nothing registered at
//! runtime.
//!
//! Everything here is `#[doc(hidden)]` plumbing for that one macro.
//!
//! [RFC 0066]: ../../../rfcs/0066-embassy-executor-and-async-io.md

use std::rc::Rc;

pub use wavedb_platform::carrier::{Here, SpawnError, rt};

pub use crate::serve::{Accept, Connection, accept, connection};
pub use crate::shard::{JobSlot, ShardCtx, Worker, job, shard};

/// Connections served at once, across the whole process. One more is
/// refused: its socket is closed rather than queued.
pub const MAX_CONNECTIONS: usize = 1024;

/// Jobs executing at once, across every shard. A shard that finds the pool
/// full runs the job itself, so its queue backs up instead of the job being
/// lost.
pub const MAX_JOBS: usize = 1024;

/// Workers a process may run — and so accept loops and BG tasks, one of each
/// per worker.
pub const MAX_SHARDS: usize = 256;

/// A registry the node can run: the four generic tasks, made concrete by
/// [`node_tasks!`](crate::node_tasks) in the crate that owns the registry type.
///
/// Each `spawn_*` either places the task on the carrier `here` names or
/// returns [`SpawnError`] because that task's pool is full, dropping the
/// arguments (the `Here` among them).
pub trait NodeTasks:
    wavedb_core::expose::Exposure + Copy + Send + 'static
{
    /// A worker's accept loop, on that worker's carrier.
    ///
    /// # Errors
    /// [`MAX_SHARDS`] accept loops already running.
    fn spawn_accept(ctx: Accept<Self>, here: Here) -> Result<(), SpawnError>;

    /// One accepted connection, on the carrier of the worker that accepted
    /// it.
    ///
    /// # Errors
    /// [`MAX_CONNECTIONS`] connections already open: the caller drops the
    /// connection, which closes its socket.
    fn spawn_connection(
        conn: Connection<Self>,
        here: Here,
    ) -> Result<(), SpawnError>;

    /// A shard worker: the root task of a shard carrier.
    ///
    /// # Errors
    /// [`MAX_SHARDS`] shards already running.
    fn spawn_shard(ctx: ShardCtx<Self>, here: Here) -> Result<(), SpawnError>;

    /// One routed request, executing on its shard.
    ///
    /// # Errors
    /// [`MAX_JOBS`] jobs already executing.
    fn spawn_job(
        job: JobSlot,
        worker: Rc<Worker<Self>>,
        here: Here,
    ) -> Result<(), SpawnError>;
}

/// Declare the node's tasks for `$registry` and implement [`NodeTasks`] for
/// it. Emitted by `expose_server!`; once per registry.
#[doc(hidden)]
#[macro_export]
macro_rules! node_tasks {
    ($registry:ty) => {
        // A block so one `allow` covers the functions Embassy generates
        // around each task too (the futures are non-`Send` by design: one
        // carrier each, never sent), and so the expansion sees `$registry`
        // wherever it is declared — a module could not reach one declared
        // inside a function.
        #[allow(clippy::future_not_send)]
        const _: () = {
            use ::std::rc::Rc;
            use $crate::__rt::{
                Accept, Connection, Here, JobSlot, NodeTasks, ShardCtx,
                SpawnError, Worker,
            };

            #[$crate::__rt::rt::task(
                                        pool_size = $crate::__rt::MAX_SHARDS,
                                        embassy_executor = $crate::__rt::rt
                                    )]
            async fn accept(ctx: Accept<$registry>, here: Here) {
                $crate::__rt::accept(ctx, here).await;
            }

            #[$crate::__rt::rt::task(
                                        pool_size = $crate::__rt::MAX_CONNECTIONS,
                                        embassy_executor = $crate::__rt::rt
                                    )]
            async fn connection(conn: Connection<$registry>, here: Here) {
                $crate::__rt::connection(conn, here).await;
            }

            #[$crate::__rt::rt::task(
                                        pool_size = $crate::__rt::MAX_SHARDS,
                                        embassy_executor = $crate::__rt::rt
                                    )]
            async fn shard(ctx: ShardCtx<$registry>, here: Here) {
                $crate::__rt::shard(ctx, here).await;
            }

            #[$crate::__rt::rt::task(
                                        pool_size = $crate::__rt::MAX_JOBS,
                                        embassy_executor = $crate::__rt::rt
                                    )]
            async fn job(
                job: JobSlot,
                worker: Rc<Worker<$registry>>,
                here: Here,
            ) {
                $crate::__rt::job(job, worker, here).await;
            }

            impl NodeTasks for $registry {
                fn spawn_accept(
                    ctx: Accept<Self>,
                    here: Here,
                ) -> ::core::result::Result<(), SpawnError> {
                    here.spawn(accept(ctx, here.clone()))
                }

                fn spawn_connection(
                    conn: Connection<Self>,
                    here: Here,
                ) -> ::core::result::Result<(), SpawnError> {
                    here.spawn(connection(conn, here.clone()))
                }

                fn spawn_shard(
                    ctx: ShardCtx<Self>,
                    here: Here,
                ) -> ::core::result::Result<(), SpawnError> {
                    here.spawn(shard(ctx, here.clone()))
                }

                fn spawn_job(
                    job_slot: JobSlot,
                    worker: Rc<Worker<Self>>,
                    here: Here,
                ) -> ::core::result::Result<(), SpawnError> {
                    here.spawn(job(job_slot, worker, here.clone()))
                }
            }
        };
    };
}
