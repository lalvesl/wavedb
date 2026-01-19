//! The node's maintenance **policy**.
//!
//! Plain numbers: when a worker opens a checkpoint, how far it evicts, when
//! it compacts. Each worker applies them to the stores it owns, on its own
//! thread, between its jobs (`shard::background`, RFC 0067 step 4).

use std::time::Duration;

use wavedb_storage::{BLOCK_SIZE, Space};

/// The background maintenance policy: how the node settles, checkpoints,
/// and bounds its caches and files while serving.
#[derive(Debug, Clone, Copy)]
pub struct Maintenance {
    /// Journal bytes that trigger a checkpoint (journal truncates to zero).
    pub checkpoint_after_bytes: u64,
    /// Cache bytes the settle task evicts down to (settled entries only).
    pub cache_budget_bytes: usize,
    /// When the data files are compacted (RFCs 0069, 0071).
    pub compaction: Compaction,
    /// Bytes of disk pages an object-backed store holds back before it
    /// uploads them (RFC 0070 §5) — per store, since each uploads its own
    /// files. Default 8 MiB: about a file's worth.
    pub upload_after_bytes: u64,
    /// The longest a write waits for an upload when its store holds less
    /// than [`upload_after_bytes`](Self::upload_after_bytes) — per store.
    /// In object mode it is the durability window: a crash loses at most
    /// this much, and each store with a write waiting costs a PUT per
    /// interval. `None` waits for the bytes alone. Default 10 s.
    pub upload_interval: Option<Duration>,
    /// RAM an object-mode database may fill with pages (RFC 0070 §9) —
    /// past each store's share, the coldest uploaded pages leave it.
    /// Default 1 GiB.
    pub object_ram_bytes: u64,
}

impl Default for Maintenance {
    fn default() -> Self {
        Self {
            checkpoint_after_bytes: 64 * 1024 * 1024, // 64 MiB of journal
            cache_budget_bytes: 1024 * 1024 * 1024,   // 1 GiB — generous
            compaction: Compaction::default(),
            upload_after_bytes: 8 << 20, // 8 MiB, about a file
            upload_interval: Some(Duration::from_secs(10)),
            object_ram_bytes: 1 << 30, // 1 GiB
        }
    }
}

/// When to compact the data files (RFCs 0069, 0071).
///
/// A compaction carries the live pages of the emptiest data files forward
/// into the active one and cuts or deletes what they leave
/// ([`Server::victim_free_percent`](crate::Server::victim_free_percent)
/// says which files). Any trigger that is set and met starts one, which
/// then runs to its end, one bounded pass per tick.
///
/// The size triggers also need something to give back — at least a pass's
/// worth of free blocks — and, once a compaction has finished, that free
/// space to have grown by a pass's worth again: a database whose live data
/// alone passes the ceiling is not compacted over and over for nothing.
#[derive(Debug, Clone, Copy)]
pub struct Compaction {
    /// Every day at this time, UTC — the quiet hour a deployment chooses.
    pub daily_at: Option<UtcTime>,
    /// Once the database's files pass this many bytes. Each shard holds its
    /// share (`/ P`), as with the cache budget: workers share no memory, so
    /// none of them sees the whole.
    pub above_bytes: Option<u64>,
    /// Once this share of a shard's files, in percent, is free.
    pub above_free_percent: Option<u8>,
    /// Blocks one pass may copy — the IO a tick spends on it.
    pub budget_blocks: u64,
}

impl Default for Compaction {
    fn default() -> Self {
        Self {
            daily_at: None,
            above_bytes: Some(10 << 30),  // 10 GiB
            above_free_percent: Some(50), // half the files are holes
            budget_blocks: 256,           // ≤ 1 MiB copied per tick
        }
    }
}

/// A time of day in UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UtcTime {
    secs: u64,
}

impl UtcTime {
    /// `hour:minute` UTC; `None` past `23:59`.
    #[must_use]
    pub const fn new(hour: u8, minute: u8) -> Option<Self> {
        if hour < 24 && minute < 60 {
            Some(Self {
                secs: hour as u64 * 3_600 + minute as u64 * 60,
            })
        } else {
            None
        }
    }
}

/// What a store's triggers remember between ticks.
#[derive(Debug, Default, Clone, Copy)]
pub struct Armed {
    /// The UTC day the daily trigger last fired — or, at start, the day
    /// whose time had already passed, so a restart does not fire at once.
    day: Option<u64>,
    /// Blocks unused when the last compaction finished.
    rest: Option<u64>,
}

impl Compaction {
    /// Whether a store with `space`, holding `shares` of the ceiling's
    /// split, should start compacting at `now` (unix seconds).
    #[must_use]
    pub fn due(
        &self,
        armed: &mut Armed,
        space: Space,
        shares: usize,
        now: u64,
    ) -> bool {
        if let Some(at) = self.daily_at {
            let (today, secs) = (now / 86_400, now % 86_400);
            let last = *armed.day.get_or_insert_with(|| {
                if secs >= at.secs {
                    today
                } else {
                    today.saturating_sub(1)
                }
            });
            if secs >= at.secs && last < today {
                armed.day = Some(today);
                return true;
            }
        }
        let unused = space.free_blocks + space.deferred_blocks;
        if unused < armed.rest.unwrap_or(0) + self.budget_blocks {
            return false;
        }
        let file_bytes = space.file_blocks * BLOCK_SIZE as u64;
        let share = u64::try_from(shares.max(1)).unwrap_or(u64::MAX);
        let over = self.above_bytes.is_some_and(|b| file_bytes > b / share);
        let holes = self
            .above_free_percent
            .is_some_and(|p| unused * 100 >= u64::from(p) * space.file_blocks);
        over || holes
    }

    /// Note a finished compaction: the size triggers wait for the free space
    /// to grow by a pass's worth from here.
    pub const fn finished(armed: &mut Armed, space: Space) {
        armed.rest = Some(space.free_blocks + space.deferred_blocks);
    }
}

#[cfg(test)]
mod tests {
    use wavedb_storage::Space;

    use super::{Armed, Compaction, UtcTime};

    const DAY: u64 = 86_400;

    fn off() -> Compaction {
        Compaction {
            daily_at: None,
            above_bytes: None,
            above_free_percent: None,
            budget_blocks: 10,
        }
    }

    fn space(file: u64, free: u64) -> Space {
        Space {
            file_blocks: file,
            free_blocks: free,
            deferred_blocks: 0,
        }
    }

    #[test]
    fn a_time_past_midnight_is_refused() {
        assert!(UtcTime::new(23, 59).is_some());
        assert!(UtcTime::new(24, 0).is_none());
        assert!(UtcTime::new(3, 60).is_none());
    }

    /// Once a day, at the hour — and not at once on a start after it.
    #[test]
    fn the_daily_trigger_fires_once_a_day_at_its_hour() {
        let policy = Compaction {
            daily_at: UtcTime::new(3, 0),
            ..off()
        };
        let mut armed = Armed::default();
        let s = space(100, 0);
        let start = 10 * DAY + 4 * 3_600; // 04:00, already past 03:00
        assert!(!policy.due(&mut armed, s, 1, start), "not on start");
        assert!(!policy.due(&mut armed, s, 1, 11 * DAY + 2 * 3_600));
        assert!(policy.due(&mut armed, s, 1, 11 * DAY + 3 * 3_600));
        assert!(!policy.due(&mut armed, s, 1, 11 * DAY + 5 * 3_600));
        assert!(policy.due(&mut armed, s, 1, 12 * DAY + 3 * 3_600 + 9));
    }

    /// The ceiling is split across the shards, and needs room to give back.
    #[test]
    fn the_ceiling_fires_on_a_shard_past_its_share() {
        let policy = Compaction {
            above_bytes: Some(64 * 4096 * 4), // 64 blocks per shard of 4
            ..off()
        };
        let mut armed = Armed::default();
        assert!(!policy.due(&mut armed, space(64, 20), 4, 0), "at its share");
        assert!(policy.due(&mut armed, space(65, 20), 4, 0));
        assert!(
            !policy.due(&mut armed, space(65, 9), 4, 0),
            "nothing to free"
        );
    }

    #[test]
    fn the_free_share_fires_at_its_percentage() {
        let policy = Compaction {
            above_free_percent: Some(50),
            ..off()
        };
        let mut armed = Armed::default();
        assert!(!policy.due(&mut armed, space(100, 49), 1, 0));
        assert!(policy.due(&mut armed, space(100, 50), 1, 0));
    }

    /// After a compaction, the size triggers wait for a pass's worth of new
    /// free space — however far past the ceiling the live data is.
    #[test]
    fn a_finished_compaction_rearms_only_on_new_free_space() {
        let policy = Compaction {
            above_bytes: Some(0),
            ..off()
        };
        let mut armed = Armed::default();
        Compaction::finished(&mut armed, space(1_000, 15));
        assert!(!policy.due(&mut armed, space(1_000, 24), 1, 0));
        assert!(policy.due(&mut armed, space(1_000, 25), 1, 0));
    }
}
