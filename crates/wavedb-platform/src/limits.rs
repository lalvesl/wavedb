//! Process limits — native only.
//!
//! A node holds a file open per data file of every shard it serves
//! ([RFC 0071] §9) — at least one per shard, plus one per closed file still
//! live — and the soft limit on open files a process starts with is often
//! 1 024, far below the hard limit the system allows.
//!
//! [RFC 0071]: ../../../rfcs/0071-data-files-by-address.md

use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

/// Raise this process's soft limit on open files to its hard limit,
/// returning the soft limit now in force (`None` when unlimited).
///
/// # Errors
/// The `setrlimit` fault, when the system refuses it.
pub fn raise_open_files() -> std::io::Result<Option<u64>> {
    let limit = getrlimit(Resource::Nofile);
    if limit.current != limit.maximum {
        setrlimit(
            Resource::Nofile,
            Rlimit {
                current: limit.maximum,
                maximum: limit.maximum,
            },
        )?;
    }
    Ok(getrlimit(Resource::Nofile).current)
}

#[cfg(test)]
mod tests {
    use rustix::process::{Resource, getrlimit};

    /// After the call, the soft limit is the hard one.
    #[test]
    fn the_soft_limit_rises_to_the_hard_one() {
        let now = super::raise_open_files().unwrap();
        assert_eq!(now, getrlimit(Resource::Nofile).maximum);
    }
}
