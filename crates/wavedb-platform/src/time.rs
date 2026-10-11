//! The wall clock — `SystemTime` natively, `Date.now()` in the browser.
//!
//! wasm32-unknown-unknown has no system clock: `SystemTime::now()` there
//! panics at runtime, so every timestamp in the workspace routes through
//! here. Browser precision is one millisecond; id minting therefore uses
//! [`key_nanos`], which fuses a process-wide counter into the dead
//! sub-millisecond digits so same-instant mints never share a key.

/// Unix seconds now — what token TTLs run on.
#[must_use]
pub fn unix_secs() -> u64 {
    unix_nanos() / 1_000_000_000
}

/// Unix nanoseconds now — the id-minting key (`CREATED_AT`). In the
/// browser the low six digits are always zero (millisecond clock).
#[cfg(not(target_arch = "wasm32"))]
#[must_use]
pub fn unix_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        // Truncation is theoretical: u64 nanoseconds overflow in 2554.
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

/// Unix nanoseconds now — the id-minting key (`CREATED_AT`). In the
/// browser the low six digits are always zero (millisecond clock).
#[cfg(target_arch = "wasm32")]
#[must_use]
pub fn unix_nanos() -> u64 {
    // `Date.now()` is finite non-negative milliseconds since the epoch;
    // the product stays far under `u64::MAX` until the year 2554.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let millis = js_sys::Date::now().max(0.0) as u64;
    millis * 1_000_000
}

/// The id-minting key: unix nanoseconds whose six sub-millisecond digits
/// are a process-wide counter instead of clock digits.
///
/// Both targets use the same formula, so native- and browser-minted keys
/// are structurally identical: chronological across milliseconds (the
/// clock part), mint-ordered and collision-free within one (the counter
/// part) — which a browser's millisecond clock cannot promise on its own.
#[must_use]
pub fn key_nanos() -> u64 {
    use core::sync::atomic::{AtomicU32, Ordering};
    static MINT: AtomicU32 = AtomicU32::new(0);
    let count = u64::from(MINT.fetch_add(1, Ordering::Relaxed)) % 1_000_000;
    (unix_nanos() / 1_000_000) * 1_000_000 + count
}

/// Sleep for `duration` — the timer the connection manager's poll loop and
/// the node's upkeep run on (`embassy-time` natively, RFC 0066; `setTimeout`
/// in the browser).
///
/// Executor-independent: the `std` driver keeps its own alarm thread and wakes
/// the task through its `Waker`, so this works under any executor.
#[cfg(not(target_arch = "wasm32"))]
pub async fn sleep(duration: core::time::Duration) {
    // Microsecond resolution, the driver's tick. Saturating, because a
    // duration past `u64` microseconds (584 000 years) means "forever".
    let micros = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
    embassy_time::Timer::after(embassy_time::Duration::from_micros(micros))
        .await;
}

/// `future` did not finish within its [`timeout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("timed out")]
pub struct Elapsed;

/// Run `future`, giving up after `duration`.
///
/// One definition for both targets: a race between `future` and [`sleep`],
/// which is itself the target's timer. A future that loses is dropped.
///
/// # Errors
/// [`Elapsed`] when `duration` passes first.
pub async fn timeout<F: core::future::Future>(
    duration: core::time::Duration,
    future: F,
) -> Result<F::Output, Elapsed> {
    use embassy_futures::select::{Either, select};

    match select(future, sleep(duration)).await {
        Either::First(output) => Ok(output),
        Either::Second(()) => Err(Elapsed),
    }
}

/// Sleep for `duration` — the timer the connection manager's poll loop
/// runs on (`embassy-time` natively, `setTimeout` in the browser).
#[cfg(target_arch = "wasm32")]
pub async fn sleep(duration: core::time::Duration) {
    let millis = i32::try_from(duration.as_millis()).unwrap_or(i32::MAX);
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        if let Some(window) = web_sys::window() {
            // A refused timer leaves the promise pending — the caller's
            // task idles rather than spinning; nothing to surface.
            let _ = window
                .set_timeout_with_callback_and_timeout_and_arguments_0(
                    &resolve, millis,
                );
        }
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::time::{Duration, Instant};

    use super::{Elapsed, key_nanos, sleep, timeout, unix_nanos, unix_secs};

    #[test]
    fn clock_is_past_2023_and_units_agree() {
        let secs = unix_secs();
        let nanos = unix_nanos();
        assert!(secs > 1_700_000_000, "clock reads before 2023: {secs}");
        // Taken microseconds apart; agree to within a second.
        assert!((nanos / 1_000_000_000).abs_diff(secs) <= 1);
    }

    #[test]
    fn key_nanos_never_collides_within_a_millisecond() {
        // Far more mints than one millisecond can hold clock-wise; every
        // key must still be distinct (the fused counter) and the sequence
        // strictly ordered (clock digits rise, counter digits rise).
        let keys: Vec<u64> = (0..10_000).map(|_| key_nanos()).collect();
        for pair in keys.windows(2) {
            assert!(pair[0] < pair[1], "{} !< {}", pair[0], pair[1]);
        }
        // The key stays wall-clock shaped: its millisecond half agrees
        // with the clock even though the low digits are counter digits.
        let key_ms = key_nanos() / 1_000_000;
        let clock_ms = unix_nanos() / 1_000_000;
        assert!(key_ms.abs_diff(clock_ms) < 1_000);
    }

    /// The timer is the driver's, not the executor's: a plain `block_on`
    /// with no runtime at all still wakes on time.
    #[test]
    fn sleep_wakes_without_a_runtime() {
        let start = Instant::now();
        futures::executor::block_on(sleep(Duration::from_millis(30)));
        assert!(start.elapsed() >= Duration::from_millis(30));
    }

    #[test]
    fn timeout_returns_the_output_when_the_future_wins() {
        let won = futures::executor::block_on(timeout(
            Duration::from_secs(5),
            async { 7 },
        ));
        assert_eq!(won, Ok(7));
    }

    #[test]
    fn timeout_is_elapsed_when_the_timer_wins() {
        let lost = futures::executor::block_on(timeout(
            Duration::from_millis(10),
            sleep(Duration::from_secs(5)),
        ));
        assert_eq!(lost, Err(Elapsed));
    }
}
