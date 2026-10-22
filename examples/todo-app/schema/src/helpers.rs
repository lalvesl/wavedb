//! Private helpers the `#[server]` bodies lean on — split from
//! [`lib`](crate) for the file budget.
//!
//! Every item here is **server-side only**. They exist to serve `#[server]`
//! bodies, so they carry `#[cfg(feature = "server-side")]` by hand and are
//! cfg-gated out of a client artifact along with the bodies themselves: that
//! is the schema author's half of the no-leak contract, and the macros cannot
//! do it for code that sits outside a `#[server]` fn.

use wavedb::prelude::*;

use crate::{AllUserNamesToTenants, Profile, UserEntry};

/// engine-local test.
#[cfg(feature = "server-side")]
pub async fn ensure_registry<D: DbHandle>(
    db: &D,
) -> core::result::Result<AllUserNamesToTenants, D::Error> {
    if let Some(r) = AllUserNamesToTenants::get(db).await? {
        return Ok(r);
    }
    let entries = UserEntry::create_pivot(db).await?;
    let r = AllUserNamesToTenants { entries };
    r.save(db).await?;
    Ok(r)
}

/// The caller tenant's profile — the root of the profile→pivot path.
#[cfg(feature = "server-side")]
pub async fn get_profile<D: DbHandle<Error = Error>>(
    db: &D,
) -> Result<Profile> {
    Profile::get(db)
        .await?
        .ok_or_else(|| Error::not_found("profile missing"))
}

#[cfg(feature = "server-side")]
pub fn hash_password(password: &str) -> String {
    use std::fmt::Write as _;

    use sha2::{Digest, Sha256};
    let digest = Sha256::new().chain_update(password).finalize();
    digest
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

/// The identity that owns the **global username registry**
/// (`AllUserNamesToTenants` + its `UserEntry` collection).
///
/// A fixed, arbitrary 48-bit constant, used as both user and tenant. The
/// registry is app-owned infrastructure, not any user's data, so it gets a
/// space of its own rather than living in whichever tenant the caller
/// happened to name — an anonymous caller picks its own tenant
/// (`Auth::Anonymous { tenant }`), so binding the registry to the connection
/// would let it read and write a *different* registry per caller. That is
/// not a WaveDB rule; the engine lets the app place its data wherever it
/// likes. It is this app choosing an explicit, isolated home for a shared
/// structure, which is what makes "who owns this?" answerable.
///
/// [`new_tenant_id`] never mints it, so no user's space can collide.
#[cfg(feature = "server-side")]
pub const USER_REGISTRY: u64 = 0x0000_A11C_0DE5;

/// Mint a 48-bit tenant id from the current nanosecond timestamp — a
/// placeholder allocator.
///
/// Two demo-grade caveats, stated because the value is an identity: masking
/// nanoseconds to 48 bits wraps roughly every 78 hours, so this is
/// collision-free only within one run, and two registrations inside the same
/// nanosecond tick collide outright. A real allocator would draw from the
/// registry. What it *does* guarantee is that [`USER_REGISTRY`] is never
/// handed out.
#[cfg(feature = "server-side")]
pub fn new_tenant_id() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let masked = nanos & u128::from(U48::MASK);
    // Masked to 48 bits, so the narrowing is infallible.
    let id = u64::try_from(masked).expect("48-bit value fits u64");
    // The registry's space is reserved: hand out its neighbour instead.
    if id == USER_REGISTRY { id + 1 } else { id }
}
