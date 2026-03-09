//! The auth half of the wire API: the four `#[server]` functions a client
//! calls before it knows its own tenant.
//!
//! Split from [`lib`](crate) for the file budget, along the seam the app
//! already has — everything here touches the global username registry, and
//! everything left behind runs inside one user's space.
//!
//! **These bodies never trust the connection's identity.** They are
//! `#[server(public)]`, so the caller is anonymous and picks its own tenant
//! (`Auth::Anonymous { tenant }`) with `user = U48::MAX`. Reading the registry
//! through that identity would mean a *different* registry per caller, and
//! every bootstrapped record would be stamped as authored by the anonymous
//! tier. So each body re-scopes explicitly: `helpers::USER_REGISTRY` for the
//! shared registry, and the new user's own `(tenant, tenant)` for their
//! records.
//!
//! `register` is where the cross-tenant seam lives: `as_identity` bootstraps
//! the new user's records from the public connection, which is a move no
//! client command can make — the node is the authority over identity, so the
//! seam exists server-side only.

use wavedb::prelude::*;

use crate::helpers::{
    USER_REGISTRY, ensure_registry, hash_password, new_tenant_id,
};
use crate::{Auth, Profile, Todo, UserEntry, UserEntrySecondaries};

/// Register a new user: allocate a tenant id, write the global `UserEntry`,
/// and bootstrap `Auth` + `Profile` (+ the todo collection) in the new
/// tenant's space. Returns the assigned tenant id — the client stores it and
/// reconnects as that tenant.
#[server(public)]
pub async fn register(
    db: &Db,
    username: String,
    password: String,
) -> Result<u64> {
    // The registry is app infrastructure, not caller data: pin it to its own
    // identity rather than to whatever tenant an anonymous caller named.
    let registry_id = U48::try_from(USER_REGISTRY)?;
    let registry_db = db.as_identity(registry_id, registry_id);
    let registry = ensure_registry(&registry_db).await?;
    let col = UserEntry::collection(registry.entries);

    // Scope the lookup stream so its borrow of `username` ends before the
    // insert consumes it.
    {
        let mut existing =
            std::pin::pin!(col.by_username(&registry_db, &username));
        if existing.next().await.is_some() {
            return Err(Error::already_exists("username already taken"));
        }
    }

    let tenant_id = new_tenant_id();
    col.insert(
        &registry_db,
        &UserEntry {
            username: username.clone(),
            tenant_id,
        },
    )
    .await?;

    // Bootstrap the new tenant's own records — the server-side seam
    // (`as_identity` never crosses the wire). Both halves move: the records
    // belong to the new user and are stamped as authored by them, not by the
    // anonymous caller that triggered the registration.
    let tenant = U48::try_from(tenant_id)?;
    let user_db = db.as_identity(tenant, tenant);
    Auth {
        password_hash: hash_password(&password),
    }
    .save(&user_db)
    .await?;
    let todos = Todo::create_pivot(&user_db).await?;
    Profile { username, todos }.save(&user_db).await?;

    Ok(tenant_id)
}

/// Verify credentials and open a session. Returns
/// `(tenant_id, token pair)`: the client reconnects with the access token
/// and keeps the refresh token to mint the next pair.
#[server(public)]
pub async fn login(
    db: &Db,
    username: String,
    password: String,
) -> Result<(u64, wavedb::TokenPair)> {
    let registry_id = U48::try_from(USER_REGISTRY)?;
    let registry_db = db.as_identity(registry_id, registry_id);
    let registry = ensure_registry(&registry_db).await?;
    let col = UserEntry::collection(registry.entries);

    let mut matches = std::pin::pin!(col.by_username(&registry_db, &username));
    let entry = matches
        .next()
        .await
        .ok_or_else(|| Error::not_found("user not found"))??;

    let tenant = U48::try_from(entry.tenant_id)?;
    let user_db = db.as_identity(tenant, tenant);
    let auth = Auth::get(&user_db)
        .await?
        .ok_or_else(|| Error::not_found("auth record missing"))?;
    if auth.view()?.password_hash != hash_password(&password) {
        return Err(Error::unauthorized("wrong password"));
    }

    let pair = wavedb::auth::issue_pair(&user_db, tenant).await?;
    Ok((entry.tenant_id, pair))
}

/// Trade a refresh token for the next pair (rotates it; a replayed token
/// revokes the whole session). Public: the caller's access token may
/// already be dead — the refresh token itself is the credential.
#[server(public)]
pub async fn refresh(
    db: &Db,
    tenant_id: u64,
    token: Vec<u8>,
) -> Result<wavedb::TokenPair> {
    // The session records this rotates belong to the user, so both halves of
    // the identity move — the refresh token is the credential being trusted
    // here, not the (anonymous) connection.
    let tenant = U48::try_from(tenant_id)?;
    let user_db = db.as_identity(tenant, tenant);
    wavedb::auth::refresh_pair(&user_db, &token).await
}

/// Revoke the session behind `token` (logout): its next refresh fails and
/// the outstanding access token dies within one TTL.
#[server(public)]
pub async fn logout(db: &Db, tenant_id: u64, token: Vec<u8>) -> Result<()> {
    let tenant = U48::try_from(tenant_id)?;
    let user_db = db.as_identity(tenant, tenant);
    wavedb::auth::revoke(&user_db, &token).await
}
