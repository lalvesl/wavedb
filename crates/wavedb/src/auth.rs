//! Session-backed token pairs — the M8 login machinery `#[server]` bodies
//! call.
//!
//! The **access token** is stateless (HMAC + TTL, verified per request by
//! the node's gate 1). The **refresh token** is bound to an [`AuthSession`]
//! record in the session tenant's space: it is stored **hashed**, rotated on
//! every use, and revocable with one record write. A replayed (already
//! rotated) refresh token is a theft signal — the session is revoked on the
//! spot, killing both halves within one access TTL.
//!
//! An app registers the two records with `store` entries (engine slots, no
//! wire surface) and drives the flow inside its own `#[server]` functions:
//!
//! ```text
//! expose_server! { fn login, fn refresh, …, store AuthSession, store AuthSessions }
//!
//! // in `login` (after verifying credentials), on the user's tenant:
//! let pair = wavedb::auth::issue_pair(&user_db, user).await?;
//! // in `refresh`:
//! let pair = wavedb::auth::refresh_pair(&user_db, old_refresh).await?;
//! ```

use sha2::{Digest, Sha256};
use wavedb_core::{DbHandle, Id, U48, WaveDbStruct};
use wavedb_macros::wavedb;
use wavedb_net::auth::{
    AccessClaims, TokenPurpose, node_secret, sign, unix_now, verify,
};

use crate::error::{Error, Result};

/// Access-token lifetime: 15 minutes.
pub const ACCESS_TTL_SECS: u64 = 15 * 60;
/// Refresh-token lifetime: 30 days.
pub const REFRESH_TTL_SECS: u64 = 30 * 24 * 60 * 60;

/// One login session (NonUnique — one record per live session). The record
/// holds only the refresh token's **hash**: reading the store never yields a
/// usable token.
#[wavedb(NonUnique)]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AuthSession {
    /// The authenticated user the session vouches for.
    pub user: u64,
    /// sha256 of the currently valid refresh token — rotated on every use.
    pub refresh_hash: Vec<u8>,
    /// Unix seconds the session was opened.
    pub issued: u64,
    /// A revoked session refuses every refresh (one record write).
    pub revoked: bool,
}

/// The tenant's session collection anchor (Unique).
#[wavedb]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthSessions {
    /// The [`AuthSession`] collection handle.
    pub sessions: <AuthSession as WaveDbStruct>::PivotId,
}

/// What a login/refresh hands the client: the short-lived access token and
/// the rotating refresh token. Wire-encodable, so a `#[server]` fn returns
/// it directly.
#[derive(Debug, Clone, PartialEq, Eq, wavedb_core::WaveWire)]
pub struct TokenPair {
    /// Authenticates every request for [`ACCESS_TTL_SECS`].
    pub access: Vec<u8>,
    /// Mints the next pair (once); dies on use, replay revokes the session.
    pub refresh: Vec<u8>,
}

/// A fixed signature tag so `TokenPair` can ride `#[server]` fn signatures
/// like a builtin (its shape is part of the platform, not an app schema).
impl wavedb_core::FnArgTag for TokenPair {
    const TAG: u64 = 0x5741_5645_4442_5450; // "WAVEDBTP"
}

fn secret() -> Result<&'static [u8; 32]> {
    node_secret()
        .ok_or_else(|| Error::unauthorized("node has no signing secret"))
}

fn sha256(bytes: &[u8]) -> Vec<u8> {
    Sha256::new().chain_update(bytes).finalize().to_vec()
}

/// The tenant's session anchor, created on first use.
async fn ensure_sessions<D: DbHandle<Error = Error>>(
    db: &D,
) -> Result<AuthSessions> {
    if let Some(anchor) = AuthSessions::get(db).await? {
        return Ok(anchor.try_into()?);
    }
    let sessions = AuthSession::create_pivot(db).await?;
    let anchor = AuthSessions { sessions };
    anchor.save(db).await?;
    Ok(anchor)
}

/// Sign the two halves of a pair for `user` under `db`'s tenant, bound to
/// the session record at `session`.
fn sign_pair(db_tenant: U48, user: U48, session: u128) -> Result<TokenPair> {
    // Two pairs minted within the same second must still differ (rotation
    // compares refresh-token hashes) — a process counter uniquifies.
    static NONCE: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);
    let secret = secret()?;
    let now = unix_now();
    let nonce = NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let claims = |expires_at, purpose| AccessClaims {
        user,
        tenant: db_tenant,
        expires_at,
        purpose,
        session,
        nonce,
    };
    Ok(TokenPair {
        access: sign(
            secret,
            &claims(now + ACCESS_TTL_SECS, TokenPurpose::Access),
        ),
        refresh: sign(
            secret,
            &claims(now + REFRESH_TTL_SECS, TokenPurpose::Refresh),
        ),
    })
}

/// Open a new session for `user` under `db`'s tenant and return its token
/// pair. Called inside a login `#[server]` body **after** the credentials
/// checked out.
///
/// # Errors
/// A store/transport fault, or a node with no signing secret.
pub async fn issue_pair<D: DbHandle<Error = Error>>(
    db: &D,
    user: U48,
) -> Result<TokenPair> {
    let anchor = ensure_sessions(db).await?;
    let col = AuthSession::collection(anchor.sessions);
    let id = col
        .insert(
            db,
            &AuthSession {
                user: user.get(),
                refresh_hash: Vec::new(),
                issued: unix_now(),
                revoked: false,
            },
        )
        .await?;
    let pair = sign_pair(db.tenant(), user, id.raw())?;
    // Bind the session to this refresh token (hash only).
    col.save(
        db,
        id,
        &AuthSession {
            user: user.get(),
            refresh_hash: sha256(&pair.refresh),
            issued: unix_now(),
            revoked: false,
        },
    )
    .await?;
    Ok(pair)
}

/// Trade a refresh token for the next pair, rotating it: the old token dies
/// with the session record's hash update.
///
/// A replayed token (hash mismatch — someone already rotated it) **revokes
/// the session** before refusing; a revoked/expired/forged token just
/// refuses.
///
/// # Errors
/// [`Error::Unauthorized`] on any bad token or revoked session; a
/// store/transport fault otherwise.
pub async fn refresh_pair<D: DbHandle<Error = Error>>(
    db: &D,
    refresh: &[u8],
) -> Result<TokenPair> {
    let unauthorized = || Error::unauthorized("invalid refresh token");
    let claims = verify(secret()?, refresh, unix_now(), TokenPurpose::Refresh)
        .map_err(|_| unauthorized())?;
    // The session lives in the tenant the token was minted for — a handle
    // scoped anywhere else must not mint (nor probe) here.
    if claims.tenant != db.tenant() {
        return Err(unauthorized());
    }

    let anchor = ensure_sessions(db).await?;
    let col = AuthSession::collection(anchor.sessions);
    let id = Id::from_raw(claims.session);
    let mut session: AuthSession = col
        .get(db, id)
        .await?
        .ok_or_else(unauthorized)?
        .try_into()?;
    if session.revoked {
        return Err(unauthorized());
    }
    if session.refresh_hash != sha256(refresh) {
        // Replay: this token was already rotated away — theft signal.
        session.revoked = true;
        col.save(db, id, &session).await?;
        return Err(unauthorized());
    }

    let pair = sign_pair(db.tenant(), claims.user, claims.session)?;
    session.refresh_hash = sha256(&pair.refresh);
    col.save(db, id, &session).await?;
    Ok(pair)
}

/// Revoke the session behind `refresh` (logout / kill-switch): its next
/// refresh fails, and the outstanding access token dies within its TTL.
/// Idempotent; a token that never matched a session is a no-op.
///
/// # Errors
/// A store/transport fault. A malformed token is **not** an error.
pub async fn revoke<D: DbHandle<Error = Error>>(
    db: &D,
    refresh: &[u8],
) -> Result<()> {
    // Expired tokens still name their session — revoke ignores expiry by
    // verifying only the signature via a far-future `now` bound? No: decode
    // strictly; a token too old to verify names a session past its refresh
    // TTL anyway (it can no longer mint).
    let Ok(claims) =
        verify(secret()?, refresh, unix_now(), TokenPurpose::Refresh)
    else {
        return Ok(());
    };
    let anchor = ensure_sessions(db).await?;
    let col = AuthSession::collection(anchor.sessions);
    let id = Id::from_raw(claims.session);
    if let Some(wire) = col.get(db, id).await?
        && !wire.view()?.revoked
    {
        let mut session: AuthSession = wire.try_into()?;
        session.revoked = true;
        col.save(db, id, &session).await?;
    }
    Ok(())
}
