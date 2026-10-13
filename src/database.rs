use std::{
    fs::{File, OpenOptions},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};

use crate::amount::validate_zatoshis;

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

// A stale in-flight claim may have reached the network, so only an operator can resolve it.
const STALE_SUBMISSION_MILLIS: i64 = 10 * 60 * 1_000;
const COMPLETED_CLAIM_RETENTION_MILLIS: i64 = 90 * 24 * 60 * 60 * 1_000;

#[derive(Clone)]
pub struct Database(Arc<DatabaseInner>);

struct DatabaseInner {
    connection: Mutex<Connection>,
    _instance_lock: Option<File>,
    path: std::path::PathBuf,
}

#[derive(Clone, Copy)]
enum OpenMode {
    Bot,
    Operator,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ClaimDecision {
    Reserved { id: i64, amount_zatoshis: i64 },
    Paused,
    InProgress,
    NeedsReconciliation,
    CooldownRemaining(i64),
    DailyLimitReached(Option<i64>),
}

pub enum PayoutOutcome {
    Paid(String),
    Failed,
    Unknown,
}

pub enum Reconciliation {
    Paid(String),
    Unpaid,
}

pub struct UnresolvedClaim {
    pub user_id: String,
    pub destination: String,
    pub created_at: i64,
    pub amount_zatoshis: i64,
}

pub struct UnresolvedClaimSummary {
    pub id: i64,
    pub user_id: String,
    pub status: String,
    pub created_at: i64,
    pub amount_zatoshis: i64,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_mode(path, OpenMode::Bot)
    }

    pub fn open_operator(path: &Path) -> Result<Self> {
        Self::open_with_mode(path, OpenMode::Operator)
    }

    fn open_with_mode(path: &Path, mode: OpenMode) -> Result<Self> {
        if matches!(mode, OpenMode::Bot)
            && let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("could not create database directory {}", parent.display())
            })?;
        }
        if matches!(mode, OpenMode::Operator) {
            anyhow::ensure!(
                path.is_file(),
                "database does not exist: {}",
                path.display()
            );
        }
        let instance_lock = if matches!(mode, OpenMode::Bot) {
            Some(try_open_lock_file(path, ".lock")?)
        } else {
            None
        };
        let open_flags = if matches!(mode, OpenMode::Bot) {
            let mut database_file = OpenOptions::new();
            database_file
                .create(true)
                .truncate(false)
                .read(true)
                .write(true);
            #[cfg(unix)]
            database_file.mode(0o600);
            let file = database_file
                .open(path)
                .with_context(|| format!("could not open database file {}", path.display()))?;
            drop(file);
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
        } else {
            OpenFlags::SQLITE_OPEN_READ_WRITE
        };
        let mut conn = Connection::open_with_flags(path, open_flags)
            .with_context(|| format!("could not open database {}", path.display()))?;
        #[cfg(unix)]
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("could not secure database file {}", path.display()))?;
        conn.busy_timeout(Duration::from_secs(10))?;
        let journal_mode: String =
            conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
        if journal_mode != "wal" {
            conn.pragma_update(None, "journal_mode", "WAL")?;
        }
        conn.pragma_update(None, "synchronous", "FULL")?;

        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let schema_version: i64 = tx.pragma_query_value(None, "user_version", |row| row.get(0))?;
        match mode {
            OpenMode::Bot => {
                anyhow::ensure!(
                    schema_version <= 1,
                    "database schema is newer than this bot"
                );
                if schema_version < 1 {
                    tx.execute_batch(include_str!("../migrations/0001_initial.sql"))?;
                    tx.pragma_update(None, "user_version", 1)?;
                }
            }
            OpenMode::Operator => anyhow::ensure!(
                schema_version == 1,
                "database schema does not match this operator; start the matching bot to migrate it"
            ),
        }
        tx.commit()?;
        Ok(Self(Arc::new(DatabaseInner {
            connection: Mutex::new(conn),
            _instance_lock: instance_lock,
            path: path.to_owned(),
        })))
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.0
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("database lock poisoned"))
    }

    pub fn acquire_control_lock(&self) -> Result<File> {
        open_lock_file(&self.0.path, ".control.lock")
    }

    pub fn try_acquire_payout_lock(&self) -> Result<Option<File>> {
        let mut options = OpenOptions::new();
        options.create(true).truncate(false).read(true).write(true);
        #[cfg(unix)]
        options.mode(0o600);
        let path = append_suffix(&self.0.path, ".payout.lock");
        let file = options
            .open(&path)
            .with_context(|| format!("could not open payout lock {}", path.display()))?;
        #[cfg(unix)]
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(file)),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error).context("could not acquire payout coordination lock"),
        }
    }

    pub fn reserve(
        &self,
        user_id: &str,
        destination: &str,
        now: i64,
        cooldown: i64,
        daily_cap: i64,
    ) -> Result<ClaimDecision> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let paused: String = tx.query_row(
            "SELECT value FROM settings WHERE name = 'paused'",
            [],
            |row| row.get(0),
        )?;
        match paused.as_str() {
            "1" => {
                tx.commit()?;
                return Ok(ClaimDecision::Paused);
            }
            "0" => {}
            _ => anyhow::bail!("persistent pause setting is invalid; refusing claims"),
        }

        let amount: String = tx.query_row(
            "SELECT value FROM settings WHERE name = 'payout_amount_zatoshis'",
            [],
            |row| row.get(0),
        )?;
        let amount_zatoshis = amount
            .parse::<i64>()
            .context("stored payout amount is not an integer number of zatoshis")?;
        validate_zatoshis(amount_zatoshis).context("invalid stored payout amount")?;
        validate_zatoshis(daily_cap).context("invalid daily payout cap")?;

        let unresolved: Option<(String, i64)> = tx.query_row(
            "SELECT status, updated_at FROM claims WHERE user_id = ?1 AND status IN ('unknown', 'submitting') ORDER BY created_at DESC LIMIT 1",
            [user_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;

        if let Some((status, updated_at)) = unresolved {
            tx.commit()?;
            return Ok(
                if status == "unknown" || updated_at <= now.saturating_sub(STALE_SUBMISSION_MILLIS)
                {
                    ClaimDecision::NeedsReconciliation
                } else {
                    ClaimDecision::InProgress
                },
            );
        }

        let previous_success: Option<i64> = tx.query_row(
            "SELECT updated_at FROM claims WHERE user_id = ?1 AND status = 'succeeded' ORDER BY updated_at DESC LIMIT 1",
            [user_id],
            |row| row.get(0),
        ).optional()?;

        if let Some(completed_at) = previous_success {
            let remaining = cooldown.saturating_sub(now.saturating_sub(completed_at));
            if remaining > 0 {
                tx.commit()?;
                return Ok(ClaimDecision::CooldownRemaining(
                    remaining.saturating_add(999) / 1_000,
                ));
            }
        }

        let daily_spend: i64 = tx.query_row(
            "SELECT COALESCE(SUM(amount_zatoshis), 0) FROM claims
             WHERE status IN ('submitting', 'unknown')
                OR (status = 'succeeded' AND updated_at > ?1)",
            [now.saturating_sub(cooldown)],
            |row| row.get(0),
        )?;
        if amount_zatoshis > daily_cap.saturating_sub(daily_spend) {
            let release_needed = daily_spend
                .saturating_add(amount_zatoshis)
                .saturating_sub(daily_cap);
            let retry_after_seconds = {
                let mut statement = tx.prepare(
                    "SELECT updated_at, amount_zatoshis FROM claims
                     WHERE status = 'succeeded' AND updated_at > ?1
                     ORDER BY updated_at",
                )?;
                let rows = statement.query_map([now.saturating_sub(cooldown)], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                })?;
                let mut released = 0_i64;
                let mut retry_after = None;
                for row in rows {
                    let (completed_at, amount) = row?;
                    released = released.saturating_add(amount);
                    if released >= release_needed {
                        let remaining_millis =
                            completed_at.saturating_add(cooldown).saturating_sub(now);
                        retry_after = Some(remaining_millis.saturating_add(999) / 1_000);
                        break;
                    }
                }
                retry_after
            };
            tx.commit()?;
            return Ok(ClaimDecision::DailyLimitReached(retry_after_seconds));
        }

        tx.execute(
            "INSERT INTO claims (user_id, destination, status, created_at, updated_at, amount_zatoshis) VALUES (?1, ?2, 'submitting', ?3, ?3, ?4)",
            params![user_id, destination, now, amount_zatoshis],
        )?;
        let id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(ClaimDecision::Reserved {
            id,
            amount_zatoshis,
        })
    }

    pub fn finish(&self, id: i64, outcome: PayoutOutcome, now: i64) -> Result<()> {
        let (status, txid) = match outcome {
            PayoutOutcome::Paid(txid) => {
                let txid = normalize_txid(txid)?;
                ("succeeded", Some(txid))
            }
            PayoutOutcome::Failed => ("failed", None),
            PayoutOutcome::Unknown => ("unknown", None),
        };
        let conn = self.connection()?;
        let changed = conn.execute(
            "UPDATE claims SET status = ?1, txid = ?2, destination = CASE WHEN ?1 = 'unknown' THEN destination ELSE NULL END, updated_at = ?3 WHERE id = ?4 AND status = 'submitting'",
            params![status, txid, now, id],
        )?;
        anyhow::ensure!(changed == 1, "claim {} was not in submitting state", id);
        Ok(())
    }

    pub fn recover_on_startup(&self) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute("UPDATE settings SET value = '1' WHERE name = 'paused'", [])?;
        anyhow::ensure!(changed == 1, "persistent pause setting is missing");
        tx.execute(
            "UPDATE claims SET status = 'unknown', updated_at = CAST(unixepoch('subsec') * 1000 AS INTEGER) WHERE status = 'submitting'",
            [],
        ).context("could not mark interrupted payouts for reconciliation")?;
        tx.execute(
            "DELETE FROM claims WHERE status IN ('succeeded', 'failed') AND updated_at < CAST(unixepoch('subsec') * 1000 AS INTEGER) - ?1",
            [COMPLETED_CLAIM_RETENTION_MILLIS],
        ).context("could not prune old completed claims")?;
        tx.commit()
            .context("could not commit startup claim recovery")
    }

    pub fn paused(&self) -> Result<bool> {
        let conn = self.connection()?;
        let paused: String = conn.query_row(
            "SELECT value FROM settings WHERE name = 'paused'",
            [],
            |row| row.get(0),
        )?;
        match paused.as_str() {
            "1" => Ok(true),
            "0" => Ok(false),
            _ => anyhow::bail!("persistent pause setting is invalid"),
        }
    }

    pub fn set_paused(&self, paused: bool) -> Result<()> {
        let conn = self.connection()?;
        let changed = conn.execute(
            "UPDATE settings SET value = ?1 WHERE name = 'paused'",
            [if paused { "1" } else { "0" }],
        )?;
        anyhow::ensure!(changed == 1, "persistent pause setting is missing");
        Ok(())
    }

    pub fn payout_amount_zatoshis(&self) -> Result<i64> {
        let conn = self.connection()?;
        let value: String = conn.query_row(
            "SELECT value FROM settings WHERE name = 'payout_amount_zatoshis'",
            [],
            |row| row.get(0),
        )?;
        let amount = value
            .parse::<i64>()
            .context("stored payout amount is not an integer number of zatoshis")?;
        validate_zatoshis(amount).context("invalid stored payout amount")?;
        Ok(amount)
    }

    pub fn set_payout_amount_zatoshis(&self, amount: i64) -> Result<()> {
        validate_zatoshis(amount)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let paused: String = tx.query_row(
            "SELECT value FROM settings WHERE name = 'paused'",
            [],
            |row| row.get(0),
        )?;
        anyhow::ensure!(paused == "1", "pause payouts before changing the amount");
        let changed = tx.execute(
            "UPDATE settings SET value = ?1 WHERE name = 'payout_amount_zatoshis'",
            [amount.to_string()],
        )?;
        anyhow::ensure!(changed == 1, "persistent payout amount setting is missing");
        tx.commit()?;
        Ok(())
    }

    pub fn unresolved_claims(&self, before_id: Option<i64>) -> Result<Vec<UnresolvedClaimSummary>> {
        let conn = self.connection()?;
        let mut statement = conn.prepare(
            "SELECT id, user_id, status, created_at, amount_zatoshis FROM claims
             WHERE (status = 'unknown'
                OR (status = 'submitting' AND updated_at <= CAST(unixepoch('subsec') * 1000 AS INTEGER) - ?1))
             AND (?2 IS NULL OR id < ?2)
             ORDER BY id DESC LIMIT 10",
        )?;
        let rows = statement.query_map(params![STALE_SUBMISSION_MILLIS, before_id], |row| {
            Ok(UnresolvedClaimSummary {
                id: row.get(0)?,
                user_id: row.get(1)?,
                status: row.get(2)?,
                created_at: row.get(3)?,
                amount_zatoshis: row.get(4)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("could not read unresolved claims")
    }

    pub fn unresolved_count(&self) -> Result<i64> {
        let conn = self.connection()?;
        conn.query_row(
            "SELECT COUNT(*) FROM claims
             WHERE status = 'unknown'
                OR (status = 'submitting' AND updated_at <= CAST(unixepoch('subsec') * 1000 AS INTEGER) - ?1)",
            [STALE_SUBMISSION_MILLIS],
            |row| row.get(0),
        )
        .context("could not count unresolved claims")
    }

    pub fn unresolved_claim(&self, id: i64) -> Result<Option<UnresolvedClaim>> {
        let conn = self.connection()?;
        conn.query_row(
            "SELECT user_id, COALESCE(destination, ''), created_at, amount_zatoshis FROM claims
             WHERE id = ?1 AND (status = 'unknown'
                OR (status = 'submitting' AND updated_at <= CAST(unixepoch('subsec') * 1000 AS INTEGER) - ?2))",
            params![id, STALE_SUBMISSION_MILLIS],
            |row| {
                Ok(UnresolvedClaim {
                    user_id: row.get(0)?,
                    destination: row.get(1)?,
                    created_at: row.get(2)?,
                    amount_zatoshis: row.get(3)?,
                })
            },
        ).optional().context("could not read unresolved claim")
    }

    pub fn reconcile(&self, id: i64, reconciliation: Reconciliation, now: i64) -> Result<()> {
        let (status, txid) = match reconciliation {
            Reconciliation::Paid(txid) => {
                let txid = normalize_txid(txid)?;
                ("succeeded", Some(txid))
            }
            Reconciliation::Unpaid => ("failed", None),
        };
        let conn = self.connection()?;
        let changed = conn.execute(
            "UPDATE claims SET status = ?1, txid = ?2, destination = NULL, updated_at = ?3
             WHERE id = ?4 AND (status = 'unknown'
                OR (status = 'submitting' AND updated_at <= ?3 - ?5))",
            params![status, txid, now, id, STALE_SUBMISSION_MILLIS],
        )?;
        anyhow::ensure!(changed == 1, "claim does not exist or is not unresolved");
        Ok(())
    }
}

fn append_suffix(path: &Path, suffix: &str) -> std::path::PathBuf {
    let mut path = path.as_os_str().to_owned();
    path.push(suffix);
    path.into()
}

fn open_lock_file(database_path: &Path, suffix: &str) -> Result<File> {
    let file = create_lock_file(database_path, suffix)?;
    file.lock_exclusive().with_context(|| {
        format!(
            "could not acquire coordination lock {}",
            append_suffix(database_path, suffix).display()
        )
    })?;
    Ok(file)
}

fn try_open_lock_file(database_path: &Path, suffix: &str) -> Result<File> {
    let file = create_lock_file(database_path, suffix)?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(file),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Err(anyhow::anyhow!(
            "another bot process already owns database {}",
            database_path.display()
        )),
        Err(error) => Err(error).context("could not acquire bot instance lock"),
    }
}

fn create_lock_file(database_path: &Path, suffix: &str) -> Result<File> {
    let path = append_suffix(database_path, suffix);
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let file = options
        .open(&path)
        .with_context(|| format!("could not open coordination lock {}", path.display()))?;
    #[cfg(unix)]
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("could not secure coordination lock {}", path.display()))?;
    Ok(file)
}

fn valid_txid(txid: &str) -> bool {
    txid.len() == 64 && txid.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn normalize_txid(txid: String) -> Result<String> {
    anyhow::ensure!(valid_txid(&txid), "invalid payout transaction ID");
    Ok(txid.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::amount::MAX_AMOUNT_ZATOSHIS;
    use std::sync::atomic::{AtomicU64, Ordering};

    const DEFAULT_PAYOUT_ZATOSHIS: i64 = 10_000_000;
    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    fn path() -> std::path::PathBuf {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("taz-faucet-{}-{nonce}.sqlite", std::process::id()))
    }

    fn cleanup(path: &Path) {
        for suffix in ["", "-shm", "-wal", ".lock", ".control.lock", ".payout.lock"] {
            let _ = std::fs::remove_file(if suffix.is_empty() {
                path.to_owned()
            } else {
                append_suffix(path, suffix)
            });
        }
    }

    #[test]
    fn successful_claim_uses_a_rolling_cooldown_from_completion_and_failed_claim_does_not() {
        let path = path();
        let db = Database::open(&path).unwrap();
        db.set_paused(false).unwrap();
        let now = 1_800_000_000_000;
        let ClaimDecision::Reserved { id: failed, .. } = db
            .reserve("user", "address", now, 86_400_000, MAX_AMOUNT_ZATOSHIS)
            .unwrap()
        else {
            panic!("first claim should reserve")
        };
        db.finish(failed, PayoutOutcome::Failed, now + 1).unwrap();

        let ClaimDecision::Reserved { id: paid, .. } = db
            .reserve("user", "address", now + 2, 86_400_000, MAX_AMOUNT_ZATOSHIS)
            .unwrap()
        else {
            panic!("a rejected payout must not consume the cooldown")
        };
        let txid = "a".repeat(64);
        let completed_at = now + 3;
        db.finish(paid, PayoutOutcome::Paid(txid), completed_at)
            .unwrap();
        let saved: i64 =
            db.0.connection
                .lock()
                .unwrap()
                .query_row(
                    "SELECT created_at FROM claims WHERE id = ?1",
                    [paid],
                    |row| row.get(0),
                )
                .unwrap();
        assert_eq!(saved, now + 2);
        assert_eq!(
            db.reserve(
                "user",
                "address",
                completed_at + 86_400_000 - 1,
                86_400_000,
                MAX_AMOUNT_ZATOSHIS
            )
            .unwrap(),
            ClaimDecision::CooldownRemaining(1)
        );
        assert!(matches!(
            db.reserve(
                "user",
                "address",
                completed_at + 86_400_000,
                86_400_000,
                MAX_AMOUNT_ZATOSHIS
            )
            .unwrap(),
            ClaimDecision::Reserved { .. }
        ));
        drop(db);
        cleanup(&path);
    }

    #[test]
    fn daily_cap_reserves_concurrent_claims_and_releases_only_definite_failures() {
        let path = path();
        let db = Database::open(&path).unwrap();
        db.set_paused(false).unwrap();
        let cap = DEFAULT_PAYOUT_ZATOSHIS;
        let now = chrono::Utc::now().timestamp_millis();

        let ClaimDecision::Reserved { id, .. } = db
            .reserve("first", "recipient", now, 86_400_000, cap)
            .unwrap()
        else {
            panic!("first claim should reserve the daily budget")
        };
        assert_eq!(
            db.reserve("second", "recipient", now + 1, 86_400_000, cap)
                .unwrap(),
            ClaimDecision::DailyLimitReached(None)
        );

        db.finish(id, PayoutOutcome::Failed, now + 2).unwrap();
        assert!(matches!(
            db.reserve("second", "recipient", now + 3, 86_400_000, cap)
                .unwrap(),
            ClaimDecision::Reserved { .. }
        ));
        cleanup(&path);
    }

    #[test]
    fn daily_cap_reports_when_successful_payouts_release_enough_capacity() {
        let path = path();
        let db = Database::open(&path).unwrap();
        db.set_paused(false).unwrap();
        let now = 1_800_000_000_000;
        let cooldown = 86_400_000;
        let ClaimDecision::Reserved { id, .. } = db
            .reserve("first", "recipient", now, cooldown, DEFAULT_PAYOUT_ZATOSHIS)
            .unwrap()
        else {
            panic!("first claim should reserve the daily budget")
        };
        let completed_at = now + 1_000;
        db.finish(id, PayoutOutcome::Paid("a".repeat(64)), completed_at)
            .unwrap();

        assert_eq!(
            db.reserve(
                "second",
                "recipient",
                now + 2_000,
                cooldown,
                DEFAULT_PAYOUT_ZATOSHIS
            )
            .unwrap(),
            ClaimDecision::DailyLimitReached(Some(86_399))
        );
        assert!(matches!(
            db.reserve(
                "second",
                "recipient",
                completed_at + cooldown,
                cooldown,
                DEFAULT_PAYOUT_ZATOSHIS
            )
            .unwrap(),
            ClaimDecision::Reserved { .. }
        ));
        cleanup(&path);
    }

    #[test]
    fn concurrent_distinct_users_cannot_exceed_the_daily_cap() {
        let path = path();
        let db = Database::open(&path).unwrap();
        db.set_paused(false).unwrap();
        let second_connection = Database::open_operator(&path).unwrap();
        let request_count = 200;
        let successful_claims = 100;
        let barrier = Arc::new(std::sync::Barrier::new(request_count + 1));
        let workers = (0..request_count)
            .map(|index| {
                let db = if index % 2 == 0 {
                    db.clone()
                } else {
                    second_connection.clone()
                };
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    db.reserve(
                        &format!("user-{index}"),
                        "recipient",
                        1_800_000_000_000,
                        86_400_000,
                        DEFAULT_PAYOUT_ZATOSHIS * successful_claims as i64,
                    )
                    .unwrap()
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let decisions = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            decisions
                .iter()
                .filter(|decision| matches!(decision, ClaimDecision::Reserved { .. }))
                .count(),
            successful_claims
        );
        assert_eq!(
            decisions
                .iter()
                .filter(|decision| **decision == ClaimDecision::DailyLimitReached(None))
                .count(),
            request_count - successful_claims
        );
        drop(second_connection);
        drop(db);
        cleanup(&path);
    }

    #[test]
    fn daily_cap_counts_ambiguous_payouts_until_manual_reconciliation() {
        let path = path();
        let db = Database::open(&path).unwrap();
        db.set_paused(false).unwrap();
        let cap = DEFAULT_PAYOUT_ZATOSHIS;
        let now = chrono::Utc::now().timestamp_millis();
        let ClaimDecision::Reserved { id, .. } = db
            .reserve("first", "recipient", now, 86_400_000, cap)
            .unwrap()
        else {
            panic!("claim should reserve")
        };
        db.finish(id, PayoutOutcome::Unknown, now + 1).unwrap();
        assert_eq!(
            db.reserve("second", "recipient", now + 2, 86_400_000, cap)
                .unwrap(),
            ClaimDecision::DailyLimitReached(None)
        );
        db.reconcile(id, Reconciliation::Unpaid, now + 3).unwrap();
        assert!(matches!(
            db.reserve("second", "recipient", now + 4, 86_400_000, cap)
                .unwrap(),
            ClaimDecision::Reserved { .. }
        ));
        cleanup(&path);
    }

    #[test]
    fn payout_amount_changes_only_while_paused_and_is_snapshotted_per_claim() {
        let path = path();
        let db = Database::open(&path).unwrap();
        assert_eq!(
            db.payout_amount_zatoshis().unwrap(),
            DEFAULT_PAYOUT_ZATOSHIS
        );
        db.set_payout_amount_zatoshis(25_000_000).unwrap();
        db.set_paused(false).unwrap();
        assert!(db.set_payout_amount_zatoshis(50_000_000).is_err());

        let now = chrono::Utc::now().timestamp_millis();
        let ClaimDecision::Reserved {
            id,
            amount_zatoshis,
        } = db
            .reserve(
                "amount-user",
                "recipient",
                now,
                86_400_000,
                MAX_AMOUNT_ZATOSHIS,
            )
            .unwrap()
        else {
            panic!("claim should reserve")
        };
        assert_eq!(amount_zatoshis, 25_000_000);
        db.finish(id, PayoutOutcome::Unknown, now + 1).unwrap();
        assert_eq!(
            db.unresolved_claim(id).unwrap().unwrap().amount_zatoshis,
            25_000_000
        );

        db.set_paused(true).unwrap();
        db.set_payout_amount_zatoshis(50_000_000).unwrap();
        assert_eq!(db.payout_amount_zatoshis().unwrap(), 50_000_000);
        assert_eq!(
            db.unresolved_claim(id).unwrap().unwrap().amount_zatoshis,
            25_000_000
        );
        drop(db);
        cleanup(&path);
    }

    #[test]
    fn ambiguous_claim_cannot_be_retried_until_reconciled() {
        let path = path();
        let db = Database::open(&path).unwrap();
        db.set_paused(false).unwrap();
        let now = chrono::Utc::now().timestamp_millis();
        let ClaimDecision::Reserved { id, .. } = db
            .reserve("user", "address", now, 86_400_000, MAX_AMOUNT_ZATOSHIS)
            .unwrap()
        else {
            panic!("claim should reserve")
        };
        assert_eq!(
            db.reserve(
                "user",
                "other-address",
                now + 1,
                86_400_000,
                MAX_AMOUNT_ZATOSHIS
            )
            .unwrap(),
            ClaimDecision::InProgress
        );
        db.finish(id, PayoutOutcome::Unknown, now + 1).unwrap();
        assert!(matches!(
            db.reserve("user", "address", now + 2, 86_400_000, MAX_AMOUNT_ZATOSHIS)
                .unwrap(),
            ClaimDecision::NeedsReconciliation
        ));
        let txid = "b".repeat(64);
        db.reconcile(id, Reconciliation::Paid(txid), now + 3)
            .unwrap();
        assert!(matches!(
            db.reserve(
                "user",
                "address",
                now + 86_399_999,
                86_400_000,
                MAX_AMOUNT_ZATOSHIS
            )
            .unwrap(),
            ClaimDecision::CooldownRemaining(1)
        ));
        drop(db);
        cleanup(&path);
    }

    #[test]
    fn simultaneous_reservations_for_one_user_create_only_one_claim() {
        let path = path();
        let db = Database::open(&path).unwrap();
        db.set_paused(false).unwrap();
        let second_connection = Database::open_operator(&path).unwrap();
        second_connection.set_paused(false).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let workers = [db.clone(), second_connection.clone()]
            .into_iter()
            .map(|db| {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    db.reserve(
                        "same-user",
                        "recipient",
                        1_800_000_000_000,
                        86_400_000,
                        MAX_AMOUNT_ZATOSHIS,
                    )
                    .unwrap()
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let decisions = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            decisions
                .iter()
                .filter(|decision| matches!(decision, ClaimDecision::Reserved { .. }))
                .count(),
            1
        );
        assert_eq!(
            decisions
                .iter()
                .filter(|decision| **decision == ClaimDecision::InProgress)
                .count(),
            1
        );
        let conn = db.0.connection.lock().unwrap();
        let claim_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM claims", [], |row| row.get(0))
            .unwrap();
        assert_eq!(claim_count, 1);
        drop(conn);
        drop(second_connection);
        drop(db);
        cleanup(&path);
    }

    #[test]
    fn startup_recovery_pauses_and_prunes_by_completion_time() {
        let path = path();
        let db = Database::open(&path).unwrap();
        db.set_paused(false).unwrap();
        let now = chrono::Utc::now().timestamp_millis();
        let old = now - COMPLETED_CLAIM_RETENTION_MILLIS - 1_000;

        let ClaimDecision::Reserved { id: retained, .. } = db
            .reserve(
                "recently-completed",
                "recipient",
                old,
                86_400_000,
                MAX_AMOUNT_ZATOSHIS,
            )
            .unwrap()
        else {
            panic!("claim should reserve")
        };
        db.finish(retained, PayoutOutcome::Paid("a".repeat(64)), now)
            .unwrap();

        let ClaimDecision::Reserved { id: pruned, .. } = db
            .reserve(
                "long-completed",
                "recipient",
                old - 1,
                86_400_000,
                MAX_AMOUNT_ZATOSHIS,
            )
            .unwrap()
        else {
            panic!("claim should reserve")
        };
        db.finish(pruned, PayoutOutcome::Failed, old).unwrap();

        db.recover_on_startup().unwrap();
        assert!(db.paused().unwrap());
        let conn = db.0.connection.lock().unwrap();
        let retained_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM claims WHERE id = ?1",
                [retained],
                |row| row.get(0),
            )
            .unwrap();
        let pruned_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM claims WHERE id = ?1",
                [pruned],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained_count, 1);
        assert_eq!(pruned_count, 0);
        drop(conn);
        drop(db);
        cleanup(&path);
    }

    #[test]
    fn one_transaction_cannot_reconcile_multiple_claims() {
        let path = path();
        let db = Database::open(&path).unwrap();
        db.set_paused(false).unwrap();
        let now = chrono::Utc::now().timestamp_millis();
        let mut ids = Vec::new();
        for user in ["first", "second"] {
            let ClaimDecision::Reserved { id, .. } = db
                .reserve(user, "recipient", now, 86_400_000, MAX_AMOUNT_ZATOSHIS)
                .unwrap()
            else {
                panic!("claim should reserve")
            };
            db.finish(id, PayoutOutcome::Unknown, now + 1).unwrap();
            ids.push(id);
        }

        let uppercase_txid = "A".repeat(64);
        db.reconcile(ids[0], Reconciliation::Paid(uppercase_txid), now + 2)
            .unwrap();
        assert!(
            db.reconcile(ids[1], Reconciliation::Paid("a".repeat(64)), now + 3,)
                .is_err()
        );
        assert!(db.unresolved_claim(ids[1]).unwrap().is_some());
        drop(db);
        cleanup(&path);
    }

    #[test]
    fn invalid_pause_setting_fails_closed() {
        let path = path();
        let db = Database::open(&path).unwrap();
        db.0.connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE settings SET value = 'unexpected' WHERE name = 'paused'",
                [],
            )
            .unwrap();
        assert!(db.paused().is_err());
        assert!(
            db.reserve(
                "user",
                "recipient",
                1_800_000_000_000,
                86_400_000,
                MAX_AMOUNT_ZATOSHIS
            )
            .is_err()
        );
        drop(db);
        cleanup(&path);
    }

    #[test]
    fn database_rejects_a_second_process_owner() {
        let path = path();
        let db = Database::open(&path).unwrap();
        assert!(Database::open(&path).is_err());
        drop(db);
        assert!(Database::open(&path).is_ok());
        let bot = Database::open(&path).unwrap();
        assert!(Database::open_operator(&path).is_ok());
        drop(bot);
        cleanup(&path);
    }

    #[test]
    fn operator_open_refuses_to_create_a_missing_database() {
        let path = path();
        assert!(!path.exists());
        assert!(Database::open_operator(&path).is_err());
        assert!(!path.exists());
    }

    #[test]
    fn payout_lock_excludes_manual_reconciliation_while_worker_is_active() {
        let path = path();
        let db = Database::open(&path).unwrap();
        let payout_guard = db.try_acquire_payout_lock().unwrap().unwrap();
        assert!(db.try_acquire_payout_lock().unwrap().is_none());
        drop(payout_guard);
        assert!(db.try_acquire_payout_lock().unwrap().is_some());
        drop(db);
        cleanup(&path);
    }

    #[test]
    fn bot_creates_the_current_schema_and_operator_requires_it() {
        let path = path();
        assert!(Database::open_operator(&path).is_err());
        let db = Database::open(&path).unwrap();
        let version: i64 =
            db.0.connection
                .lock()
                .unwrap()
                .pragma_query_value(None, "user_version", |row| row.get(0))
                .unwrap();
        assert_eq!(version, 1);
        assert_eq!(
            db.payout_amount_zatoshis().unwrap(),
            DEFAULT_PAYOUT_ZATOSHIS
        );
        assert!(Database::open_operator(&path).is_ok());
        drop(db);
        cleanup(&path);
    }
}
