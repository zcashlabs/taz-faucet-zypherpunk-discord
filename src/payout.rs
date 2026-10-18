use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use serenity::async_trait;
use tracing::{error, warn};

use crate::{
    database::{ClaimDecision, Database, PayoutOutcome},
    readiness,
    wallet::{FaucetWallet, PreparedPayment},
};

const COOLDOWN_MILLIS: i64 = 24 * 60 * 60 * 1_000;
const QUEUE_WAIT: Duration = Duration::from_secs(15);

// This seam isolates third-party wallet effects so failure and cancellation boundaries can be tested.
#[async_trait]
pub(crate) trait PayoutWallet: Send + Sync + 'static {
    async fn check_readiness(&self, amount: i64) -> Result<()>;
    async fn prepare(&self, address: &str, amount: i64) -> Result<PreparedPayment>;
    async fn persist(&self) -> Result<()>;
    async fn transmit(&self, payment: &PreparedPayment) -> Result<()>;
}

#[async_trait]
impl PayoutWallet for FaucetWallet {
    async fn check_readiness(&self, amount: i64) -> Result<()> {
        self.check_readiness(amount).await
    }
    async fn prepare(&self, address: &str, amount: i64) -> Result<PreparedPayment> {
        self.prepare(address, amount).await
    }
    async fn persist(&self) -> Result<()> {
        self.persist().await
    }
    async fn transmit(&self, payment: &PreparedPayment) -> Result<()> {
        self.transmit(payment).await
    }
}

pub(crate) struct PayoutService<W = FaucetWallet> {
    daily_cap_zatoshis: i64,
    db: Database,
    wallet: Arc<W>,
    gate: Arc<tokio::sync::Mutex<()>>,
    accepting: AtomicBool,
}

#[derive(Debug)]
pub(crate) enum ClaimResult {
    Busy,
    Paused,
    NeedsReconciliation,
    InProgress,
    Cooldown(i64),
    DailyLimitReached(Option<i64>),
    InsufficientFunds,
    NotReady,
    Paid { txid: String, amount_zatoshis: i64 },
    RecordingFailed { txid: String },
    Rejected,
    OutcomeUnknown,
}

impl<W: PayoutWallet> PayoutService<W> {
    pub(crate) fn new(daily_cap_zatoshis: i64, db: Database, wallet: Arc<W>) -> Self {
        Self {
            daily_cap_zatoshis,
            db,
            wallet,
            gate: Arc::new(tokio::sync::Mutex::new(())),
            accepting: AtomicBool::new(true),
        }
    }

    pub(crate) async fn claim(
        self: &Arc<Self>,
        user_id: u64,
        address: &str,
    ) -> Result<ClaimResult> {
        let Ok(gate) = tokio::time::timeout(QUEUE_WAIT, self.gate.clone().lock_owned()).await
        else {
            return Ok(ClaimResult::Busy);
        };
        if !self.accepting.load(Ordering::Acquire) {
            return Ok(ClaimResult::NotReady);
        }
        let service = self.clone();
        let address = address.to_owned();
        // Dropping a Discord handler must not cancel a reserved payment. The owned gate
        // stays with this task through all wallet and database effects, including finalization.
        tokio::spawn(async move {
            let _gate = gate;
            let result = service.process_claim(user_id, &address).await;
            if let Err(error) = &result {
                service.fault(error).await;
            }
            result
        })
        .await
        .context("payout worker panicked; claim may require reconciliation")?
    }

    pub(crate) async fn drain(&self) {
        self.accepting.store(false, Ordering::Release);
        let _gate = self.gate.lock().await;
    }

    async fn fault(&self, reason: &anyhow::Error) {
        self.accepting.store(false, Ordering::Release);
        error!(%reason, "payout worker disabled; operator recovery required");
        let db = self.db.clone();
        if let Err(error) = tokio::task::spawn_blocking(move || db.set_paused(true))
            .await
            .context("pause worker failed")
            .and_then(|result| result)
        {
            error!(%error, "could not persist payout pause; process remains disabled");
        }
    }

    async fn process_claim(&self, user_id: u64, address: &str) -> Result<ClaimResult> {
        let db = self.db.clone();
        let Some(_payout_lock) = tokio::task::spawn_blocking(move || db.try_acquire_payout_lock())
            .await
            .context("payout lock worker failed")??
        else {
            return Ok(ClaimResult::Busy);
        };
        let db = self.db.clone();
        if tokio::task::spawn_blocking(move || db.unresolved_count()).await?? > 0 {
            return Ok(ClaimResult::NeedsReconciliation);
        }
        let db = self.db.clone();
        let user_id = user_id.to_string();
        let destination = address.to_owned();
        let now = chrono::Utc::now().timestamp_millis();
        let daily_cap = self.daily_cap_zatoshis;
        let decision = tokio::task::spawn_blocking(move || {
            db.reserve(&user_id, &destination, now, COOLDOWN_MILLIS, daily_cap)
        })
        .await
        .context("claim reservation worker failed")??;
        let (claim_id, amount_zatoshis) = match decision {
            ClaimDecision::Reserved {
                id,
                amount_zatoshis,
            } => (id, amount_zatoshis),
            ClaimDecision::Paused => return Ok(ClaimResult::Paused),
            ClaimDecision::NeedsReconciliation => return Ok(ClaimResult::NeedsReconciliation),
            ClaimDecision::InProgress => return Ok(ClaimResult::InProgress),
            ClaimDecision::CooldownRemaining(seconds) => return Ok(ClaimResult::Cooldown(seconds)),
            ClaimDecision::DailyLimitReached(seconds) => {
                return Ok(ClaimResult::DailyLimitReached(seconds));
            }
        };
        if let Err(error) = self.wallet.check_readiness(amount_zatoshis).await {
            warn!(%error, "faucet wallet is not ready");
            self.finish_claim(claim_id, PayoutOutcome::Failed).await?;
            return Ok(
                if error
                    .downcast_ref::<readiness::InsufficientFunds>()
                    .is_some()
                {
                    ClaimResult::InsufficientFunds
                } else {
                    ClaimResult::NotReady
                },
            );
        }
        let payment = match self.wallet.prepare(address, amount_zatoshis).await {
            Ok(payment) => payment,
            Err(error) => {
                warn!(%error, claim_id, "wallet rejected payout before transmission");
                self.finish_claim(claim_id, PayoutOutcome::Failed).await?;
                return Ok(
                    if error
                        .downcast_ref::<readiness::InsufficientFunds>()
                        .is_some()
                    {
                        ClaimResult::InsufficientFunds
                    } else {
                        ClaimResult::Rejected
                    },
                );
            }
        };
        let db = self.db.clone();
        let txid = payment.txid.clone();
        let expiry_height = payment.expiry_height;
        if let Err(error) = tokio::task::spawn_blocking(move || {
            db.record_transaction(claim_id, txid, expiry_height)
        })
        .await
        .context("transaction journal worker failed")
        .and_then(|result| result)
        {
            error!(claim_id, txid = %payment.txid, "transaction was built but will not be broadcast");
            return Err(error);
        }
        if let Err(error) = self.wallet.persist().await {
            self.fault(&error).await;
            self.finish_claim(claim_id, PayoutOutcome::Unknown).await?;
            return Ok(ClaimResult::OutcomeUnknown);
        }
        let accepted = match self.wallet.transmit(&payment).await {
            Ok(()) => true,
            Err(error) => {
                self.fault(&error).await;
                false
            }
        };
        let saved = match self.wallet.persist().await {
            Ok(()) => true,
            Err(error) => {
                self.fault(&error).await;
                false
            }
        };
        let outcome = if accepted && saved {
            PayoutOutcome::Paid(payment.txid.clone())
        } else {
            PayoutOutcome::Unknown
        };
        if let Err(error) = self.finish_claim(claim_id, outcome).await {
            self.fault(&error).await;
            return Ok(if accepted {
                ClaimResult::RecordingFailed { txid: payment.txid }
            } else {
                ClaimResult::OutcomeUnknown
            });
        }
        Ok(if accepted && saved {
            ClaimResult::Paid {
                txid: payment.txid,
                amount_zatoshis,
            }
        } else if accepted {
            ClaimResult::RecordingFailed { txid: payment.txid }
        } else {
            ClaimResult::OutcomeUnknown
        })
    }

    async fn finish_claim(&self, claim_id: i64, outcome: PayoutOutcome) -> Result<()> {
        let db = self.db.clone();
        let now = chrono::Utc::now().timestamp_millis();
        tokio::task::spawn_blocking(move || db.finish(claim_id, outcome, now))
            .await
            .context("database worker failed")??;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Notify;

    const TXID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Stage {
        Readiness,
        Build,
        BeforeBroadcastSave,
        Broadcast,
        AfterBroadcastSave,
    }

    const STAGES: [Stage; 5] = [
        Stage::Readiness,
        Stage::Build,
        Stage::BeforeBroadcastSave,
        Stage::Broadcast,
        Stage::AfterBroadcastSave,
    ];

    struct Wallet {
        db: Database,
        block: Option<Stage>,
        fail: Option<Stage>,
        entered: Notify,
        release: Notify,
        saves: AtomicUsize,
        broadcasts: AtomicUsize,
    }

    impl Wallet {
        async fn boundary(&self, stage: Stage) -> Result<()> {
            if self.block == Some(stage) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            anyhow::ensure!(
                self.fail != Some(stage),
                "injected wallet failure at {stage:?}"
            );
            Ok(())
        }
    }

    #[async_trait]
    impl PayoutWallet for Wallet {
        async fn check_readiness(&self, _: i64) -> Result<()> {
            self.boundary(Stage::Readiness).await
        }
        async fn prepare(&self, _: &str, _: i64) -> Result<PreparedPayment> {
            self.boundary(Stage::Build).await?;
            Ok(PreparedPayment {
                txid: TXID.to_owned(),
                expiry_height: 4_500_100,
            })
        }
        async fn persist(&self) -> Result<()> {
            let stage = if self.saves.fetch_add(1, Ordering::SeqCst) == 0 {
                Stage::BeforeBroadcastSave
            } else {
                Stage::AfterBroadcastSave
            };
            self.boundary(stage).await
        }
        async fn transmit(&self, _: &PreparedPayment) -> Result<()> {
            // The wallet can only be called after the durable journal and pre-broadcast save.
            let claim = self.db.unresolved_claim(1)?.unwrap();
            assert_eq!(claim.txid.as_deref(), Some(TXID));
            assert_eq!(claim.expiry_height, Some(4_500_100));
            assert_eq!(self.saves.load(Ordering::SeqCst), 1);
            self.broadcasts.fetch_add(1, Ordering::SeqCst);
            self.boundary(Stage::Broadcast).await
        }
    }

    struct Fixture {
        service: Arc<PayoutService<Wallet>>,
        wallet: Arc<Wallet>,
        db: Database,
        directory: tempfile::TempDir,
    }

    impl Fixture {
        fn new(block: Option<Stage>, fail: Option<Stage>) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let db = Database::open(&directory.path().join("claims.sqlite")).unwrap();
            db.set_paused(false).unwrap();
            let wallet = Arc::new(Wallet {
                db: db.clone(),
                block,
                fail,
                entered: Notify::new(),
                release: Notify::new(),
                saves: AtomicUsize::new(0),
                broadcasts: AtomicUsize::new(0),
            });
            let service = Arc::new(PayoutService::new(
                1_000_000_000,
                db.clone(),
                wallet.clone(),
            ));
            Self {
                service,
                wallet,
                db,
                directory,
            }
        }

        fn connection(&self) -> rusqlite::Connection {
            rusqlite::Connection::open(self.directory.path().join("claims.sqlite")).unwrap()
        }

        fn status(&self) -> String {
            self.connection()
                .query_row("SELECT status FROM claims WHERE id = 1", [], |row| {
                    row.get(0)
                })
                .unwrap()
        }
    }

    #[tokio::test]
    async fn success_records_transaction_before_broadcast_and_finalizes_once() {
        let fixture = Fixture::new(None, None);
        assert!(
            matches!(fixture.service.claim(1, "recipient").await.unwrap(), ClaimResult::Paid { txid, .. } if txid == TXID)
        );
        assert_eq!(fixture.status(), "succeeded");
        assert_eq!(fixture.wallet.broadcasts.load(Ordering::SeqCst), 1);
        assert!(matches!(
            fixture.service.claim(1, "recipient").await.unwrap(),
            ClaimResult::Cooldown(_)
        ));
        assert_eq!(fixture.wallet.broadcasts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pre_broadcast_rejections_release_the_claim() {
        for stage in [Stage::Readiness, Stage::Build] {
            let fixture = Fixture::new(None, Some(stage));
            fixture.service.claim(1, "recipient").await.unwrap();
            assert_eq!(fixture.status(), "failed");
            assert_eq!(fixture.wallet.broadcasts.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.db.unresolved_count().unwrap(), 0);
            assert!(!fixture.db.paused().unwrap());
        }
    }

    #[tokio::test]
    async fn persistence_and_network_failures_hold_claims_and_disable_all_further_sends() {
        for stage in [
            Stage::BeforeBroadcastSave,
            Stage::Broadcast,
            Stage::AfterBroadcastSave,
        ] {
            let fixture = Fixture::new(None, Some(stage));
            let result = fixture.service.claim(1, "recipient").await.unwrap();
            if stage == Stage::AfterBroadcastSave {
                assert!(matches!(result, ClaimResult::RecordingFailed { txid } if txid == TXID));
            } else {
                assert!(matches!(result, ClaimResult::OutcomeUnknown));
            }
            assert_eq!(fixture.status(), "unknown");
            assert_eq!(
                fixture
                    .db
                    .unresolved_claim(1)
                    .unwrap()
                    .unwrap()
                    .txid
                    .as_deref(),
                Some(TXID)
            );
            assert!(fixture.db.paused().unwrap());
            assert!(matches!(
                fixture.service.claim(2, "other").await.unwrap(),
                ClaimResult::NotReady
            ));
            assert_eq!(
                fixture.wallet.broadcasts.load(Ordering::SeqCst),
                usize::from(stage != Stage::BeforeBroadcastSave)
            );
        }
    }

    #[tokio::test]
    async fn journal_failure_prevents_broadcast() {
        let fixture = Fixture::new(None, None);
        fixture.connection().execute_batch("CREATE TRIGGER reject_journal BEFORE UPDATE OF txid ON claims BEGIN SELECT RAISE(ABORT, 'injected journal failure'); END;").unwrap();
        assert!(fixture.service.claim(1, "recipient").await.is_err());
        assert_eq!(fixture.wallet.broadcasts.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.status(), "submitting");
        assert!(fixture.db.paused().unwrap());
    }

    #[tokio::test]
    async fn finalization_failure_retains_transaction_and_never_reports_normal_success() {
        let fixture = Fixture::new(None, None);
        fixture.connection().execute_batch("CREATE TRIGGER reject_finalization BEFORE UPDATE OF status ON claims WHEN NEW.status = 'succeeded' BEGIN SELECT RAISE(ABORT, 'injected finalization failure'); END;").unwrap();
        assert!(
            matches!(fixture.service.claim(1, "recipient").await.unwrap(), ClaimResult::RecordingFailed { txid } if txid == TXID)
        );
        assert_eq!(fixture.status(), "submitting");
        assert_eq!(
            fixture
                .db
                .unresolved_claim(1)
                .unwrap()
                .unwrap()
                .txid
                .as_deref(),
            Some(TXID)
        );
        fixture.db.recover_on_startup().unwrap();
        assert_eq!(fixture.status(), "unknown");
        assert!(fixture.db.paused().unwrap());
        assert_eq!(fixture.wallet.broadcasts.load(Ordering::SeqCst), 1);
    }

    async fn blocked_request(fixture: &Fixture) -> tokio::task::JoinHandle<Result<ClaimResult>> {
        let service = fixture.service.clone();
        let request = tokio::spawn(async move { service.claim(1, "recipient").await });
        tokio::time::timeout(Duration::from_secs(5), fixture.wallet.entered.notified())
            .await
            .unwrap();
        request
    }

    #[tokio::test]
    async fn cancelled_discord_request_and_shutdown_drain_preserve_each_critical_stage() {
        for stage in STAGES {
            let fixture = Fixture::new(Some(stage), None);
            let request = blocked_request(&fixture).await;
            request.abort();
            let service = fixture.service.clone();
            let drain = tokio::spawn(async move { service.drain().await });
            tokio::task::yield_now().await;
            assert!(
                !drain.is_finished(),
                "drain returned early during {stage:?}"
            );
            fixture.wallet.release.notify_one();
            tokio::time::timeout(Duration::from_secs(5), drain)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(fixture.status(), "succeeded", "{stage:?}");
            assert_eq!(fixture.wallet.broadcasts.load(Ordering::SeqCst), 1);
            assert!(!fixture.db.paused().unwrap());
            assert!(matches!(
                fixture.service.claim(2, "recipient").await.unwrap(),
                ClaimResult::NotReady
            ));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sigterm_child() {
        let Ok(directory) = std::env::var("FAUCET_SIGNAL_TEST_DIR") else {
            return;
        };
        let index: usize = std::env::var("FAUCET_SIGNAL_TEST_STAGE")
            .unwrap()
            .parse()
            .unwrap();
        let fixture = Fixture::new(Some(STAGES[index]), None);
        let signal = tokio::spawn(crate::bot::shutdown_signal());
        let request = blocked_request(&fixture).await;
        std::fs::write(Path::new(&directory).join("ready"), b"ready").unwrap();
        signal.await.unwrap().unwrap();
        request.abort();
        fixture.wallet.release.notify_one();
        fixture.service.drain().await;
        assert_eq!(fixture.status(), "succeeded");
        assert_eq!(fixture.wallet.broadcasts.load(Ordering::SeqCst), 1);
        std::fs::write(Path::new(&directory).join("drained"), b"drained").unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn hard_kill_child() {
        let Ok(directory) = std::env::var("FAUCET_KILL_TEST_DIR") else {
            return;
        };
        let index: usize = std::env::var("FAUCET_KILL_TEST_STAGE")
            .unwrap()
            .parse()
            .unwrap();
        let directory = Path::new(&directory);
        let db = Database::open(&directory.join("claims.sqlite")).unwrap();
        db.set_paused(false).unwrap();
        let wallet = Arc::new(Wallet {
            db: db.clone(),
            block: Some(STAGES[index]),
            fail: None,
            entered: Notify::new(),
            release: Notify::new(),
            saves: AtomicUsize::new(0),
            broadcasts: AtomicUsize::new(0),
        });
        let service = Arc::new(PayoutService::new(1_000_000_000, db, wallet.clone()));
        let request = tokio::spawn(async move { service.claim(1, "recipient").await });
        wallet.entered.notified().await;
        std::fs::write(directory.join("ready"), b"ready").unwrap();
        request.await.unwrap().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn hard_kill_recovers_each_critical_stage_without_releasing_the_claim() {
        use std::{
            process::{Command, Stdio},
            thread,
            time::Instant,
        };
        for (index, stage) in STAGES.iter().enumerate() {
            let directory = tempfile::tempdir().unwrap();
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "payout::tests::hard_kill_child", "--nocapture"])
                .env("FAUCET_KILL_TEST_DIR", directory.path())
                .env("FAUCET_KILL_TEST_STAGE", index.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            let start = Instant::now();
            while !directory.path().join("ready").exists() {
                if start.elapsed() > Duration::from_secs(10) || child.try_wait().unwrap().is_some()
                {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("kill fixture did not become ready at {stage:?}");
                }
                thread::sleep(Duration::from_millis(10));
            }
            child.kill().unwrap();
            assert!(!child.wait().unwrap().success());
            let db = Database::open(&directory.path().join("claims.sqlite")).unwrap();
            db.recover_on_startup().unwrap();
            assert!(db.paused().unwrap());
            let claim = db.unresolved_claim(1).unwrap().unwrap();
            assert_eq!(claim.destination, "recipient");
            if matches!(
                stage,
                Stage::BeforeBroadcastSave | Stage::Broadcast | Stage::AfterBroadcastSave
            ) {
                assert_eq!(claim.txid.as_deref(), Some(TXID));
                assert_eq!(claim.expiry_height, Some(4_500_100));
            }
            db.set_paused(false).unwrap();
            assert_eq!(
                db.reserve(
                    "another-user",
                    "recipient",
                    chrono::Utc::now().timestamp_millis(),
                    COOLDOWN_MILLIS,
                    1_000_000_000
                )
                .unwrap(),
                ClaimDecision::NeedsReconciliation
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn real_sigterm_drains_reservation_build_broadcast_and_persistence() {
        use std::{
            path::Path,
            process::{Command, Stdio},
            thread,
            time::Instant,
        };
        for (index, stage) in STAGES.iter().enumerate() {
            let directory = tempfile::tempdir().unwrap();
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "payout::tests::sigterm_child", "--nocapture"])
                .env("FAUCET_SIGNAL_TEST_DIR", directory.path())
                .env("FAUCET_SIGNAL_TEST_STAGE", index.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            let start = Instant::now();
            while !directory.path().join("ready").exists() {
                if start.elapsed() > Duration::from_secs(10) || child.try_wait().unwrap().is_some()
                {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("signal fixture did not become ready at {stage:?}");
                }
                thread::sleep(Duration::from_millis(10));
            }
            assert!(
                Command::new("kill")
                    .args(["-TERM", &child.id().to_string()])
                    .status()
                    .unwrap()
                    .success()
            );
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    assert!(status.success());
                    break;
                }
                if start.elapsed() > Duration::from_secs(10) {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("SIGTERM did not drain at {stage:?}");
                }
                thread::sleep(Duration::from_millis(10));
            }
            assert!(Path::new(directory.path()).join("drained").exists());
        }
    }
}
