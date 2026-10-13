use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tracing::{error, warn};

use crate::{
    database::{ClaimDecision, Database, PayoutOutcome},
    readiness,
    wallet::{FaucetWallet, SendError},
};

const COOLDOWN_MILLIS: i64 = 24 * 60 * 60 * 1_000;
const QUEUE_WAIT: Duration = Duration::from_secs(15);

pub(crate) struct PayoutService {
    daily_cap_zatoshis: i64,
    db: Database,
    wallet: Arc<FaucetWallet>,
    gate: tokio::sync::Mutex<()>,
}

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
    Rejected,
    OutcomeUnknown,
}

impl PayoutService {
    pub(crate) fn new(daily_cap_zatoshis: i64, db: Database, wallet: Arc<FaucetWallet>) -> Self {
        Self {
            daily_cap_zatoshis,
            db,
            wallet,
            gate: tokio::sync::Mutex::new(()),
        }
    }

    pub(crate) async fn claim(&self, user_id: u64, address: &str) -> Result<ClaimResult> {
        let Ok(_gate) = tokio::time::timeout(QUEUE_WAIT, self.gate.lock()).await else {
            return Ok(ClaimResult::Busy);
        };
        let db = self.db.clone();
        let Some(_payout_lock) = tokio::task::spawn_blocking(move || db.try_acquire_payout_lock())
            .await
            .context("payout lock worker failed")??
        else {
            return Ok(ClaimResult::Busy);
        };

        let db = self.db.clone();
        let user_id = user_id.to_string();
        let destination = address.to_owned();
        let now = chrono::Utc::now().timestamp_millis();
        let daily_cap_zatoshis = self.daily_cap_zatoshis;
        let decision = tokio::task::spawn_blocking(move || {
            db.reserve(
                &user_id,
                &destination,
                now,
                COOLDOWN_MILLIS,
                daily_cap_zatoshis,
            )
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
            ClaimDecision::CooldownRemaining(seconds) => {
                return Ok(ClaimResult::Cooldown(seconds));
            }
            ClaimDecision::DailyLimitReached(retry_after_seconds) => {
                return Ok(ClaimResult::DailyLimitReached(retry_after_seconds));
            }
        };

        if let Err(err) = self.wallet.check_readiness(amount_zatoshis).await {
            warn!(%err, "faucet wallet is not ready");
            if let Err(db_err) = self.finish_claim(claim_id, PayoutOutcome::Failed).await {
                error!(%db_err, claim_id, "could not release claim after preflight failure");
            }
            return Ok(
                if err.downcast_ref::<readiness::InsufficientFunds>().is_some() {
                    ClaimResult::InsufficientFunds
                } else {
                    ClaimResult::NotReady
                },
            );
        }

        match self.wallet.send(address, amount_zatoshis).await {
            Ok(txid) => {
                if let Err(err) = self
                    .finish_claim(claim_id, PayoutOutcome::Paid(txid.clone()))
                    .await
                {
                    error!(%err, claim_id, "payout was accepted but its database record could not be finalized");
                }
                Ok(ClaimResult::Paid {
                    txid,
                    amount_zatoshis,
                })
            }
            Err(SendError::Rejected) => {
                warn!(
                    claim_id,
                    "wallet rejected faucet payout before transmission"
                );
                if let Err(err) = self.finish_claim(claim_id, PayoutOutcome::Failed).await {
                    error!(%err, claim_id, "could not release rejected claim");
                }
                Ok(ClaimResult::Rejected)
            }
            Err(SendError::Unknown) => {
                error!(
                    claim_id,
                    "payout result is ambiguous; retry disabled pending reconciliation"
                );
                if let Err(err) = self.finish_claim(claim_id, PayoutOutcome::Unknown).await {
                    error!(%err, claim_id, "could not record ambiguous payout");
                }
                Ok(ClaimResult::OutcomeUnknown)
            }
        }
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
