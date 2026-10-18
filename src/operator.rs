use crate::{
    amount::{format_taz, parse_taz},
    config,
    database::{Database, Reconciliation},
    wallet,
};
use anyhow::{Context, Result, bail};

const USAGE: &str = "Usage:\n  taz-faucet-zypherpunk-discord operator wallet-init\n  taz-faucet-zypherpunk-discord operator status\n  taz-faucet-zypherpunk-discord operator pause\n  taz-faucet-zypherpunk-discord operator resume\n  taz-faucet-zypherpunk-discord operator set-amount <TAZ>\n  taz-faucet-zypherpunk-discord operator unresolved [before-claim-id]\n  taz-faucet-zypherpunk-discord operator inspect <claim-id>\n  taz-faucet-zypherpunk-discord operator reconcile <claim-id> paid <txid>\n  taz-faucet-zypherpunk-discord operator reconcile <claim-id> unpaid\n  taz-faucet-zypherpunk-discord operator override-unpaid <claim-id> --accept-double-payment-risk <reason>";

enum OperatorCommand {
    Status,
    WalletInit,
    Pause,
    Resume,
    SetAmount(i64),
    Unresolved(Option<i64>),
    Inspect(i64),
    ReconcilePaid { claim_id: i64, txid: String },
    ReconcileUnpaid { claim_id: i64 },
    OverrideUnpaid { claim_id: i64, reason: String },
}

pub async fn run(args: &[String]) -> Result<()> {
    let command = parse_command(args)?;
    if matches!(command, OperatorCommand::WalletInit) {
        return wallet::initialize(&config::NetworkConfig::from_env()?).await;
    }
    let db = Database::open_operator(&config::database_path()?)?;
    match command {
        OperatorCommand::Status => {
            let amount = db.payout_amount_zatoshis()?;
            let daily_cap = config::daily_payout_cap_zatoshis()?;
            let paused = db.paused()?;
            let unresolved = db.unresolved_count()?;
            let readiness = match config::NetworkConfig::from_env() {
                Ok(network) => wallet::check_configured(&network, amount).await,
                Err(error) => Err(error),
            };
            println!("Payouts: {}", if paused { "paused" } else { "enabled" });
            println!("Payout amount: {} TAZ", format_taz(amount));
            println!(
                "Aggregate payout cap: {} TAZ per rolling 24 hours",
                format_taz(daily_cap)
            );
            println!("Unresolved claims: {unresolved}");
            match readiness {
                Ok(()) => println!("Lightwallet server and wallet readiness: ready"),
                Err(error) => {
                    println!("Lightwallet server and wallet readiness: not ready ({error:#})")
                }
            }
        }
        OperatorCommand::WalletInit => unreachable!("handled before opening the database"),
        OperatorCommand::Pause => {
            let _control_lock = db.acquire_control_lock()?;
            db.set_paused(true)?;
            println!("Payouts paused. A payout already reserved may still finish.");
        }
        OperatorCommand::Resume => {
            let _control_lock = db.acquire_control_lock()?;
            let _payout_lock = db
                .try_acquire_payout_lock()?
                .context("a payout is active; retry resume after it finishes")?;
            let network = config::NetworkConfig::from_env()?;
            let amount = db.payout_amount_zatoshis()?;
            let daily_cap = config::daily_payout_cap_zatoshis()?;
            anyhow::ensure!(
                amount <= daily_cap,
                "payout amount exceeds the aggregate daily cap"
            );
            anyhow::ensure!(
                db.unresolved_count()? == 0,
                "reconcile unresolved claims before resuming"
            );
            wallet::check_configured(&network, amount)
                .await
                .context("readiness checks failed; payouts remain paused")?;
            db.set_paused(false)?;
            println!("Payouts resumed; readiness checks passed.");
        }
        OperatorCommand::SetAmount(amount) => {
            let _control_lock = db.acquire_control_lock()?;
            let _payout_lock = db
                .try_acquire_payout_lock()?
                .context("a payout is active; pause it and retry after it finishes")?;
            anyhow::ensure!(
                amount <= config::daily_payout_cap_zatoshis()?,
                "payout amount exceeds the aggregate daily cap"
            );
            db.set_payout_amount_zatoshis(amount)?;
            println!("Payout amount set to {} TAZ.", format_taz(amount));
        }
        OperatorCommand::Unresolved(before) => {
            let claims = db.unresolved_claims(before)?;
            if claims.is_empty() {
                println!("No unresolved claims.");
            } else {
                for claim in &claims {
                    println!(
                        "claim {} | user {} | {} TAZ | {} | {}",
                        claim.id,
                        claim.user_id,
                        format_taz(claim.amount_zatoshis),
                        claim.status,
                        format_time(claim.created_at)
                    );
                }
                if let Some(last) = claims.last().filter(|_| claims.len() == 10) {
                    println!(
                        "Next page: taz-faucet-zypherpunk-discord operator unresolved {}",
                        last.id
                    );
                }
            }
        }
        OperatorCommand::Inspect(id) => {
            let Some(claim) = db.unresolved_claim(id)? else {
                bail!("claim {id} is not unresolved or does not exist");
            };
            println!("Claim: {id}");
            println!("Discord user ID: {}", claim.user_id);
            println!("Requested: {}", format_time(claim.created_at));
            println!("Payout amount: {} TAZ", format_taz(claim.amount_zatoshis));
            println!("Recipient: {}", claim.destination);
            println!(
                "Transaction ID: {}",
                claim.txid.as_deref().unwrap_or("not recorded")
            );
            println!(
                "Expiry height: {}",
                claim
                    .expiry_height
                    .map_or_else(|| "not recorded".to_owned(), |height| height.to_string())
            );
        }
        OperatorCommand::ReconcilePaid { claim_id, txid } => {
            let _control_lock = db.acquire_control_lock()?;
            let _payout_lock = db
                .try_acquire_payout_lock()?
                .context("a payout is active; retry reconciliation after it finishes")?;
            anyhow::ensure!(db.paused()?, "pause payouts before reconciliation");
            let claim = db
                .unresolved_claim(claim_id)?
                .context("claim is not unresolved")?;
            if let Some(recorded) = &claim.txid {
                anyhow::ensure!(
                    recorded.eq_ignore_ascii_case(&txid),
                    "transaction ID differs from the claim journal"
                );
            }
            let txid = txid.to_ascii_lowercase();
            let verified =
                wallet::verify_payment(&config::NetworkConfig::from_env()?, &claim, &txid).await?;
            anyhow::ensure!(
                matches!(verified, wallet::VerifiedPayment::Confirmed),
                "transaction is not a confirmed payment"
            );
            db.reconcile(
                claim_id,
                Reconciliation::Paid(txid),
                chrono::Utc::now().timestamp_millis(),
            )?;
            println!("Claim {claim_id} recorded as paid.");
        }
        OperatorCommand::ReconcileUnpaid { claim_id } => {
            let _control_lock = db.acquire_control_lock()?;
            let _payout_lock = db
                .try_acquire_payout_lock()?
                .context("a payout is active; retry reconciliation after it finishes")?;
            anyhow::ensure!(db.paused()?, "pause payouts before reconciliation");
            let claim = db
                .unresolved_claim(claim_id)?
                .context("claim is not unresolved")?;
            let txid = claim
                .txid
                .as_deref()
                .context("no recorded transaction ID; nonpayment cannot be verified")?;
            let verified =
                wallet::verify_payment(&config::NetworkConfig::from_env()?, &claim, txid).await?;
            anyhow::ensure!(
                matches!(verified, wallet::VerifiedPayment::Expired),
                "transaction paid the recipient; use paid reconciliation"
            );
            db.reconcile(
                claim_id,
                Reconciliation::Expired,
                chrono::Utc::now().timestamp_millis(),
            )?;
            println!("Claim {claim_id} verified expired and released for retry.");
        }
        OperatorCommand::OverrideUnpaid { claim_id, reason } => {
            let _control_lock = db.acquire_control_lock()?;
            let _payout_lock = db
                .try_acquire_payout_lock()?
                .context("a payout is active; retry after it finishes")?;
            anyhow::ensure!(db.paused()?, "pause payouts before reconciliation");
            let claim = db
                .unresolved_claim(claim_id)?
                .context("claim is not unresolved")?;
            anyhow::ensure!(
                claim.txid.is_none(),
                "a journaled transaction requires verified reconciliation"
            );
            let network = config::NetworkConfig::from_env()?;
            let _wallet_lock = wallet::lock_configured(&network)?;
            db.reconcile(
                claim_id,
                Reconciliation::OverrideUnpaid(reason),
                chrono::Utc::now().timestamp_millis(),
            )?;
            println!(
                "WARNING: Claim {claim_id} released by an UNVERIFIED override. Nonpayment was not established; a duplicate payment is possible. The reason has been recorded."
            );
        }
    }
    Ok(())
}

fn parse_command(args: &[String]) -> Result<OperatorCommand> {
    let command = match args.first().map(String::as_str) {
        Some("status") if args.len() == 1 => OperatorCommand::Status,
        Some("wallet-init") if args.len() == 1 => OperatorCommand::WalletInit,
        Some("pause") if args.len() == 1 => OperatorCommand::Pause,
        Some("resume") if args.len() == 1 => OperatorCommand::Resume,
        Some("set-amount") if args.len() == 2 => OperatorCommand::SetAmount(parse_taz(&args[1])?),
        Some("unresolved") if args.len() == 1 => OperatorCommand::Unresolved(None),
        Some("unresolved") if args.len() == 2 => {
            OperatorCommand::Unresolved(Some(parse_id(&args[1])?))
        }
        Some("inspect") if args.len() == 2 => OperatorCommand::Inspect(parse_id(&args[1])?),
        Some("reconcile") if args.len() == 4 && args[2] == "paid" => {
            validate_txid(&args[3])?;
            OperatorCommand::ReconcilePaid {
                claim_id: parse_id(&args[1])?,
                txid: args[3].clone(),
            }
        }
        Some("reconcile") if args.len() == 3 && args[2] == "unpaid" => {
            OperatorCommand::ReconcileUnpaid {
                claim_id: parse_id(&args[1])?,
            }
        }
        Some("override-unpaid") if args.len() == 4 && args[2] == "--accept-double-payment-risk" => {
            anyhow::ensure!(
                !args[3].trim().is_empty() && args[3].len() <= 512,
                "override requires a reason of 1–512 bytes"
            );
            OperatorCommand::OverrideUnpaid {
                claim_id: parse_id(&args[1])?,
                reason: args[3].clone(),
            }
        }
        _ => bail!("{USAGE}"),
    };
    Ok(command)
}

fn parse_id(value: &str) -> Result<i64> {
    let id = value
        .parse::<i64>()
        .context("claim ID must be a positive integer")?;
    anyhow::ensure!(id > 0, "claim ID must be a positive integer");
    Ok(id)
}

fn validate_txid(value: &str) -> Result<()> {
    anyhow::ensure!(
        value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "transaction ID must be exactly 64 hexadecimal characters"
    );
    Ok(())
}

fn format_time(timestamp: i64) -> String {
    chrono::DateTime::from_timestamp_millis(timestamp)
        .map(|time| time.to_rfc3339())
        .unwrap_or_else(|| "unknown time".to_owned())
}

#[cfg(test)]
mod tests {
    use super::{OperatorCommand, parse_command};

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn parser_separates_verified_reconciliation_from_risky_override() {
        assert!(matches!(
            parse_command(&args(&["reconcile", "12", "unpaid"])).unwrap(),
            OperatorCommand::ReconcileUnpaid { claim_id: 12 }
        ));
        assert!(parse_command(&args(&["override-unpaid", "12"])).is_err());
        assert!(
            parse_command(&args(&[
                "override-unpaid",
                "12",
                "--accept-double-payment-risk",
                ""
            ]))
            .is_err()
        );
        assert!(
            parse_command(&args(&[
                "override-unpaid",
                "12",
                "--accept-double-payment-risk",
                "manually established nonpayment"
            ]))
            .is_ok()
        );
    }

    #[test]
    fn parser_validates_paid_reconciliation_input_before_opening_database() {
        let txid = "a".repeat(64);
        assert!(matches!(
            parse_command(&args(&["reconcile", "12", "paid", &txid])).unwrap(),
            OperatorCommand::ReconcilePaid {
                claim_id: 12,
                txid: parsed
            } if parsed == txid
        ));
        assert!(parse_command(&args(&["reconcile", "12", "paid", "bad-txid"])).is_err());
    }

    #[test]
    fn parser_accepts_exact_amounts_and_rejects_invalid_precision() {
        assert!(matches!(
            parse_command(&args(&["set-amount", "0.25000001"])).unwrap(),
            OperatorCommand::SetAmount(25_000_001)
        ));
        for amount in ["0", "-1", "1e-3", "0.000000001", "21000000.00000001"] {
            assert!(
                parse_command(&args(&["set-amount", amount])).is_err(),
                "{amount}"
            );
        }
    }
}
