use anyhow::{Context, Result};
use zingolib::{data::ServerInfo, lightclient::LightClient};

use crate::amount::validate_zatoshis;

const REQUIRED_TESTNET_HEIGHT: u64 = 4_465_026;
const NU7_BRANCH_ID: &str = "77190ad9";
const MINIMUM_FEE_RESERVE_ZATOSHIS: u64 = 100_000;
const MAX_WALLET_LAG: u64 = 100;

#[derive(Debug)]
pub struct InsufficientFunds;

impl std::fmt::Display for InsufficientFunds {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(
            "faucet wallet does not have the payout plus the fee reserve in spendable shielded funds",
        )
    }
}

impl std::error::Error for InsufficientFunds {}

pub async fn check(client: &mut LightClient, amount_zatoshis: i64) -> Result<()> {
    check_wallet(client, amount_zatoshis).await?;
    anyhow::ensure!(
        client
            .latest_sync_status()
            .is_some_and(|status| status.is_complete()),
        "light wallet has not completed its initial scan"
    );
    Ok(())
}

pub async fn check_configured(client: &mut LightClient, amount_zatoshis: i64) -> Result<()> {
    check_wallet(client, amount_zatoshis).await
}

async fn check_wallet(client: &mut LightClient, amount_zatoshis: i64) -> Result<()> {
    validate_zatoshis(amount_zatoshis)?;
    let info = client
        .info()
        .await
        .context("could not read lightwalletd network status")?;
    check_server_info(&info)?;

    let wallet = client.wallet().read().await;
    anyhow::ensure!(
        wallet
            .unified_key_store
            .get(&zip32::AccountId::ZERO)
            .is_some_and(|keys| keys.is_spending_key()),
        "faucet wallet does not contain the account spending key"
    );
    let scanned_height = wallet
        .sync_state
        .fully_scanned_height()
        .map(u64::from)
        .context("light wallet has no fully scanned chain height")?;
    anyhow::ensure!(
        scanned_height.saturating_add(MAX_WALLET_LAG) >= info.latest_block_height,
        "light wallet is more than 100 blocks behind lightwalletd"
    );

    let spendable_zatoshis = wallet
        .shielded_spendable_balance(zip32::AccountId::ZERO, false)
        .context("could not read faucet wallet spendable balance")?
        .into_u64();
    let required = u64::try_from(amount_zatoshis)
        .context("configured payout amount is outside the supported range")?
        .saturating_add(MINIMUM_FEE_RESERVE_ZATOSHIS);
    if spendable_zatoshis < required {
        return Err(InsufficientFunds.into());
    }
    Ok(())
}

pub fn check_server_info(info: &ServerInfo) -> Result<()> {
    anyhow::ensure!(
        info.chain_name == "test",
        "configured lightwalletd is not on public testnet"
    );
    anyhow::ensure!(
        info.latest_block_height >= REQUIRED_TESTNET_HEIGHT,
        "lightwalletd has not reached the testnet NU7 activation height"
    );
    anyhow::ensure!(
        info.consensus_branch_id
            .trim_start_matches("0x")
            .eq_ignore_ascii_case(NU7_BRANCH_ID),
        "lightwalletd does not report the NU7 consensus branch"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::check_server_info;
    use zingolib::data::ServerInfo;

    fn server_info(chain_name: &str, height: u64, branch: &str) -> ServerInfo {
        ServerInfo {
            version: String::new(),
            git_commit: String::new(),
            server_uri: "https://lightwalletd.example".parse().unwrap(),
            vendor: String::new(),
            taddr_support: true,
            chain_name: chain_name.to_owned(),
            sapling_activation_height: 0,
            consensus_branch_id: branch.to_owned(),
            latest_block_height: height,
        }
    }

    #[test]
    fn accepts_testnet_reporting_the_nu7_branch() {
        assert!(check_server_info(&server_info("test", 4_465_026, "0x77190AD9")).is_ok());
    }

    #[test]
    fn rejects_wrong_network_old_tip_and_wrong_active_branch() {
        assert!(check_server_info(&server_info("main", 4_500_000, "77190ad9")).is_err());
        assert!(check_server_info(&server_info("test", 4_465_025, "77190ad9")).is_err());
        assert!(check_server_info(&server_info("test", 4_500_000, "c2d6d0b4")).is_err());
    }
}
