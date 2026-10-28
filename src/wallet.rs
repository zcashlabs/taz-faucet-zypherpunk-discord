use std::{
    fs::{File, OpenOptions},
    io::IsTerminal,
    num::NonZeroU32,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result};
use fs2::FileExt;
use tokio::sync::Mutex;
use tracing::{error, info, warn};
use zcash_client_backend::zip321::{Payment, TransactionRequest};
use zingolib::{
    config::{ChainType, ClientConfig, WalletConfig},
    data::ServerInfo,
    destination::servers::Trust,
    lightclient::LightClient,
    netutils::{GrpcIndexer, Indexer},
    wallet::WalletSettings,
};

use crate::{config::NetworkConfig, database::UnresolvedClaim, readiness};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const WALLET_FILE: &str = "zingo-wallet.dat";

pub(crate) struct FaucetWallet {
    client: Mutex<LightClient>,
    _lock: File,
}

pub(crate) struct PreparedPayment {
    pub txid: String,
    pub expiry_height: u32,
}

pub(crate) enum VerifiedPayment {
    Confirmed,
    Expired,
}

impl FaucetWallet {
    pub async fn open(network: &NetworkConfig) -> Result<Self> {
        let wallet_path = wallet_path(network);
        let _lock = acquire_wallet_lock(&wallet_path)?;
        anyhow::ensure!(
            wallet_path.is_file(),
            "wallet is not initialized; run `operator wallet-init` first"
        );
        secure_wallet_file(&wallet_path)?;

        let config = client_config(network, WalletConfig::Read, true)?;
        let mut client = LightClient::new(config, false)
            .await
            .context("could not open the testnet light wallet")?;
        let info = client
            .info()
            .await
            .context("could not read lightwalletd network status")?;
        readiness::check_server_info(&info)?;

        let mut settings = client.wallet().read().await.wallet_settings.clone();
        settings.sync_config.shutdown_on_completion = false;
        client.update_wallet_settings(settings).await;
        persist(&mut client).await?;
        client
            .sync()
            .await
            .context("could not start light-wallet sync")?;
        client.save_task().await;

        Ok(Self {
            client: Mutex::new(client),
            _lock,
        })
    }

    pub async fn prepare(&self, address: &str, amount_zatoshis: i64) -> Result<PreparedPayment> {
        let request = payment_request(address, amount_zatoshis)?;
        let mut client = self.client.lock().await;
        let proposal = client
            .propose_send(request, zip32::AccountId::ZERO)
            .await
            .map_err(|error| match error {
                zingolib::wallet::error::ProposeSendError::Proposal(
                    zcash_client_backend::data_api::error::Error::InsufficientFunds { .. },
                ) => readiness::InsufficientFunds.into(),
                error => anyhow::Error::new(error).context("could not propose payout"),
            })?;
        if proposal.steps().len() != 1 {
            client.clear_proposal().await;
            anyhow::bail!("faucet payouts must build exactly one transaction");
        }
        let txids = client
            .calculate_stored_proposal()
            .await
            .context("could not build payout transaction")?;
        anyhow::ensure!(
            txids.len() == 1,
            "wallet built more than one payout transaction"
        );
        let txid = txids.first();
        let wallet = client.wallet().read().await;
        let transaction = wallet
            .wallet_transactions
            .get(txid)
            .context("built payout is missing from the wallet")?;
        let expiry_height = u32::from(transaction.transaction().expiry_height());
        anyhow::ensure!(expiry_height > 0, "payout transaction has no expiry height");
        Ok(PreparedPayment {
            txid: txid.to_string(),
            expiry_height,
        })
    }

    pub async fn persist(&self) -> Result<()> {
        let mut client = self.client.lock().await;
        persist(&mut client).await?;
        client.save_task().await;
        Ok(())
    }

    pub async fn transmit(&self, payment: &PreparedPayment) -> Result<()> {
        let txid = zingolib::utils::conversion::txid_from_hex_encoded_str(&payment.txid)?;
        let mut client = self.client.lock().await;
        client
            .transmit_calculated(nonempty::NonEmpty::new(txid))
            .await
            .context("wallet transmission outcome is uncertain")?;
        Ok(())
    }

    pub async fn check_readiness(&self, amount_zatoshis: i64) -> Result<()> {
        let mut client = self.client.lock().await;
        client
            .check_save_error()
            .await
            .context("light-wallet background save failed")?;
        ensure_sync_running(&mut client).await?;
        readiness::check(&mut client, amount_zatoshis).await
    }

    pub async fn maintain_sync(&self) -> Result<()> {
        let mut client = self.client.lock().await;
        ensure_sync_running(&mut client).await
    }

    pub async fn shutdown(&self) {
        let mut client = self.client.lock().await;
        let _ = client.stop_sync();
        let _ = client.await_sync().await;
        if let Err(err) = client.shutdown_save_task().await {
            error!(%err, "wallet save task failed during shutdown");
        }
        if let Err(err) = persist(&mut client).await {
            error!(%err, "final wallet save failed during shutdown");
        }
    }
}

async fn ensure_sync_running(client: &mut LightClient) -> Result<()> {
    if let Some((_, error)) = client.poll_sync_recovery() {
        warn!(%error, "light-wallet sync failed; restarting");
    }
    if client.sync_mode() == zingolib::sync::SyncMode::NotRunning {
        client
            .sync()
            .await
            .context("could not restart light-wallet sync")?;
        info!("light-wallet sync restarted");
    }
    Ok(())
}

pub async fn check_configured(network: &NetworkConfig, amount_zatoshis: i64) -> Result<()> {
    let path = wallet_path(network);
    anyhow::ensure!(
        path.is_file(),
        "wallet is not initialized; run `operator wallet-init` first"
    );
    secure_wallet_file(&path)?;
    let config = client_config(network, WalletConfig::Read, true)?;
    let mut client = LightClient::new(config, false)
        .await
        .context("could not open the testnet light wallet")?;
    readiness::check_configured(&mut client, amount_zatoshis).await
}

pub async fn initialize(network: &NetworkConfig) -> Result<()> {
    anyhow::ensure!(
        std::io::stdout().is_terminal(),
        "wallet initialization requires an interactive terminal so the recovery phrase is not redirected"
    );

    let wallet_path = wallet_path(network);
    prepare_wallet_directory(&network.wallet_dir)?;
    let _lock = acquire_wallet_lock(&wallet_path)?;
    anyhow::ensure!(
        !wallet_path.exists(),
        "wallet already exists; refusing to overwrite it"
    );

    let mut indexer = GrpcIndexer::new(network.lightwalletd_uri.clone())
        .await
        .context("could not connect to the configured lightwalletd")?;
    let info = indexer
        .get_lightd_info(Duration::from_secs(15))
        .await
        .context("could not get lightwalletd network information")?;
    let chain_height = u32::try_from(info.block_height)
        .context("lightwalletd reported a block height outside the supported range")?;
    readiness::check_server_info(&ServerInfo::from_lightd_info(
        info,
        network.lightwalletd_uri.clone(),
    ))?;

    let wallet_config = WalletConfig::NewSeed {
        no_of_accounts: NonZeroU32::MIN,
        chain_height,
        wallet_settings: WalletSettings::default(),
    };
    let config = client_config(network, wallet_config, false)?;
    let mut client = LightClient::new(config, false)
        .await
        .context("could not create the testnet light wallet")?;
    let recovery = client
        .recovery_info()
        .await
        .context("new wallet did not provide recovery information")?;
    let (_, address) = client
        .generate_unified_address(
            zingolib::wallet::keys::unified::ReceiverSelection::all_shielded(),
            zip32::AccountId::ZERO,
        )
        .await
        .context("could not generate the faucet receive address")?;
    persist(&mut client).await?;
    secure_wallet_file(&wallet_path)?;

    println!("Wallet created at {}", wallet_path.display());
    println!("Birthday height: {}", recovery.birthday);
    println!(
        "Fund this testnet address: {}",
        address.encode(&ChainType::Testnet)
    );
    println!("Write this recovery phrase down offline. It will not be shown again:");
    println!("{}", recovery.seed_phrase);
    println!(
        "The wallet file contains spending keys. Keep its directory private and back it up securely."
    );
    Ok(())
}

fn client_config(
    network: &NetworkConfig,
    wallet_config: WalletConfig,
    online: bool,
) -> Result<ClientConfig> {
    let mut builder = ClientConfig::builder()
        .set_chain_type(ChainType::Testnet)
        .set_wallet_dir(network.wallet_dir.clone())
        .set_wallet_name(WALLET_FILE.to_owned())
        .set_wallet_config(wallet_config);
    if online {
        builder = builder
            .set_indexer_uri(network.lightwalletd_uri.clone())
            .set_remote_indexer_trust(Trust::Trusted);
    }
    builder
        .build()
        .context("invalid light-wallet configuration")
}

fn wallet_path(network: &NetworkConfig) -> PathBuf {
    network.wallet_dir.join(WALLET_FILE)
}

fn wallet_lock_path(wallet_path: &Path) -> PathBuf {
    wallet_path.with_extension("dat.lock")
}

pub fn lock_configured(network: &NetworkConfig) -> Result<File> {
    acquire_wallet_lock(&wallet_path(network))
}

fn acquire_wallet_lock(wallet_path: &Path) -> Result<File> {
    if let Some(parent) = wallet_path.parent() {
        prepare_wallet_directory(parent)?;
    }
    let lock_path = wallet_lock_path(wallet_path);
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(&lock_path)
        .with_context(|| format!("could not open wallet lock {}", lock_path.display()))?;
    secure_wallet_file(&lock_path)?;
    file.try_lock_exclusive()
        .with_context(|| format!("wallet is already open: {}", wallet_path.display()))?;
    Ok(file)
}

fn prepare_wallet_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("could not create wallet directory {}", path.display()))?;
    #[cfg(unix)]
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .with_context(|| format!("could not secure wallet directory {}", path.display()))?;
    Ok(())
}

fn secure_wallet_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("could not secure wallet file {}", path.display()))?;
    Ok(())
}

fn payment_request(address: &str, amount_zatoshis: i64) -> Result<TransactionRequest> {
    crate::amount::validate_zatoshis(amount_zatoshis)?;
    let address = zingolib::utils::conversion::address_from_str(address)?;
    let amount = zingolib::utils::conversion::zatoshis_from_u64(u64::try_from(amount_zatoshis)?)?;
    let payment = Payment::new(address, Some(amount), None, None, None, vec![])?;
    Ok(TransactionRequest::new(vec![payment])?)
}

// Zingolib's flush and background saver use the same temporary filename. Serialize them,
// and force serialization because a failed library write may already have cleared its dirty flag.
async fn persist(client: &mut LightClient) -> Result<()> {
    client
        .shutdown_save_task()
        .await
        .context("wallet background save failed")?;
    client.wallet().write().await.mark_dirty();
    if let Err(error) = client.flush().await {
        client.wallet().write().await.mark_dirty();
        return Err(error).context("could not durably save the wallet");
    }
    secure_wallet_file(&client.wallet_path())?;
    #[cfg(unix)]
    if let Some(parent) = client.wallet_path().parent() {
        File::open(parent)?
            .sync_all()
            .context("could not sync wallet directory")?;
    }
    Ok(())
}

pub async fn verify_payment(
    network: &NetworkConfig,
    claim: &UnresolvedClaim,
    txid: &str,
) -> Result<VerifiedPayment> {
    // Verification can update expired inputs, so it must never share a wallet with the bot.
    let _lock = acquire_wallet_lock(&wallet_path(network))?;
    let config = client_config(network, WalletConfig::Read, true)?;
    let mut client = LightClient::new(config, false)
        .await
        .context("could not open wallet for reconciliation")?;
    readiness::check_server_info(&client.info().await?)?;
    client
        .sync_to_tip_and_await()
        .await
        .context("reconciliation requires a complete wallet scan")?;
    let parsed = zingolib::utils::conversion::txid_from_hex_encoded_str(txid)?;
    let wallet = client.wallet().read().await;
    let transaction = wallet.wallet_transactions.get(&parsed);
    let local_expiry =
        transaction.map(|transaction| u32::from(transaction.transaction().expiry_height()));
    if let (Some(recorded), Some(local)) = (claim.expiry_height, local_expiry) {
        anyhow::ensure!(
            recorded == local,
            "transaction expiry differs from the claim journal"
        );
    }
    let expiry_height = claim.expiry_height.or(local_expiry).unwrap_or(0);
    let scanned_height = wallet
        .sync_state
        .fully_scanned_height()
        .map(u32::from)
        .context("wallet has no fully scanned height")?;
    let locally_confirmed =
        transaction.is_some_and(|transaction| transaction.status().is_confirmed());
    let expected_bytes = transaction
        .map(|transaction| {
            let mut bytes = Vec::new();
            transaction.transaction().write(&mut bytes).map(|()| bytes)
        })
        .transpose()?;
    drop(wallet);
    let mut indexer = GrpcIndexer::new(network.lightwalletd_uri.clone()).await?;
    let lookup = indexer
        .get_transaction(
            lightwallet_protocol::TxFilter {
                hash: parsed.as_ref().to_vec(),
                ..Default::default()
            },
            Duration::from_secs(15),
        )
        .await;
    let evidence = transaction_evidence(
        lookup,
        expected_bytes.as_deref(),
        locally_confirmed,
        scanned_height,
        expiry_height,
    )?;
    let confirmed = matches!(evidence, VerifiedPayment::Confirmed);
    let outcome = if confirmed {
        let summaries = client.transaction_summaries(false).await?;
        let summary = summaries
            .iter()
            .find(|summary| summary.txid == parsed)
            .context("transaction summary is unavailable")?;
        anyhow::ensure!(
            matches!(
                summary.kind,
                zingolib::wallet::summary::data::TransactionKind::Sent(_)
            ) && !summary.pools_sent_from.is_empty(),
            "transaction does not spend the faucet wallet's funds"
        );
        let amount = u64::try_from(claim.amount_zatoshis)?;
        let matches = summary
            .outgoing_ironwood_notes
            .iter()
            .chain(summary.outgoing_orchard_notes.iter())
            .chain(summary.outgoing_sapling_notes.iter())
            .any(|note| {
                note.account_id == zip32::AccountId::ZERO
                    && note.value == amount
                    && (note.recipient == claim.destination
                        || note.recipient_unified_address.as_deref() == Some(&claim.destination))
            })
            || summary
                .outgoing_transparent_coins
                .iter()
                .any(|coin| coin.value == amount && coin.recipient == claim.destination);
        anyhow::ensure!(
            matches,
            "confirmed transaction does not pay the claim's recipient and amount"
        );
        VerifiedPayment::Confirmed
    } else if claim.txid.as_deref() == Some(txid) {
        VerifiedPayment::Expired
    } else {
        anyhow::bail!(
            "transaction is neither a verified payment nor verified expired; claim remains blocked"
        );
    };
    persist(&mut client).await?;
    Ok(outcome)
}

fn transaction_evidence(
    lookup: std::result::Result<lightwallet_protocol::RawTransaction, zingolib::netutils::Status>,
    expected_bytes: Option<&[u8]>,
    locally_confirmed: bool,
    scanned_height: u32,
    expiry_height: u32,
) -> Result<VerifiedPayment> {
    match lookup {
        Ok(raw) => {
            anyhow::ensure!(
                locally_confirmed,
                "wallet and indexer disagree on confirmation; claim remains blocked"
            );
            anyhow::ensure!(
                expected_bytes == Some(raw.data.as_slice()),
                "indexer transaction cannot be matched to the wallet"
            );
            anyhow::ensure!(
                raw.height > 0 && raw.height <= u64::from(scanned_height),
                "transaction is pending or above the fully scanned height"
            );
            Ok(VerifiedPayment::Confirmed)
        }
        Err(error) if error.code() == zingolib::netutils::Status::not_found("").code() => {
            anyhow::ensure!(
                !locally_confirmed,
                "wallet and indexer disagree on confirmation"
            );
            // A lost wallet-file write may leave only the durable SQLite expiry journal.
            // Absence before expiry never proves the transaction cannot still be mined.
            anyhow::ensure!(
                expiry_height > 0 && scanned_height > expiry_height,
                "transaction has not verifiably expired"
            );
            Ok(VerifiedPayment::Expired)
        }
        Err(error) => Err(error).context("transaction lookup failed; absence is not established"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zingolib::netutils::Status;

    #[test]
    fn nonpayment_requires_expiry_and_explicit_absence_after_full_scan() {
        assert!(matches!(
            transaction_evidence(Err(Status::not_found("absent")), None, false, 101, 100).unwrap(),
            VerifiedPayment::Expired
        ));
        for (height, expiry, confirmed) in [
            (99, 100, false),
            (100, 100, false),
            (101, 0, false),
            (101, 100, true),
        ] {
            assert!(
                transaction_evidence(
                    Err(Status::not_found("absent")),
                    None,
                    confirmed,
                    height,
                    expiry
                )
                .is_err()
            );
        }
        for error in [
            Status::unavailable("offline"),
            Status::deadline_exceeded("timeout"),
            Status::unknown("unknown"),
            Status::permission_denied("denied"),
        ] {
            assert!(transaction_evidence(Err(error), None, false, 101, 100).is_err());
        }
    }

    #[test]
    fn paid_evidence_requires_matching_bytes_and_a_scanned_mined_height() {
        let raw = |height| lightwallet_protocol::RawTransaction {
            data: vec![1, 2],
            height,
        };
        assert!(matches!(
            transaction_evidence(Ok(raw(99)), Some(&[1, 2]), true, 101, 100).unwrap(),
            VerifiedPayment::Confirmed
        ));
        for (height, bytes) in [
            (0, Some(&[1, 2][..])),
            (102, Some(&[1, 2][..])),
            (99, Some(&[9][..])),
            (99, None),
        ] {
            assert!(transaction_evidence(Ok(raw(height)), bytes, true, 101, 100).is_err());
        }
        assert!(transaction_evidence(Ok(raw(99)), Some(&[1, 2]), false, 101, 100).is_err());
    }

    #[test]
    fn reconciliation_lock_excludes_another_wallet_owner() {
        let directory = tempfile::tempdir().unwrap();
        let network = NetworkConfig {
            wallet_dir: directory.path().to_owned(),
            lightwalletd_uri: "https://testnet.zec.rocks:443".parse().unwrap(),
        };
        let lock = lock_configured(&network).unwrap();
        assert!(lock_configured(&network).is_err());
        drop(lock);
        assert!(lock_configured(&network).is_ok());
    }

    #[tokio::test]
    async fn failed_continuous_sync_is_restarted() {
        use tonic::Code;
        use zingolib::{
            data::PollReport,
            testutils::mock_indexer::{Fault, MockNet, Rpc},
        };

        let mut net = MockNet::launch().await;
        net.chain.write().await.mine_empty_blocks(2);
        let mut client = net
            .client(
                "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon art",
                None,
            )
            .await;
        let mut settings = client.wallet().read().await.wallet_settings.clone();
        settings.sync_config.shutdown_on_completion = false;
        client.update_wallet_settings(settings).await;
        client.sync().await.unwrap();

        let mut progress = client.subscribe_sync_status();
        tokio::time::timeout(Duration::from_secs(30), async {
            while progress
                .borrow()
                .as_ref()
                .is_none_or(|status| !status.is_complete())
            {
                progress.changed().await.unwrap();
            }
        })
        .await
        .expect("initial scan completes");

        net.chain.write().await.faults.inject(
            Rpc::LatestBlock,
            Fault::Fail(Code::Unavailable, "mock outage".to_owned()),
        );
        tokio::time::timeout(Duration::from_secs(30), async {
            while progress.changed().await.is_ok() {}
        })
        .await
        .expect("failed sync session exits");

        net.chain.write().await.mine_empty_blocks(101);
        ensure_sync_running(&mut client).await.unwrap();
        assert!(matches!(client.poll_sync(), PollReport::NotReady));
        let mut progress = client.subscribe_sync_status();
        tokio::time::timeout(Duration::from_secs(30), async {
            while client
                .wallet()
                .read()
                .await
                .sync_state
                .fully_scanned_height()
                .is_none_or(|height| u32::from(height) < 103)
            {
                progress.changed().await.unwrap();
            }
        })
        .await
        .expect("restarted session catches up after falling over 100 blocks behind");
    }
    #[tokio::test]
    #[ignore = "requires a funded testnet wallet snapshot and network access; never broadcasts"]
    async fn funded_snapshot_builds_and_persists_supported_recipient_types() -> Result<()> {
        let snapshot = std::env::var_os("FAUCET_TEST_WALLET_PATH")
            .context("FAUCET_TEST_WALLET_PATH is required")?;
        let uri: http::Uri = std::env::var("FAUCET_TEST_LIGHTWALLETD_URI")
            .unwrap_or_else(|_| "https://testnet.zec.rocks:443".to_owned())
            .parse()?;
        let recipient_dir = tempfile::tempdir()?;
        let recipient_network = NetworkConfig {
            wallet_dir: recipient_dir.path().to_owned(),
            lightwalletd_uri: uri.clone(),
        };
        let mut indexer = GrpcIndexer::new(uri.clone()).await?;
        let info = indexer.get_lightd_info(Duration::from_secs(15)).await?;
        let recipient_config = client_config(
            &recipient_network,
            WalletConfig::NewSeed {
                no_of_accounts: NonZeroU32::MIN,
                chain_height: u32::try_from(info.block_height)?,
                wallet_settings: WalletSettings::default(),
            },
            false,
        )?;
        let mut recipient = LightClient::new(recipient_config, false).await?;
        let (_, unified) = recipient
            .generate_unified_address(
                zingolib::wallet::keys::unified::ReceiverSelection::all_shielded(),
                zip32::AccountId::ZERO,
            )
            .await?;
        let (_, transparent) = recipient
            .generate_transparent_address(zip32::AccountId::ZERO, false)
            .await?;
        let recipients = [
            ("unified", unified.encode(&ChainType::Testnet)),
            (
                "sapling",
                zcash_keys::address::Address::Sapling(Box::new(
                    *unified.sapling().context("no Sapling receiver")?,
                ))
                .encode(&ChainType::Testnet),
            ),
            (
                "transparent",
                zcash_keys::address::Address::Transparent(transparent).encode(&ChainType::Testnet),
            ),
        ];
        for (kind, address) in recipients {
            let directory = tempfile::tempdir()?;
            prepare_wallet_directory(directory.path())?;
            let path = directory.path().join(WALLET_FILE);
            std::fs::copy(&snapshot, &path)?;
            secure_wallet_file(&path)?;
            let network = NetworkConfig {
                wallet_dir: directory.path().to_owned(),
                lightwalletd_uri: uri.clone(),
            };
            let faucet = FaucetWallet::open(&network).await?;
            {
                let mut client = faucet.client.lock().await;
                client.sync_to_tip_and_await().await?;
            }
            faucet.check_readiness(100_000).await?;
            let payment = faucet.prepare(&address, 100_000).await?;
            faucet.persist().await?;
            faucet.shutdown().await;
            drop(faucet);
            let loaded =
                LightClient::new(client_config(&network, WalletConfig::Read, false)?, false)
                    .await?;
            let wallet = loaded.wallet().read().await;
            let txid = zingolib::utils::conversion::txid_from_hex_encoded_str(&payment.txid)?;
            let transaction = wallet
                .wallet_transactions
                .get(&txid)
                .context("prepared payout missing after reload")?;
            assert_eq!(transaction.status().to_string(), "calculated");
            assert_eq!(
                u32::from(transaction.transaction().expiry_height()),
                payment.expiry_height
            );
            let summaries = wallet.transaction_summaries(false).await?;
            let summary = summaries
                .iter()
                .find(|summary| summary.txid == txid)
                .context("prepared payout summary missing")?;
            assert!(
                summary
                    .outgoing_ironwood_notes
                    .iter()
                    .chain(summary.outgoing_orchard_notes.iter())
                    .chain(summary.outgoing_sapling_notes.iter())
                    .any(|note| note.value == 100_000
                        && (note.recipient == address
                            || note.recipient_unified_address.as_deref() == Some(&address)))
                    || summary
                        .outgoing_transparent_coins
                        .iter()
                        .any(|coin| coin.value == 100_000 && coin.recipient == address)
            );
            println!(
                "{kind}: built and reloaded 0.001 TAZ transaction {}, expiry {} (not broadcast)",
                payment.txid, payment.expiry_height
            );
        }
        Ok(())
    }
}
