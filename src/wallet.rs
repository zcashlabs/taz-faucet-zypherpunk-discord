use std::{
    fs::{File, OpenOptions},
    io::IsTerminal,
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use fs2::FileExt;
use tokio::sync::Mutex;
use tracing::error;
use zcash_client_backend::zip321::{Payment, TransactionRequest};
use zingolib::{
    config::{ChainType, ClientConfig, WalletConfig},
    data::ServerInfo,
    destination::servers::Trust,
    lightclient::{
        LightClient,
        error::{LightClientError, SendError as ZingoSendError},
    },
    netutils::{GrpcIndexer, Indexer},
    wallet::WalletSettings,
};

use crate::{config::NetworkConfig, readiness};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const WALLET_FILE: &str = "zingo-wallet.dat";

pub(crate) struct FaucetWallet {
    client: Arc<Mutex<LightClient>>,
    _lock: File,
}

pub(crate) enum SendError {
    Rejected,
    Unknown,
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
        client
            .flush()
            .await
            .context("could not save wallet settings")?;
        client
            .sync()
            .await
            .context("could not start light-wallet sync")?;
        client.save_task().await;

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            _lock,
        })
    }

    pub async fn send(
        &self,
        address: &str,
        amount_zatoshis: i64,
    ) -> std::result::Result<String, SendError> {
        let amount = u64::try_from(amount_zatoshis).map_err(|_| SendError::Rejected)?;
        let address = zingolib::utils::conversion::address_from_str(address)
            .map_err(|_| SendError::Rejected)?;
        let amount = zingolib::utils::conversion::zatoshis_from_u64(amount)
            .map_err(|_| SendError::Rejected)?;
        let payment = Payment::new(address, Some(amount), None, None, None, vec![])
            .map_err(|_| SendError::Rejected)?;
        let request = TransactionRequest::new(vec![payment]).map_err(|_| SendError::Rejected)?;

        let mut client = self.client.lock().await;
        match client
            .quick_send_reported(request, zip32::AccountId::ZERO, true)
            .await
        {
            Ok(reports) => {
                let txid = reports.last().txid.to_string();
                if let Err(err) = client.flush().await {
                    error!(%err, "wallet accepted payout but wallet state could not be saved");
                }
                Ok(txid)
            }
            Err(err) => {
                if let Err(save_error) = client.flush().await {
                    error!(%save_error, "could not persist wallet state after a failed send attempt");
                }
                if is_pre_broadcast_rejection(&err) {
                    Err(SendError::Rejected)
                } else {
                    error!(?err, "wallet send outcome is ambiguous");
                    Err(SendError::Unknown)
                }
            }
        }
    }

    pub async fn check_readiness(&self, amount_zatoshis: i64) -> Result<()> {
        let mut client = self.client.lock().await;
        client
            .check_save_error()
            .await
            .context("light-wallet background save failed")?;
        readiness::check(&mut client, amount_zatoshis).await
    }

    pub async fn shutdown(&self) {
        let mut client = self.client.lock().await;
        let _ = client.stop_sync();
        let _ = client.await_sync().await;
        if let Err(err) = client.shutdown_save_task().await {
            error!(%err, "wallet save task failed during shutdown");
        }
        if let Err(err) = client.flush().await {
            error!(%err, "final wallet save failed during shutdown");
        }
    }
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
    client
        .flush()
        .await
        .context("could not persist the new wallet")?;
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

fn is_pre_broadcast_rejection(error: &LightClientError) -> bool {
    matches!(
        error,
        LightClientError::SendError(
            ZingoSendError::ProposeSendError(_) | ZingoSendError::CalculateSendError(_)
        )
    )
}

#[cfg(test)]
mod tests {
    use super::is_pre_broadcast_rejection;

    #[test]
    fn classifies_only_proposal_and_transaction_build_errors_as_rejected() {
        use zingolib::{
            lightclient::error::{LightClientError, SendError},
            wallet::error::{CalculateTransactionError, KeyError},
        };

        let rejected = LightClientError::SendError(SendError::CalculateSendError(
            CalculateTransactionError::NoSpendingKey(KeyError::NoSpendCapability),
        ));
        assert!(is_pre_broadcast_rejection(&rejected));

        let ambiguous = LightClientError::Offline;
        assert!(!is_pre_broadcast_rejection(&ambiguous));
    }
}
