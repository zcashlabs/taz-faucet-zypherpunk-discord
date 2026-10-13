use std::{env, path::PathBuf};

use anyhow::{Context, Result, bail};

use crate::amount::parse_taz;

pub struct NetworkConfig {
    pub lightwalletd_uri: http::Uri,
    pub wallet_dir: PathBuf,
}

impl NetworkConfig {
    pub fn from_env() -> Result<Self> {
        let endpoint = required("LIGHTWALLETD_URI")?;
        let lightwalletd_uri: http::Uri = endpoint
            .parse()
            .context("LIGHTWALLETD_URI must be a valid gRPC endpoint URI")?;
        let scheme = lightwalletd_uri
            .scheme_str()
            .context("LIGHTWALLETD_URI must include a scheme")?;
        anyhow::ensure!(
            matches!(scheme, "http" | "https"),
            "LIGHTWALLETD_URI must use HTTP or HTTPS"
        );
        let authority = lightwalletd_uri
            .authority()
            .context("LIGHTWALLETD_URI must include a host")?;
        anyhow::ensure!(
            !authority.as_str().contains('@'),
            "LIGHTWALLETD_URI must not include credentials"
        );
        anyhow::ensure!(
            lightwalletd_uri
                .path_and_query()
                .is_none_or(|path| path.as_str() == "/"),
            "LIGHTWALLETD_URI must not include a path or query"
        );
        anyhow::ensure!(
            lightwalletd_uri.authority().is_some_and(|authority| {
                scheme == "https"
                    || authority.host().eq_ignore_ascii_case("localhost")
                    || authority
                        .host()
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback())
            }),
            "LIGHTWALLETD_URI must use HTTPS unless it targets loopback"
        );

        Ok(Self {
            lightwalletd_uri,
            wallet_dir: path("WALLET_DATA_DIR", "./data/wallet")?,
        })
    }
}

pub struct Config {
    pub discord_token: String,
    pub application_id: u64,
    pub guild_id: u64,
    pub channel_id: u64,
    pub database_path: PathBuf,
    pub daily_payout_cap_zatoshis: i64,
    pub network: NetworkConfig,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let config = Self {
            discord_token: required("DISCORD_TOKEN")?,
            application_id: number("DISCORD_APPLICATION_ID")?,
            guild_id: number("FAUCET_GUILD_ID")?,
            channel_id: number("FAUCET_CHANNEL_ID")?,
            database_path: database_path()?,
            daily_payout_cap_zatoshis: daily_payout_cap_zatoshis()?,
            network: NetworkConfig::from_env()?,
        };
        anyhow::ensure!(
            config.application_id > 0 && config.guild_id > 0 && config.channel_id > 0,
            "Discord application, guild, and channel IDs must be nonzero"
        );
        Ok(config)
    }
}

pub fn database_path() -> Result<PathBuf> {
    path("DATABASE_PATH", "./data/faucet.sqlite")
}

pub fn daily_payout_cap_zatoshis() -> Result<i64> {
    let value = env::var("DAILY_PAYOUT_CAP_TAZ").unwrap_or_else(|_| "10".to_owned());
    parse_taz(&value).context("DAILY_PAYOUT_CAP_TAZ must be a positive TAZ amount")
}

fn required(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    if value.trim().is_empty() {
        bail!("{name} cannot be empty");
    }
    Ok(value)
}

fn number(name: &str) -> Result<u64> {
    required(name)?
        .parse()
        .with_context(|| format!("{name} must be an unsigned integer"))
}

fn path(name: &str, default: &str) -> Result<PathBuf> {
    let path = env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default));
    anyhow::ensure!(!path.as_os_str().is_empty(), "{name} cannot be empty");
    Ok(path)
}
