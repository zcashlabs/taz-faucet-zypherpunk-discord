use crate::amount::format_taz;
use crate::config::Config;
use crate::database::Database;
use crate::payout::{ClaimResult, PayoutService};
use crate::wallet::FaucetWallet;
use anyhow::{Context as AnyhowContext, Result};
use serenity::{
    all::{
        CommandOptionType, CreateAllowedMentions, CreateCommand, CreateCommandOption,
        CreateInteractionResponse, CreateInteractionResponseMessage, EditInteractionResponse,
        GatewayIntents, Guild, Interaction, Ready,
    },
    async_trait,
    prelude::*,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const MAX_ADDRESS_LENGTH: usize = 512;

struct Handler {
    guild_id: u64,
    channel_id: u64,
    payout: Arc<PayoutService>,
    shutdown: CancellationToken,
    registering: AtomicBool,
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, ctx: Context, ready: Ready) {
        info!(bot = %ready.user.name, "Discord bot connected");
        self.register_commands(&ctx).await;
    }

    async fn guild_create(&self, ctx: Context, guild: Guild, _is_new: Option<bool>) {
        if guild.id.get() == self.guild_id {
            self.register_commands(&ctx).await;
        }
    }

    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        let Interaction::Command(command) = interaction else {
            return;
        };
        if command.data.name != "drip" {
            return;
        }

        if command.guild_id.map(|id| id.get()) != Some(self.guild_id)
            || command.channel_id.get() != self.channel_id
        {
            respond_ephemeral(
                &command,
                &ctx,
                "Use `/drip` in the configured faucet channel.",
            )
            .await;
            return;
        }
        let Some(address) = command
            .data
            .options
            .first()
            .and_then(|option| option.value.as_str())
            .map(str::trim)
        else {
            respond_ephemeral(&command, &ctx, "Provide a Zcash testnet address.").await;
            return;
        };
        let Some(address) = parse_testnet_address(address) else {
            respond_ephemeral(
                &command,
                &ctx,
                "Enter a valid Zcash testnet address: `utest1…`, `ztestsapling…`, `tm…`, or `t2…`.",
            )
            .await;
            return;
        };
        if let Err(err) = command
            .create_response(
                &ctx.http,
                CreateInteractionResponse::Message(
                    CreateInteractionResponseMessage::new()
                        .content("Processing your request…")
                        .allowed_mentions(no_mentions())
                        .ephemeral(true),
                ),
            )
            .await
        {
            warn!(%err, "could not acknowledge Discord interaction");
            return;
        }

        let reply = match self.payout.claim(command.user.id.get(), &address).await {
            Ok(result) => claim_response(result),
            Err(err) => {
                error!(%err, "could not process faucet claim");
                "The faucet is temporarily unavailable. Please try again later.".to_owned()
            }
        };
        finish_response(&command, &ctx, &reply).await;
    }
}

impl Handler {
    async fn register_commands(&self, ctx: &Context) {
        if self.registering.swap(true, Ordering::AcqRel) {
            return;
        }
        let guild = serenity::all::GuildId::new(self.guild_id);
        for attempt in 0..6 {
            let result = tokio::select! {
                _ = self.shutdown.cancelled() => break,
                result = guild.set_commands(&ctx.http, vec![drip_command()]) => result,
            };
            match result {
                Ok(_) => {
                    info!(guild_id = self.guild_id, "Discord /drip command registered");
                    break;
                }
                Err(err) => {
                    let permanent = matches!(&err, serenity::Error::Http(error)
                        if error.status_code().is_some_and(|status|
                            status.is_client_error() && status.as_u16() != 429));
                    if permanent || attempt == 5 {
                        error!(%err, guild_id = self.guild_id, "Discord command registration stopped; check bot installation and server access, then restart");
                        break;
                    }
                    let backoff = 1_u64 << attempt;
                    warn!(%err, backoff, "Discord command registration failed; retrying");
                    tokio::select! {
                        _ = self.shutdown.cancelled() => break,
                        _ = tokio::time::sleep(std::time::Duration::from_secs(backoff)) => {}
                    }
                }
            }
        }
        self.registering.store(false, Ordering::Release);
    }
}

fn parse_testnet_address(address: &str) -> Option<String> {
    if address.len() > MAX_ADDRESS_LENGTH {
        return None;
    }
    let encoded = zingolib::utils::conversion::address_from_str(address)
        .ok()?
        .to_string();
    ["utest1", "ztestsapling", "tm", "t2"]
        .iter()
        .any(|prefix| encoded.starts_with(prefix))
        .then_some(encoded)
}

fn claim_response(result: ClaimResult) -> String {
    match result {
        ClaimResult::Busy => {
            "Another payout is processing. Please try `/drip` again shortly; your claim was not used."
                .to_owned()
        }
        ClaimResult::Paused => "The faucet is temporarily paused.".to_owned(),
        ClaimResult::NeedsReconciliation => "A payout needs operator review. The faucet is temporarily unavailable.".to_owned(),
        ClaimResult::InProgress => "Your previous payout is still processing. Please wait for its result before requesting another drip.".to_owned(),
        ClaimResult::Cooldown(seconds) => {
            format!("Next claim available in {}.", human_duration(seconds))
        }
        ClaimResult::DailyLimitReached(Some(seconds)) => format!(
            "The faucet's 24-hour payout limit has been reached. Next payout available in {}.",
            human_duration(seconds)
        ),
        ClaimResult::DailyLimitReached(None) => {
            "The faucet's 24-hour payout limit has been reached.".to_owned()
        }
        ClaimResult::InsufficientFunds => {
            "The faucet doesn't have enough TAZ available right now.".to_owned()
        }
        ClaimResult::NotReady => {
            "The faucet is temporarily unavailable. Try again shortly.".to_owned()
        }
        ClaimResult::RecordingFailed { txid } => format!(
            "The payout was broadcast, but its local record needs operator review. Transaction: `{txid}`. Please contact an operator."
        ),
        ClaimResult::Paid {
            txid,
            amount_zatoshis,
        } => format!(
            "Sent {} TAZ. [View transaction](https://testnet.zecblock.com/tx/{txid}). It may take a little while to confirm.",
            format_taz(amount_zatoshis)
        ),
        ClaimResult::Rejected => "The wallet declined this payout, and your claim was not used. Check the address and try again; contact an operator if it continues.".to_owned(),
        ClaimResult::OutcomeUnknown => "The wallet did not return a clear result. To prevent a duplicate payment, this claim is held for operator review. Please contact an operator.".to_owned(),
    }
}

fn drip_command() -> CreateCommand {
    CreateCommand::new("drip")
        .description("Receive 0.1 TAZ once every 24 hours")
        .add_option(
            CreateCommandOption::new(
                CommandOptionType::String,
                "address",
                "Your Zcash testnet transparent or shielded address",
            )
            .required(true)
            .max_length(MAX_ADDRESS_LENGTH as u16),
        )
}

async fn respond_ephemeral(
    command: &serenity::all::CommandInteraction,
    ctx: &Context,
    content: &str,
) {
    if let Err(err) = command
        .create_response(
            &ctx.http,
            CreateInteractionResponse::Message(
                CreateInteractionResponseMessage::new()
                    .content(content)
                    .allowed_mentions(no_mentions())
                    .ephemeral(true),
            ),
        )
        .await
    {
        warn!(%err, "could not reply to Discord interaction");
    }
}

async fn finish_response(
    command: &serenity::all::CommandInteraction,
    ctx: &Context,
    content: &str,
) {
    if let Err(err) = command
        .edit_response(
            &ctx.http,
            EditInteractionResponse::new()
                .content(content)
                .allowed_mentions(no_mentions()),
        )
        .await
    {
        warn!(%err, "could not finish Discord interaction response");
    }
}

fn no_mentions() -> CreateAllowedMentions {
    CreateAllowedMentions::new()
        .all_users(false)
        .all_roles(false)
        .everyone(false)
}

fn human_duration(seconds: i64) -> String {
    let total_minutes = (seconds + 59) / 60;
    let hours = total_minutes / 60;
    let minutes = total_minutes % 60;
    if hours > 0 {
        format!("{hours}h {minutes}m")
    } else {
        format!("{minutes}m")
    }
}

pub async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let Config {
        discord_token,
        application_id,
        guild_id,
        channel_id,
        database_path,
        daily_payout_cap_zatoshis,
        network,
    } = Config::from_env()?;
    let db = Database::open(&database_path)?;
    db.recover_on_startup()?;
    let wallet = Arc::new(FaucetWallet::open(&network).await?);
    let payout = Arc::new(PayoutService::new(
        daily_payout_cap_zatoshis,
        db.clone(),
        wallet.clone(),
    ));
    let shutdown = CancellationToken::new();
    let handler = Handler {
        guild_id,
        channel_id,
        payout: payout.clone(),
        shutdown: shutdown.clone(),
        registering: AtomicBool::new(false),
    };

    info!(
        guild_id,
        channel_id, "Discord faucet bot starting; every payout is gated by wallet readiness"
    );

    let client = Client::builder(&discord_token, GatewayIntents::GUILDS)
        .event_handler(handler)
        .application_id(serenity::all::ApplicationId::new(application_id))
        .await
        .context("could not create Discord client");
    let mut client = match client {
        Ok(client) => client,
        Err(error) => {
            wallet.shutdown().await;
            return Err(error);
        }
    };

    let shard_manager = client.shard_manager.clone();
    let result = tokio::select! {
        result = client.start() => result.context("Discord client stopped with an error"),
        signal = shutdown_signal() => {
            match signal {
                Ok(()) => {
                    info!("shutdown signal received");
                    Ok(())
                }
                Err(error) => Err(error),
            }
        },
    };
    shutdown.cancel();
    payout.drain().await;
    shard_manager.shutdown_all().await;
    wallet.shutdown().await;
    result
}

pub(crate) async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate =
            signal(SignalKind::terminate()).context("could not install the SIGTERM handler")?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .context("could not install the Ctrl-C handler")?;
    Ok(())
}
