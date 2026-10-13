use anyhow::Context;

mod amount;
mod bot;
mod config;
mod database;
mod operator;
mod payout;
mod readiness;
mod wallet;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::path::Path::new(".env").exists() {
        dotenvy::dotenv().context("could not load .env")?;
    }
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|argument| argument == "operator") {
        operator::run(&args[1..]).await
    } else if args.is_empty() {
        bot::run().await
    } else {
        anyhow::bail!("Usage: taz-faucet-zypherpunk-discord [operator <command>]")
    }
}
