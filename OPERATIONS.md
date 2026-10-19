# Running and recovering the faucet

The Discord interface is `/drip address:<testnet-address>`, restricted to the configured guild and channel. Replies in that channel are public and identify the requester without pinging them; attempts elsewhere receive a private redirect. Successful replies include a transaction link at `https://testnet.zecblock.com/tx/<txid>`; the explorer can take time to index a broadcast.

Configuration comes from `.env` as shown in `.env.example`. The payout amount is persisted in SQLite, initially 0.1 TAZ. Each user has a rolling 6-hour cooldown from payout completion. `DAILY_PAYOUT_CAP_TAZ` caps aggregate payouts over a rolling 24 hours; unresolved claims continue consuming that budget.

## Local commands

These commands assume the matching image has been built and the bot has started once to migrate the database:

```sh
docker compose up -d
docker compose run --rm faucet operator status
docker compose run --rm faucet operator pause
docker compose run --rm faucet operator resume
```

New installations start paused. Initialize the wallet interactively before starting the bot:

```sh
docker compose run --rm faucet operator wallet-init
```

Write down the recovery phrase offline and fund the printed public-testnet address. The wallet uses Zingolib and the configured remote lightwalletd; no local full node is required.

Change the amount while paused and no payout is active:

```sh
docker compose run --rm faucet operator pause
docker compose run --rm faucet operator set-amount 0.1
docker compose run --rm faucet operator resume
```

Pause stops new reservations; a reserved payout can finish. Graceful shutdown stops admitting new payouts and drains the active payout through database finalization before shutting down the wallet. Docker allows ten minutes for shutdown; a forced kill or power loss leaves the claim blocked for recovery.

Ordinary restarts preserve the operator's pause setting. Every payout checks current network, sync and spendable-balance readiness, then uses the wallet's real input selector and fee calculation. A restart with unresolved claims pauses the faucet. Clean history is retained, including transaction IDs, expiry heights and reconciliation reasons. Recipient addresses are cleared on completed claims; unresolved destinations are retained for verification.

## Unresolved payouts

Inspect claims locally:

```sh
docker compose run --rm faucet operator unresolved
docker compose run --rm faucet operator unresolved 100
docker compose run --rm faucet operator inspect 12
```

The optional ID pages backwards, excluding that ID. `inspect` prints the recipient, amount, transaction ID and expiry height when available. These details stay out of Discord.

A broadcast error may still mean the transaction reached the network. Wallet or SQLite persistence failures disable the payout worker and retain the claim. Restart that worker only after investigating and reconciling its unresolved claims.

For reconciliation, pause and stop the bot so the CLI can exclusively open the wallet. Stopping the bot does not alter the persisted administrative pause:

```sh
docker compose run --rm faucet operator pause
docker compose stop faucet
docker compose run --rm faucet operator reconcile 12 paid <txid>
# Or, for a transaction that has expired without being mined:
docker compose run --rm faucet operator reconcile 12 unpaid
docker compose up -d
docker compose run --rm faucet operator resume
```

`paid` requires a transaction matching the claim journal, a full wallet scan, the identical transaction bytes from the configured indexer, a mined height within the scan, and the claim's recipient and exact amount in the wallet's outgoing records. It does not equate network acceptance with confirmation.

`unpaid` requires a journaled transaction ID and expiry height, a complete wallet scan past expiry, and an explicit Not Found transaction lookup from the indexer. A timeout, connection error, pending transaction, missing expiry or conflicting wallet confirmation leaves the claim blocked. Merely finding a local Failed or Calculated wallet status never establishes nonpayment. The wallet state must be saved successfully before the database can release the claim. This evidence relies on the configured trusted indexer accurately serving the public testnet.

Older unresolved claims may lack a transaction journal. A separate exceptional command is available only for such claims, while the bot is stopped and payouts are paused:

```sh
docker compose run --rm faucet operator override-unpaid 12 --accept-double-payment-risk "reason and evidence reviewed by the operator"
```

This command does not verify nonpayment. It records an **UNVERIFIED OPERATOR OVERRIDE** and the reason, and can permit a duplicate payout if the operator's assessment is wrong. It cannot override a claim with a recorded transaction ID. Do not use it as routine reconciliation.

If a payout was accepted but local recording failed, the Discord response reports that distinction and retains the transaction ID. It does not report normal success. Further payouts stop until recovery.

## Discord installation

The bot application needs Guild Install with `bot` and `applications.commands`; privileged gateway intents and Administrator are unnecessary. The installer needs Manage Server. Grant the bot View Channel and Send Messages in the faucet channel, and allow members to Use Application Commands. `FAUCET_GUILD_ID` and `FAUCET_CHANNEL_ID` define the accepted location.

Registration stops on permanent HTTP errors such as Missing Access. Transient failures get six attempts with bounded backoff, cancelled on shutdown. Joining the configured guild or reconnecting retries registration; after correcting channel or application settings, restart the bot.

## Verification

Use Rust 1.99 from `rust-toolchain.toml`:

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --release --locked
```

The normal suite includes real SIGTERM and SIGKILL subprocess tests against a simulated wallet, plus SQLite failure injection and recovery tests. It does not send funds.

An optional test syncs temporary copies of a funded wallet, builds 0.001 TAZ payouts for unified, Sapling and transparent recipients, and checks that signed transactions reload from disk. It never broadcasts and does not write to the source wallet:

```sh
FAUCET_TEST_WALLET_PATH=/absolute/path/to/a/testnet/wallet-snapshot.dat \
FAUCET_TEST_LIGHTWALLETD_URI=https://testnet.zec.rocks:443 \
cargo test --locked funded_snapshot_builds_and_persists_supported_recipient_types -- --ignored --nocapture
```

A real payout and confirmation test must still be performed deliberately after deployment. Construction tests and simulated failures do not establish live broadcast, confirmation, or server outage behavior.
