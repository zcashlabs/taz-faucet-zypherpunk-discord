CREATE TABLE IF NOT EXISTS claims (
    id          INTEGER PRIMARY KEY,
    user_id     TEXT NOT NULL,
    destination TEXT,
    status      TEXT NOT NULL CHECK (status IN ('submitting', 'succeeded', 'failed', 'unknown')),
    txid        TEXT,
    created_at  INTEGER NOT NULL, -- Unix epoch milliseconds.
    updated_at  INTEGER NOT NULL, -- Unix epoch milliseconds.
    amount_zatoshis INTEGER NOT NULL
        CHECK (amount_zatoshis BETWEEN 1 AND 2100000000000000)
);

CREATE INDEX IF NOT EXISTS claims_user_cooldown
    ON claims (user_id, updated_at DESC)
    WHERE status = 'succeeded';

CREATE INDEX IF NOT EXISTS claims_unresolved
    ON claims (status) WHERE status IN ('submitting', 'unknown');

CREATE UNIQUE INDEX IF NOT EXISTS claims_unique_txid
    ON claims (txid COLLATE NOCASE)
    WHERE txid IS NOT NULL;

CREATE TABLE IF NOT EXISTS settings (
    name  TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT OR IGNORE INTO settings (name, value) VALUES ('paused', '1');
INSERT OR IGNORE INTO settings (name, value) VALUES ('payout_amount_zatoshis', '10000000');
