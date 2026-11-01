-- =====================================================================
-- Phase 4: mark trades that Coinbase replays after a (re)connect
-- New databases run this automatically after 002.
-- Existing databases: psql "$DATABASE_URL" -f db/migrations/003_trade_replay_flag.sql
-- =====================================================================

-- Right after connecting, Coinbase sends a batch of recent trades. They are
-- real trades and count in candles, but they are seconds to minutes old, so
-- they would make the ingestion-latency figures look far worse than they are.
-- Nullable with a default: works on compressed chunks, old rows read as false.
ALTER TABLE trades ADD COLUMN IF NOT EXISTS is_replay BOOLEAN DEFAULT false;
