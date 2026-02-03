-- Migration: add polygon metadata columns to incrementum.stock
-- Adds columns only if they do not already exist

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS description TEXT;

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS market_cap BIGINT;

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS primary_exchange VARCHAR(100);

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS type VARCHAR(50);

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS currency_name VARCHAR(50);

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS cik VARCHAR(50);

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS composite_figi VARCHAR(50);

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS share_class_figi VARCHAR(50);

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS outstanding_shares BIGINT;

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS eps NUMERIC(20,6);

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS homepage_url VARCHAR(255);

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS total_employees INTEGER;

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS list_date DATE;

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS locale VARCHAR(20);

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS sic_code VARCHAR(20);

ALTER TABLE IF EXISTS incrementum.stock
  ADD COLUMN IF NOT EXISTS sic_description VARCHAR(255);

-- Optional: create an index on symbol for faster upserts (if not present)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'i' AND c.relname = 'idx_stock_symbol'
  ) THEN
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stock_symbol ON incrementum.stock (symbol);
  END IF;
EXCEPTION WHEN others THEN
  -- ignore index creation errors in simple migration runs
  RAISE NOTICE 'Index creation skipped or failed';
END$$;
