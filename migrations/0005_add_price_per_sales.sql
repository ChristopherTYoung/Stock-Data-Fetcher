-- Add price_per_sales column with two-decimal precision
ALTER TABLE incrementum.stock
ADD COLUMN IF NOT EXISTS price_per_sales NUMERIC(20, 2);