-- Add revenue_per_share column to stock table
ALTER TABLE incrementum.stock
ADD COLUMN IF NOT EXISTS revenue_per_share NUMERIC(20, 2);

ALTER TABLE incrementum.stock
ALTER COLUMN revenue_per_share TYPE NUMERIC(20, 2)
USING ROUND(revenue_per_share::numeric, 2);