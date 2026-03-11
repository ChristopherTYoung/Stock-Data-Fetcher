-- Add debt_to_equity column to stock table
ALTER TABLE incrementum.stock
ADD COLUMN IF NOT EXISTS debt_to_equity NUMERIC(20, 6);
