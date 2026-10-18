ALTER TABLE claims ADD COLUMN expiry_height INTEGER CHECK (expiry_height > 0);
ALTER TABLE claims ADD COLUMN resolution TEXT;
