-- Add missing balances.contract_id for environments created from older schemas.
-- Safe to run multiple times.

ALTER TABLE balances
    ADD COLUMN IF NOT EXISTS contract_id BIGINT REFERENCES contracts(id);

CREATE INDEX IF NOT EXISTS idx_balances_contract
    ON balances(contract_id);
