-- Resource addressing v0.1
-- Goal:
--   1) Add stable v:// ref columns for core scarce resources.
--   2) Backfill existing records.
--   3) Add unique indexes for direct addressing.

BEGIN;

ALTER TABLE credentials
  ADD COLUMN IF NOT EXISTS ref VARCHAR(500);

ALTER TABLE qualifications
  ADD COLUMN IF NOT EXISTS ref VARCHAR(500);

ALTER TABLE contracts
  ADD COLUMN IF NOT EXISTS ref VARCHAR(500);

ALTER TABLE achievement_utxos
  ADD COLUMN IF NOT EXISTS experience_ref VARCHAR(500);

ALTER TABLE balances
  ADD COLUMN IF NOT EXISTS settlement_ref VARCHAR(500);

-- credentials -> v://{tenant}/credential/{company|person}/{holder_key}/{cert_type}/{cert_no}@v1
UPDATE credentials
SET ref = CONCAT(
  'v://', tenant_id::text,
  '/credential/',
  CASE WHEN holder_type = 'PERSON' THEN 'person' ELSE 'company' END,
  '/',
  regexp_replace(COALESCE(NULLIF(holder_ref, ''), holder_type || '-' || id::text), '^v://', 'v:%2F%2F'),
  '/',
  lower(COALESCE(cert_type, 'unknown')),
  '/',
  COALESCE(NULLIF(cert_number, ''), id::text),
  '@v1'
)
WHERE (ref IS NULL OR ref = '');

-- qualifications -> v://{tenant}/credential/{company|person}/{holder_id}/{qual_type}/{cert_no}@v1
UPDATE qualifications
SET ref = CONCAT(
  'v://', tenant_id::text,
  '/credential/',
  CASE WHEN holder_type = 'PERSON' THEN 'person' ELSE 'company' END,
  '/',
  holder_id::text,
  '/',
  lower(COALESCE(qual_type, 'unknown')),
  '/',
  COALESCE(NULLIF(cert_no, ''), id::text),
  '@v1'
)
WHERE (ref IS NULL OR ref = '');

-- contracts -> v://{tenant}/finance/contract/{id}@v1
UPDATE contracts
SET ref = CONCAT('v://', tenant_id::text, '/finance/contract/', id::text, '@v1')
WHERE (ref IS NULL OR ref = '');

-- achievement_utxos -> v://{tenant}/experience/project/{project_ref}/{utxo_ref}@v1
UPDATE achievement_utxos
SET experience_ref = CONCAT(
  'v://', tenant_id::text,
  '/experience/project/',
  regexp_replace(COALESCE(NULLIF(project_ref, ''), 'unknown'), '^v://', 'v:%2F%2F'),
  '/',
  regexp_replace(COALESCE(NULLIF(utxo_ref, ''), id::text), '^v://', 'v:%2F%2F'),
  '@v1'
)
WHERE (experience_ref IS NULL OR experience_ref = '');

-- balances -> v://{tenant}/finance/settlement/{id}@v1
UPDATE balances
SET settlement_ref = CONCAT('v://', tenant_id::text, '/finance/settlement/', id::text, '@v1')
WHERE (settlement_ref IS NULL OR settlement_ref = '');

CREATE UNIQUE INDEX IF NOT EXISTS idx_credentials_ref_uq
  ON credentials(ref)
  WHERE ref IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_qualifications_ref_uq
  ON qualifications(ref)
  WHERE ref IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_contracts_ref_uq
  ON contracts(ref)
  WHERE ref IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_achievement_experience_ref_uq
  ON achievement_utxos(experience_ref)
  WHERE experience_ref IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_balances_settlement_ref_uq
  ON balances(settlement_ref)
  WHERE settlement_ref IS NOT NULL;

COMMIT;
