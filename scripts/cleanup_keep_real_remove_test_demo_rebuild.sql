-- Purpose:
--   Rebuild-compatible cleanup for obvious test/demo/mock/sample/sandbox data.
--   This variant targets schemas where employees.id_card and some optional tables do not exist.
--
-- Usage:
--   Dry run (default):
--     psql "$DATABASE_URL" -f scripts/cleanup_keep_real_remove_test_demo_rebuild.sql
--   Apply delete:
--     psql "$DATABASE_URL" -v apply=true -f scripts/cleanup_keep_real_remove_test_demo_rebuild.sql
--
-- Safety:
--   Default tail is ROLLBACK; use -v apply=true to COMMIT.

\if :{?apply}
\else
\set apply false
\endif

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

CREATE TEMP TABLE _kw (word TEXT PRIMARY KEY) ON COMMIT DROP;
INSERT INTO _kw(word) VALUES ('test'), ('demo'), ('mock'), ('sample'), ('sandbox');

CREATE TEMP TABLE _target_projects (ref TEXT PRIMARY KEY) ON COMMIT DROP;
CREATE TEMP TABLE _target_contract_ids (id BIGINT PRIMARY KEY) ON COMMIT DROP;
CREATE TEMP TABLE _target_achievement_ids (id BIGINT PRIMARY KEY) ON COMMIT DROP;

INSERT INTO _target_contract_ids(id)
SELECT c.id
FROM contracts c
WHERE c.deleted = FALSE
  AND EXISTS (
      SELECT 1 FROM _kw k
      WHERE LOWER(COALESCE(c.contract_name, '')) LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(c.num, '')) LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(c.project_ref, '')) LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(c.ref, '')) LIKE '%' || k.word || '%'
  );

INSERT INTO _target_projects(ref)
SELECT DISTINCT c.project_ref
FROM contracts c
JOIN _target_contract_ids tc ON tc.id = c.id
WHERE COALESCE(c.project_ref, '') <> ''
ON CONFLICT DO NOTHING;

INSERT INTO _target_projects(ref)
SELECT DISTINCT pn.ref
FROM project_nodes pn
WHERE EXISTS (
    SELECT 1 FROM _kw k
    WHERE LOWER(COALESCE(pn.ref, '')) LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(pn.name, '')) LIKE '%' || k.word || '%'
)
ON CONFLICT DO NOTHING;

INSERT INTO _target_achievement_ids(id)
SELECT DISTINCT a.id
FROM achievement_utxos a
WHERE a.project_ref IN (SELECT ref FROM _target_projects)
   OR a.contract_id IN (SELECT id FROM _target_contract_ids)
   OR EXISTS (
      SELECT 1 FROM _kw k
      WHERE LOWER(COALESCE(a.project_ref, '')) LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(a.ref, '')) LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(a.utxo_ref, '')) LIKE '%' || k.word || '%'
   );

SELECT 'TARGET COUNTS' AS section;
SELECT 'projects' AS entity, COUNT(*)::BIGINT AS cnt FROM _target_projects
UNION ALL
SELECT 'contracts', COUNT(*)::BIGINT FROM _target_contract_ids
UNION ALL
SELECT 'achievements', COUNT(*)::BIGINT FROM _target_achievement_ids
ORDER BY entity;

DELETE FROM engineer_achievement_receipts er
WHERE er.achievement_ref IN (
    SELECT COALESCE(NULLIF(a.ref, ''), a.utxo_ref)
    FROM achievement_utxos a
    JOIN _target_achievement_ids ta ON ta.id = a.id
)
   OR EXISTS (
      SELECT 1 FROM _kw k
      WHERE LOWER(COALESCE(er.ref, '')) LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(er.achievement_ref, '')) LIKE '%' || k.word || '%'
   );

DELETE FROM resource_bindings rb
WHERE rb.project_ref IN (SELECT ref FROM _target_projects)
   OR rb.achievement_utxo_id IN (SELECT id FROM _target_achievement_ids)
   OR EXISTS (
      SELECT 1 FROM _kw k
      WHERE LOWER(COALESCE(rb.project_ref, '')) LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(rb.resource_ref, '')) LIKE '%' || k.word || '%'
   );

DELETE FROM drawing_versions dv
WHERE dv.project_ref IN (SELECT ref FROM _target_projects)
   OR EXISTS (
      SELECT 1 FROM _kw k
      WHERE LOWER(COALESCE(dv.project_ref, '')) LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(dv.drawing_no, '')) LIKE '%' || k.word || '%'
   );

DELETE FROM review_certificates rc
WHERE rc.project_ref IN (SELECT ref FROM _target_projects)
   OR EXISTS (
      SELECT 1 FROM _kw k
      WHERE LOWER(COALESCE(rc.project_ref, '')) LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(rc.cert_ref, '')) LIKE '%' || k.word || '%'
   );

DELETE FROM drawings d
WHERE d.project_ref IN (SELECT ref FROM _target_projects)
   OR d.contract_id IN (SELECT id FROM _target_contract_ids)
   OR EXISTS (
      SELECT 1 FROM _kw k
      WHERE LOWER(COALESCE(d.project_ref, '')) LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(d.num, '')) LIKE '%' || k.word || '%'
   );

DELETE FROM payments p
WHERE p.project_ref IN (SELECT ref FROM _target_projects)
   OR p.contract_id IN (SELECT id FROM _target_contract_ids);

DELETE FROM costtickets ct
WHERE ct.project_ref IN (SELECT ref FROM _target_projects)
   OR ct.contract_id IN (SELECT id FROM _target_contract_ids);

DELETE FROM balances b
WHERE b.project_ref IN (SELECT ref FROM _target_projects)
   OR b.contract_id IN (SELECT id FROM _target_contract_ids);

DELETE FROM gatherings g
WHERE g.project_ref IN (SELECT ref FROM _target_projects)
   OR g.contract_id IN (SELECT id FROM _target_contract_ids);

DELETE FROM invoices i
WHERE i.project_ref IN (SELECT ref FROM _target_projects)
   OR i.contract_id IN (SELECT id FROM _target_contract_ids);

DELETE FROM achievement_profile_personnel pp
WHERE pp.profile_id IN (
    SELECT ap.id
    FROM achievement_profiles ap
    WHERE ap.contract_id IN (SELECT id FROM _target_contract_ids)
       OR ap.project_ref IN (SELECT ref FROM _target_projects)
       OR EXISTS (
            SELECT 1 FROM _kw k
            WHERE LOWER(COALESCE(ap.project_name, '')) LIKE '%' || k.word || '%'
       )
);

DELETE FROM achievement_profile_attachments pa
WHERE pa.profile_id IN (
    SELECT ap.id
    FROM achievement_profiles ap
    WHERE ap.contract_id IN (SELECT id FROM _target_contract_ids)
       OR ap.project_ref IN (SELECT ref FROM _target_projects)
       OR EXISTS (
            SELECT 1 FROM _kw k
            WHERE LOWER(COALESCE(ap.project_name, '')) LIKE '%' || k.word || '%'
       )
);

DELETE FROM achievement_profiles ap
WHERE ap.contract_id IN (SELECT id FROM _target_contract_ids)
   OR ap.project_ref IN (SELECT ref FROM _target_projects)
   OR EXISTS (
        SELECT 1 FROM _kw k
        WHERE LOWER(COALESCE(ap.project_name, '')) LIKE '%' || k.word || '%'
   );

DELETE FROM achievement_utxos a
WHERE a.id IN (SELECT id FROM _target_achievement_ids)
   OR a.contract_id IN (SELECT id FROM _target_contract_ids);

DELETE FROM contracts c
WHERE c.id IN (SELECT id FROM _target_contract_ids)
   OR c.project_ref IN (SELECT ref FROM _target_projects);

DELETE FROM project_nodes pn
WHERE pn.ref IN (SELECT ref FROM _target_projects)
   OR EXISTS (
      SELECT 1 FROM _kw k
      WHERE LOWER(COALESCE(pn.ref, '')) LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(pn.name, '')) LIKE '%' || k.word || '%'
   );

SELECT 'POST CHECK COUNTS' AS section;
SELECT 'contracts' AS table_name, COUNT(*)::BIGINT AS cnt FROM contracts
UNION ALL
SELECT 'achievement_utxos', COUNT(*)::BIGINT FROM achievement_utxos
UNION ALL
SELECT 'project_nodes', COUNT(*)::BIGINT FROM project_nodes
UNION ALL
SELECT 'gatherings', COUNT(*)::BIGINT FROM gatherings
UNION ALL
SELECT 'balances', COUNT(*)::BIGINT FROM balances
UNION ALL
SELECT 'invoices', COUNT(*)::BIGINT FROM invoices
UNION ALL
SELECT 'drawings', COUNT(*)::BIGINT FROM drawings
ORDER BY table_name;

\if :apply
COMMIT;
\else
ROLLBACK;
\endif
