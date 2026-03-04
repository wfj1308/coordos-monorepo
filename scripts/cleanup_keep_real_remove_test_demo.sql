-- Purpose:
--   Keep real business data, remove obvious test/demo/mock/sample/sandbox data.
-- Safety:
--   1) This script runs in one transaction.
--   2) Default tail is ROLLBACK (dry run). Change to COMMIT after review.
--
-- Usage:
--   Dry run (default):
--     psql "$DATABASE_URL" -f scripts/cleanup_keep_real_remove_test_demo.sql
--   Apply delete:
--     psql "$DATABASE_URL" -v apply=true -f scripts/cleanup_keep_real_remove_test_demo.sql
--
-- Recommended:
--   Run once as dry run (default), check "TARGET COUNTS" and sample rows,
--   then replace final ROLLBACK with COMMIT.

\if :{?apply}
\else
\set apply false
\endif

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

-- ---------------------------------------------------------------------------
-- 0) Keyword set (strict, explicit markers only)
-- ---------------------------------------------------------------------------
CREATE TEMP TABLE _kw (
    word TEXT PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO _kw(word) VALUES
('test'),
('demo'),
('mock'),
('sample'),
('sandbox');

-- ---------------------------------------------------------------------------
-- 1) Collect target entities
-- ---------------------------------------------------------------------------
CREATE TEMP TABLE _target_projects (
    ref TEXT PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO _target_projects(ref)
SELECT DISTINCT pn.ref
FROM project_nodes pn
WHERE COALESCE(pn.ref, '') <> ''
  AND EXISTS (
      SELECT 1
      FROM _kw k
      WHERE LOWER(COALESCE(pn.ref, ''))  LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(pn.name, '')) LIKE '%' || k.word || '%'
  );

INSERT INTO _target_projects(ref)
SELECT DISTINCT c.project_ref
FROM contracts c
WHERE COALESCE(c.project_ref, '') <> ''
  AND EXISTS (
      SELECT 1
      FROM _kw k
      WHERE LOWER(COALESCE(c.project_ref, ''))    LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(c.contract_name, ''))  LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(c.num, ''))            LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(c.ref, ''))            LIKE '%' || k.word || '%'
  )
ON CONFLICT DO NOTHING;

INSERT INTO _target_projects(ref)
SELECT DISTINCT a.project_ref
FROM achievement_utxos a
WHERE COALESCE(a.project_ref, '') <> ''
  AND (
      LOWER(COALESCE(a.project_ref, '')) LIKE '%/project/demo-%'
      OR EXISTS (
          SELECT 1
          FROM _kw k
          WHERE LOWER(COALESCE(a.project_ref, '')) LIKE '%' || k.word || '%'
             OR LOWER(COALESCE(a.ref, ''))         LIKE '%' || k.word || '%'
             OR LOWER(COALESCE(a.utxo_ref, ''))    LIKE '%' || k.word || '%'
      )
  )
ON CONFLICT DO NOTHING;

CREATE TEMP TABLE _target_contract_ids (
    id BIGINT PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO _target_contract_ids(id)
SELECT DISTINCT c.id
FROM contracts c
LEFT JOIN _target_projects p ON p.ref = c.project_ref
WHERE p.ref IS NOT NULL
   OR EXISTS (
      SELECT 1
      FROM _kw k
      WHERE LOWER(COALESCE(c.contract_name, '')) LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(c.num, ''))           LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(c.ref, ''))           LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(c.project_ref, ''))   LIKE '%' || k.word || '%'
   );

CREATE TEMP TABLE _target_achievement_ids (
    id BIGINT PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO _target_achievement_ids(id)
SELECT DISTINCT a.id
FROM achievement_utxos a
LEFT JOIN _target_projects p ON p.ref = a.project_ref
WHERE p.ref IS NOT NULL
   OR LOWER(COALESCE(a.project_ref, '')) LIKE '%/project/demo-%'
   OR EXISTS (
      SELECT 1
      FROM _kw k
      WHERE LOWER(COALESCE(a.project_ref, '')) LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(a.ref, ''))         LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(a.utxo_ref, ''))    LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(a.source, ''))      LIKE '%' || k.word || '%'
   );

CREATE TEMP TABLE _target_company_ids (
    id BIGINT PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO _target_company_ids(id)
SELECT DISTINCT c.id
FROM companies c
WHERE EXISTS (
    SELECT 1
    FROM _kw k
    WHERE LOWER(COALESCE(c.name, ''))         LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(c.executor_ref, '')) LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(c.code, ''))         LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(c.license_num, ''))  LIKE '%' || k.word || '%'
);

CREATE TEMP TABLE _target_employee_ids (
    id BIGINT PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO _target_employee_ids(id)
SELECT DISTINCT e.id
FROM employees e
LEFT JOIN _target_company_ids tc ON tc.id = e.company_id
WHERE tc.id IS NOT NULL
   OR EXISTS (
      SELECT 1
      FROM _kw k
      WHERE LOWER(COALESCE(e.name, ''))         LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(e.account, ''))      LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(e.executor_ref, '')) LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(e.id_card, ''))      LIKE '%' || k.word || '%'
   );

CREATE TEMP TABLE _target_qualification_ids (
    id BIGINT PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO _target_qualification_ids(id)
SELECT DISTINCT q.id
FROM qualifications q
LEFT JOIN employees e ON e.executor_ref = q.executor_ref
LEFT JOIN _target_employee_ids te ON te.id = e.id
WHERE te.id IS NOT NULL
   OR EXISTS (
      SELECT 1
      FROM _kw k
      WHERE LOWER(COALESCE(q.executor_ref, '')) LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(q.cert_no, ''))      LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(q.holder_name, ''))  LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(q.ref, ''))          LIKE '%' || k.word || '%'
   );

CREATE TEMP TABLE _target_namespace_refs (
    ref TEXT PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO _target_namespace_refs(ref)
SELECT DISTINCT n.ref
FROM namespaces n
WHERE EXISTS (
    SELECT 1
    FROM _kw k
    WHERE LOWER(COALESCE(n.ref, ''))        LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(n.short_code, '')) LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(n.name, ''))       LIKE '%' || k.word || '%'
);

CREATE TEMP TABLE _target_bid_document_ids (
    id BIGINT PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO _target_bid_document_ids(id)
SELECT DISTINCT bd.id
FROM bid_documents bd
WHERE bd.project_ref IN (SELECT ref FROM _target_projects)
   OR bd.contract_id IN (SELECT id FROM _target_contract_ids)
   OR EXISTS (
      SELECT 1
      FROM _kw k
      WHERE LOWER(COALESCE(bd.bid_ref, ''))       LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(bd.project_name, ''))  LIKE '%' || k.word || '%'
         OR LOWER(COALESCE(bd.project_ref, ''))   LIKE '%' || k.word || '%'
   );

-- ---------------------------------------------------------------------------
-- 2) Preview target scope
-- ---------------------------------------------------------------------------
SELECT 'TARGET COUNTS' AS section;
SELECT 'projects'       AS entity, COUNT(*)::BIGINT AS cnt FROM _target_projects
UNION ALL
SELECT 'contracts',     COUNT(*)::BIGINT FROM _target_contract_ids
UNION ALL
SELECT 'achievements',  COUNT(*)::BIGINT FROM _target_achievement_ids
UNION ALL
SELECT 'companies',     COUNT(*)::BIGINT FROM _target_company_ids
UNION ALL
SELECT 'employees',     COUNT(*)::BIGINT FROM _target_employee_ids
UNION ALL
SELECT 'qualifications',COUNT(*)::BIGINT FROM _target_qualification_ids
UNION ALL
SELECT 'namespaces',    COUNT(*)::BIGINT FROM _target_namespace_refs
UNION ALL
SELECT 'bid_documents', COUNT(*)::BIGINT FROM _target_bid_document_ids
ORDER BY entity;

SELECT 'TARGET PROJECT SAMPLE' AS section;
SELECT ref FROM _target_projects ORDER BY ref LIMIT 50;

SELECT 'TARGET CONTRACT SAMPLE' AS section;
SELECT c.id, c.project_ref, c.num, c.contract_name, c.state, c.created_at
FROM contracts c
JOIN _target_contract_ids t ON t.id = c.id
ORDER BY c.created_at DESC
LIMIT 50;

SELECT 'TARGET ACHIEVEMENT SAMPLE' AS section;
SELECT a.id,
       COALESCE(NULLIF(a.ref, ''), a.utxo_ref) AS ref_or_utxo,
       a.project_ref,
       a.source,
       a.status,
       a.ingested_at
FROM achievement_utxos a
JOIN _target_achievement_ids t ON t.id = a.id
ORDER BY a.ingested_at DESC
LIMIT 50;

-- ---------------------------------------------------------------------------
-- 3) Delete in dependency-safe order
-- ---------------------------------------------------------------------------

-- 3.1 Achievement-related
DELETE FROM proof_anchors pa
WHERE pa.ref IN (
    SELECT COALESCE(NULLIF(a.ref, ''), a.utxo_ref)
    FROM achievement_utxos a
    JOIN _target_achievement_ids ta ON ta.id = a.id
)
   OR EXISTS (
    SELECT 1 FROM _kw k
    WHERE LOWER(COALESCE(pa.ref, '')) LIKE '%' || k.word || '%'
);

DELETE FROM engineer_achievement_receipts er
WHERE er.achievement_ref IN (
    SELECT COALESCE(NULLIF(a.ref, ''), a.utxo_ref)
    FROM achievement_utxos a
    JOIN _target_achievement_ids ta ON ta.id = a.id
)
   OR EXISTS (
    SELECT 1 FROM _kw k
    WHERE LOWER(COALESCE(er.ref, ''))             LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(er.achievement_ref, '')) LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(er.executor_ref, ''))    LIKE '%' || k.word || '%'
);

DELETE FROM step_achievement_utxos s
WHERE s.project_ref IN (SELECT ref FROM _target_projects)
   OR EXISTS (
    SELECT 1 FROM _kw k
    WHERE LOWER(COALESCE(s.ref, ''))        LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(s.project_ref, ''))LIKE '%' || k.word || '%'
);

DELETE FROM resource_bindings rb
WHERE rb.project_ref IN (SELECT ref FROM _target_projects)
   OR rb.achievement_utxo_id IN (SELECT id FROM _target_achievement_ids)
   OR EXISTS (
    SELECT 1 FROM _kw k
    WHERE LOWER(COALESCE(rb.resource_ref, '')) LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(rb.project_ref, ''))  LIKE '%' || k.word || '%'
);

DELETE FROM achievement_utxos a
WHERE a.id IN (SELECT id FROM _target_achievement_ids)
   OR a.contract_id IN (SELECT id FROM _target_contract_ids);

-- 3.2 Project/contract-related
DELETE FROM bid_resources br
WHERE br.bid_id IN (SELECT id FROM _target_bid_document_ids)
   OR EXISTS (
    SELECT 1 FROM _kw k
    WHERE LOWER(COALESCE(br.resource_ref, ''))  LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(br.resource_name, '')) LIKE '%' || k.word || '%'
);

DELETE FROM bid_documents bd
WHERE bd.id IN (SELECT id FROM _target_bid_document_ids)
   OR EXISTS (
    SELECT 1 FROM _kw k
    WHERE LOWER(COALESCE(bd.project_ref, '')) LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(bd.bid_ref, ''))     LIKE '%' || k.word || '%'
);

DELETE FROM drawing_versions dv
WHERE dv.project_ref IN (SELECT ref FROM _target_projects)
   OR EXISTS (
    SELECT 1 FROM _kw k
    WHERE LOWER(COALESCE(dv.project_ref, '')) LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(dv.drawing_no, ''))  LIKE '%' || k.word || '%'
);

DELETE FROM drawings d
WHERE d.project_ref IN (SELECT ref FROM _target_projects)
   OR d.contract_id IN (SELECT id FROM _target_contract_ids)
   OR EXISTS (
    SELECT 1 FROM _kw k
    WHERE LOWER(COALESCE(d.project_ref, '')) LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(d.num, ''))         LIKE '%' || k.word || '%'
);

DELETE FROM review_certificates rc
WHERE rc.project_ref IN (SELECT ref FROM _target_projects)
   OR EXISTS (
    SELECT 1 FROM _kw k
    WHERE LOWER(COALESCE(rc.project_ref, '')) LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(rc.cert_ref, ''))    LIKE '%' || k.word || '%'
);

DELETE FROM review_opinions ro
WHERE ro.project_ref IN (SELECT ref FROM _target_projects)
   OR EXISTS (
    SELECT 1 FROM _kw k
    WHERE LOWER(COALESCE(ro.project_ref, '')) LIKE '%' || k.word || '%'
);

DELETE FROM balances b
WHERE b.project_ref IN (SELECT ref FROM _target_projects)
   OR b.contract_id IN (SELECT id FROM _target_contract_ids);

DELETE FROM gatherings g
WHERE g.project_ref IN (SELECT ref FROM _target_projects)
   OR g.contract_id IN (SELECT id FROM _target_contract_ids);

DELETE FROM invoices i
WHERE i.project_ref IN (SELECT ref FROM _target_projects)
   OR i.contract_id IN (SELECT id FROM _target_contract_ids);

DELETE FROM payments p
WHERE p.project_ref IN (SELECT ref FROM _target_projects)
   OR p.contract_id IN (SELECT id FROM _target_contract_ids);

DELETE FROM costtickets ct
WHERE ct.project_ref IN (SELECT ref FROM _target_projects)
   OR ct.contract_id IN (SELECT id FROM _target_contract_ids);

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

DELETE FROM contracts c
WHERE c.id IN (SELECT id FROM _target_contract_ids)
   OR c.project_ref IN (SELECT ref FROM _target_projects);

DELETE FROM project_nodes p
WHERE p.ref IN (SELECT ref FROM _target_projects)
   OR EXISTS (
    SELECT 1 FROM _kw k
    WHERE LOWER(COALESCE(p.ref, ''))  LIKE '%' || k.word || '%'
       OR LOWER(COALESCE(p.name, '')) LIKE '%' || k.word || '%'
);

-- 3.3 Org/person/qualification
DELETE FROM qualification_assignments qa
WHERE qa.executor_ref IN (
    SELECT e.executor_ref
    FROM employees e
    JOIN _target_employee_ids te ON te.id = e.id
)
   OR qa.project_ref IN (SELECT ref FROM _target_projects);

DELETE FROM qualifications q
WHERE q.id IN (SELECT id FROM _target_qualification_ids);

DELETE FROM employees e
WHERE e.id IN (SELECT id FROM _target_employee_ids);

DELETE FROM companies c
WHERE c.id IN (SELECT id FROM _target_company_ids);

DELETE FROM namespaces n
WHERE n.ref IN (SELECT ref FROM _target_namespace_refs);

-- ---------------------------------------------------------------------------
-- 4) Post-check summary (inside same transaction)
-- ---------------------------------------------------------------------------
SELECT 'POST CHECK COUNTS' AS section;
SELECT 'project_nodes' AS table_name, COUNT(*)::BIGINT AS cnt FROM project_nodes
UNION ALL
SELECT 'contracts', COUNT(*)::BIGINT FROM contracts
UNION ALL
SELECT 'achievement_utxos', COUNT(*)::BIGINT FROM achievement_utxos
UNION ALL
SELECT 'engineer_achievement_receipts', COUNT(*)::BIGINT FROM engineer_achievement_receipts
UNION ALL
SELECT 'companies', COUNT(*)::BIGINT FROM companies
UNION ALL
SELECT 'employees', COUNT(*)::BIGINT FROM employees
UNION ALL
SELECT 'qualifications', COUNT(*)::BIGINT FROM qualifications
UNION ALL
SELECT 'genesis_utxos', COUNT(*)::BIGINT FROM genesis_utxos
UNION ALL
SELECT 'proof_anchors', COUNT(*)::BIGINT FROM proof_anchors
ORDER BY table_name;

-- ===========================================================================
-- Default is dry run. Set -v apply=true to COMMIT.
-- ===========================================================================
\if :apply
COMMIT;
\else
ROLLBACK;
\endif
