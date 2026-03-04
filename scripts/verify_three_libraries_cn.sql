-- Verify three libraries after iCRM -> CoordOS migration
-- Tenant default: 10000

\echo '=== 1) 资质库（qualifications）总量 ==='
SELECT 'qualifications_total' AS metric, COUNT(*) AS cnt
FROM qualifications
WHERE tenant_id = 10000 AND deleted = FALSE;

\echo '=== 2) 资质类型分布 ==='
SELECT qual_type, COUNT(*) AS cnt
FROM qualifications
WHERE tenant_id = 10000 AND deleted = FALSE
GROUP BY qual_type
ORDER BY cnt DESC, qual_type;

\echo '=== 3) 资质附件覆盖率（attachment_url）==='
SELECT
  COUNT(*) FILTER (WHERE COALESCE(attachment_url, '') <> '') AS with_attachment,
  COUNT(*) FILTER (WHERE COALESCE(attachment_url, '') = '')  AS without_attachment
FROM qualifications
WHERE tenant_id = 10000 AND deleted = FALSE;

\echo '=== 4) 工程资料库（drawings）总量 ==='
SELECT 'drawings_total' AS metric, COUNT(*) AS cnt
FROM drawings
WHERE tenant_id = 10000 AND deleted = FALSE;

\echo '=== 5) 工程资料附件库（drawing_attachments）总量 ==='
SELECT 'drawing_attachments_total' AS metric, COUNT(*) AS cnt
FROM drawing_attachments
WHERE tenant_id = 10000;

\echo '=== 6) 图纸附件孤儿记录检查（应为0）==='
SELECT 'drawing_attachments_missing_drawing' AS metric, COUNT(*) AS cnt
FROM drawing_attachments da
LEFT JOIN drawings d ON d.id = da.drawing_id
WHERE da.tenant_id = 10000
  AND da.drawing_id IS NOT NULL
  AND d.id IS NULL;

\echo '=== 7) 法规库表存在性（当前通常为空/不存在）==='
SELECT
  to_regclass('public.regulation_documents') AS regulation_documents_table,
  to_regclass('public.regulation_versions')  AS regulation_versions_table;

\echo '=== 7b) 法规库近似行数（来自 pg_stat_user_tables）==='
SELECT
  COALESCE(MAX(CASE WHEN relname='regulation_documents' THEN n_live_tup::bigint END), 0) AS regulation_documents_rows_est,
  COALESCE(MAX(CASE WHEN relname='regulation_versions'  THEN n_live_tup::bigint END), 0) AS regulation_versions_rows_est
FROM pg_stat_user_tables;

\echo '=== 8) 旧系统字典表迁移状态（当前链路未迁移）==='
SELECT
  to_regclass('public.system_dictionary')      AS system_dictionary_table,
  to_regclass('public.system_dictionary_item') AS system_dictionary_item_table,
  to_regclass('public.sys_dict_type')          AS sys_dict_type_table,
  to_regclass('public.sys_dict_data')          AS sys_dict_data_table;
