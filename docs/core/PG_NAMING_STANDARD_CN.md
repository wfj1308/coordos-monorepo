# PostgreSQL 命名标准（cn.zhongbei）

## 1. 目标
- 新库（PostgreSQL）只使用 `snake_case` 命名。
- 业务语义表使用“复数名词”。
- 历史兼容表统一加 `legacy` 语义后缀或放入通用归档表。

## 2. 命名规则
- 表名：`snake_case`，复数优先，例如 `contracts`、`gatherings`。
- 列名：`snake_case`，时间字段统一 `created_at` / `updated_at`。
- 主键：统一 `id`。
- 旧系统主键：统一 `legacy_id`。
- 多租户：统一 `tenant_id`。

## 3. 历史表处理策略
- 仍有业务价值但不参与主流程聚合的旧表：
  - 采用标准命名迁移到 PG（如 `invoice_scraps`、`project_reports`、`bank_infos`）。
- 系统/日志/流程引擎类旧表：
  - 统一归档到 `legacy_source_rows`（保留 `source_table`、`row_hash`、`raw`）。

## 4. 旧名到新名示例
- `blance_fast_settlement_quota` -> `balance_fast_settlement_quotas`
- `balance_fast_settlement_quota_file` -> `balance_fast_settlement_quota_files`
- `project_fileupload` -> `project_file_uploads`
- `projectpartner` -> `project_partners`
- `audit_receipt_invoiced` -> `audit_receipt_invoiced_links`

## 5. 验收标准
- 最新成功批次 `icrm_raw.landing_table_stats` 中 `source_row_count > 0` 的所有表：
  - 均应在 `migration_log` 存在 `status='SUCCESS'` 记录。
- 结果目标：
  - `remain_tables = 0`
  - `remain_rows = 0`

