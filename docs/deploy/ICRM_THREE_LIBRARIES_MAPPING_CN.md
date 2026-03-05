# iCRM 三类库迁移映射（法规库 / 资质库 / 工程标准库）

本文是针对 `icrm_20260122020001.7z` 与当前 CoordOS 迁移链路的最小落地说明。

## 1. 结论

- 法规库：旧库未发现独立法规主数据表，当前迁移脚本也未定义法规库迁移 phase。
- 资质库：已支持迁移。来源 `worker + profession`，目标 `qualifications`。
- 工程标准库：旧库更接近“工程资料/图纸库”（非标准条文库），已支持迁移到 `drawings + drawing_attachments`。

## 2. 旧表 -> 新表映射

### 2.1 资质库

- 旧来源主表：`worker`、`profession`
- 迁移逻辑：`scripts/migrate.py` 的 `migrate_qualifications()`
- 目标表：`qualifications`（`scripts/migrate_pg_schema.sql`）

关键字段映射（核心）：

- `worker.regprofession_id + profession.code/name` -> `qualifications.qual_type/specialty/scope/level`
- `worker.register` -> `qualifications.cert_no`
- `worker.certificateFile` -> `qualifications.attachment_url`
- `worker.user_id` -> 先关联 `employees.user_id`，再落 `qualifications.holder_id/executor_ref`

### 2.2 工程标准库（工程资料/图纸库）

- 旧来源主表：`drawing`
- 旧来源附件：`drawing_files`、`drawing_result_file`
- 迁移逻辑：
  - `migrate_drawings()`：`drawing -> drawings`
  - `migrate_artifacts()`：`drawing_files/drawing_result_file -> drawing_attachments`
- 目标表：`drawings`、`drawing_attachments`

### 2.3 法规库

- 在 `icrm_schema_only.sql` 与当前迁移脚本中均未发现法规主表/法规迁移 phase。
- 当前仓库仅有资质与图纸（工程资料）完整链路。
- 若需要法规库，建议新增：
  - 目标表 `regulation_documents`（法规元数据）
  - 目标表 `regulation_versions`（版本与生效时间）
  - 新 phase `migrate_regulations`（可从外部法规源导入）

## 3. 直接执行顺序（已有脚本）

建议最小执行：

1. 初始化目标库结构
2. 迁移公司/员工（资质迁移依赖）
3. 迁移资质
4. 迁移图纸
5. 迁移图纸附件
6. 校验

示例命令（PowerShell）：

```powershell
psql "$env:DATABASE_URL" -f scripts/migrate_pg_schema.sql

python scripts/migrate.py --phase company
python scripts/migrate.py --phase employee
python scripts/migrate.py --phase qualification
python scripts/migrate.py --phase achievement
python scripts/migrate.py --phase drawing
python scripts/migrate.py --phase artifacts
python scripts/migrate.py --phase verify

psql "$env:DATABASE_URL" -f scripts/verify_three_libraries_cn.sql
```

## 4. 代码定位

- 资质迁移函数：`scripts/migrate.py`（`migrate_qualifications`）
- 图纸迁移函数：`scripts/migrate.py`（`migrate_drawings`）
- 图纸附件迁移函数：`scripts/migrate.py`（`migrate_artifacts` 内对 `drawing_files` / `drawing_result_file` 的处理）
- 目标表定义：`scripts/migrate_pg_schema.sql`（`qualifications`、`drawings`、`drawing_attachments`）

## 5. 法规库新增执行方式

仓库已新增法规库目标表与迁移 phase：

- 表：`regulation_documents`、`regulation_versions`
- phase：`python scripts/migrate.py --phase regulation`

执行前设置法规 CSV 路径：

```powershell
$env:REGULATION_SOURCE_CSV="scripts/regulations_manual_template.csv"
python scripts/migrate.py --phase regulation
```

CSV 模板文件：

- `scripts/regulations_manual_template.csv`
