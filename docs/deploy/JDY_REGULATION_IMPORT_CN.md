# 简道云法规库导入说明（公开看板）

## 1. 目标
把简道云公开看板中的法规明细，导出为 `migrate.py --phase regulation` 可直接导入的 CSV。

## 2. 导出命令
```powershell
python scripts/export_jdy_regulations.py `
  --dash-url "https://<tenant>.jiandaoyun.com/dash/<dash_id>" `
  --output "scripts/regulations_jdy_export.csv"
```

已验证示例：
```powershell
python scripts/export_jdy_regulations.py `
  --dash-url "https://tnwwzyqy43.jiandaoyun.com/dash/6329a1a6aaf3e400095a23ad" `
  --output "scripts/regulations_jdy_export_20260304.csv"
```

## 3. 导入命令
`migrate.py` 默认使用：
- `PG_HOST=localhost`
- `PG_PORT=5432`
- `PG_USER=coordos`
- `PG_DATABASE=coordos`
- `PG_PASSWORD=YOUR_PG_PASSWORD`

请先设置真实数据库密码，再执行：
```powershell
$env:PG_PASSWORD="<你的PostgreSQL密码>"
$env:REGULATION_SOURCE_CSV="scripts/regulations_jdy_export.csv"
python scripts/migrate.py --phase regulation
```

## 4. 校验
```powershell
psql "$env:DATABASE_URL" -f scripts/verify_three_libraries_cn.sql
```

重点看：
- `regulation_documents_rows_est`
- `regulation_versions_rows_est`

## 5. 字段映射（导出脚本内置）
- 法规名称 -> `title`
- 发布文号（为空时回退编号）-> `doc_no`
- 法规分类 -> `category`
- 发布机关 -> `publisher`
- 实施状态 -> `status`（归一化为 `EFFECTIVE/REPEALED/EXPIRED`）
- 标准文件附件 -> `attachment_url`
- 创建/更新时间 -> `effective_from/published_at`

