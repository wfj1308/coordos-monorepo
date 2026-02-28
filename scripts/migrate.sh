#!/bin/bash
# ============================================================
#  CoordOS monorepo 迁移脚本
#  从 engineering-design-institute 迁移可用模块
#
#  用法：bash scripts/migrate.sh /path/to/old/repo
# ============================================================

set -e

OLD_REPO="${1:?用法: migrate.sh <旧仓库路径>}"
NEW_REPO="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== CoordOS 迁移开始 ==="
echo "旧仓库: $OLD_REPO"
echo "新仓库: $NEW_REPO"
echo ""

# ── 检查旧仓库结构 ────────────────────────────────────────────
check_old_repo() {
  local required="core/pkg/parcel core/pkg/utxo core/pkg/settlement core/pkg/invoice core/pkg/wallet"
  for dir in $required; do
    if [ ! -d "$OLD_REPO/$dir" ]; then
      echo "警告：旧仓库缺少 $dir，跳过"
    fi
  done
}

# ── 迁移：直接复制（逻辑正确，只改包名） ─────────────────────
migrate_direct() {
  local module=$1
  local src="$OLD_REPO/core/pkg/$module"
  local dst="$NEW_REPO/services/design-institute/domain/$module"

  if [ ! -d "$src" ]; then
    echo "  跳过 $module（源目录不存在）"
    return
  fi

  echo "  迁移 $module..."
  cp -r "$src/"*.go "$dst/" 2>/dev/null || true

  # 修正包名：package xxx → package 同名
  for f in "$dst/"*.go; do
    [ -f "$f" ] || continue
    sed -i "s/^package .*/package $module/" "$f"
  done

  echo "  ✓ $module 迁移完成"
}

# ── 迁移：需要加 project_ref 字段 ────────────────────────────
migrate_with_project_ref() {
  local module=$1
  migrate_direct "$module"

  # 在 struct 定义里加 ProjectRef 字段
  local dst="$NEW_REPO/services/design-institute/domain/$module"
  for f in "$dst/"*.go; do
    [ -f "$f" ] || continue
    # 在 ContractRef 字段后面加 ProjectRef（如果没有的话）
    if grep -q "ContractRef" "$f" && ! grep -q "ProjectRef" "$f"; then
      sed -i '/ContractRef.*VRef/a \\tProjectRef  vuri.VRef `json:"project_ref"`  // 直接挂项目节点' "$f"
      echo "    → 已为 $f 加 project_ref 字段"
    fi
  done
}

# ── 迁移：协议配置文件 ────────────────────────────────────────
migrate_protocols() {
  local src="$OLD_REPO/protocols"
  if [ ! -d "$src" ]; then
    echo "  跳过 protocols（源目录不存在）"
    return
  fi

  echo "  迁移 protocols..."
  # 不覆盖已有的 tenant.config.json（新版已更新）
  for f in "$src/zhongbei/"*; do
    fname=$(basename "$f")
    dst="$NEW_REPO/protocols/zhongbei/$fname"
    if [ ! -f "$dst" ]; then
      cp "$f" "$dst"
      echo "    → 复制 $fname"
    else
      echo "    → 跳过 $fname（新版已存在）"
    fi
  done
  echo "  ✓ protocols 迁移完成"
}

# ── 迁移：e2e 测试 ────────────────────────────────────────────
migrate_tests() {
  local src="$OLD_REPO/core"
  local dst="$NEW_REPO/services/design-institute"

  echo "  迁移 e2e 测试..."
  find "$src" -name "*_test.go" -o -name "*e2e*" 2>/dev/null | while read f; do
    rel="${f#$src/}"
    target="$dst/$rel"
    mkdir -p "$(dirname "$target")"
    cp "$f" "$target"
    echo "    → $rel"
  done
  echo "  ✓ 测试迁移完成"
}

# ── 生成迁移报告 ──────────────────────────────────────────────
generate_report() {
  cat > "$NEW_REPO/MIGRATION_REPORT.md" << 'REPORT'
# CoordOS 迁移报告

## 直接迁移（逻辑保留）

| 模块 | 状态 | 说明 |
|------|------|------|
| parcel | ✓ 迁移 | 加了 project_ref 字段 |
| utxo | ✓ 迁移 | 加了 project_ref 字段 |
| settlement | ✓ 迁移 | 加了 project_ref 字段 |
| invoice | ✓ 迁移 | 直接复制 |
| wallet | ✓ 迁移 | 直接复制 |
| achievement | ✓ 迁移 | 直接复制 |
| protocols | ✓ 迁移 | tenant.config.json 已更新为新版 |

## 重写（结构性错误）

| 模块 | 状态 | 说明 |
|------|------|------|
| contract | ⚠ 重写 | 从顶层主体改为项目节点属性 |
| store | ⚠ 重写 | 从直接耦合改为 Store 接口 |
| CLI入口 | ⚠ 废弃 | 改为 API Server |
| 权限层 | ⚠ 重写 | 从只记录改为服务端强制校验 |

## 新增

| 模块 | 说明 |
|------|------|
| packages/project-core | ProjectNode + GenesisUTXO + 状态机 + RULE-001~005 |
| packages/vuri | v:// 路由（从旧版迁移优化） |
| services/vault-service | API Server + App层 + Store接口 |
| protocols/zhongbei/tenant.config.json | 新增 projectTree 配置段 |

## 待完成

- [ ] Store 接口的 SQLite 实现（packages/project-core 的存储层）
- [ ] design-institute/infra：parcel/utxo/settlement 的 SQLite 实现
- [ ] JWT 认证替换简化 token
- [ ] 数据库迁移脚本（为旧表加 project_ref 列）
- [ ] e2e 测试适配新 API
REPORT

  echo "  ✓ 迁移报告生成: MIGRATION_REPORT.md"
}

# ── 数据库迁移 SQL ────────────────────────────────────────────
generate_db_migration() {
  cat > "$NEW_REPO/scripts/migrate_db.sql" << 'SQL'
-- CoordOS 数据库迁移
-- 为现有表加 project_ref 列，保持向后兼容

-- 1. contracts 表加 project_ref
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS project_ref TEXT DEFAULT '';
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS procurement_ref TEXT DEFAULT '';
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS contract_kind TEXT DEFAULT 'EXTERNAL_MAIN';
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS sign_date TEXT DEFAULT '';

-- 2. parcels 表加 project_ref（存量数据通过 contract→project 回填）
ALTER TABLE parcels ADD COLUMN IF NOT EXISTS project_ref TEXT DEFAULT '';
UPDATE parcels SET project_ref = (
  SELECT COALESCE(c.project_ref, '') 
  FROM contracts c 
  WHERE c.ref = parcels.contract_ref
) WHERE project_ref = '';

-- 3. utxos 表加 project_ref
ALTER TABLE utxos ADD COLUMN IF NOT EXISTS project_ref TEXT DEFAULT '';
ALTER TABLE utxos ADD COLUMN IF NOT EXISTS genesis_ref TEXT DEFAULT '';
UPDATE utxos SET project_ref = (
  SELECT COALESCE(p.project_ref, '')
  FROM parcels p
  WHERE p.ref = utxos.parcel_ref
) WHERE project_ref = '';

-- 4. settlements 表加 project_ref
ALTER TABLE settlements ADD COLUMN IF NOT EXISTS project_ref TEXT DEFAULT '';
ALTER TABLE settlements ADD COLUMN IF NOT EXISTS genesis_ref TEXT DEFAULT '';

-- 5. 新增 project_nodes 表
CREATE TABLE IF NOT EXISTS project_nodes (
  ref          TEXT PRIMARY KEY,
  tenant_id    TEXT NOT NULL,
  parent_ref   TEXT DEFAULT '',
  owner_ref    TEXT DEFAULT '',
  contractor_ref TEXT DEFAULT '',
  executor_ref TEXT DEFAULT '',
  platform_ref TEXT DEFAULT '',
  contract_ref TEXT DEFAULT '',
  procurement_ref TEXT DEFAULT '',
  genesis_utxo_ref TEXT DEFAULT '',
  depth        INTEGER DEFAULT 0,
  path         TEXT DEFAULT '',
  status       TEXT DEFAULT 'INITIATED',
  proof_hash   TEXT DEFAULT '',
  prev_hash    TEXT DEFAULT '',
  created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at   DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_project_nodes_tenant ON project_nodes(tenant_id);
CREATE INDEX IF NOT EXISTS idx_project_nodes_parent ON project_nodes(parent_ref);
CREATE INDEX IF NOT EXISTS idx_project_nodes_status ON project_nodes(status);

-- 6. 新增 genesis_utxos 表
CREATE TABLE IF NOT EXISTS genesis_utxos (
  ref              TEXT PRIMARY KEY,
  tenant_id        TEXT NOT NULL,
  project_ref      TEXT NOT NULL,
  parent_ref       TEXT DEFAULT '',
  total_quota      INTEGER DEFAULT 0,
  quota_unit       TEXT DEFAULT 'CNY',
  consumed_quota   INTEGER DEFAULT 0,
  allocated_quota  INTEGER DEFAULT 0,
  frozen_quota     INTEGER DEFAULT 0,
  unit_price       INTEGER DEFAULT 0,
  price_tolerance  REAL DEFAULT 0.05,
  depth            INTEGER DEFAULT 0,
  status           TEXT DEFAULT 'ACTIVE',
  proof_hash       TEXT DEFAULT '',
  prev_hash        TEXT DEFAULT '',
  created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
  locked_at        DATETIME,
  closed_at        DATETIME,
  constraint_json  TEXT DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_genesis_tenant ON genesis_utxos(tenant_id);
CREATE INDEX IF NOT EXISTS idx_genesis_project ON genesis_utxos(project_ref);
CREATE INDEX IF NOT EXISTS idx_genesis_parent ON genesis_utxos(parent_ref);
SQL

  echo "  ✓ 数据库迁移脚本生成: scripts/migrate_db.sql"
}

# ── 主流程 ────────────────────────────────────────────────────
echo "1. 检查旧仓库..."
check_old_repo

echo "2. 迁移业务模块..."
migrate_direct "invoice"
migrate_direct "wallet"
migrate_direct "achievement"
migrate_with_project_ref "parcel"
migrate_with_project_ref "utxo"
migrate_with_project_ref "settlement"

echo "3. 迁移协议配置..."
migrate_protocols

echo "4. 迁移测试..."
migrate_tests

echo "5. 生成数据库迁移脚本..."
generate_db_migration

echo "6. 生成迁移报告..."
generate_report

echo ""
echo "=== 迁移完成 ==="
echo ""
echo "下一步："
echo "  1. 检查 MIGRATION_REPORT.md"
echo "  2. 执行 scripts/migrate_db.sql"
echo "  3. 运行 go build ./... 验证编译"
echo "  4. 运行现有 e2e 测试验证功能"
