# v://zhongbei 部署手册（可执行版）

本文档给出最小可执行路径：初始化数据、启动服务、验证对外接口。

## 前置条件
- PostgreSQL 可连接
- 当前目录为仓库根目录 `coordos/`

建议环境变量：

```bash
export DATABASE_URL="postgres://coordos:coordos@localhost:5432/coordos?sslmode=disable"
export DESIGN_INSTITUTE_HTTP_ADDR=":8081"
```

## 1. 初始化数据库结构
```bash
psql "$DATABASE_URL" -f scripts/migrate_pg_schema.sql
psql "$DATABASE_URL" -f scripts/add_namespaces_table.sql
psql "$DATABASE_URL" -f scripts/add_publishing_tables.sql
psql "$DATABASE_URL" -f scripts/add_capability_engine.sql
```

## 2. 导入中北基础数据
```bash
psql "$DATABASE_URL" -f scripts/seed_zhongbei_genesis.sql
psql "$DATABASE_URL" -f scripts/seed_zhongbei_engineers.sql
```

## 3. 启动 design-institute 服务
```bash
go run ./services/design-institute
```

期望日志包含：
- `design-institute listening on :8081 tenant=10000`

## 4. 验证能力声明接口
```bash
curl -s http://localhost:8081/public/v1/partner-profile/zhongbei
```

重点检查：
- `qualification_layer.count`
- `capability_layer.registered_engineer_count`
- `capability_layer.qualification_type_counts`

## 5. 验证 Resolver
```bash
curl -s -X POST http://localhost:8081/api/v1/resolve/resolve \
  -H "Content-Type: application/json" \
  -d '{
    "spu_ref":"v://zhongbei/spu/bridge/pier_rebar_drawing@v1",
    "required_quals":["REG_STRUCTURE"],
    "tenant_id":10000
  }'
```

## 常见问题
1. `404`：服务未启动或端口不一致。
2. `400 unknown field`：运行的是旧版本进程。
3. `total=0`：人员资质未导入或状态无效。
4. 页面中文显示异常：终端/编辑器编码不是 UTF-8。
