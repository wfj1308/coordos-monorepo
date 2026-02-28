// dual_write.go
// 双写中间件：新业务同时写 MySQL（旧） 和 PostgreSQL（新）
// 挂在应用层，保证新旧数据库数据一致
// 切换完成后移除 MySQL 写入，只保留 PG

package dualwrite

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

// ── 双写开关（可通过环境变量或配置热切换） ──────────────────
type DualWriteConfig struct {
	WriteMySQL    bool // 默认 true，切换完成后改为 false
	WritePostgres bool // 默认 true
	ReadFrom      string // "mysql" | "postgres"（新业务默认 postgres）
}

var DefaultConfig = DualWriteConfig{
	WriteMySQL:    true,
	WritePostgres: true,
	ReadFrom:      "postgres",
}

// ── 合同双写 ──────────────────────────────────────────────────

type ContractDualWriter struct {
	mysql  *sql.DB
	pg     *sql.DB
	config DualWriteConfig
	log    *slog.Logger
}

func NewContractDualWriter(mysql, pg *sql.DB, cfg DualWriteConfig) *ContractDualWriter {
	return &ContractDualWriter{mysql: mysql, pg: pg, config: cfg,
		log: slog.Default().With("component", "dual_write_contract")}
}

// CreateContract 双写创建合同
func (w *ContractDualWriter) CreateContract(ctx context.Context, c *ContractInput) (*ContractResult, error) {
	var mysqlID, pgID int64
	var err error

	// ── 写 MySQL（旧系统） ──────────────────────────────────
	if w.config.WriteMySQL {
		mysqlID, err = w.insertContractMySQL(ctx, c)
		if err != nil {
			w.log.Error("MySQL写入失败", "err", err)
			return nil, err // MySQL 失败直接报错（旧系统不能丢数据）
		}
	}

	// ── 写 PostgreSQL（新系统） ─────────────────────────────
	if w.config.WritePostgres {
		pgID, err = w.insertContractPG(ctx, c, mysqlID)
		if err != nil {
			// PG 失败只记录，不影响旧系统
			// 异步补偿：写入 dual_write_error 表，后台任务重试
			w.log.Error("PG写入失败，记录补偿队列", "mysql_id", mysqlID, "err", err)
			w.recordCompensation(ctx, "contract", mysqlID, err)
			// 返回 MySQL 的结果，业务不感知 PG 失败
		}
	}

	return &ContractResult{MySQLID: mysqlID, PGID: pgID}, nil
}

func (w *ContractDualWriter) insertContractMySQL(ctx context.Context, c *ContractInput) (int64, error) {
	result, err := w.mysql.ExecContext(ctx, `
		INSERT INTO contract (
			num, contractName, contractBalance, manageRatio,
			signing_subject, signing_time, company_id, customer_id,
			employee_id, parent, catalog, payType, type, state,
			note, draft, tenant_id, addDate, lastDate
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW(),NOW())
	`,
		c.Num, c.ContractName, c.ContractBalance, c.ManageRatio,
		c.SigningSubject, c.SigningTime, c.CompanyLegacyID, c.CustomerID,
		c.EmployeeLegacyID, c.ParentLegacyID, c.Catalog, c.PayType,
		c.ContractType, c.State, c.Note, 0, c.TenantID,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (w *ContractDualWriter) insertContractPG(ctx context.Context, c *ContractInput, legacyID int64) (int64, error) {
	// 查 PG 中的 company id
	var pgCompanyID *int
	if c.CompanyLegacyID != 0 {
		err := w.pg.QueryRowContext(ctx,
			"SELECT id FROM companies WHERE legacy_id = $1", c.CompanyLegacyID,
		).Scan(&pgCompanyID)
		if err != nil {
			w.log.Warn("找不到对应的PG company", "legacy_id", c.CompanyLegacyID)
		}
	}

	// 查父合同
	var pgParentID *int64
	if c.ParentLegacyID != 0 {
		err := w.pg.QueryRowContext(ctx,
			"SELECT id FROM contracts WHERE legacy_id = $1", c.ParentLegacyID,
		).Scan(&pgParentID)
		if err != nil {
			w.log.Warn("找不到对应的PG parent contract", "legacy_id", c.ParentLegacyID)
		}
	}

	var id int64
	err := w.pg.QueryRowContext(ctx, `
		INSERT INTO contracts (
			legacy_id, num, contract_name, contract_balance,
			manage_ratio, signing_subject, signing_time,
			company_id, customer_id, employee_id, parent_id,
			catalog, pay_type, contract_type, state,
			note, draft, tenant_id, migrate_status
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,'NEW'
		) RETURNING id
	`,
		legacyID, c.Num, c.ContractName, c.ContractBalance,
		c.ManageRatio, c.SigningSubject, c.SigningTime,
		pgCompanyID, c.CustomerID, c.EmployeeLegacyID, pgParentID,
		c.Catalog, c.PayType, c.ContractType, c.State,
		c.Note, 0, c.TenantID,
	).Scan(&id)
	return id, err
}

// ── 收款双写 ──────────────────────────────────────────────────

type GatheringDualWriter struct {
	mysql  *sql.DB
	pg     *sql.DB
	config DualWriteConfig
	log    *slog.Logger
}

func (w *GatheringDualWriter) CreateGathering(ctx context.Context, g *GatheringInput) (*GatheringResult, error) {
	var mysqlID, pgID int64
	var err error

	if w.config.WriteMySQL {
		mysqlID, err = w.insertGatheringMySQL(ctx, g)
		if err != nil {
			return nil, err
		}
	}

	if w.config.WritePostgres {
		pgID, err = w.insertGatheringPG(ctx, g, mysqlID)
		if err != nil {
			w.log.Error("PG收款写入失败", "mysql_id", mysqlID, "err", err)
			w.recordCompensation(ctx, "gathering", mysqlID, err)
		}
	}

	return &GatheringResult{MySQLID: mysqlID, PGID: pgID}, nil
}

func (w *GatheringDualWriter) insertGatheringMySQL(ctx context.Context, g *GatheringInput) (int64, error) {
	result, err := w.mysql.ExecContext(ctx, `
		INSERT INTO gathering (
			gatheringNumber, gatheringMoney, gatheringdate,
			gatheringState, gatheringType, gatheringperson,
			contract_id, company_id, employee_id,
			state, note, draft, tenant_id, addDate
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())
	`,
		g.GatheringNumber, g.GatheringMoney, g.GatheringDate,
		g.GatheringState, g.GatheringType, g.GatheringPerson,
		g.ContractLegacyID, g.CompanyLegacyID, g.EmployeeLegacyID,
		g.State, g.Note, 0, g.TenantID,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (w *GatheringDualWriter) insertGatheringPG(ctx context.Context, g *GatheringInput, legacyID int64) (int64, error) {
	var pgContractID *int64
	w.pg.QueryRowContext(ctx,
		"SELECT id FROM contracts WHERE legacy_id=$1", g.ContractLegacyID,
	).Scan(&pgContractID)

	var id int64
	err := w.pg.QueryRowContext(ctx, `
		INSERT INTO gatherings (
			legacy_id, gathering_number, gathering_money,
			gathering_date, gathering_state, gathering_type,
			gathering_person, contract_id, company_id,
			employee_id, state, note, draft, tenant_id, migrate_status
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'NEW'
		) RETURNING id
	`,
		legacyID, g.GatheringNumber, g.GatheringMoney,
		g.GatheringDate, g.GatheringState, g.GatheringType,
		g.GatheringPerson, pgContractID, g.CompanyLegacyID,
		g.EmployeeLegacyID, g.State, g.Note, 0, g.TenantID,
	).Scan(&id)
	return id, err
}

// ── 补偿队列（PG写失败时记录，后台重试） ─────────────────────

type CompensationRecord struct {
	TableName string
	LegacyID  int64
	Error     string
	CreatedAt time.Time
	Retries   int
}

func (w *ContractDualWriter) recordCompensation(ctx context.Context, table string, legacyID int64, err error) {
	// 写入 dual_write_compensation 表，后台 goroutine 每5分钟重试一次
	_, _ = w.pg.ExecContext(ctx, `
		INSERT INTO dual_write_compensation (table_name, legacy_id, error_msg, created_at, retries)
		VALUES ($1, $2, $3, NOW(), 0)
		ON CONFLICT (table_name, legacy_id) DO UPDATE
		SET retries = dual_write_compensation.retries + 1,
		    error_msg = EXCLUDED.error_msg
	`, table, legacyID, err.Error())
}

func (w *GatheringDualWriter) recordCompensation(ctx context.Context, table string, legacyID int64, err error) {
	_, _ = w.pg.ExecContext(ctx, `
		INSERT INTO dual_write_compensation (table_name, legacy_id, error_msg, created_at, retries)
		VALUES ($1, $2, $3, NOW(), 0)
		ON CONFLICT (table_name, legacy_id) DO UPDATE
		SET retries = dual_write_compensation.retries + 1
	`, table, legacyID, err.Error())
}

// ── 补偿表建表 SQL ─────────────────────────────────────────────
// CREATE TABLE dual_write_compensation (
//     id          BIGSERIAL PRIMARY KEY,
//     table_name  VARCHAR(100) NOT NULL,
//     legacy_id   BIGINT NOT NULL,
//     error_msg   TEXT,
//     created_at  TIMESTAMPTZ NOT NULL,
//     retries     INT NOT NULL DEFAULT 0,
//     resolved    BOOLEAN NOT NULL DEFAULT FALSE,
//     UNIQUE (table_name, legacy_id)
// );

// ── 数据类型 ──────────────────────────────────────────────────

type ContractInput struct {
	Num             string
	ContractName    string
	ContractBalance float64
	ManageRatio     float64
	SigningSubject   string
	SigningTime      *time.Time
	CompanyLegacyID int
	CustomerID      int64
	EmployeeLegacyID int64
	ParentLegacyID  int64
	Catalog         int
	PayType         int
	ContractType    string
	State           string
	Note            string
	TenantID        int
}

type ContractResult struct {
	MySQLID int64
	PGID    int64
}

type GatheringInput struct {
	GatheringNumber   string
	GatheringMoney    float64
	GatheringDate     string
	GatheringState    string
	GatheringType     string
	GatheringPerson   string
	ContractLegacyID  int64
	CompanyLegacyID   int
	EmployeeLegacyID  int64
	State             string
	Note              string
	TenantID          int
}

type GatheringResult struct {
	MySQLID int64
	PGID    int64
}
