package contract

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ══════════════════════════════════════════════════════════════
//  类型定义
// ══════════════════════════════════════════════════════════════

type State string

const (
	StateDraft     State = "DRAFT"
	StatePending   State = "PENDING"
	StateApproved  State = "APPROVED"
	StateRejected  State = "REJECTED"
	StateExecuting State = "EXECUTING"
	StateCompleted State = "COMPLETED"
	StateVoided    State = "VOIDED"
)

type PayType int

const (
	PayTypeFixed     PayType = 1 // 总价
	PayTypeRate      PayType = 2 // 费率
	PayTypeUnit      PayType = 3 // 单价
	PayTypeFramework PayType = 4 // 框架
)

type Contract struct {
	ID              int64
	LegacyID        *int64
	Num             string
	ContractName    string
	ContractBalance float64
	ManageRatio     float64
	SigningSubject   string
	SigningTime      *time.Time
	ContractDate    *time.Time
	PayType         PayType
	ContractType    string   // 中标/挂靠
	State           State
	StoreState      int
	CompanyID       *int
	CustomerID      *int64
	EmployeeID      *int64
	ParentID        *int64   // 父合同（委托链）
	OwnerID         *int64
	Catalog         int
	TotleBalance    float64  // 累计结算
	TotleGathering  float64  // 累计收款
	TotleInvoice    float64  // 累计开票
	ProjectRef      *string  // 对应 ProjectNode
	GenesisRef      *string  // 对应 GenesisUTXO
	Note            string
	Deleted         bool
	Draft           int
	TenantID        int
	CreatedAt       time.Time
	UpdatedAt       time.Time
	MigrateStatus   string
}

type CreateContractInput struct {
	Num             string
	ContractName    string
	ContractBalance float64
	ManageRatio     float64
	SigningSubject   string
	SigningTime      *time.Time
	PayType         PayType
	ContractType    string
	CompanyID       *int
	CustomerID      *int64
	EmployeeID      *int64
	ParentID        *int64
	OwnerID         *int64
	Note            string
	TenantID        int
}

type ContractFilter struct {
	CompanyID  *int
	EmployeeID *int64
	ParentID   *int64
	State      *State
	TenantID   int
	Keyword    string
	Limit      int
	Offset     int
}

// 合同财务快照（给结算/发票/收款用）
type FinanceSummary struct {
	ContractID      int64
	ContractBalance float64
	TotleBalance    float64
	TotleGathering  float64
	TotleInvoice    float64
	Remaining       float64 // 剩余可结算
}

// ══════════════════════════════════════════════════════════════
//  Store 接口
// ══════════════════════════════════════════════════════════════

type Store interface {
	Create(ctx context.Context, c *Contract) error
	Get(ctx context.Context, id int64) (*Contract, error)
	GetByLegacyID(ctx context.Context, legacyID int64) (*Contract, error)
	Update(ctx context.Context, c *Contract) error
	SoftDelete(ctx context.Context, id int64) error
	List(ctx context.Context, f ContractFilter) ([]*Contract, int, error)
	GetChildren(ctx context.Context, parentID int64) ([]*Contract, error)
	GetAncestors(ctx context.Context, id int64) ([]*Contract, error)
	UpdateTotals(ctx context.Context, id int64, balance, gathering, invoice float64) error
	SetProjectRef(ctx context.Context, id int64, projectRef, genesisRef *string) error
	FinanceSummary(ctx context.Context, id int64) (*FinanceSummary, error)
}

// ══════════════════════════════════════════════════════════════
//  Service
// ══════════════════════════════════════════════════════════════

type Service struct {
	store    Store
	tenantID int
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

func (s *Service) Create(ctx context.Context, in CreateContractInput) (*Contract, error) {
	if in.ContractName == "" {
		return nil, fmt.Errorf("合同名称不能为空")
	}
	if in.ContractBalance <= 0 {
		return nil, fmt.Errorf("合同金额必须大于0")
	}

	// RULE-001：子合同金额不超过父合同
	if in.ParentID != nil {
		parent, err := s.store.Get(ctx, *in.ParentID)
		if err != nil {
			return nil, fmt.Errorf("父合同不存在: %w", err)
		}
		summary, _ := s.store.FinanceSummary(ctx, parent.ID)
		childrenSum := summary.TotleBalance
		if childrenSum+in.ContractBalance > parent.ContractBalance {
			return nil, fmt.Errorf(
				"RULE-001: 子合同总额(%.2f+%.2f)超过父合同金额(%.2f)",
				childrenSum, in.ContractBalance, parent.ContractBalance,
			)
		}
	}

	c := &Contract{
		Num:             in.Num,
		ContractName:    in.ContractName,
		ContractBalance: in.ContractBalance,
		ManageRatio:     in.ManageRatio,
		SigningSubject:   in.SigningSubject,
		SigningTime:      in.SigningTime,
		PayType:         in.PayType,
		ContractType:    in.ContractType,
		CompanyID:       in.CompanyID,
		CustomerID:      in.CustomerID,
		EmployeeID:      in.EmployeeID,
		ParentID:        in.ParentID,
		OwnerID:         in.OwnerID,
		State:           StateDraft,
		StoreState:      2,
		Catalog:         1,
		Note:            in.Note,
		TenantID:        s.tenantID,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
		MigrateStatus:   "NEW",
	}

	if err := s.store.Create(ctx, c); err != nil {
		return nil, fmt.Errorf("创建合同失败: %w", err)
	}
	return c, nil
}

func (s *Service) Get(ctx context.Context, id int64) (*Contract, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) List(ctx context.Context, f ContractFilter) ([]*Contract, int, error) {
	f.TenantID = s.tenantID
	if f.Limit == 0 {
		f.Limit = 20
	}
	return s.store.List(ctx, f)
}

// 委托链：获取完整祖先链
func (s *Service) GetAncestors(ctx context.Context, id int64) ([]*Contract, error) {
	return s.store.GetAncestors(ctx, id)
}

// 委托链：获取子合同
func (s *Service) GetChildren(ctx context.Context, id int64) ([]*Contract, error) {
	return s.store.GetChildren(ctx, id)
}

// 财务快照
func (s *Service) FinanceSummary(ctx context.Context, id int64) (*FinanceSummary, error) {
	return s.store.FinanceSummary(ctx, id)
}

// 付款前校验 RULE-003：对外付款必须有合同
func (s *Service) ValidatePayment(ctx context.Context, id int64, amount float64) error {
	c, err := s.store.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("合同不存在: %w", err)
	}
	if c.State == StateVoided {
		return fmt.Errorf("RULE-003: 合同已作废，不能付款")
	}
	summary, err := s.store.FinanceSummary(ctx, id)
	if err != nil {
		return err
	}
	if summary.TotleBalance+amount > c.ContractBalance {
		return fmt.Errorf(
			"RULE-003: 付款金额(%.2f)超过合同剩余可付额度(%.2f)",
			amount, c.ContractBalance-summary.TotleBalance,
		)
	}
	return nil
}

func (s *Service) Approve(ctx context.Context, id int64) error {
	c, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if c.State != StatePending {
		return fmt.Errorf("只有待审核状态可以审批")
	}
	c.State = StateApproved
	c.UpdatedAt = time.Now()
	return s.store.Update(ctx, c)
}

func (s *Service) Void(ctx context.Context, id int64, reason string) error {
	c, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	summary, _ := s.store.FinanceSummary(ctx, id)
	if summary.TotleBalance > 0 {
		return fmt.Errorf("合同已有结算记录，不能作废")
	}
	c.State = StateVoided
	c.Note = fmt.Sprintf("[作废原因] %s | %s", reason, c.Note)
	c.UpdatedAt = time.Now()
	return s.store.Update(ctx, c)
}

// ══════════════════════════════════════════════════════════════
//  PostgreSQL Store 实现
// ══════════════════════════════════════════════════════════════

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) Create(ctx context.Context, c *Contract) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO contracts (
			num, contract_name, contract_balance, manage_ratio,
			signing_subject, signing_time, contract_date,
			pay_type, contract_type, state, store_state,
			company_id, customer_id, employee_id, parent_id, owner_id,
			catalog, note, deleted, draft, tenant_id,
			created_at, updated_at, migrate_status
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
			$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24
		) RETURNING id`,
		c.Num, c.ContractName, c.ContractBalance, c.ManageRatio,
		c.SigningSubject, c.SigningTime, c.ContractDate,
		c.PayType, c.ContractType, c.State, c.StoreState,
		c.CompanyID, c.CustomerID, c.EmployeeID, c.ParentID, c.OwnerID,
		c.Catalog, c.Note, c.Deleted, c.Draft, c.TenantID,
		c.CreatedAt, c.UpdatedAt, c.MigrateStatus,
	).Scan(&c.ID)
}

func (s *PGStore) Get(ctx context.Context, id int64) (*Contract, error) {
	c := &Contract{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id,legacy_id,num,contract_name,contract_balance,manage_ratio,
		       signing_subject,signing_time,contract_date,pay_type,contract_type,
		       state,store_state,company_id,customer_id,employee_id,
		       parent_id,owner_id,catalog,
		       totle_balance,totle_gathering,totle_invoice,
		       project_ref,genesis_ref,note,deleted,draft,tenant_id,
		       created_at,updated_at,migrate_status
		FROM contracts WHERE id=$1 AND deleted=FALSE`, id,
	).Scan(
		&c.ID, &c.LegacyID, &c.Num, &c.ContractName, &c.ContractBalance,
		&c.ManageRatio, &c.SigningSubject, &c.SigningTime, &c.ContractDate,
		&c.PayType, &c.ContractType, &c.State, &c.StoreState,
		&c.CompanyID, &c.CustomerID, &c.EmployeeID,
		&c.ParentID, &c.OwnerID, &c.Catalog,
		&c.TotleBalance, &c.TotleGathering, &c.TotleInvoice,
		&c.ProjectRef, &c.GenesisRef, &c.Note, &c.Deleted, &c.Draft,
		&c.TenantID, &c.CreatedAt, &c.UpdatedAt, &c.MigrateStatus,
	)
	return c, err
}

func (s *PGStore) GetByLegacyID(ctx context.Context, legacyID int64) (*Contract, error) {
	c := &Contract{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id,legacy_id,num,contract_name,contract_balance,manage_ratio,
		       signing_subject,signing_time,contract_date,pay_type,contract_type,
		       state,store_state,company_id,customer_id,employee_id,
		       parent_id,owner_id,catalog,
		       totle_balance,totle_gathering,totle_invoice,
		       project_ref,genesis_ref,note,deleted,draft,tenant_id,
		       created_at,updated_at,migrate_status
		FROM contracts WHERE legacy_id=$1`, legacyID,
	).Scan(
		&c.ID, &c.LegacyID, &c.Num, &c.ContractName, &c.ContractBalance,
		&c.ManageRatio, &c.SigningSubject, &c.SigningTime, &c.ContractDate,
		&c.PayType, &c.ContractType, &c.State, &c.StoreState,
		&c.CompanyID, &c.CustomerID, &c.EmployeeID,
		&c.ParentID, &c.OwnerID, &c.Catalog,
		&c.TotleBalance, &c.TotleGathering, &c.TotleInvoice,
		&c.ProjectRef, &c.GenesisRef, &c.Note, &c.Deleted, &c.Draft,
		&c.TenantID, &c.CreatedAt, &c.UpdatedAt, &c.MigrateStatus,
	)
	return c, err
}

func (s *PGStore) Update(ctx context.Context, c *Contract) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE contracts SET
			state=$1, store_state=$2, note=$3,
			signing_time=$4, updated_at=$5
		WHERE id=$6`,
		c.State, c.StoreState, c.Note, c.SigningTime, c.UpdatedAt, c.ID,
	)
	return err
}

func (s *PGStore) SoftDelete(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE contracts SET deleted=TRUE, updated_at=NOW() WHERE id=$1`, id)
	return err
}

func (s *PGStore) List(ctx context.Context, f ContractFilter) ([]*Contract, int, error) {
	where := "tenant_id=$1 AND deleted=FALSE"
	args := []any{f.TenantID}
	i := 2
	if f.CompanyID != nil {
		where += fmt.Sprintf(" AND company_id=$%d", i); args = append(args, *f.CompanyID); i++
	}
	if f.EmployeeID != nil {
		where += fmt.Sprintf(" AND employee_id=$%d", i); args = append(args, *f.EmployeeID); i++
	}
	if f.ParentID != nil {
		where += fmt.Sprintf(" AND parent_id=$%d", i); args = append(args, *f.ParentID); i++
	}
	if f.State != nil {
		where += fmt.Sprintf(" AND state=$%d", i); args = append(args, *f.State); i++
	}
	if f.Keyword != "" {
		where += fmt.Sprintf(" AND (contract_name ILIKE $%d OR num ILIKE $%d)", i, i)
		args = append(args, "%"+f.Keyword+"%"); i++
	}

	var total int
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM contracts WHERE "+where, args...).Scan(&total)

	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT id,legacy_id,num,contract_name,contract_balance,manage_ratio,
			signing_subject,signing_time,contract_date,pay_type,contract_type,
			state,store_state,company_id,customer_id,employee_id,
			parent_id,owner_id,catalog,
			totle_balance,totle_gathering,totle_invoice,
			project_ref,genesis_ref,note,deleted,draft,tenant_id,
			created_at,updated_at,migrate_status
			FROM contracts WHERE %s ORDER BY created_at DESC LIMIT $%d OFFSET $%d`,
			where, i, i+1),
		append(args, f.Limit, f.Offset)...,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var result []*Contract
	for rows.Next() {
		c := &Contract{}
		rows.Scan(
			&c.ID, &c.LegacyID, &c.Num, &c.ContractName, &c.ContractBalance,
			&c.ManageRatio, &c.SigningSubject, &c.SigningTime, &c.ContractDate,
			&c.PayType, &c.ContractType, &c.State, &c.StoreState,
			&c.CompanyID, &c.CustomerID, &c.EmployeeID,
			&c.ParentID, &c.OwnerID, &c.Catalog,
			&c.TotleBalance, &c.TotleGathering, &c.TotleInvoice,
			&c.ProjectRef, &c.GenesisRef, &c.Note, &c.Deleted, &c.Draft,
			&c.TenantID, &c.CreatedAt, &c.UpdatedAt, &c.MigrateStatus,
		)
		result = append(result, c)
	}
	return result, total, rows.Err()
}

func (s *PGStore) GetChildren(ctx context.Context, parentID int64) ([]*Contract, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id,legacy_id,num,contract_name,contract_balance,manage_ratio,
		       signing_subject,signing_time,contract_date,pay_type,contract_type,
		       state,store_state,company_id,customer_id,employee_id,
		       parent_id,owner_id,catalog,
		       totle_balance,totle_gathering,totle_invoice,
		       project_ref,genesis_ref,note,deleted,draft,tenant_id,
		       created_at,updated_at,migrate_status
		FROM contracts WHERE parent_id=$1 AND deleted=FALSE ORDER BY created_at`, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*Contract
	for rows.Next() {
		c := &Contract{}
		rows.Scan(
			&c.ID, &c.LegacyID, &c.Num, &c.ContractName, &c.ContractBalance,
			&c.ManageRatio, &c.SigningSubject, &c.SigningTime, &c.ContractDate,
			&c.PayType, &c.ContractType, &c.State, &c.StoreState,
			&c.CompanyID, &c.CustomerID, &c.EmployeeID,
			&c.ParentID, &c.OwnerID, &c.Catalog,
			&c.TotleBalance, &c.TotleGathering, &c.TotleInvoice,
			&c.ProjectRef, &c.GenesisRef, &c.Note, &c.Deleted, &c.Draft,
			&c.TenantID, &c.CreatedAt, &c.UpdatedAt, &c.MigrateStatus,
		)
		result = append(result, c)
	}
	return result, rows.Err()
}

func (s *PGStore) GetAncestors(ctx context.Context, id int64) ([]*Contract, error) {
	rows, err := s.db.QueryContext(ctx, `
		WITH RECURSIVE ancestors AS (
			SELECT * FROM contracts WHERE id=$1
			UNION ALL
			SELECT c.* FROM contracts c
			JOIN ancestors a ON c.id = a.parent_id
		)
		SELECT id,legacy_id,num,contract_name,contract_balance,manage_ratio,
		       signing_subject,signing_time,contract_date,pay_type,contract_type,
		       state,store_state,company_id,customer_id,employee_id,
		       parent_id,owner_id,catalog,
		       totle_balance,totle_gathering,totle_invoice,
		       project_ref,genesis_ref,note,deleted,draft,tenant_id,
		       created_at,updated_at,migrate_status
		FROM ancestors WHERE id!=$1 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*Contract
	for rows.Next() {
		c := &Contract{}
		rows.Scan(
			&c.ID, &c.LegacyID, &c.Num, &c.ContractName, &c.ContractBalance,
			&c.ManageRatio, &c.SigningSubject, &c.SigningTime, &c.ContractDate,
			&c.PayType, &c.ContractType, &c.State, &c.StoreState,
			&c.CompanyID, &c.CustomerID, &c.EmployeeID,
			&c.ParentID, &c.OwnerID, &c.Catalog,
			&c.TotleBalance, &c.TotleGathering, &c.TotleInvoice,
			&c.ProjectRef, &c.GenesisRef, &c.Note, &c.Deleted, &c.Draft,
			&c.TenantID, &c.CreatedAt, &c.UpdatedAt, &c.MigrateStatus,
		)
		result = append(result, c)
	}
	return result, rows.Err()
}

func (s *PGStore) UpdateTotals(ctx context.Context, id int64, balance, gathering, invoice float64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE contracts SET
			totle_balance=$1, totle_gathering=$2, totle_invoice=$3, updated_at=NOW()
		WHERE id=$4`, balance, gathering, invoice, id)
	return err
}

func (s *PGStore) SetProjectRef(ctx context.Context, id int64, projectRef, genesisRef *string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE contracts SET project_ref=$1, genesis_ref=$2, updated_at=NOW() WHERE id=$3`,
		projectRef, genesisRef, id)
	return err
}

func (s *PGStore) FinanceSummary(ctx context.Context, id int64) (*FinanceSummary, error) {
	s1 := &FinanceSummary{ContractID: id}
	err := s.db.QueryRowContext(ctx, `
		SELECT contract_balance,
		       COALESCE(totle_balance,0),
		       COALESCE(totle_gathering,0),
		       COALESCE(totle_invoice,0)
		FROM contracts WHERE id=$1`, id,
	).Scan(&s1.ContractBalance, &s1.TotleBalance, &s1.TotleGathering, &s1.TotleInvoice)
	if err != nil {
		return nil, err
	}
	s1.Remaining = s1.ContractBalance - s1.TotleBalance
	return s1, nil
}
