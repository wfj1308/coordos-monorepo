package invoice

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
	StateDraft    State = "DRAFT"
	StatePending  State = "PENDING"   // 待开票
	StateApproved State = "APPROVED"  // 审核通过
	StateIssued   State = "ISSUED"    // 已开票
	StateRejected State = "REJECTED"
	StateScrap    State = "SCRAP"     // 作废
)

type InvoiceType string

const (
	TypeVAT     InvoiceType = "VAT"      // 增值税专票
	TypeGeneral InvoiceType = "GENERAL"  // 普通发票
	TypeElec    InvoiceType = "ELEC"     // 电子发票
)

type Invoice struct {
	ID              int64
	LegacyID        *int64
	InvoiceCode     string
	InvoiceNumber   string
	InvoiceType     InvoiceType
	InvoiceState    State
	InvoiceDate     string
	InvoiceContent  string
	InvoicePerson   string
	CurAmount       float64  // 本次开票金额
	Money           float64  // 合同金额
	UsedMoney       float64  // 已使用金额
	ContractID      *int64
	ProjectRef      *string  // CoordOS项目节点引用
	CustomerID      *int64
	EmployeeID      *int64
	State           State
	Draft           int
	Note            string
	TenantID        int
	CreatedAt       time.Time
	UpdatedAt       time.Time
	MigrateStatus   string
}

type CreateInvoiceInput struct {
	InvoiceType    InvoiceType
	InvoiceContent string
	CurAmount      float64
	ContractID     *int64
	ProjectRef     *string
	CustomerID     *int64
	EmployeeID     *int64
	Note           string
	TenantID       int
}

type InvoiceFilter struct {
	ContractID *int64
	ProjectRef *string
	State      *State
	TenantID   int
	Limit      int
	Offset     int
}

// ══════════════════════════════════════════════════════════════
//  Store 接口
// ══════════════════════════════════════════════════════════════

type Store interface {
	Create(ctx context.Context, inv *Invoice) error
	Get(ctx context.Context, id int64) (*Invoice, error)
	Update(ctx context.Context, inv *Invoice) error
	List(ctx context.Context, f InvoiceFilter) ([]*Invoice, int, error)
	SumByContract(ctx context.Context, contractID int64) (float64, error)
	SumByProject(ctx context.Context, projectRef string) (float64, error)
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

func (s *Service) Create(ctx context.Context, in CreateInvoiceInput) (*Invoice, error) {
	if in.CurAmount <= 0 {
		return nil, fmt.Errorf("开票金额必须大于0")
	}
	inv := &Invoice{
		InvoiceType:    in.InvoiceType,
		InvoiceContent: in.InvoiceContent,
		CurAmount:      in.CurAmount,
		ContractID:     in.ContractID,
		ProjectRef:     in.ProjectRef,
		CustomerID:     in.CustomerID,
		EmployeeID:     in.EmployeeID,
		Note:           in.Note,
		State:          StateDraft,
		TenantID:       s.tenantID,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
		MigrateStatus:  "NEW",
	}
	if err := s.store.Create(ctx, inv); err != nil {
		return nil, fmt.Errorf("创建发票失败: %w", err)
	}
	return inv, nil
}

func (s *Service) Submit(ctx context.Context, id int64) error {
	inv, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if inv.State != StateDraft {
		return fmt.Errorf("只有草稿状态的发票可以提交")
	}
	inv.State = StatePending
	inv.UpdatedAt = time.Now()
	return s.store.Update(ctx, inv)
}

func (s *Service) Approve(ctx context.Context, id int64) error {
	inv, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if inv.State != StatePending {
		return fmt.Errorf("只有待审核状态的发票可以审批")
	}
	inv.State = StateApproved
	inv.UpdatedAt = time.Now()
	return s.store.Update(ctx, inv)
}

func (s *Service) Issue(ctx context.Context, id int64, code, number, date string) error {
	inv, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if inv.State != StateApproved {
		return fmt.Errorf("只有审批通过的发票可以开具")
	}
	inv.InvoiceCode = code
	inv.InvoiceNumber = number
	inv.InvoiceDate = date
	inv.State = StateIssued
	inv.UpdatedAt = time.Now()
	return s.store.Update(ctx, inv)
}

func (s *Service) Scrap(ctx context.Context, id int64, reason string) error {
	inv, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if inv.State == StateScrap {
		return fmt.Errorf("发票已作废")
	}
	inv.State = StateScrap
	inv.Note = fmt.Sprintf("[作废原因] %s | %s", reason, inv.Note)
	inv.UpdatedAt = time.Now()
	return s.store.Update(ctx, inv)
}

func (s *Service) Get(ctx context.Context, id int64) (*Invoice, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) List(ctx context.Context, f InvoiceFilter) ([]*Invoice, int, error) {
	f.TenantID = s.tenantID
	if f.Limit == 0 {
		f.Limit = 20
	}
	return s.store.List(ctx, f)
}

func (s *Service) SumByContract(ctx context.Context, contractID int64) (float64, error) {
	return s.store.SumByContract(ctx, contractID)
}

// ══════════════════════════════════════════════════════════════
//  PostgreSQL Store 实现
// ══════════════════════════════════════════════════════════════

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) Create(ctx context.Context, inv *Invoice) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO invoices (
			invoice_code, invoice_number, invoice_type, invoice_state,
			invoice_date, invoice_content, invoice_person,
			cur_amount, money, used_money,
			contract_id, project_ref, customer_id, employee_id,
			state, draft, note, tenant_id,
			created_at, updated_at, migrate_status
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
		RETURNING id`,
		inv.InvoiceCode, inv.InvoiceNumber, inv.InvoiceType, inv.InvoiceState,
		inv.InvoiceDate, inv.InvoiceContent, inv.InvoicePerson,
		inv.CurAmount, inv.Money, inv.UsedMoney,
		inv.ContractID, inv.ProjectRef, inv.CustomerID, inv.EmployeeID,
		inv.State, inv.Draft, inv.Note, inv.TenantID,
		inv.CreatedAt, inv.UpdatedAt, inv.MigrateStatus,
	).Scan(&inv.ID)
}

func (s *PGStore) Get(ctx context.Context, id int64) (*Invoice, error) {
	inv := &Invoice{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id,legacy_id,invoice_code,invoice_number,invoice_type,invoice_state,
		       invoice_date,invoice_content,invoice_person,
		       cur_amount,money,used_money,
		       contract_id,project_ref,customer_id,employee_id,
		       state,draft,note,tenant_id,created_at,updated_at,migrate_status
		FROM invoices WHERE id=$1`, id,
	).Scan(
		&inv.ID, &inv.LegacyID, &inv.InvoiceCode, &inv.InvoiceNumber,
		&inv.InvoiceType, &inv.InvoiceState, &inv.InvoiceDate,
		&inv.InvoiceContent, &inv.InvoicePerson,
		&inv.CurAmount, &inv.Money, &inv.UsedMoney,
		&inv.ContractID, &inv.ProjectRef, &inv.CustomerID, &inv.EmployeeID,
		&inv.State, &inv.Draft, &inv.Note, &inv.TenantID,
		&inv.CreatedAt, &inv.UpdatedAt, &inv.MigrateStatus,
	)
	return inv, err
}

func (s *PGStore) Update(ctx context.Context, inv *Invoice) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE invoices SET
			invoice_code=$1, invoice_number=$2, invoice_type=$3,
			invoice_state=$4, invoice_date=$5, state=$6, note=$7,
			updated_at=$8
		WHERE id=$9`,
		inv.InvoiceCode, inv.InvoiceNumber, inv.InvoiceType,
		inv.InvoiceState, inv.InvoiceDate, inv.State, inv.Note,
		inv.UpdatedAt, inv.ID,
	)
	return err
}

func (s *PGStore) List(ctx context.Context, f InvoiceFilter) ([]*Invoice, int, error) {
	where := "tenant_id=$1"
	args := []any{f.TenantID}
	i := 2
	if f.ContractID != nil {
		where += fmt.Sprintf(" AND contract_id=$%d", i); args = append(args, *f.ContractID); i++
	}
	if f.ProjectRef != nil {
		where += fmt.Sprintf(" AND project_ref=$%d", i); args = append(args, *f.ProjectRef); i++
	}
	if f.State != nil {
		where += fmt.Sprintf(" AND state=$%d", i); args = append(args, *f.State); i++
	}

	var total int
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM invoices WHERE "+where, args...).Scan(&total)

	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT id,legacy_id,invoice_code,invoice_number,invoice_type,
		       invoice_state,invoice_date,invoice_content,invoice_person,
		       cur_amount,money,used_money,contract_id,project_ref,
		       customer_id,employee_id,state,draft,note,tenant_id,
		       created_at,updated_at,migrate_status
		FROM invoices WHERE %s ORDER BY created_at DESC LIMIT $%d OFFSET $%d`,
			where, i, i+1),
		append(args, f.Limit, f.Offset)...,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var result []*Invoice
	for rows.Next() {
		inv := &Invoice{}
		rows.Scan(
			&inv.ID, &inv.LegacyID, &inv.InvoiceCode, &inv.InvoiceNumber,
			&inv.InvoiceType, &inv.InvoiceState, &inv.InvoiceDate,
			&inv.InvoiceContent, &inv.InvoicePerson,
			&inv.CurAmount, &inv.Money, &inv.UsedMoney,
			&inv.ContractID, &inv.ProjectRef, &inv.CustomerID, &inv.EmployeeID,
			&inv.State, &inv.Draft, &inv.Note, &inv.TenantID,
			&inv.CreatedAt, &inv.UpdatedAt, &inv.MigrateStatus,
		)
		result = append(result, inv)
	}
	return result, total, rows.Err()
}

func (s *PGStore) SumByContract(ctx context.Context, contractID int64) (float64, error) {
	var sum float64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(cur_amount),0) FROM invoices
		 WHERE contract_id=$1 AND state='ISSUED'`, contractID,
	).Scan(&sum)
	return sum, err
}

func (s *PGStore) SumByProject(ctx context.Context, projectRef string) (float64, error) {
	var sum float64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(cur_amount),0) FROM invoices
		 WHERE project_ref=$1 AND state='ISSUED'`, projectRef,
	).Scan(&sum)
	return sum, err
}
