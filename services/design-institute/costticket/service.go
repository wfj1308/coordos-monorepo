package costticket

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
	StatePending  State = "PENDING"
	StateApproved State = "APPROVED"
	StateRejected State = "REJECTED"
	StatePaid     State = "PAID"
)

type BalanceType int

const (
	BalanceTypeBank State = "BANK" // 银行转账
	BalanceTypeCash State = "CASH" // 现金
)

type CostTicket struct {
	ID                 int64
	LegacyID           *int64
	TenantID           int
	CostTicketNumber   string
	BalanceType        int
	BankMoney          float64
	CashMoney          float64
	BankSettlement     float64
	CashSettlement     float64
	VATRate            string
	VATSum             float64
	DeductRate         string
	DeductSum          float64
	ManagementCostSum  float64
	CostTicketSum      float64
	TotalInvoice       float64
	NoTicketSum        float64
	TaxExpensesSum     float64
	State              State
	PayDate            *time.Time
	EmployeeID         *int64
	BankID             *int64
	PayEmployeeID      *int64
	InvoiceID          *int64
	FlowID             *int64
	// CoordOS 新增
	ProjectRef         *string
	ContractID         *int64
	Note               string
	Draft              bool
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type CreateInput struct {
	ContractID        int64
	ProjectRef        *string
	EmployeeID        *int64
	BankMoney         float64
	CashMoney         float64
	VATRate           string
	DeductRate        string
	ManagementCost    string
	Note              string
	TenantID          int
}

type Summary struct {
	ContractID        int64
	TotalAmount       float64
	TotalPaid         float64
	PendingAmount     float64
	TicketCount       int
}

// ══════════════════════════════════════════════════════════════
//  Store 接口
// ══════════════════════════════════════════════════════════════

type Store interface {
	Create(ctx context.Context, ct *CostTicket) error
	Get(ctx context.Context, id int64) (*CostTicket, error)
	Update(ctx context.Context, ct *CostTicket) error
	UpdateState(ctx context.Context, id int64, state State) error
	ListByContract(ctx context.Context, contractID int64) ([]*CostTicket, error)
	ListByTenant(ctx context.Context, tenantID int, state *State, limit, offset int) ([]*CostTicket, int, error)
	SummaryByContract(ctx context.Context, contractID int64) (*Summary, error)
	TotalPaidByPeriod(ctx context.Context, tenantID int, from, to time.Time) (float64, error)
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

func (s *Service) Create(ctx context.Context, in CreateInput) (*CostTicket, error) {
	if in.BankMoney == 0 && in.CashMoney == 0 {
		return nil, fmt.Errorf("银行金额和现金金额不能同时为0")
	}
	ct := &CostTicket{
		TenantID:    s.tenantID,
		ContractID:  &in.ContractID,
		ProjectRef:  in.ProjectRef,
		EmployeeID:  in.EmployeeID,
		BankMoney:   in.BankMoney,
		CashMoney:   in.CashMoney,
		VATRate:     in.VATRate,
		DeductRate:  in.DeductRate,
		Note:        in.Note,
		State:       StatePending,
		Draft:       false,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	if err := s.store.Create(ctx, ct); err != nil {
		return nil, fmt.Errorf("创建费用单失败: %w", err)
	}
	return ct, nil
}

// ── 审批通过后标记已付款 ──────────────────────────────────────
func (s *Service) MarkPaid(ctx context.Context, id int64, payDate time.Time) (*CostTicket, error) {
	ct, err := s.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if ct.State != StateApproved {
		return nil, fmt.Errorf("费用单状态为 %s，必须先审批通过才能付款", ct.State)
	}
	ct.State   = StatePaid
	ct.PayDate = &payDate
	ct.UpdatedAt = time.Now()
	if err := s.store.Update(ctx, ct); err != nil {
		return nil, err
	}
	return ct, nil
}

func (s *Service) Get(ctx context.Context, id int64) (*CostTicket, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) ListByContract(ctx context.Context, contractID int64) ([]*CostTicket, error) {
	return s.store.ListByContract(ctx, contractID)
}

func (s *Service) List(ctx context.Context, state *State, limit, offset int) ([]*CostTicket, int, error) {
	return s.store.ListByTenant(ctx, s.tenantID, state, limit, offset)
}

func (s *Service) Summary(ctx context.Context, contractID int64) (*Summary, error) {
	return s.store.SummaryByContract(ctx, contractID)
}

func (s *Service) TotalPaidByPeriod(ctx context.Context, from, to time.Time) (float64, error) {
	return s.store.TotalPaidByPeriod(ctx, s.tenantID, from, to)
}

// ══════════════════════════════════════════════════════════════
//  PostgreSQL Store 实现
// ══════════════════════════════════════════════════════════════

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) Create(ctx context.Context, ct *CostTicket) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO costtickets (
			tenant_id, cost_ticket_number, balance_type,
			bank_money, cash_money, bank_settlement, cash_settlement,
			vat_rate, vat_sum, deduct_rate, deduct_sum,
			management_cost_sum, cost_ticket_sum, total_invoice,
			state, employee_id, bank_id, pay_employee_id,
			contract_id, project_ref, note, draft,
			created_at, updated_at
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
			$15,$16,$17,$18,$19,$20,$21,$22,$23,$24
		) RETURNING id`,
		ct.TenantID, ct.CostTicketNumber, ct.BalanceType,
		ct.BankMoney, ct.CashMoney, ct.BankSettlement, ct.CashSettlement,
		ct.VATRate, ct.VATSum, ct.DeductRate, ct.DeductSum,
		ct.ManagementCostSum, ct.CostTicketSum, ct.TotalInvoice,
		ct.State, ct.EmployeeID, ct.BankID, ct.PayEmployeeID,
		ct.ContractID, ct.ProjectRef, ct.Note, ct.Draft,
		ct.CreatedAt, ct.UpdatedAt,
	).Scan(&ct.ID)
}

func (s *PGStore) Get(ctx context.Context, id int64) (*CostTicket, error) {
	ct := &CostTicket{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id,legacy_id,tenant_id,cost_ticket_number,balance_type,
		       bank_money,cash_money,vat_rate,vat_sum,
		       deduct_rate,deduct_sum,management_cost_sum,
		       cost_ticket_sum,total_invoice,state,
		       pay_date,employee_id,bank_id,pay_employee_id,
		       contract_id,project_ref,note,draft,created_at,updated_at
		FROM costtickets WHERE id=$1`, id,
	).Scan(
		&ct.ID, &ct.LegacyID, &ct.TenantID, &ct.CostTicketNumber, &ct.BalanceType,
		&ct.BankMoney, &ct.CashMoney, &ct.VATRate, &ct.VATSum,
		&ct.DeductRate, &ct.DeductSum, &ct.ManagementCostSum,
		&ct.CostTicketSum, &ct.TotalInvoice, &ct.State,
		&ct.PayDate, &ct.EmployeeID, &ct.BankID, &ct.PayEmployeeID,
		&ct.ContractID, &ct.ProjectRef, &ct.Note, &ct.Draft,
		&ct.CreatedAt, &ct.UpdatedAt,
	)
	return ct, err
}

func (s *PGStore) Update(ctx context.Context, ct *CostTicket) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE costtickets SET state=$1, pay_date=$2, updated_at=$3 WHERE id=$4`,
		ct.State, ct.PayDate, ct.UpdatedAt, ct.ID)
	return err
}

func (s *PGStore) UpdateState(ctx context.Context, id int64, state State) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE costtickets SET state=$1, updated_at=NOW() WHERE id=$2`, state, id)
	return err
}

func (s *PGStore) ListByContract(ctx context.Context, contractID int64) ([]*CostTicket, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,legacy_id,tenant_id,cost_ticket_number,balance_type,
		        bank_money,cash_money,vat_rate,vat_sum,
		        deduct_rate,deduct_sum,management_cost_sum,
		        cost_ticket_sum,total_invoice,state,
		        pay_date,employee_id,bank_id,pay_employee_id,
		        contract_id,project_ref,note,draft,created_at,updated_at
		 FROM costtickets WHERE contract_id=$1 ORDER BY created_at DESC`, contractID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCostTickets(rows)
}

func (s *PGStore) ListByTenant(ctx context.Context, tenantID int,
	state *State, limit, offset int) ([]*CostTicket, int, error) {
	where, args := "tenant_id=$1", []any{tenantID}
	if state != nil {
		where += " AND state=$2"
		args = append(args, *state)
	}
	var total int
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM costtickets WHERE "+where, args...).Scan(&total)
	args = append(args, limit, offset)
	n := len(args)
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,legacy_id,tenant_id,cost_ticket_number,balance_type,
		        bank_money,cash_money,vat_rate,vat_sum,
		        deduct_rate,deduct_sum,management_cost_sum,
		        cost_ticket_sum,total_invoice,state,
		        pay_date,employee_id,bank_id,pay_employee_id,
		        contract_id,project_ref,note,draft,created_at,updated_at
		 FROM costtickets WHERE `+where+
			fmt.Sprintf(` ORDER BY created_at DESC LIMIT $%d OFFSET $%d`, n-1, n), args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	cts, err := scanCostTickets(rows)
	return cts, total, err
}

func (s *PGStore) SummaryByContract(ctx context.Context, contractID int64) (*Summary, error) {
	sum := &Summary{ContractID: contractID}
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(cost_ticket_sum),0),
		       COALESCE(SUM(CASE WHEN state='PAID' THEN cost_ticket_sum ELSE 0 END),0),
		       COALESCE(SUM(CASE WHEN state!='PAID' THEN cost_ticket_sum ELSE 0 END),0),
		       COUNT(*)
		FROM costtickets WHERE contract_id=$1`, contractID,
	).Scan(&sum.TotalAmount, &sum.TotalPaid, &sum.PendingAmount, &sum.TicketCount)
	return sum, err
}

func (s *PGStore) TotalPaidByPeriod(ctx context.Context, tenantID int, from, to time.Time) (float64, error) {
	var total float64
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(cost_ticket_sum),0) FROM costtickets
		WHERE tenant_id=$1 AND state='PAID' AND pay_date>=$2 AND pay_date<$3`,
		tenantID, from, to,
	).Scan(&total)
	return total, err
}

func scanCostTickets(rows *sql.Rows) ([]*CostTicket, error) {
	var list []*CostTicket
	for rows.Next() {
		ct := &CostTicket{}
		if err := rows.Scan(
			&ct.ID, &ct.LegacyID, &ct.TenantID, &ct.CostTicketNumber, &ct.BalanceType,
			&ct.BankMoney, &ct.CashMoney, &ct.VATRate, &ct.VATSum,
			&ct.DeductRate, &ct.DeductSum, &ct.ManagementCostSum,
			&ct.CostTicketSum, &ct.TotalInvoice, &ct.State,
			&ct.PayDate, &ct.EmployeeID, &ct.BankID, &ct.PayEmployeeID,
			&ct.ContractID, &ct.ProjectRef, &ct.Note, &ct.Draft,
			&ct.CreatedAt, &ct.UpdatedAt,
		); err != nil {
			return nil, err
		}
		list = append(list, ct)
	}
	return list, rows.Err()
}
