// payment/service.go
// 付款服务 - RULE-003：对外付款必须有合同

package payment

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type State string

const (
	StateDraft    State = "DRAFT"
	StatePending  State = "PENDING"
	StateApproved State = "APPROVED"
	StatePaid     State = "PAID"
	StateRejected State = "REJECTED"
)

type Payment struct {
	ID           int64
	LegacyID     *int64
	Amount       float64
	PayDate      *time.Time
	ContractID   int64    // RULE-003：必须有合同，不可为空
	ContractRef  string   // 合同编号（冗余，便于查询）
	ProjectRef   *string
	BankID       *int64
	EmployeeID   *int64
	State        State
	Note         string
	TenantID     int
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type CreatePaymentInput struct {
	Amount     float64
	ContractID int64
	ProjectRef *string
	BankID     *int64
	EmployeeID *int64
	Note       string
	TenantID   int
}

type Store interface {
	Create(ctx context.Context, p *Payment) error
	Get(ctx context.Context, id int64) (*Payment, error)
	Update(ctx context.Context, p *Payment) error
	List(ctx context.Context, tenantID int, contractID *int64, limit, offset int) ([]*Payment, int, error)
	SumByContract(ctx context.Context, contractID int64) (float64, error)
}

type ContractChecker interface {
	ValidatePayment(ctx context.Context, contractID int64, amount float64) error
}

type Service struct {
	store           Store
	contractChecker ContractChecker
	tenantID        int
}

func NewService(store Store, checker ContractChecker, tenantID int) *Service {
	return &Service{store: store, contractChecker: checker, tenantID: tenantID}
}

func (s *Service) Create(ctx context.Context, in CreatePaymentInput) (*Payment, error) {
	// RULE-003：付款前必须校验合同
	if err := s.contractChecker.ValidatePayment(ctx, in.ContractID, in.Amount); err != nil {
		return nil, err
	}

	p := &Payment{
		Amount:     in.Amount,
		ContractID: in.ContractID,
		ProjectRef: in.ProjectRef,
		BankID:     in.BankID,
		EmployeeID: in.EmployeeID,
		State:      StateDraft,
		Note:       in.Note,
		TenantID:   s.tenantID,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	if err := s.store.Create(ctx, p); err != nil {
		return nil, fmt.Errorf("创建付款单失败: %w", err)
	}
	return p, nil
}

func (s *Service) Approve(ctx context.Context, id int64) error {
	p, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if p.State != StatePending {
		return fmt.Errorf("只有待审核状态可以审批")
	}
	p.State = StateApproved
	p.UpdatedAt = time.Now()
	return s.store.Update(ctx, p)
}

func (s *Service) MarkPaid(ctx context.Context, id int64, payDate time.Time) error {
	p, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if p.State != StateApproved {
		return fmt.Errorf("只有审批通过的付款单可以标记已付")
	}
	p.State = StatePaid
	p.PayDate = &payDate
	p.UpdatedAt = time.Now()
	return s.store.Update(ctx, p)
}

func (s *Service) Get(ctx context.Context, id int64) (*Payment, error) {
	return s.store.Get(ctx, id)
}

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) Create(ctx context.Context, p *Payment) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO payments (
			amount, pay_date, contract_id, contract_ref, project_ref,
			bank_id, employee_id, state, note, tenant_id, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING id`,
		p.Amount, p.PayDate, p.ContractID, p.ContractRef, p.ProjectRef,
		p.BankID, p.EmployeeID, p.State, p.Note, p.TenantID,
		p.CreatedAt, p.UpdatedAt,
	).Scan(&p.ID)
}

func (s *PGStore) Get(ctx context.Context, id int64) (*Payment, error) {
	p := &Payment{}
	err := s.db.QueryRowContext(ctx,
		`SELECT id,legacy_id,amount,pay_date,contract_id,contract_ref,project_ref,
		        bank_id,employee_id,state,note,tenant_id,created_at,updated_at
		 FROM payments WHERE id=$1`, id,
	).Scan(&p.ID, &p.LegacyID, &p.Amount, &p.PayDate, &p.ContractID,
		&p.ContractRef, &p.ProjectRef, &p.BankID, &p.EmployeeID,
		&p.State, &p.Note, &p.TenantID, &p.CreatedAt, &p.UpdatedAt)
	return p, err
}

func (s *PGStore) Update(ctx context.Context, p *Payment) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE payments SET state=$1, pay_date=$2, updated_at=$3 WHERE id=$4`,
		p.State, p.PayDate, p.UpdatedAt, p.ID)
	return err
}

func (s *PGStore) List(ctx context.Context, tenantID int, contractID *int64, limit, offset int) ([]*Payment, int, error) {
	where := "tenant_id=$1"
	args := []any{tenantID}
	if contractID != nil {
		where += " AND contract_id=$2"; args = append(args, *contractID)
	}
	var total int
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM payments WHERE "+where, args...).Scan(&total)
	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT id,legacy_id,amount,pay_date,contract_id,contract_ref,
			project_ref,bank_id,employee_id,state,note,tenant_id,created_at,updated_at
			FROM payments WHERE %s ORDER BY created_at DESC LIMIT $%d OFFSET $%d`,
			where, len(args)+1, len(args)+2),
		append(args, limit, offset)...,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var result []*Payment
	for rows.Next() {
		p := &Payment{}
		rows.Scan(&p.ID, &p.LegacyID, &p.Amount, &p.PayDate, &p.ContractID,
			&p.ContractRef, &p.ProjectRef, &p.BankID, &p.EmployeeID,
			&p.State, &p.Note, &p.TenantID, &p.CreatedAt, &p.UpdatedAt)
		result = append(result, p)
	}
	return result, total, rows.Err()
}

func (s *PGStore) SumByContract(ctx context.Context, contractID int64) (float64, error) {
	var sum float64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(amount),0) FROM payments WHERE contract_id=$1 AND state='PAID'`,
		contractID).Scan(&sum)
	return sum, err
}
