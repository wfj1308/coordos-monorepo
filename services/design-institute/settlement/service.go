package settlement

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
	StateRejected State = "REJECTED"
	StatePaid     State = "PAID"
)

type Balance struct {
	ID                int64
	LegacyID          *int64
	BalanceNumber     string
	ContractName      string
	CustomerName      string
	GatheringMoney    float64
	BankMoney         float64
	CashMoney         float64
	BankSettlement    float64
	CashSettlement    float64
	VATRate           string
	VATSum            float64
	DeductRate        string
	DeductSum         float64
	ManagementCostSum float64
	CostTicketSum     float64
	TotalInvoice      float64
	BalanceType       int
	State             State
	PayDate           *time.Time
	GatheringID       *int64
	ContractID        *int64
	ProjectRef        *string
	GenesisRef        *string
	UTXORef           *string
	EmployeeID        *int64
	BankID            *int64
	PayEmployeeID     *int64
	Note              string
	Draft             int
	TenantID          int
	CreatedAt         time.Time
	UpdatedAt         time.Time
	MigrateStatus     string
}

type CreateBalanceInput struct {
	ContractID        *int64
	GatheringID       *int64
	ProjectRef        *string
	GatheringMoney    float64
	BankSettlement    float64
	CashSettlement    float64
	VATRate           string
	DeductRate        string
	ManagementCostSum float64
	EmployeeID        *int64
	Note              string
	TenantID          int
}

type BalanceFilter struct {
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
	Create(ctx context.Context, b *Balance) error
	Get(ctx context.Context, id int64) (*Balance, error)
	Update(ctx context.Context, b *Balance) error
	List(ctx context.Context, f BalanceFilter) ([]*Balance, int, error)
	SumByContract(ctx context.Context, contractID int64) (float64, error)
	SumByProject(ctx context.Context, projectRef string) (float64, error)
	SetUTXORef(ctx context.Context, id int64, utxoRef string) error
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

func (s *Service) Create(ctx context.Context, in CreateBalanceInput) (*Balance, error) {
	total := in.BankSettlement + in.CashSettlement
	if total <= 0 {
		return nil, fmt.Errorf("结算金额必须大于0")
	}

	b := &Balance{
		GatheringMoney:    in.GatheringMoney,
		BankSettlement:    in.BankSettlement,
		CashSettlement:    in.CashSettlement,
		VATRate:           in.VATRate,
		DeductRate:        in.DeductRate,
		ManagementCostSum: in.ManagementCostSum,
		ContractID:        in.ContractID,
		GatheringID:       in.GatheringID,
		ProjectRef:        in.ProjectRef,
		EmployeeID:        in.EmployeeID,
		Note:              in.Note,
		State:             StateDraft,
		TenantID:          s.tenantID,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
		MigrateStatus:     "NEW",
	}

	if err := s.store.Create(ctx, b); err != nil {
		return nil, fmt.Errorf("创建结算单失败: %w", err)
	}
	return b, nil
}

func (s *Service) Submit(ctx context.Context, id int64) error {
	b, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if b.State != StateDraft {
		return fmt.Errorf("只有草稿状态可以提交")
	}
	b.State = StatePending
	b.UpdatedAt = time.Now()
	return s.store.Update(ctx, b)
}

func (s *Service) Approve(ctx context.Context, id int64) error {
	b, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if b.State != StatePending {
		return fmt.Errorf("只有待审核状态可以审批")
	}
	b.State = StateApproved
	b.UpdatedAt = time.Now()
	return s.store.Update(ctx, b)
}

func (s *Service) MarkPaid(ctx context.Context, id int64, payDate time.Time, bankID int64) error {
	b, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if b.State != StateApproved {
		return fmt.Errorf("只有审批通过的结算单可以标记付款")
	}
	b.State = StatePaid
	b.PayDate = &payDate
	b.BankID = &bankID
	b.UpdatedAt = time.Now()
	return s.store.Update(ctx, b)
}

// RULE-005：有产出UTXO才能结算，触发后挂 utxo_ref
func (s *Service) TriggerFromUTXO(ctx context.Context, id int64, utxoRef string) error {
	return s.store.SetUTXORef(ctx, id, utxoRef)
}

func (s *Service) Get(ctx context.Context, id int64) (*Balance, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) List(ctx context.Context, f BalanceFilter) ([]*Balance, int, error) {
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

func (s *PGStore) Create(ctx context.Context, b *Balance) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO balances (
			balance_number, contract_name, customer_name,
			gathering_money, bank_money, cash_money,
			bank_settlement, cash_settlement,
			vat_rate, vat_sum, deduct_rate, deduct_sum,
			management_cost_sum, cost_ticket_sum, total_invoice,
			balance_type, state, pay_date,
			gathering_id, contract_id, project_ref, genesis_ref, utxo_ref,
			employee_id, bank_id, pay_employee_id,
			note, draft, tenant_id, created_at, updated_at, migrate_status
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
			$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32
		) RETURNING id`,
		b.BalanceNumber, b.ContractName, b.CustomerName,
		b.GatheringMoney, b.BankMoney, b.CashMoney,
		b.BankSettlement, b.CashSettlement,
		b.VATRate, b.VATSum, b.DeductRate, b.DeductSum,
		b.ManagementCostSum, b.CostTicketSum, b.TotalInvoice,
		b.BalanceType, b.State, b.PayDate,
		b.GatheringID, b.ContractID, b.ProjectRef, b.GenesisRef, b.UTXORef,
		b.EmployeeID, b.BankID, b.PayEmployeeID,
		b.Note, b.Draft, b.TenantID, b.CreatedAt, b.UpdatedAt, b.MigrateStatus,
	).Scan(&b.ID)
}

func (s *PGStore) Get(ctx context.Context, id int64) (*Balance, error) {
	b := &Balance{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id,legacy_id,balance_number,contract_name,customer_name,
		       gathering_money,bank_money,cash_money,bank_settlement,cash_settlement,
		       vat_rate,vat_sum,deduct_rate,deduct_sum,
		       management_cost_sum,cost_ticket_sum,total_invoice,
		       balance_type,state,pay_date,
		       gathering_id,contract_id,project_ref,genesis_ref,utxo_ref,
		       employee_id,bank_id,pay_employee_id,
		       note,draft,tenant_id,created_at,updated_at,migrate_status
		FROM balances WHERE id=$1`, id,
	).Scan(
		&b.ID, &b.LegacyID, &b.BalanceNumber, &b.ContractName, &b.CustomerName,
		&b.GatheringMoney, &b.BankMoney, &b.CashMoney,
		&b.BankSettlement, &b.CashSettlement,
		&b.VATRate, &b.VATSum, &b.DeductRate, &b.DeductSum,
		&b.ManagementCostSum, &b.CostTicketSum, &b.TotalInvoice,
		&b.BalanceType, &b.State, &b.PayDate,
		&b.GatheringID, &b.ContractID, &b.ProjectRef, &b.GenesisRef, &b.UTXORef,
		&b.EmployeeID, &b.BankID, &b.PayEmployeeID,
		&b.Note, &b.Draft, &b.TenantID, &b.CreatedAt, &b.UpdatedAt, &b.MigrateStatus,
	)
	return b, err
}

func (s *PGStore) Update(ctx context.Context, b *Balance) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE balances SET
			state=$1, pay_date=$2, bank_id=$3,
			utxo_ref=$4, note=$5, updated_at=$6
		WHERE id=$7`,
		b.State, b.PayDate, b.BankID,
		b.UTXORef, b.Note, b.UpdatedAt, b.ID,
	)
	return err
}

func (s *PGStore) List(ctx context.Context, f BalanceFilter) ([]*Balance, int, error) {
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
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM balances WHERE "+where, args...).Scan(&total)

	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT id,legacy_id,balance_number,contract_name,customer_name,
			gathering_money,bank_money,cash_money,bank_settlement,cash_settlement,
			vat_rate,vat_sum,deduct_rate,deduct_sum,
			management_cost_sum,cost_ticket_sum,total_invoice,
			balance_type,state,pay_date,
			gathering_id,contract_id,project_ref,genesis_ref,utxo_ref,
			employee_id,bank_id,pay_employee_id,
			note,draft,tenant_id,created_at,updated_at,migrate_status
			FROM balances WHERE %s ORDER BY created_at DESC LIMIT $%d OFFSET $%d`,
			where, i, i+1),
		append(args, f.Limit, f.Offset)...,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var result []*Balance
	for rows.Next() {
		b := &Balance{}
		rows.Scan(
			&b.ID, &b.LegacyID, &b.BalanceNumber, &b.ContractName, &b.CustomerName,
			&b.GatheringMoney, &b.BankMoney, &b.CashMoney,
			&b.BankSettlement, &b.CashSettlement,
			&b.VATRate, &b.VATSum, &b.DeductRate, &b.DeductSum,
			&b.ManagementCostSum, &b.CostTicketSum, &b.TotalInvoice,
			&b.BalanceType, &b.State, &b.PayDate,
			&b.GatheringID, &b.ContractID, &b.ProjectRef, &b.GenesisRef, &b.UTXORef,
			&b.EmployeeID, &b.BankID, &b.PayEmployeeID,
			&b.Note, &b.Draft, &b.TenantID, &b.CreatedAt, &b.UpdatedAt, &b.MigrateStatus,
		)
		result = append(result, b)
	}
	return result, total, rows.Err()
}

func (s *PGStore) SumByContract(ctx context.Context, contractID int64) (float64, error) {
	var sum float64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(bank_settlement+cash_settlement),0)
		 FROM balances WHERE contract_id=$1 AND state='PAID'`, contractID,
	).Scan(&sum)
	return sum, err
}

func (s *PGStore) SumByProject(ctx context.Context, projectRef string) (float64, error) {
	var sum float64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(bank_settlement+cash_settlement),0)
		 FROM balances WHERE project_ref=$1 AND state='PAID'`, projectRef,
	).Scan(&sum)
	return sum, err
}

func (s *PGStore) SetUTXORef(ctx context.Context, id int64, utxoRef string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE balances SET utxo_ref=$1, updated_at=NOW() WHERE id=$2`, utxoRef, id)
	return err
}
