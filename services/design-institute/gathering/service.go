package gathering

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
)

type Gathering struct {
	ID              int64
	LegacyID        *int64
	GatheringNumber string
	GatheringMoney  float64   // 本次收款金额
	GatheringDate   string
	GatheringState  State
	GatheringType   string    // 银行/现金
	GatheringPerson string
	ContractID      *int64
	ProjectRef      *string
	CompanyID       *int
	EmployeeID      *int64
	BalanceID       *int64    // 关联结算单
	BankInfoID      *int64
	BeforeMoney     float64   // 收款前余额
	AfterMoney      float64   // 收款后余额
	ManageRatio     float64   // 管理费比率
	NeedManageFee   float64   // 应收管理费
	State           State
	Note            string
	Draft           int
	TenantID        int
	CreatedAt       time.Time
	UpdatedAt       time.Time
	MigrateStatus   string
}

type CreateGatheringInput struct {
	GatheringMoney  float64
	GatheringDate   string
	GatheringType   string
	GatheringPerson string
	ContractID      *int64
	ProjectRef      *string
	CompanyID       *int
	EmployeeID      *int64
	ManageRatio     float64
	Note            string
	TenantID        int
}

type GatheringFilter struct {
	ContractID *int64
	ProjectRef *string
	CompanyID  *int
	State      *State
	TenantID   int
	Limit      int
	Offset     int
}

// ══════════════════════════════════════════════════════════════
//  Store 接口
// ══════════════════════════════════════════════════════════════

type Store interface {
	Create(ctx context.Context, g *Gathering) error
	Get(ctx context.Context, id int64) (*Gathering, error)
	Update(ctx context.Context, g *Gathering) error
	List(ctx context.Context, f GatheringFilter) ([]*Gathering, int, error)
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

func (s *Service) Create(ctx context.Context, in CreateGatheringInput) (*Gathering, error) {
	if in.GatheringMoney <= 0 {
		return nil, fmt.Errorf("收款金额必须大于0")
	}

	// 计算管理费
	needManageFee := 0.0
	if in.ManageRatio > 0 {
		needManageFee = in.GatheringMoney * in.ManageRatio / 100
	}

	g := &Gathering{
		GatheringMoney:  in.GatheringMoney,
		GatheringDate:   in.GatheringDate,
		GatheringType:   in.GatheringType,
		GatheringPerson: in.GatheringPerson,
		ContractID:      in.ContractID,
		ProjectRef:      in.ProjectRef,
		CompanyID:       in.CompanyID,
		EmployeeID:      in.EmployeeID,
		ManageRatio:     in.ManageRatio,
		NeedManageFee:   needManageFee,
		Note:            in.Note,
		State:           StateDraft,
		TenantID:        s.tenantID,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
		MigrateStatus:   "NEW",
	}

	if err := s.store.Create(ctx, g); err != nil {
		return nil, fmt.Errorf("创建收款单失败: %w", err)
	}
	return g, nil
}

func (s *Service) Approve(ctx context.Context, id int64) error {
	g, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if g.State != StatePending {
		return fmt.Errorf("只有待审核状态可以审批")
	}
	g.State = StateApproved
	g.UpdatedAt = time.Now()
	return s.store.Update(ctx, g)
}

func (s *Service) Get(ctx context.Context, id int64) (*Gathering, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) List(ctx context.Context, f GatheringFilter) ([]*Gathering, int, error) {
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

func (s *PGStore) Create(ctx context.Context, g *Gathering) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO gatherings (
			gathering_number, gathering_money, gathering_date,
			gathering_state, gathering_type, gathering_person,
			contract_id, project_ref, company_id, employee_id,
			before_money, after_money, manage_ratio, need_manage_fee,
			state, note, draft, tenant_id,
			created_at, updated_at, migrate_status
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
		RETURNING id`,
		g.GatheringNumber, g.GatheringMoney, g.GatheringDate,
		g.GatheringState, g.GatheringType, g.GatheringPerson,
		g.ContractID, g.ProjectRef, g.CompanyID, g.EmployeeID,
		g.BeforeMoney, g.AfterMoney, g.ManageRatio, g.NeedManageFee,
		g.State, g.Note, g.Draft, g.TenantID,
		g.CreatedAt, g.UpdatedAt, g.MigrateStatus,
	).Scan(&g.ID)
}

func (s *PGStore) Get(ctx context.Context, id int64) (*Gathering, error) {
	g := &Gathering{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id,legacy_id,gathering_number,gathering_money,gathering_date,
		       gathering_state,gathering_type,gathering_person,
		       contract_id,project_ref,company_id,employee_id,balance_id,
		       before_money,after_money,manage_ratio,need_manage_fee,
		       state,note,draft,tenant_id,created_at,updated_at,migrate_status
		FROM gatherings WHERE id=$1`, id,
	).Scan(
		&g.ID, &g.LegacyID, &g.GatheringNumber, &g.GatheringMoney, &g.GatheringDate,
		&g.GatheringState, &g.GatheringType, &g.GatheringPerson,
		&g.ContractID, &g.ProjectRef, &g.CompanyID, &g.EmployeeID, &g.BalanceID,
		&g.BeforeMoney, &g.AfterMoney, &g.ManageRatio, &g.NeedManageFee,
		&g.State, &g.Note, &g.Draft, &g.TenantID,
		&g.CreatedAt, &g.UpdatedAt, &g.MigrateStatus,
	)
	return g, err
}

func (s *PGStore) Update(ctx context.Context, g *Gathering) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE gatherings SET
			gathering_state=$1, state=$2, balance_id=$3,
			before_money=$4, after_money=$5, note=$6, updated_at=$7
		WHERE id=$8`,
		g.GatheringState, g.State, g.BalanceID,
		g.BeforeMoney, g.AfterMoney, g.Note, g.UpdatedAt, g.ID,
	)
	return err
}

func (s *PGStore) List(ctx context.Context, f GatheringFilter) ([]*Gathering, int, error) {
	where := "tenant_id=$1"
	args := []any{f.TenantID}
	i := 2
	if f.ContractID != nil {
		where += fmt.Sprintf(" AND contract_id=$%d", i); args = append(args, *f.ContractID); i++
	}
	if f.ProjectRef != nil {
		where += fmt.Sprintf(" AND project_ref=$%d", i); args = append(args, *f.ProjectRef); i++
	}
	if f.CompanyID != nil {
		where += fmt.Sprintf(" AND company_id=$%d", i); args = append(args, *f.CompanyID); i++
	}
	if f.State != nil {
		where += fmt.Sprintf(" AND state=$%d", i); args = append(args, *f.State); i++
	}

	var total int
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM gatherings WHERE "+where, args...).Scan(&total)

	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT id,legacy_id,gathering_number,gathering_money,gathering_date,
			gathering_state,gathering_type,gathering_person,
			contract_id,project_ref,company_id,employee_id,balance_id,
			before_money,after_money,manage_ratio,need_manage_fee,
			state,note,draft,tenant_id,created_at,updated_at,migrate_status
			FROM gatherings WHERE %s ORDER BY created_at DESC LIMIT $%d OFFSET $%d`,
			where, i, i+1),
		append(args, f.Limit, f.Offset)...,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var result []*Gathering
	for rows.Next() {
		g := &Gathering{}
		rows.Scan(
			&g.ID, &g.LegacyID, &g.GatheringNumber, &g.GatheringMoney, &g.GatheringDate,
			&g.GatheringState, &g.GatheringType, &g.GatheringPerson,
			&g.ContractID, &g.ProjectRef, &g.CompanyID, &g.EmployeeID, &g.BalanceID,
			&g.BeforeMoney, &g.AfterMoney, &g.ManageRatio, &g.NeedManageFee,
			&g.State, &g.Note, &g.Draft, &g.TenantID,
			&g.CreatedAt, &g.UpdatedAt, &g.MigrateStatus,
		)
		result = append(result, g)
	}
	return result, total, rows.Err()
}

func (s *PGStore) SumByContract(ctx context.Context, contractID int64) (float64, error) {
	var sum float64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(gathering_money),0) FROM gatherings
		 WHERE contract_id=$1 AND state='APPROVED'`, contractID,
	).Scan(&sum)
	return sum, err
}

func (s *PGStore) SumByProject(ctx context.Context, projectRef string) (float64, error) {
	var sum float64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(gathering_money),0) FROM gatherings
		 WHERE project_ref=$1 AND state='APPROVED'`, projectRef,
	).Scan(&sum)
	return sum, err
}
