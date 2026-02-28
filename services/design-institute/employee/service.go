package employee

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Employee struct {
	ID            int64
	LegacyID      *int64
	Name          string
	Phone         string
	Account       string
	CompanyID     *int
	DepartmentID  *int
	UserID        *int64
	Position      string
	StartDate     *time.Time
	EndDate       *time.Time
	ExecutorRef   *string   // v://zhongbei/executor/person/{id}
	TenantID      int
	Deleted       bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
	MigrateStatus string
}

type CreateEmployeeInput struct {
	Name         string
	Phone        string
	Account      string
	CompanyID    *int
	DepartmentID *int
	UserID       *int64
	Position     string
	StartDate    *time.Time
	TenantID     int
}

type EmployeeFilter struct {
	CompanyID    *int
	DepartmentID *int
	TenantID     int
	Keyword      string
	Limit        int
	Offset       int
}

// ══════════════════════════════════════════════════════════════
//  Store 接口
// ══════════════════════════════════════════════════════════════

type Store interface {
	Create(ctx context.Context, e *Employee) error
	Get(ctx context.Context, id int64) (*Employee, error)
	GetByUserID(ctx context.Context, userID int64) (*Employee, error)
	Update(ctx context.Context, e *Employee) error
	List(ctx context.Context, f EmployeeFilter) ([]*Employee, int, error)
	SetExecutorRef(ctx context.Context, id int64, executorRef string) error
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

func (s *Service) Create(ctx context.Context, in CreateEmployeeInput) (*Employee, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("姓名不能为空")
	}
	e := &Employee{
		Name:          in.Name,
		Phone:         in.Phone,
		Account:       in.Account,
		CompanyID:     in.CompanyID,
		DepartmentID:  in.DepartmentID,
		UserID:        in.UserID,
		Position:      in.Position,
		StartDate:     in.StartDate,
		TenantID:      s.tenantID,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
		MigrateStatus: "NEW",
	}
	// 生成 executor_ref
	ref := fmt.Sprintf("v://zhongbei/executor/person/%d", time.Now().UnixNano())
	e.ExecutorRef = &ref

	if err := s.store.Create(ctx, e); err != nil {
		return nil, fmt.Errorf("创建员工失败: %w", err)
	}
	return e, nil
}

func (s *Service) Get(ctx context.Context, id int64) (*Employee, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) List(ctx context.Context, f EmployeeFilter) ([]*Employee, int, error) {
	f.TenantID = s.tenantID
	if f.Limit == 0 {
		f.Limit = 20
	}
	return s.store.List(ctx, f)
}

func (s *Service) Resign(ctx context.Context, id int64, endDate time.Time) error {
	e, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	e.EndDate = &endDate
	e.UpdatedAt = time.Now()
	return s.store.Update(ctx, e)
}

// ══════════════════════════════════════════════════════════════
//  PostgreSQL Store 实现
// ══════════════════════════════════════════════════════════════

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) Create(ctx context.Context, e *Employee) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO employees (
			name, phone, account, company_id, department_id, user_id,
			position, start_date, end_date, executor_ref,
			tenant_id, deleted, created_at, updated_at, migrate_status
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		RETURNING id`,
		e.Name, e.Phone, e.Account, e.CompanyID, e.DepartmentID, e.UserID,
		e.Position, e.StartDate, e.EndDate, e.ExecutorRef,
		e.TenantID, e.Deleted, e.CreatedAt, e.UpdatedAt, e.MigrateStatus,
	).Scan(&e.ID)
}

func (s *PGStore) Get(ctx context.Context, id int64) (*Employee, error) {
	e := &Employee{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id,legacy_id,name,phone,account,company_id,department_id,user_id,
		       position,start_date,end_date,executor_ref,
		       tenant_id,deleted,created_at,updated_at,migrate_status
		FROM employees WHERE id=$1 AND deleted=FALSE`, id,
	).Scan(
		&e.ID, &e.LegacyID, &e.Name, &e.Phone, &e.Account,
		&e.CompanyID, &e.DepartmentID, &e.UserID,
		&e.Position, &e.StartDate, &e.EndDate, &e.ExecutorRef,
		&e.TenantID, &e.Deleted, &e.CreatedAt, &e.UpdatedAt, &e.MigrateStatus,
	)
	return e, err
}

func (s *PGStore) GetByUserID(ctx context.Context, userID int64) (*Employee, error) {
	e := &Employee{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id,legacy_id,name,phone,account,company_id,department_id,user_id,
		       position,start_date,end_date,executor_ref,
		       tenant_id,deleted,created_at,updated_at,migrate_status
		FROM employees WHERE user_id=$1 AND deleted=FALSE`, userID,
	).Scan(
		&e.ID, &e.LegacyID, &e.Name, &e.Phone, &e.Account,
		&e.CompanyID, &e.DepartmentID, &e.UserID,
		&e.Position, &e.StartDate, &e.EndDate, &e.ExecutorRef,
		&e.TenantID, &e.Deleted, &e.CreatedAt, &e.UpdatedAt, &e.MigrateStatus,
	)
	return e, err
}

func (s *PGStore) Update(ctx context.Context, e *Employee) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE employees SET
			name=$1, phone=$2, position=$3,
			end_date=$4, updated_at=$5
		WHERE id=$6`,
		e.Name, e.Phone, e.Position, e.EndDate, e.UpdatedAt, e.ID,
	)
	return err
}

func (s *PGStore) List(ctx context.Context, f EmployeeFilter) ([]*Employee, int, error) {
	where := "tenant_id=$1 AND deleted=FALSE"
	args := []any{f.TenantID}
	i := 2
	if f.CompanyID != nil {
		where += fmt.Sprintf(" AND company_id=$%d", i); args = append(args, *f.CompanyID); i++
	}
	if f.DepartmentID != nil {
		where += fmt.Sprintf(" AND department_id=$%d", i); args = append(args, *f.DepartmentID); i++
	}
	if f.Keyword != "" {
		where += fmt.Sprintf(" AND (name ILIKE $%d OR phone ILIKE $%d)", i, i)
		args = append(args, "%"+f.Keyword+"%"); i++
	}

	var total int
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM employees WHERE "+where, args...).Scan(&total)

	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT id,legacy_id,name,phone,account,company_id,department_id,user_id,
			position,start_date,end_date,executor_ref,
			tenant_id,deleted,created_at,updated_at,migrate_status
			FROM employees WHERE %s ORDER BY name LIMIT $%d OFFSET $%d`,
			where, i, i+1),
		append(args, f.Limit, f.Offset)...,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var result []*Employee
	for rows.Next() {
		e := &Employee{}
		rows.Scan(
			&e.ID, &e.LegacyID, &e.Name, &e.Phone, &e.Account,
			&e.CompanyID, &e.DepartmentID, &e.UserID,
			&e.Position, &e.StartDate, &e.EndDate, &e.ExecutorRef,
			&e.TenantID, &e.Deleted, &e.CreatedAt, &e.UpdatedAt, &e.MigrateStatus,
		)
		result = append(result, e)
	}
	return result, total, rows.Err()
}

func (s *PGStore) SetExecutorRef(ctx context.Context, id int64, executorRef string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE employees SET executor_ref=$1, updated_at=NOW() WHERE id=$2`,
		executorRef, id)
	return err
}
