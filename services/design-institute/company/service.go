// ============================================================
// company/service.go - 分公司服务
// ============================================================

package company

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type CompanyType int

const (
	TypeHeadOffice CompanyType = 1 // 总公司
	TypeBranch     CompanyType = 2 // 分公司
	TypePartner    CompanyType = 3 // 合伙人
)

type Company struct {
	ID              int
	LegacyID        *int
	Name            string
	CompanyType     CompanyType
	ParentID        *int
	Code            string
	LicenseNum      string
	Charger         string
	ChargerPhone    string
	FinanceCharger  string
	BankAccount     string
	Address         string
	AreaID          *int
	ZoneID          *int64
	Note            string
	ExecutorRef     *string
	GenesisRef      *string
	Deleted         bool
	TenantID        int
	CreatedAt       time.Time
	UpdatedAt       time.Time
	MigrateStatus   string
}

type Store interface {
	Create(ctx context.Context, c *Company) error
	Get(ctx context.Context, id int) (*Company, error)
	List(ctx context.Context, tenantID int, companyType *CompanyType, limit, offset int) ([]*Company, int, error)
	GetChildren(ctx context.Context, parentID int) ([]*Company, error)
	SetExecutorRef(ctx context.Context, id int, ref string) error
	Update(ctx context.Context, c *Company) error
}

type Service struct {
	store    Store
	tenantID int
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

func (s *Service) Get(ctx context.Context, id int) (*Company, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) List(ctx context.Context, companyType *CompanyType, limit, offset int) ([]*Company, int, error) {
	if limit == 0 {
		limit = 50
	}
	return s.store.List(ctx, s.tenantID, companyType, limit, offset)
}

func (s *Service) GetBranches(ctx context.Context, parentID int) ([]*Company, error) {
	return s.store.GetChildren(ctx, parentID)
}

// 生成 executor_ref，迁移后调用
func (s *Service) BindExecutorRef(ctx context.Context, id int) error {
	ref := fmt.Sprintf("v://zhongbei/executor/branch/%d", id)
	return s.store.SetExecutorRef(ctx, id, ref)
}

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) Create(ctx context.Context, c *Company) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO companies (
			name, company_type, parent_id, code, license_num,
			charger, charger_phone, finance_charger, bank_account,
			address, area_id, zone_id, note,
			deleted, tenant_id, created_at, updated_at, migrate_status
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
		RETURNING id`,
		c.Name, c.CompanyType, c.ParentID, c.Code, c.LicenseNum,
		c.Charger, c.ChargerPhone, c.FinanceCharger, c.BankAccount,
		c.Address, c.AreaID, c.ZoneID, c.Note,
		c.Deleted, c.TenantID, c.CreatedAt, c.UpdatedAt, c.MigrateStatus,
	).Scan(&c.ID)
}

func (s *PGStore) Get(ctx context.Context, id int) (*Company, error) {
	c := &Company{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id,legacy_id,name,company_type,parent_id,code,license_num,
		       charger,charger_phone,finance_charger,bank_account,
		       address,area_id,zone_id,note,
		       executor_ref,genesis_ref,deleted,tenant_id,
		       created_at,updated_at,migrate_status
		FROM companies WHERE id=$1 AND deleted=FALSE`, id,
	).Scan(&c.ID, &c.LegacyID, &c.Name, &c.CompanyType, &c.ParentID,
		&c.Code, &c.LicenseNum, &c.Charger, &c.ChargerPhone,
		&c.FinanceCharger, &c.BankAccount, &c.Address, &c.AreaID, &c.ZoneID,
		&c.Note, &c.ExecutorRef, &c.GenesisRef, &c.Deleted, &c.TenantID,
		&c.CreatedAt, &c.UpdatedAt, &c.MigrateStatus)
	return c, err
}

func (s *PGStore) List(ctx context.Context, tenantID int, companyType *CompanyType, limit, offset int) ([]*Company, int, error) {
	where := "tenant_id=$1 AND deleted=FALSE"
	args := []any{tenantID}
	if companyType != nil {
		where += " AND company_type=$2"; args = append(args, *companyType)
	}
	var total int
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM companies WHERE "+where, args...).Scan(&total)
	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT id,legacy_id,name,company_type,parent_id,code,license_num,
			charger,charger_phone,finance_charger,bank_account,
			address,area_id,zone_id,note,
			executor_ref,genesis_ref,deleted,tenant_id,
			created_at,updated_at,migrate_status
			FROM companies WHERE %s ORDER BY name LIMIT $%d OFFSET $%d`,
			where, len(args)+1, len(args)+2),
		append(args, limit, offset)...,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var result []*Company
	for rows.Next() {
		c := &Company{}
		rows.Scan(&c.ID, &c.LegacyID, &c.Name, &c.CompanyType, &c.ParentID,
			&c.Code, &c.LicenseNum, &c.Charger, &c.ChargerPhone,
			&c.FinanceCharger, &c.BankAccount, &c.Address, &c.AreaID, &c.ZoneID,
			&c.Note, &c.ExecutorRef, &c.GenesisRef, &c.Deleted, &c.TenantID,
			&c.CreatedAt, &c.UpdatedAt, &c.MigrateStatus)
		result = append(result, c)
	}
	return result, total, rows.Err()
}

func (s *PGStore) GetChildren(ctx context.Context, parentID int) ([]*Company, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,legacy_id,name,company_type,parent_id,code,license_num,
		        charger,charger_phone,finance_charger,bank_account,
		        address,area_id,zone_id,note,
		        executor_ref,genesis_ref,deleted,tenant_id,
		        created_at,updated_at,migrate_status
		 FROM companies WHERE parent_id=$1 AND deleted=FALSE ORDER BY name`, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*Company
	for rows.Next() {
		c := &Company{}
		rows.Scan(&c.ID, &c.LegacyID, &c.Name, &c.CompanyType, &c.ParentID,
			&c.Code, &c.LicenseNum, &c.Charger, &c.ChargerPhone,
			&c.FinanceCharger, &c.BankAccount, &c.Address, &c.AreaID, &c.ZoneID,
			&c.Note, &c.ExecutorRef, &c.GenesisRef, &c.Deleted, &c.TenantID,
			&c.CreatedAt, &c.UpdatedAt, &c.MigrateStatus)
		result = append(result, c)
	}
	return result, rows.Err()
}

func (s *PGStore) SetExecutorRef(ctx context.Context, id int, ref string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE companies SET executor_ref=$1, updated_at=NOW() WHERE id=$2`, ref, id)
	return err
}

func (s *PGStore) Update(ctx context.Context, c *Company) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE companies SET name=$1, charger=$2, charger_phone=$3,
		 finance_charger=$4, note=$5, updated_at=NOW() WHERE id=$6`,
		c.Name, c.Charger, c.ChargerPhone, c.FinanceCharger, c.Note, c.ID)
	return err
}
