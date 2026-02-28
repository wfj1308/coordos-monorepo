package report

import (
	"context"
	"database/sql"
	"time"
)

type Overview struct {
	Period string

	NewContracts   int
	ContractAmount float64

	GatheringAmount float64
	GatheringCount  int

	SettledAmount float64
	SettledCount  int

	InvoicedAmount float64
	InvoicedCount  int

	CostAmount float64
	NetIncome  float64

	ActiveProjects    int
	DeliveredProjects int
}

type CompanyReport struct {
	CompanyID       int
	CompanyName     string
	CompanyType     int
	ContractCount   int
	ContractAmount  float64
	GatheringAmount float64
	SettledAmount   float64
	InvoicedAmount  float64
	CostAmount      float64
	ManageFee       float64
	NetAmount       float64
}

type EmployeeReport struct {
	EmployeeID       int64
	EmployeeName     string
	CompanyName      string
	ContractCount    int
	ContractAmount   float64
	GatheringAmount  float64
	AchievementCount int
}

type ContractAnalysis struct {
	ContractID      int64
	ContractName    string
	ContractAmount  float64
	GatheringAmount float64
	SettledAmount   float64
	InvoicedAmount  float64
	CostAmount      float64
	ManageFee       float64
	GatheringRate   float64
	SettleRate      float64
	InvoiceRate     float64
	Depth           int
	ChildCount      int
}

type GatheringProgress struct {
	Month            string
	Amount           float64
	Count            int
	CumulativeAmount float64
}

type Store interface {
	GetOverview(ctx context.Context, tenantID int, from, to time.Time) (*Overview, error)
	GetCompanyReport(ctx context.Context, tenantID int, from, to time.Time) ([]*CompanyReport, error)
	GetContractAnalysis(ctx context.Context, tenantID int, contractID int64) (*ContractAnalysis, error)
	GetGatheringProgress(ctx context.Context, tenantID int, year int) ([]*GatheringProgress, error)
	GetEmployeeReport(ctx context.Context, tenantID int, from, to time.Time) ([]*EmployeeReport, error)
}

type Service struct {
	store    Store
	tenantID int
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

type PGStore struct {
	db *sql.DB
}

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *Service) GetOverview(ctx context.Context, from, to time.Time) (*Overview, error) {
	return s.store.GetOverview(ctx, s.tenantID, from, to)
}

func (s *Service) GetCompanyReport(ctx context.Context, from, to time.Time) ([]*CompanyReport, error) {
	return s.store.GetCompanyReport(ctx, s.tenantID, from, to)
}

func (s *Service) GetContractAnalysis(ctx context.Context, contractID int64) (*ContractAnalysis, error) {
	return s.store.GetContractAnalysis(ctx, s.tenantID, contractID)
}

func (s *Service) GetGatheringProgress(ctx context.Context, year int) ([]*GatheringProgress, error) {
	return s.store.GetGatheringProgress(ctx, s.tenantID, year)
}

func (s *Service) GetEmployeeReport(ctx context.Context, from, to time.Time) ([]*EmployeeReport, error) {
	return s.store.GetEmployeeReport(ctx, s.tenantID, from, to)
}

func (s *PGStore) GetOverview(ctx context.Context, tenantID int, from, to time.Time) (*Overview, error) {
	ov := &Overview{
		Period: from.Format("2006-01") + " ~ " + to.Format("2006-01"),
	}

	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(SUM(contract_balance),0)
		FROM contracts
		WHERE tenant_id=$1 AND created_at>=$2 AND created_at<$3 AND deleted=FALSE`,
		tenantID, from, to,
	).Scan(&ov.NewContracts, &ov.ContractAmount); err != nil {
		return nil, err
	}

	if err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(gathering_money),0), COUNT(*)
		FROM gatherings
		WHERE tenant_id=$1 AND created_at>=$2 AND created_at<$3`,
		tenantID, from, to,
	).Scan(&ov.GatheringAmount, &ov.GatheringCount); err != nil {
		return nil, err
	}

	if err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(bank_settlement+cash_settlement),0), COUNT(*)
		FROM balances
		WHERE tenant_id=$1 AND state='PAID' AND created_at>=$2 AND created_at<$3`,
		tenantID, from, to,
	).Scan(&ov.SettledAmount, &ov.SettledCount); err != nil {
		return nil, err
	}

	if err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(cur_amount),0), COUNT(*)
		FROM invoices
		WHERE tenant_id=$1 AND invoice_state='ISSUED'
		  AND created_at>=$2 AND created_at<$3`,
		tenantID, from, to,
	).Scan(&ov.InvoicedAmount, &ov.InvoicedCount); err != nil {
		return nil, err
	}

	if err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(cost_ticket_sum),0)
		FROM costtickets
		WHERE tenant_id=$1 AND state='PAID'
		  AND created_at>=$2 AND created_at<$3`,
		tenantID, from, to,
	).Scan(&ov.CostAmount); err != nil {
		return nil, err
	}
	ov.NetIncome = ov.GatheringAmount - ov.CostAmount

	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM project_nodes
		WHERE tenant_id=$1 AND status='IN_PROGRESS'`,
		tenantID,
	).Scan(&ov.ActiveProjects); err != nil {
		return nil, err
	}

	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM project_nodes
		WHERE tenant_id=$1 AND status='DELIVERED'
		  AND updated_at>=$2 AND updated_at<$3`,
		tenantID, from, to,
	).Scan(&ov.DeliveredProjects); err != nil {
		return nil, err
	}

	return ov, nil
}

func (s *PGStore) GetCompanyReport(ctx context.Context, tenantID int, from, to time.Time) ([]*CompanyReport, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			c.id, c.name, c.company_type,
			COUNT(DISTINCT ct.id) AS contract_count,
			COALESCE(SUM(ct.contract_balance),0) AS contract_amount,
			COALESCE(SUM(g.gathering_money),0) AS gathering_amount,
			COALESCE(SUM(b.bank_settlement+b.cash_settlement),0) AS settled_amount,
			COALESCE(SUM(inv.cur_amount),0) AS invoiced_amount,
			COALESCE(SUM(ck.cost_ticket_sum),0) AS cost_amount,
			COALESCE(SUM(ct.manage_ratio * ct.totle_gathering / 100),0) AS manage_fee
		FROM companies c
		LEFT JOIN contracts ct ON ct.company_id=c.id
			AND ct.created_at>=$2 AND ct.created_at<$3 AND ct.deleted=FALSE
		LEFT JOIN gatherings g ON g.company_id=c.id
			AND g.created_at>=$2 AND g.created_at<$3
		LEFT JOIN balances b ON b.gathering_id IN (
			SELECT id FROM gatherings WHERE company_id=c.id)
			AND b.state='PAID'
		LEFT JOIN invoices inv ON inv.contract_id=ct.id
			AND inv.invoice_state='ISSUED'
		LEFT JOIN costtickets ck ON ck.contract_id=ct.id
			AND ck.state='PAID'
		WHERE c.tenant_id=$1 AND c.deleted=FALSE
		GROUP BY c.id, c.name, c.company_type
		ORDER BY gathering_amount DESC`,
		tenantID, from, to,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*CompanyReport
	for rows.Next() {
		r := &CompanyReport{}
		if err := rows.Scan(
			&r.CompanyID, &r.CompanyName, &r.CompanyType,
			&r.ContractCount, &r.ContractAmount,
			&r.GatheringAmount, &r.SettledAmount,
			&r.InvoicedAmount, &r.CostAmount, &r.ManageFee,
		); err != nil {
			return nil, err
		}
		r.NetAmount = r.GatheringAmount - r.ManageFee - r.CostAmount
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PGStore) GetContractAnalysis(ctx context.Context, tenantID int, contractID int64) (*ContractAnalysis, error) {
	ca := &ContractAnalysis{}
	err := s.db.QueryRowContext(ctx, `
		SELECT
			ct.id, ct.contract_name, COALESCE(ct.contract_balance,0),
			COALESCE(ct.totle_gathering,0),
			COALESCE(ct.totle_balance,0),
			COALESCE(ct.totle_invoice,0),
			COALESCE(ck_sum.cost_amount,0),
			COALESCE(ct.manage_ratio,0),
			COALESCE(pn.depth,0),
			COALESCE(child_count.cnt,0)
		FROM contracts ct
		LEFT JOIN (
			SELECT contract_id, SUM(cost_ticket_sum) AS cost_amount
			FROM costtickets WHERE state='PAID' GROUP BY contract_id
		) ck_sum ON ck_sum.contract_id=ct.id
		LEFT JOIN project_nodes pn ON pn.legacy_contract_id=ct.id
		LEFT JOIN (
			SELECT parent_id, COUNT(*) AS cnt
			FROM contracts WHERE deleted=FALSE GROUP BY parent_id
		) child_count ON child_count.parent_id=ct.id
		WHERE ct.id=$1 AND ct.tenant_id=$2`,
		contractID, tenantID,
	).Scan(
		&ca.ContractID, &ca.ContractName, &ca.ContractAmount,
		&ca.GatheringAmount, &ca.SettledAmount, &ca.InvoicedAmount,
		&ca.CostAmount, &ca.ManageFee, &ca.Depth, &ca.ChildCount,
	)
	if err != nil {
		return nil, err
	}

	if ca.ContractAmount > 0 {
		ca.GatheringRate = ca.GatheringAmount / ca.ContractAmount * 100
		ca.SettleRate = ca.SettledAmount / ca.ContractAmount * 100
		ca.InvoiceRate = ca.InvoicedAmount / ca.ContractAmount * 100
	}
	return ca, nil
}

func (s *PGStore) GetGatheringProgress(ctx context.Context, tenantID int, year int) ([]*GatheringProgress, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			TO_CHAR(created_at, 'YYYY-MM') AS month,
			COALESCE(SUM(gathering_money),0) AS amount,
			COUNT(*) AS cnt
		FROM gatherings
		WHERE tenant_id=$1
		  AND EXTRACT(YEAR FROM created_at) = $2
		GROUP BY month ORDER BY month`,
		tenantID, year,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*GatheringProgress
	var cumulative float64
	for rows.Next() {
		gp := &GatheringProgress{}
		if err := rows.Scan(&gp.Month, &gp.Amount, &gp.Count); err != nil {
			return nil, err
		}
		cumulative += gp.Amount
		gp.CumulativeAmount = cumulative
		out = append(out, gp)
	}
	return out, rows.Err()
}

func (s *PGStore) GetEmployeeReport(ctx context.Context, tenantID int, from, to time.Time) ([]*EmployeeReport, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			e.id, e.name,
			COALESCE(co.name,''),
			COUNT(DISTINCT ct.id) AS contract_count,
			COALESCE(SUM(ct.contract_balance),0) AS contract_amount,
			COALESCE(SUM(g.gathering_money),0) AS gathering_amount,
			COUNT(DISTINCT au.id) AS achievement_count
		FROM employees e
		LEFT JOIN companies co ON co.id=e.company_id
		LEFT JOIN contracts ct ON ct.employee_id=e.id
			AND ct.created_at>=$2 AND ct.created_at<$3 AND ct.deleted=FALSE
		LEFT JOIN gatherings g ON g.employee_id=e.id
			AND g.created_at>=$2 AND g.created_at<$3
		LEFT JOIN achievement_utxos au ON au.executor_ref = e.executor_ref
			AND au.ingested_at>=$2 AND au.ingested_at<$3
		WHERE e.tenant_id=$1 AND e.deleted=FALSE
		GROUP BY e.id, e.name, co.name
		ORDER BY gathering_amount DESC`,
		tenantID, from, to,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*EmployeeReport
	for rows.Next() {
		r := &EmployeeReport{}
		if err := rows.Scan(
			&r.EmployeeID, &r.EmployeeName, &r.CompanyName,
			&r.ContractCount, &r.ContractAmount,
			&r.GatheringAmount, &r.AchievementCount,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
