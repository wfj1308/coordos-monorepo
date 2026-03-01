package report

import (
	"context"
	"database/sql"
	"strings"
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

type QualificationReport struct {
	GeneratedAt       time.Time          `json:"generated_at"`
	ReportYear        int                `json:"report_year"`
	Company           CompanyInfo        `json:"company"`
	CompanyCerts      []CertRecord       `json:"company_certs"`
	RegisteredPersons []RegisteredPerson `json:"registered_persons"`
	ProjectRecords    []ProjectRecord    `json:"project_records"`
	Finance           FinanceSummary     `json:"finance"`
	ExpiryWarnings    []ExpiryItem       `json:"expiry_warnings"`
}

type CompanyInfo struct {
	Name            string `json:"name"`
	UnifiedCode     string `json:"unified_code"`
	LegalRep        string `json:"legal_rep"`
	TechDirector    string `json:"tech_director"`
	Address         string `json:"address"`
	Phone           string `json:"phone"`
	EstablishedYear int    `json:"established_year"`
}

type CertRecord struct {
	CertType   string `json:"cert_type"`
	CertNo     string `json:"cert_no"`
	IssuedBy   string `json:"issued_by"`
	ValidFrom  string `json:"valid_from"`
	ValidUntil string `json:"valid_until"`
	Status     string `json:"status"`
	Level      string `json:"level"`
	Specialty  string `json:"specialty"`
}

type RegisteredPerson struct {
	Name           string          `json:"name"`
	CertType       string          `json:"cert_type"`
	CertNo         string          `json:"cert_no"`
	ValidUntil     string          `json:"valid_until"`
	Specialty      string          `json:"specialty"`
	RecentProjects []PersonProject `json:"recent_projects"`
}

type PersonProject struct {
	ProjectName string `json:"project_name"`
	Role        string `json:"role"`
	Year        int    `json:"year"`
	SPURef      string `json:"spu_ref"`
}

type ProjectRecord struct {
	ProjectName    string  `json:"project_name"`
	ContractNo     string  `json:"contract_no"`
	ContractAmount float64 `json:"contract_amount"`
	OwnerName      string  `json:"owner_name"`
	CompletedYear  int     `json:"completed_year"`
	ProjectType    string  `json:"project_type"`
	ProofUTXORef   string  `json:"proof_utxo_ref"`
}

type FinanceSummary struct {
	Year1          int     `json:"year1"`
	Year1Gathering float64 `json:"year1_gathering"`
	Year2          int     `json:"year2"`
	Year2Gathering float64 `json:"year2_gathering"`
	Year3          int     `json:"year3"`
	Year3Gathering float64 `json:"year3_gathering"`
}

type ExpiryItem struct {
	HolderName string `json:"holder_name"`
	CertType   string `json:"cert_type"`
	CertNo     string `json:"cert_no"`
	ValidUntil string `json:"valid_until"`
	DaysLeft   int    `json:"days_left"`
}

type RiskEvent struct {
	EventType  string    `json:"event_type"`
	Severity   string    `json:"severity"`
	EntityRef  string    `json:"entity_ref"`
	Message    string    `json:"message"`
	OccurredAt time.Time `json:"occurred_at"`
}

type Store interface {
	GetOverview(ctx context.Context, tenantID int, from, to time.Time) (*Overview, error)
	GetCompanyReport(ctx context.Context, tenantID int, from, to time.Time) ([]*CompanyReport, error)
	GetContractAnalysis(ctx context.Context, tenantID int, contractID int64) (*ContractAnalysis, error)
	GetGatheringProgress(ctx context.Context, tenantID int, year int) ([]*GatheringProgress, error)
	GetEmployeeReport(ctx context.Context, tenantID int, from, to time.Time) ([]*EmployeeReport, error)
	GetQualificationReport(ctx context.Context, tenantID int, reportYear int) (*QualificationReport, error)
	GetRiskEvents(ctx context.Context, tenantID int, from time.Time) ([]*RiskEvent, error)
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

func (s *Service) GetQualificationReport(ctx context.Context, reportYear int) (*QualificationReport, error) {
	return s.store.GetQualificationReport(ctx, s.tenantID, reportYear)
}

func (s *Service) GetRiskEvents(ctx context.Context, days int) ([]*RiskEvent, error) {
	if days <= 0 {
		days = 30
	}
	if days > 365 {
		days = 365
	}
	from := time.Now().AddDate(0, 0, -days)
	return s.store.GetRiskEvents(ctx, s.tenantID, from)
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

func (s *PGStore) GetQualificationReport(ctx context.Context, tenantID int, reportYear int) (*QualificationReport, error) {
	now := time.Now()
	r := &QualificationReport{
		GeneratedAt: now,
		ReportYear:  reportYear,
	}

	row := s.db.QueryRowContext(ctx, `
		SELECT name, COALESCE(note,''), COALESCE(phone,'')
		FROM companies
		WHERE company_type=1 AND tenant_id=$1 AND deleted=FALSE
		LIMIT 1`,
		tenantID,
	)
	var companyName, unifiedCode, phone string
	if err := row.Scan(&companyName, &unifiedCode, &phone); err == nil {
		r.Company = CompanyInfo{
			Name:        companyName,
			UnifiedCode: unifiedCode,
			Phone:       phone,
		}
	}

	certRows, err := s.db.QueryContext(ctx, `
		SELECT qual_type, cert_no, COALESCE(issued_by,''),
		       COALESCE(TO_CHAR(valid_from,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(valid_until,'YYYY-MM-DD'),'长期'),
		       status, COALESCE(level,''), COALESCE(specialty,'')
		FROM qualifications
		WHERE holder_type='COMPANY' AND tenant_id=$1
		  AND status IN ('VALID','EXPIRE_SOON','APPLYING')
		  AND deleted=FALSE
		ORDER BY qual_type`,
		tenantID,
	)
	if err == nil {
		defer certRows.Close()
		for certRows.Next() {
			c := CertRecord{}
			if err := certRows.Scan(
				&c.CertType, &c.CertNo, &c.IssuedBy,
				&c.ValidFrom, &c.ValidUntil, &c.Status, &c.Level, &c.Specialty,
			); err == nil {
				r.CompanyCerts = append(r.CompanyCerts, c)
			}
		}
	}

	threeYearsAgo := time.Date(reportYear-3, 1, 1, 0, 0, 0, 0, time.UTC)
	personRows, err := s.db.QueryContext(ctx, `
		SELECT q.holder_name, q.qual_type, q.cert_no,
		       COALESCE(TO_CHAR(q.valid_until,'YYYY-MM-DD'),'长期'),
		       COALESCE(q.specialty,''), COALESCE(q.executor_ref,'')
		FROM qualifications q
		WHERE q.holder_type='PERSON'
		  AND q.tenant_id=$1
		  AND q.status IN ('VALID','EXPIRE_SOON')
		  AND q.deleted=FALSE
		  AND q.qual_type LIKE 'REG_%'
		ORDER BY q.qual_type, q.holder_name`,
		tenantID,
	)
	if err == nil {
		defer personRows.Close()
		for personRows.Next() {
			p := RegisteredPerson{}
			var executorRef string
			if err := personRows.Scan(&p.Name, &p.CertType, &p.CertNo, &p.ValidUntil, &p.Specialty, &executorRef); err != nil {
				continue
			}

			if executorRef != "" {
				projRows, err := s.db.QueryContext(ctx, `
					SELECT COALESCE(pn.name,'未知项目'), a.spu_ref,
					       EXTRACT(YEAR FROM a.ingested_at)::int
					FROM achievement_utxos a
					LEFT JOIN project_nodes pn ON pn.ref = a.project_ref
					WHERE a.executor_ref=$1
					  AND a.ingested_at >= $2
					  AND a.status != 'DISPUTED'
					ORDER BY a.ingested_at DESC
					LIMIT 10`,
					executorRef, threeYearsAgo,
				)
				if err == nil {
					for projRows.Next() {
						pp := PersonProject{}
						if err := projRows.Scan(&pp.ProjectName, &pp.SPURef, &pp.Year); err == nil {
							pp.Role = roleFromSPU(pp.SPURef)
							p.RecentProjects = append(p.RecentProjects, pp)
						}
					}
					projRows.Close()
				}
			}
			r.RegisteredPersons = append(r.RegisteredPersons, p)
		}
	}

	projectRows, err := s.db.QueryContext(ctx, `
		SELECT c.contract_name, c.num, c.contract_balance,
		       COALESCE(c.signing_subject,''),
		       EXTRACT(YEAR FROM COALESCE(s.updated_at, c.created_at))::int,
		       COALESCE(a.utxo_ref,'')
		FROM contracts c
		LEFT JOIN settlements s ON s.contract_id=c.id AND s.state='PAID'
		LEFT JOIN achievement_utxos a ON a.contract_id=c.id AND a.spu_ref LIKE '%settlement_cert%'
		WHERE c.tenant_id=$1
		  AND c.contract_balance >= 500000
		  AND c.created_at >= $2
		  AND c.deleted=FALSE
		ORDER BY c.contract_balance DESC
		LIMIT 30`,
		tenantID, threeYearsAgo,
	)
	if err == nil {
		defer projectRows.Close()
		for projectRows.Next() {
			pr := ProjectRecord{}
			if err := projectRows.Scan(
				&pr.ProjectName, &pr.ContractNo, &pr.ContractAmount,
				&pr.OwnerName, &pr.CompletedYear, &pr.ProofUTXORef,
			); err == nil {
				pr.ProjectType = inferProjectType(pr.ProjectName)
				r.ProjectRecords = append(r.ProjectRecords, pr)
			}
		}
	}

	for i, y := range []int{reportYear - 3, reportYear - 2, reportYear - 1} {
		var total float64
		_ = s.db.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(gathering_money),0)
			FROM gatherings
			WHERE tenant_id=$1
			  AND EXTRACT(YEAR FROM created_at)=$2`,
			tenantID, y,
		).Scan(&total)
		switch i {
		case 0:
			r.Finance.Year1, r.Finance.Year1Gathering = y, total
		case 1:
			r.Finance.Year2, r.Finance.Year2Gathering = y, total
		case 2:
			r.Finance.Year3, r.Finance.Year3Gathering = y, total
		}
	}

	deadline := now.Add(90 * 24 * time.Hour)
	warnRows, err := s.db.QueryContext(ctx, `
		SELECT holder_name, qual_type, cert_no,
		       TO_CHAR(valid_until,'YYYY-MM-DD'),
		       (valid_until::date - CURRENT_DATE)::int
		FROM qualifications
		WHERE tenant_id=$1 AND deleted=FALSE
		  AND status IN ('VALID','EXPIRE_SOON')
		  AND valid_until IS NOT NULL
		  AND valid_until <= $2
		ORDER BY valid_until`,
		tenantID, deadline,
	)
	if err == nil {
		defer warnRows.Close()
		for warnRows.Next() {
			w := ExpiryItem{}
			if err := warnRows.Scan(&w.HolderName, &w.CertType, &w.CertNo, &w.ValidUntil, &w.DaysLeft); err == nil {
				r.ExpiryWarnings = append(r.ExpiryWarnings, w)
			}
		}
	}

	return r, nil
}

func (s *PGStore) GetRiskEvents(ctx context.Context, tenantID int, from time.Time) ([]*RiskEvent, error) {
	expireDeadline := time.Now().Add(60 * 24 * time.Hour)
	rows, err := s.db.QueryContext(ctx, `
		SELECT event_type, severity, entity_ref, message, occurred_at
		FROM (
			SELECT
				'CONTRACT_OVERPAID' AS event_type,
				'HIGH' AS severity,
				COALESCE(c.num, 'contract-' || c.id::text) AS entity_ref,
				'contract settled amount exceeds contract balance' AS message,
				COALESCE(c.updated_at, c.created_at) AS occurred_at
			FROM contracts c
			WHERE c.tenant_id=$1
			  AND c.deleted=FALSE
			  AND COALESCE(c.totle_balance, 0) > COALESCE(c.contract_balance, 0)
			  AND COALESCE(c.updated_at, c.created_at) >= $2

			UNION ALL

			SELECT
				'APPROVAL_REJECTED' AS event_type,
				'MEDIUM' AS severity,
				COALESCE(f.title, f.biz_type || '-' || f.biz_id::text) AS entity_ref,
				'approval flow rejected' AS message,
				COALESCE(f.finished_at, f.updated_at, f.created_at) AS occurred_at
			FROM approve_flows f
			WHERE f.tenant_id=$1
			  AND f.state='REJECTED'
			  AND COALESCE(f.finished_at, f.updated_at, f.created_at) >= $2

			UNION ALL

			SELECT
				'QUALIFICATION_EXPIRING' AS event_type,
				'LOW' AS severity,
				COALESCE(q.cert_no, q.qual_type || '-' || q.id::text) AS entity_ref,
				'qualification expires within 60 days' AS message,
				COALESCE(q.valid_until, q.updated_at, q.created_at) AS occurred_at
			FROM qualifications q
			WHERE q.tenant_id=$1
			  AND q.deleted=FALSE
			  AND q.valid_until IS NOT NULL
			  AND q.status IN ('VALID', 'EXPIRE_SOON')
			  AND q.valid_until <= $3
			  AND COALESCE(q.updated_at, q.created_at) >= $2
		) t
		ORDER BY occurred_at DESC
		LIMIT 200`,
		tenantID, from, expireDeadline,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*RiskEvent
	for rows.Next() {
		e := &RiskEvent{}
		if err := rows.Scan(&e.EventType, &e.Severity, &e.EntityRef, &e.Message, &e.OccurredAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func roleFromSPU(spuRef string) string {
	switch {
	case strings.Contains(spuRef, "review_certificate"):
		return "REVIEWER"
	case strings.Contains(spuRef, "settlement_cert"):
		return "MANAGER"
	case strings.Contains(spuRef, "drawing"):
		return "DESIGNER"
	case strings.Contains(spuRef, "acceptance"):
		return "INSPECTOR"
	default:
		return "ENGINEER"
	}
}

func inferProjectType(name string) string {
	switch {
	case strings.Contains(name, "桥"):
		return "BRIDGE"
	case strings.Contains(name, "隧道"), strings.Contains(name, "隧"):
		return "TUNNEL"
	case strings.Contains(name, "公路"), strings.Contains(name, "高速"), strings.Contains(name, "路"):
		return "ROAD"
	default:
		return "GENERAL"
	}
}
