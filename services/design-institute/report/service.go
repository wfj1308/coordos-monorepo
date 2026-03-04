package report

import (
	"context"
	"database/sql"
	"fmt"
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

type ThreeLibrariesQuery struct {
	QualificationLimit  int
	QualificationOffset int
	StandardLimit       int
	StandardOffset      int
	RegulationLimit     int
	RegulationOffset    int
}

type QualificationLibraryItem struct {
	ID          int64     `json:"id"`
	QualType    string    `json:"qual_type"`
	HolderName  string    `json:"holder_name"`
	ExecutorRef string    `json:"executor_ref"`
	Status      string    `json:"status"`
	ValidUntil  string    `json:"valid_until"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type QualificationLibraryPage struct {
	Total  int                         `json:"total"`
	Limit  int                         `json:"limit"`
	Offset int                         `json:"offset"`
	Items  []*QualificationLibraryItem `json:"items"`
}

type EngineeringStandardItem struct {
	ID              int64     `json:"id"`
	DrawingNo       string    `json:"drawing_no"`
	Major           string    `json:"major"`
	Status          string    `json:"status"`
	ProjectRef      string    `json:"project_ref"`
	ContractID      int64     `json:"contract_id"`
	AttachmentCount int       `json:"attachment_count"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type EngineeringStandardPage struct {
	Total  int                        `json:"total"`
	Limit  int                        `json:"limit"`
	Offset int                        `json:"offset"`
	Items  []*EngineeringStandardItem `json:"items"`
}

type RegulationLibraryItem struct {
	ID        int64     `json:"id"`
	DocNo     string    `json:"doc_no"`
	Title     string    `json:"title"`
	Category  string    `json:"category"`
	Publisher string    `json:"publisher"`
	Status    string    `json:"status"`
	UpdatedAt time.Time `json:"updated_at"`
}

type RegulationLibraryPage struct {
	Total  int                      `json:"total"`
	Limit  int                      `json:"limit"`
	Offset int                      `json:"offset"`
	Items  []*RegulationLibraryItem `json:"items"`
}

type ThreeLibrariesOverview struct {
	Qualifications      QualificationLibraryPage `json:"qualifications"`
	EngineeringStandard EngineeringStandardPage  `json:"engineering_standards"`
	Regulations         RegulationLibraryPage    `json:"regulations"`
	UpdatedAt           time.Time                `json:"updated_at"`
}

type QualificationAssignmentBrief struct {
	ID              int64      `json:"id"`
	QualificationID int64      `json:"qualification_id"`
	ProjectRef      string     `json:"project_ref"`
	Status          string     `json:"status"`
	ReleasedAt      *time.Time `json:"released_at,omitempty"`
}

type QualificationLibraryDetail struct {
	Item          QualificationLibraryItem        `json:"item"`
	HolderType    string                          `json:"holder_type"`
	HolderID      int64                           `json:"holder_id"`
	CertNo        string                          `json:"cert_no"`
	IssuedBy      string                          `json:"issued_by"`
	IssuedAt      *time.Time                      `json:"issued_at,omitempty"`
	ValidFrom     *time.Time                      `json:"valid_from,omitempty"`
	ValidUntilAt  *time.Time                      `json:"valid_until_at,omitempty"`
	Specialty     string                          `json:"specialty"`
	Level         string                          `json:"level"`
	Scope         string                          `json:"scope"`
	AttachmentURL string                          `json:"attachment_url"`
	Note          string                          `json:"note"`
	Assignments   []*QualificationAssignmentBrief `json:"assignments"`
}

type DrawingAttachmentDetail struct {
	ID          int64      `json:"id"`
	SourceTable string     `json:"source_table"`
	Name        string     `json:"name"`
	URL         string     `json:"url"`
	Version     string     `json:"version"`
	State       int        `json:"state"`
	ApproveDate *time.Time `json:"approve_date,omitempty"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

type EngineeringStandardDetail struct {
	Item        EngineeringStandardItem    `json:"item"`
	Reviewer    string                     `json:"reviewer"`
	Remarks     string                     `json:"remarks"`
	CreatedAt   time.Time                  `json:"created_at"`
	Attachments []*DrawingAttachmentDetail `json:"attachments"`
}

type RegulationVersionDetail struct {
	ID            int64      `json:"id"`
	VersionNo     int        `json:"version_no"`
	EffectiveFrom *time.Time `json:"effective_from,omitempty"`
	EffectiveTo   *time.Time `json:"effective_to,omitempty"`
	PublishedAt   *time.Time `json:"published_at,omitempty"`
	ContentHash   string     `json:"content_hash"`
	AttachmentURL string     `json:"attachment_url"`
	SourceNote    string     `json:"source_note"`
}

type RegulationLibraryDetail struct {
	Item         RegulationLibraryItem      `json:"item"`
	DocType      string                     `json:"doc_type"`
	Jurisdiction string                     `json:"jurisdiction"`
	Keywords     string                     `json:"keywords"`
	Summary      string                     `json:"summary"`
	SourceURL    string                     `json:"source_url"`
	Ref          string                     `json:"ref"`
	Versions     []*RegulationVersionDetail `json:"versions"`
}

type VaultQualification struct {
	ID         int64     `json:"id"`
	QualType   string    `json:"qual_type"`
	HolderName string    `json:"holder_name"`
	CertNo     string    `json:"cert_no"`
	Status     string    `json:"status"`
	ValidUntil string    `json:"valid_until"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type ExecutorCredentialVault struct {
	ExecutorRef        string                          `json:"executor_ref"`
	Qualifications     []*VaultQualification           `json:"qualifications"`
	Assignments        []*QualificationAssignmentBrief `json:"assignments"`
	Drawings           []*EngineeringStandardItem      `json:"drawings"`
	QualificationCount int                             `json:"qualification_count"`
	AssignmentCount    int                             `json:"assignment_count"`
	DrawingCount       int                             `json:"drawing_count"`
	UpdatedAt          time.Time                       `json:"updated_at"`
}

type LibrarySearchQuery struct {
	Keyword     string
	Type        string
	Status      string
	UpdatedFrom *time.Time
	UpdatedTo   *time.Time
	HasExecutor *bool
	Limit       int
	Offset      int
}

type LibrarySearchItem struct {
	Type        string    `json:"type"`
	ID          int64     `json:"id"`
	Title       string    `json:"title"`
	Subtitle    string    `json:"subtitle"`
	Status      string    `json:"status"`
	UpdatedAt   time.Time `json:"updated_at"`
	ExecutorRef string    `json:"executor_ref"`
	ProjectRef  string    `json:"project_ref"`
	HasExecutor bool      `json:"has_executor"`
}

type LibrarySearchResult struct {
	Total     int                  `json:"total"`
	Limit     int                  `json:"limit"`
	Offset    int                  `json:"offset"`
	Items     []*LibrarySearchItem `json:"items"`
	UpdatedAt time.Time            `json:"updated_at"`
}

type LibraryRelationsQuery struct {
	Limit int
}

type LibraryRelationNode struct {
	NodeRef     string `json:"node_ref"`
	NodeType    string `json:"node_type"`
	LibraryType string `json:"library_type,omitempty"`
	ID          int64  `json:"id,omitempty"`
	Ref         string `json:"ref,omitempty"`
	Label       string `json:"label"`
	Status      string `json:"status,omitempty"`
}

type LibraryRelationEdge struct {
	From     string `json:"from"`
	To       string `json:"to"`
	Relation string `json:"relation"`
}

type LibraryRelationGraph struct {
	RootType  string                 `json:"root_type"`
	RootID    int64                  `json:"root_id"`
	Nodes     []*LibraryRelationNode `json:"nodes"`
	Edges     []*LibraryRelationEdge `json:"edges"`
	UpdatedAt time.Time              `json:"updated_at"`
}

type Store interface {
	GetOverview(ctx context.Context, tenantID int, from, to time.Time) (*Overview, error)
	GetCompanyReport(ctx context.Context, tenantID int, from, to time.Time) ([]*CompanyReport, error)
	GetContractAnalysis(ctx context.Context, tenantID int, contractID int64) (*ContractAnalysis, error)
	GetGatheringProgress(ctx context.Context, tenantID int, year int) ([]*GatheringProgress, error)
	GetEmployeeReport(ctx context.Context, tenantID int, from, to time.Time) ([]*EmployeeReport, error)
	GetQualificationReport(ctx context.Context, tenantID int, reportYear int) (*QualificationReport, error)
	GetRiskEvents(ctx context.Context, tenantID int, from time.Time) ([]*RiskEvent, error)
	GetThreeLibrariesOverview(ctx context.Context, tenantID int, query ThreeLibrariesQuery) (*ThreeLibrariesOverview, error)
	GetLibraryDetail(ctx context.Context, tenantID int, libraryType string, id int64) (any, error)
	GetExecutorCredentialVault(ctx context.Context, tenantID int, executorRef string, drawingLimit int) (*ExecutorCredentialVault, error)
	SearchLibraries(ctx context.Context, tenantID int, query LibrarySearchQuery) (*LibrarySearchResult, error)
	GetLibraryRelations(ctx context.Context, tenantID int, libraryType string, id int64, query LibraryRelationsQuery) (*LibraryRelationGraph, error)
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

func (s *Service) GetThreeLibrariesOverview(ctx context.Context, query ThreeLibrariesQuery) (*ThreeLibrariesOverview, error) {
	return s.store.GetThreeLibrariesOverview(ctx, s.tenantID, query)
}

func (s *Service) GetLibraryDetail(ctx context.Context, libraryType string, id int64) (any, error) {
	return s.store.GetLibraryDetail(ctx, s.tenantID, libraryType, id)
}

func (s *Service) GetExecutorCredentialVault(ctx context.Context, executorRef string, drawingLimit int) (*ExecutorCredentialVault, error) {
	return s.store.GetExecutorCredentialVault(ctx, s.tenantID, executorRef, drawingLimit)
}

func (s *Service) SearchLibraries(ctx context.Context, query LibrarySearchQuery) (*LibrarySearchResult, error) {
	return s.store.SearchLibraries(ctx, s.tenantID, query)
}

func (s *Service) GetLibraryRelations(ctx context.Context, libraryType string, id int64, query LibraryRelationsQuery) (*LibraryRelationGraph, error) {
	return s.store.GetLibraryRelations(ctx, s.tenantID, libraryType, id, query)
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

func (s *PGStore) GetThreeLibrariesOverview(ctx context.Context, tenantID int, query ThreeLibrariesQuery) (*ThreeLibrariesOverview, error) {
	query = normalizeThreeLibrariesQuery(query)
	out := &ThreeLibrariesOverview{
		Qualifications: QualificationLibraryPage{
			Limit:  query.QualificationLimit,
			Offset: query.QualificationOffset,
			Items:  make([]*QualificationLibraryItem, 0),
		},
		EngineeringStandard: EngineeringStandardPage{
			Limit:  query.StandardLimit,
			Offset: query.StandardOffset,
			Items:  make([]*EngineeringStandardItem, 0),
		},
		Regulations: RegulationLibraryPage{
			Limit:  query.RegulationLimit,
			Offset: query.RegulationOffset,
			Items:  make([]*RegulationLibraryItem, 0),
		},
		UpdatedAt: time.Now().UTC(),
	}

	quals, qualTotal, err := s.listQualificationLibrary(ctx, tenantID, query.QualificationLimit, query.QualificationOffset)
	if err != nil {
		if !isMissingRelationErr(err, "qualifications") {
			return nil, err
		}
	} else {
		out.Qualifications.Items = quals
		out.Qualifications.Total = qualTotal
	}

	standards, standardTotal, err := s.listEngineeringStandards(ctx, tenantID, query.StandardLimit, query.StandardOffset)
	if err != nil {
		if !isMissingRelationErr(err, "drawings", "drawing_attachments") {
			return nil, err
		}
	} else {
		out.EngineeringStandard.Items = standards
		out.EngineeringStandard.Total = standardTotal
	}

	regulations, regulationTotal, err := s.listRegulations(ctx, tenantID, query.RegulationLimit, query.RegulationOffset)
	if err != nil {
		if !isMissingRelationErr(err, "regulation_documents") {
			return nil, err
		}
	} else {
		out.Regulations.Items = regulations
		out.Regulations.Total = regulationTotal
	}

	return out, nil
}

func (s *PGStore) SearchLibraries(ctx context.Context, tenantID int, query LibrarySearchQuery) (*LibrarySearchResult, error) {
	query = normalizeLibrarySearchQuery(query)

	typeFilter := normalizeLibraryType(query.Type)
	switch typeFilter {
	case "", "qualification", "standard", "regulation":
	default:
		return nil, fmt.Errorf("unsupported library type: %s", query.Type)
	}

	parts := make([]string, 0, 3)

	hasQualifications, err := s.hasColumn(ctx, "qualifications", "id")
	if err != nil {
		return nil, err
	}
	hasQualificationAssignments, err := s.hasColumn(ctx, "qualification_assignments", "id")
	if err != nil {
		return nil, err
	}
	if hasQualifications {
		assignJoin := "LEFT JOIN LATERAL (SELECT ''::text AS project_ref) qa ON TRUE"
		if hasQualificationAssignments {
			assignJoin = `LEFT JOIN LATERAL (
				SELECT COALESCE(a.project_ref, '') AS project_ref
				FROM qualification_assignments a
				WHERE a.tenant_id = q.tenant_id
				  AND a.qualification_id = q.id
				ORDER BY a.created_at DESC, a.id DESC
				LIMIT 1
			) qa ON TRUE`
		}
		parts = append(parts, `
			SELECT 'qualification'::text AS type,
			       q.id,
			       COALESCE(q.qual_type, '') AS title,
			       COALESCE(NULLIF(q.holder_name, ''), NULLIF(q.cert_no, ''), '') AS subtitle,
			       COALESCE(q.status, '') AS status,
			       q.updated_at,
			       COALESCE(q.executor_ref, '') AS executor_ref,
			       COALESCE(qa.project_ref, '') AS project_ref,
			       (COALESCE(q.executor_ref, '') <> '') AS has_executor
			FROM qualifications q
			`+assignJoin+`
			WHERE q.tenant_id = $1
			  AND COALESCE(q.deleted, FALSE) = FALSE`)
	}

	hasDrawings, err := s.hasColumn(ctx, "drawings", "id")
	if err != nil {
		return nil, err
	}
	if hasDrawings {
		drawingNoExpr, statusExpr, exprErr := s.buildDrawingDisplayExpressions(ctx)
		if exprErr != nil {
			return nil, exprErr
		}
		hasDrawingsDeleted, deletedErr := s.hasColumn(ctx, "drawings", "deleted")
		if deletedErr != nil {
			return nil, deletedErr
		}
		drawWhere := "d.tenant_id = $1"
		if hasDrawingsDeleted {
			drawWhere += " AND d.deleted = FALSE"
		}

		executorJoin := "LEFT JOIN LATERAL (SELECT ''::text AS executor_ref) ex ON TRUE"
		if hasQualifications && hasQualificationAssignments {
			executorJoin = `LEFT JOIN LATERAL (
				SELECT COALESCE(q.executor_ref, '') AS executor_ref
				FROM qualification_assignments a
				JOIN qualifications q
				  ON q.id = a.qualification_id
				 AND q.tenant_id = a.tenant_id
				 AND COALESCE(q.deleted, FALSE) = FALSE
				WHERE a.tenant_id = d.tenant_id
				  AND a.project_ref = d.project_ref
				  AND COALESCE(q.executor_ref, '') <> ''
				ORDER BY a.created_at DESC, a.id DESC
				LIMIT 1
			) ex ON TRUE`
		}

		parts = append(parts, `
			SELECT 'standard'::text AS type,
			       d.id,
			       `+drawingNoExpr+` AS title,
			       COALESCE(d.major, '') AS subtitle,
			       `+statusExpr+` AS status,
			       d.updated_at,
			       COALESCE(ex.executor_ref, '') AS executor_ref,
			       COALESCE(d.project_ref, '') AS project_ref,
			       (COALESCE(ex.executor_ref, '') <> '') AS has_executor
			FROM drawings d
			`+executorJoin+`
			WHERE `+drawWhere)
	}

	hasRegulations, err := s.hasColumn(ctx, "regulation_documents", "id")
	if err != nil {
		return nil, err
	}
	if hasRegulations {
		parts = append(parts, `
			SELECT 'regulation'::text AS type,
			       r.id,
			       COALESCE(r.title, '') AS title,
			       COALESCE(r.doc_no, '') AS subtitle,
			       COALESCE(r.status, '') AS status,
			       r.updated_at,
			       ''::text AS executor_ref,
			       ''::text AS project_ref,
			       FALSE AS has_executor
			FROM regulation_documents r
			WHERE r.tenant_id = $1
			  AND COALESCE(r.deleted, FALSE) = FALSE`)
	}

	out := &LibrarySearchResult{
		Limit:     query.Limit,
		Offset:    query.Offset,
		Items:     make([]*LibrarySearchItem, 0),
		UpdatedAt: time.Now().UTC(),
	}
	if len(parts) == 0 {
		return out, nil
	}

	base := `SELECT type, id, title, subtitle, status, updated_at, executor_ref, project_ref, has_executor
	         FROM (` + strings.Join(parts, "\nUNION ALL\n") + `) x`

	where := make([]string, 0, 8)
	args := make([]any, 0, 10)
	args = append(args, tenantID)

	if typeFilter != "" {
		args = append(args, typeFilter)
		where = append(where, fmt.Sprintf("x.type = $%d", len(args)))
	}
	keyword := strings.TrimSpace(query.Keyword)
	if keyword != "" {
		args = append(args, "%"+keyword+"%")
		idx := len(args)
		where = append(where, fmt.Sprintf("(x.title ILIKE $%d OR x.subtitle ILIKE $%d OR x.executor_ref ILIKE $%d OR x.project_ref ILIKE $%d)", idx, idx, idx, idx))
	}
	status := strings.TrimSpace(query.Status)
	if status != "" {
		args = append(args, strings.ToUpper(status))
		where = append(where, fmt.Sprintf("UPPER(x.status) = $%d", len(args)))
	}
	if query.UpdatedFrom != nil {
		args = append(args, *query.UpdatedFrom)
		where = append(where, fmt.Sprintf("x.updated_at >= $%d", len(args)))
	}
	if query.UpdatedTo != nil {
		args = append(args, *query.UpdatedTo)
		where = append(where, fmt.Sprintf("x.updated_at <= $%d", len(args)))
	}
	if query.HasExecutor != nil {
		args = append(args, *query.HasExecutor)
		where = append(where, fmt.Sprintf("x.has_executor = $%d", len(args)))
	}

	filterSQL := base
	if len(where) > 0 {
		filterSQL += "\nWHERE " + strings.Join(where, " AND ")
	}

	countSQL := `SELECT COUNT(*) FROM (` + filterSQL + `) c`
	if err := s.db.QueryRowContext(ctx, countSQL, args...).Scan(&out.Total); err != nil {
		return nil, err
	}

	listArgs := append([]any{}, args...)
	listArgs = append(listArgs, query.Limit, query.Offset)
	limitArg := len(args) + 1
	offsetArg := len(args) + 2
	listSQL := filterSQL + fmt.Sprintf("\nORDER BY x.updated_at DESC, x.id DESC LIMIT $%d OFFSET $%d", limitArg, offsetArg)

	rows, err := s.db.QueryContext(ctx, listSQL, listArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		item := &LibrarySearchItem{}
		if err := rows.Scan(
			&item.Type,
			&item.ID,
			&item.Title,
			&item.Subtitle,
			&item.Status,
			&item.UpdatedAt,
			&item.ExecutorRef,
			&item.ProjectRef,
			&item.HasExecutor,
		); err != nil {
			return nil, err
		}
		out.Items = append(out.Items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PGStore) GetLibraryRelations(ctx context.Context, tenantID int, libraryType string, id int64, query LibraryRelationsQuery) (*LibraryRelationGraph, error) {
	query = normalizeLibraryRelationsQuery(query)
	switch normalizeLibraryType(libraryType) {
	case "qualification":
		return s.getQualificationRelations(ctx, tenantID, id, query)
	case "standard":
		return s.getEngineeringStandardRelations(ctx, tenantID, id, query)
	case "regulation":
		return s.getRegulationRelations(ctx, tenantID, id, query)
	default:
		return nil, fmt.Errorf("unsupported library type: %s", libraryType)
	}
}

func (s *PGStore) getQualificationRelations(ctx context.Context, tenantID int, id int64, query LibraryRelationsQuery) (*LibraryRelationGraph, error) {
	var qualType, holderName, executorRef, status string
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(qual_type, ''), COALESCE(holder_name, ''), COALESCE(executor_ref, ''), COALESCE(status, '')
		FROM qualifications
		WHERE tenant_id=$1 AND id=$2 AND COALESCE(deleted, FALSE)=FALSE
	`, tenantID, id).Scan(&qualType, &holderName, &executorRef, &status)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	builder := newRelationGraphBuilder()
	rootRef := fmt.Sprintf("qualification:%d", id)
	rootLabel := strings.TrimSpace(qualType)
	if rootLabel == "" {
		rootLabel = fmt.Sprintf("qualification-%d", id)
	}
	if holder := strings.TrimSpace(holderName); holder != "" {
		rootLabel += " / " + holder
	}
	builder.addNode(&LibraryRelationNode{
		NodeRef:     rootRef,
		NodeType:    "qualification",
		LibraryType: "qualification",
		ID:          id,
		Label:       rootLabel,
		Status:      status,
	})

	if exec := strings.TrimSpace(executorRef); exec != "" {
		execNodeRef := "executor:" + exec
		builder.addNode(&LibraryRelationNode{
			NodeRef:  execNodeRef,
			NodeType: "executor",
			Ref:      exec,
			Label:    s.executorLabelByRef(ctx, tenantID, exec),
		})
		builder.addEdge(rootRef, execNodeRef, "held_by")
	}

	hasAssignments, err := s.hasColumn(ctx, "qualification_assignments", "id")
	if err != nil {
		return nil, err
	}
	projectRefs := make([]string, 0)
	if hasAssignments {
		rows, qErr := s.db.QueryContext(ctx, `
			SELECT COALESCE(project_ref, '')
			FROM qualification_assignments
			WHERE tenant_id=$1 AND qualification_id=$2
			ORDER BY created_at DESC, id DESC
			LIMIT $3
		`, tenantID, id, query.Limit)
		if qErr != nil {
			return nil, qErr
		}
		defer rows.Close()

		seenProject := make(map[string]struct{})
		for rows.Next() {
			var projectRef string
			if scanErr := rows.Scan(&projectRef); scanErr != nil {
				return nil, scanErr
			}
			projectRef = strings.TrimSpace(projectRef)
			if projectRef == "" {
				continue
			}
			if _, ok := seenProject[projectRef]; ok {
				continue
			}
			seenProject[projectRef] = struct{}{}
			projectRefs = append(projectRefs, projectRef)
			projectNodeRef := "project:" + projectRef
			builder.addNode(&LibraryRelationNode{
				NodeRef:  projectNodeRef,
				NodeType: "project",
				Ref:      projectRef,
				Label:    s.projectLabelByRef(ctx, tenantID, projectRef),
			})
			builder.addEdge(rootRef, projectNodeRef, "assigned_to_project")
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return nil, rowsErr
		}
	}

	if err := s.appendProjectDrawingRelations(ctx, tenantID, builder, projectRefs, query.Limit*2); err != nil {
		return nil, err
	}
	return builder.build("qualification", id), nil
}

func (s *PGStore) getEngineeringStandardRelations(ctx context.Context, tenantID int, id int64, query LibraryRelationsQuery) (*LibraryRelationGraph, error) {
	hasDeleted, err := s.hasColumn(ctx, "drawings", "deleted")
	if err != nil {
		return nil, err
	}
	drawingNoExpr, statusExpr, err := s.buildDrawingDisplayExpressions(ctx)
	if err != nil {
		return nil, err
	}
	whereClause := "d.tenant_id=$1 AND d.id=$2"
	if hasDeleted {
		whereClause += " AND d.deleted=FALSE"
	}

	var drawingNo, major, status, projectRef string
	err = s.db.QueryRowContext(ctx, `
		SELECT `+drawingNoExpr+`, COALESCE(d.major, ''), `+statusExpr+`, COALESCE(d.project_ref, '')
		FROM drawings d
		WHERE `+whereClause, tenantID, id).Scan(&drawingNo, &major, &status, &projectRef)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	builder := newRelationGraphBuilder()
	rootRef := fmt.Sprintf("standard:%d", id)
	rootLabel := strings.TrimSpace(drawingNo)
	if rootLabel == "" {
		rootLabel = fmt.Sprintf("standard-%d", id)
	}
	if m := strings.TrimSpace(major); m != "" {
		rootLabel += " / " + m
	}
	builder.addNode(&LibraryRelationNode{
		NodeRef:     rootRef,
		NodeType:    "standard",
		LibraryType: "standard",
		ID:          id,
		Label:       rootLabel,
		Status:      status,
		Ref:         projectRef,
	})

	projectRef = strings.TrimSpace(projectRef)
	if projectRef != "" {
		projectNodeRef := "project:" + projectRef
		builder.addNode(&LibraryRelationNode{
			NodeRef:  projectNodeRef,
			NodeType: "project",
			Ref:      projectRef,
			Label:    s.projectLabelByRef(ctx, tenantID, projectRef),
		})
		builder.addEdge(projectNodeRef, rootRef, "contains_standard")

		hasAssignments, err := s.hasColumn(ctx, "qualification_assignments", "id")
		if err != nil {
			return nil, err
		}
		hasQualifications, err := s.hasColumn(ctx, "qualifications", "id")
		if err != nil {
			return nil, err
		}
		if hasAssignments && hasQualifications {
			rows, qErr := s.db.QueryContext(ctx, `
				SELECT q.id,
				       COALESCE(q.qual_type, ''),
				       COALESCE(q.holder_name, ''),
				       COALESCE(q.executor_ref, ''),
				       COALESCE(q.status, '')
				FROM qualification_assignments a
				JOIN qualifications q
				  ON q.id = a.qualification_id
				 AND q.tenant_id = a.tenant_id
				 AND COALESCE(q.deleted, FALSE)=FALSE
				WHERE a.tenant_id=$1 AND a.project_ref=$2
				ORDER BY a.created_at DESC, a.id DESC
				LIMIT $3
			`, tenantID, projectRef, query.Limit)
			if qErr != nil {
				return nil, qErr
			}
			defer rows.Close()

			seenQual := make(map[int64]struct{})
			for rows.Next() {
				var qualID int64
				var qualType, holderName, executorRef, qualStatus string
				if scanErr := rows.Scan(&qualID, &qualType, &holderName, &executorRef, &qualStatus); scanErr != nil {
					return nil, scanErr
				}
				if _, ok := seenQual[qualID]; ok {
					continue
				}
				seenQual[qualID] = struct{}{}
				qualNodeRef := fmt.Sprintf("qualification:%d", qualID)
				label := strings.TrimSpace(qualType)
				if label == "" {
					label = fmt.Sprintf("qualification-%d", qualID)
				}
				if holder := strings.TrimSpace(holderName); holder != "" {
					label += " / " + holder
				}
				builder.addNode(&LibraryRelationNode{
					NodeRef:     qualNodeRef,
					NodeType:    "qualification",
					LibraryType: "qualification",
					ID:          qualID,
					Label:       label,
					Status:      qualStatus,
				})
				builder.addEdge(projectNodeRef, qualNodeRef, "requires_qualification")

				if exec := strings.TrimSpace(executorRef); exec != "" {
					execNodeRef := "executor:" + exec
					builder.addNode(&LibraryRelationNode{
						NodeRef:  execNodeRef,
						NodeType: "executor",
						Ref:      exec,
						Label:    s.executorLabelByRef(ctx, tenantID, exec),
					})
					builder.addEdge(qualNodeRef, execNodeRef, "held_by")
				}
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				return nil, rowsErr
			}
		}

		if err := s.appendProjectDrawingRelations(ctx, tenantID, builder, []string{projectRef}, query.Limit); err != nil {
			return nil, err
		}
	}
	return builder.build("standard", id), nil
}

func (s *PGStore) getRegulationRelations(ctx context.Context, tenantID int, id int64, query LibraryRelationsQuery) (*LibraryRelationGraph, error) {
	var title, docNo, status, category string
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(title, ''), COALESCE(doc_no, ''), COALESCE(status, ''), COALESCE(category, '')
		FROM regulation_documents
		WHERE tenant_id=$1 AND id=$2 AND COALESCE(deleted, FALSE)=FALSE
	`, tenantID, id).Scan(&title, &docNo, &status, &category)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	builder := newRelationGraphBuilder()
	rootRef := fmt.Sprintf("regulation:%d", id)
	label := strings.TrimSpace(title)
	if label == "" {
		label = fmt.Sprintf("regulation-%d", id)
	}
	if no := strings.TrimSpace(docNo); no != "" {
		label += " / " + no
	}
	builder.addNode(&LibraryRelationNode{
		NodeRef:     rootRef,
		NodeType:    "regulation",
		LibraryType: "regulation",
		ID:          id,
		Label:       label,
		Status:      status,
	})

	hasVersions, err := s.hasColumn(ctx, "regulation_versions", "id")
	if err != nil {
		return nil, err
	}
	if hasVersions {
		rows, qErr := s.db.QueryContext(ctx, `
			SELECT id, version_no, effective_from, effective_to
			FROM regulation_versions
			WHERE tenant_id=$1 AND document_id=$2
			ORDER BY version_no DESC, id DESC
			LIMIT $3
		`, tenantID, id, query.Limit)
		if qErr != nil {
			return nil, qErr
		}
		defer rows.Close()
		for rows.Next() {
			var versionID int64
			var versionNo int
			var effectiveFrom, effectiveTo sql.NullTime
			if scanErr := rows.Scan(&versionID, &versionNo, &effectiveFrom, &effectiveTo); scanErr != nil {
				return nil, scanErr
			}
			versionLabel := fmt.Sprintf("版本 v%d", versionNo)
			if effectiveFrom.Valid || effectiveTo.Valid {
				fromStr := ""
				toStr := ""
				if effectiveFrom.Valid {
					fromStr = effectiveFrom.Time.Format("2006-01-02")
				}
				if effectiveTo.Valid {
					toStr = effectiveTo.Time.Format("2006-01-02")
				}
				versionLabel += " [" + fromStr + " ~ " + toStr + "]"
			}
			versionNodeRef := fmt.Sprintf("regulation_version:%d", versionID)
			builder.addNode(&LibraryRelationNode{
				NodeRef:  versionNodeRef,
				NodeType: "regulation_version",
				ID:       versionID,
				Label:    versionLabel,
			})
			builder.addEdge(rootRef, versionNodeRef, "has_version")
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return nil, rowsErr
		}
	}

	if strings.TrimSpace(category) != "" {
		rows, qErr := s.db.QueryContext(ctx, `
			SELECT id, COALESCE(title, ''), COALESCE(status, '')
			FROM regulation_documents
			WHERE tenant_id=$1
			  AND COALESCE(deleted, FALSE)=FALSE
			  AND COALESCE(category, '')=$2
			  AND id<>$3
			ORDER BY updated_at DESC, id DESC
			LIMIT $4
		`, tenantID, category, id, query.Limit)
		if qErr != nil {
			return nil, qErr
		}
		defer rows.Close()
		for rows.Next() {
			var peerID int64
			var peerTitle, peerStatus string
			if scanErr := rows.Scan(&peerID, &peerTitle, &peerStatus); scanErr != nil {
				return nil, scanErr
			}
			peerRef := fmt.Sprintf("regulation:%d", peerID)
			builder.addNode(&LibraryRelationNode{
				NodeRef:     peerRef,
				NodeType:    "regulation",
				LibraryType: "regulation",
				ID:          peerID,
				Label:       strings.TrimSpace(peerTitle),
				Status:      peerStatus,
			})
			builder.addEdge(rootRef, peerRef, "same_category")
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return nil, rowsErr
		}
	}

	return builder.build("regulation", id), nil
}

func (s *PGStore) GetLibraryDetail(ctx context.Context, tenantID int, libraryType string, id int64) (any, error) {
	switch normalizeLibraryType(libraryType) {
	case "qualification":
		return s.getQualificationLibraryDetail(ctx, tenantID, id)
	case "standard":
		return s.getEngineeringStandardDetail(ctx, tenantID, id)
	case "regulation":
		return s.getRegulationLibraryDetail(ctx, tenantID, id)
	default:
		return nil, fmt.Errorf("unsupported library type: %s", libraryType)
	}
}

func (s *PGStore) GetExecutorCredentialVault(ctx context.Context, tenantID int, executorRef string, drawingLimit int) (*ExecutorCredentialVault, error) {
	executorRef = strings.TrimSpace(executorRef)
	if executorRef == "" {
		return nil, fmt.Errorf("executor_ref is required")
	}
	drawingLimit = normalizePageLimit(drawingLimit)

	out := &ExecutorCredentialVault{
		ExecutorRef:    executorRef,
		Qualifications: make([]*VaultQualification, 0),
		Assignments:    make([]*QualificationAssignmentBrief, 0),
		Drawings:       make([]*EngineeringStandardItem, 0),
		UpdatedAt:      time.Now().UTC(),
	}

	qualRows, err := s.db.QueryContext(ctx, `
		SELECT qualification_id, qual_type, holder_name, cert_no, qualification_status,
		       COALESCE(TO_CHAR(valid_until,'YYYY-MM-DD'), ''), updated_at
		FROM credential_vault
		WHERE tenant_id=$1 AND executor_ref=$2
		ORDER BY updated_at DESC, qualification_id DESC
	`, tenantID, executorRef)
	if err != nil {
		if !isMissingRelationErr(err, "credential_vault") {
			return nil, err
		}
		qualRows, err = s.db.QueryContext(ctx, `
			SELECT id, qual_type, COALESCE(holder_name, ''), COALESCE(cert_no, ''), status,
			       COALESCE(TO_CHAR(valid_until,'YYYY-MM-DD'), ''), updated_at
			FROM qualifications
			WHERE tenant_id=$1 AND deleted=FALSE AND executor_ref=$2
			ORDER BY updated_at DESC, id DESC
		`, tenantID, executorRef)
		if err != nil {
			return nil, err
		}
	}
	defer qualRows.Close()

	qualIDs := make([]int64, 0)
	for qualRows.Next() {
		item := &VaultQualification{}
		if err := qualRows.Scan(
			&item.ID,
			&item.QualType,
			&item.HolderName,
			&item.CertNo,
			&item.Status,
			&item.ValidUntil,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		qualIDs = append(qualIDs, item.ID)
		out.Qualifications = append(out.Qualifications, item)
	}
	if err := qualRows.Err(); err != nil {
		return nil, err
	}
	out.QualificationCount = len(out.Qualifications)

	projectRefSet := make(map[string]struct{})
	if len(qualIDs) > 0 {
		args := make([]any, 0, len(qualIDs)+1)
		args = append(args, tenantID)
		holders := make([]string, 0, len(qualIDs))
		for i, qid := range qualIDs {
			args = append(args, qid)
			holders = append(holders, fmt.Sprintf("$%d", i+2))
		}
		assignSQL := `
			SELECT id, qualification_id, project_ref, status, released_at
			FROM qualification_assignments
			WHERE tenant_id=$1
			  AND qualification_id IN (` + strings.Join(holders, ",") + `)
			ORDER BY created_at DESC, id DESC
			LIMIT 500
		`
		assignRows, assignErr := s.db.QueryContext(ctx, assignSQL, args...)
		if assignErr != nil {
			if !isMissingRelationErr(assignErr, "qualification_assignments") {
				return nil, assignErr
			}
		} else {
			defer assignRows.Close()
			for assignRows.Next() {
				item := &QualificationAssignmentBrief{}
				var releasedAt sql.NullTime
				if err := assignRows.Scan(
					&item.ID,
					&item.QualificationID,
					&item.ProjectRef,
					&item.Status,
					&releasedAt,
				); err != nil {
					return nil, err
				}
				if releasedAt.Valid {
					t := releasedAt.Time
					item.ReleasedAt = &t
				}
				if item.ProjectRef != "" {
					projectRefSet[item.ProjectRef] = struct{}{}
				}
				out.Assignments = append(out.Assignments, item)
			}
			if err := assignRows.Err(); err != nil {
				return nil, err
			}
		}
	}
	out.AssignmentCount = len(out.Assignments)

	if len(projectRefSet) > 0 {
		projectRefs := make([]string, 0, len(projectRefSet))
		for ref := range projectRefSet {
			projectRefs = append(projectRefs, ref)
		}

		hasDeleted, err := s.hasColumn(ctx, "drawings", "deleted")
		if err != nil {
			return nil, err
		}
		drawingNoExpr, statusExpr, err := s.buildDrawingDisplayExpressions(ctx)
		if err != nil {
			return nil, err
		}

		args := make([]any, 0, len(projectRefs)+2)
		args = append(args, tenantID)
		holders := make([]string, 0, len(projectRefs))
		for i, ref := range projectRefs {
			args = append(args, ref)
			holders = append(holders, fmt.Sprintf("$%d", i+2))
		}
		args = append(args, drawingLimit)
		limitArg := fmt.Sprintf("$%d", len(args))

		whereClause := "d.tenant_id=$1 AND d.project_ref IN (" + strings.Join(holders, ",") + ")"
		if hasDeleted {
			whereClause += " AND d.deleted=FALSE"
		}

		drawSQL := `
			SELECT d.id,
			       ` + drawingNoExpr + `,
			       COALESCE(d.major, ''),
			       ` + statusExpr + `,
			       COALESCE(d.project_ref, ''),
			       COALESCE(d.contract_id, 0),
			       COALESCE(att.cnt, 0),
			       d.updated_at
			FROM drawings d
			LEFT JOIN LATERAL (
				SELECT COUNT(*)::int AS cnt
				FROM drawing_attachments da
				WHERE da.drawing_id = d.id
			) att ON TRUE
			WHERE ` + whereClause + `
			ORDER BY d.updated_at DESC, d.id DESC
			LIMIT ` + limitArg
		drawRows, drawErr := s.db.QueryContext(ctx, drawSQL, args...)
		if drawErr != nil {
			if !isMissingRelationErr(drawErr, "drawings", "drawing_attachments") {
				return nil, drawErr
			}
		} else {
			defer drawRows.Close()
			for drawRows.Next() {
				item := &EngineeringStandardItem{}
				if err := drawRows.Scan(
					&item.ID,
					&item.DrawingNo,
					&item.Major,
					&item.Status,
					&item.ProjectRef,
					&item.ContractID,
					&item.AttachmentCount,
					&item.UpdatedAt,
				); err != nil {
					return nil, err
				}
				out.Drawings = append(out.Drawings, item)
			}
			if err := drawRows.Err(); err != nil {
				return nil, err
			}
		}
	}
	out.DrawingCount = len(out.Drawings)

	return out, nil
}

func (s *PGStore) getQualificationLibraryDetail(ctx context.Context, tenantID int, id int64) (*QualificationLibraryDetail, error) {
	detail := &QualificationLibraryDetail{
		Assignments: make([]*QualificationAssignmentBrief, 0),
	}
	var issuedAt, validFrom, validUntil sql.NullTime

	err := s.db.QueryRowContext(ctx, `
		SELECT
			id, qual_type, COALESCE(holder_name, ''), COALESCE(executor_ref, ''), status,
			COALESCE(TO_CHAR(valid_until,'YYYY-MM-DD'), ''), updated_at,
			COALESCE(holder_type, ''), COALESCE(holder_id, 0), COALESCE(cert_no, ''), COALESCE(issued_by, ''),
			issued_at, valid_from, valid_until,
			COALESCE(specialty, ''), COALESCE(level, ''), COALESCE(scope, ''), COALESCE(attachment_url, ''), COALESCE(note, '')
		FROM qualifications
		WHERE tenant_id=$1 AND id=$2 AND deleted=FALSE
	`, tenantID, id).Scan(
		&detail.Item.ID,
		&detail.Item.QualType,
		&detail.Item.HolderName,
		&detail.Item.ExecutorRef,
		&detail.Item.Status,
		&detail.Item.ValidUntil,
		&detail.Item.UpdatedAt,
		&detail.HolderType,
		&detail.HolderID,
		&detail.CertNo,
		&detail.IssuedBy,
		&issuedAt,
		&validFrom,
		&validUntil,
		&detail.Specialty,
		&detail.Level,
		&detail.Scope,
		&detail.AttachmentURL,
		&detail.Note,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if issuedAt.Valid {
		t := issuedAt.Time
		detail.IssuedAt = &t
	}
	if validFrom.Valid {
		t := validFrom.Time
		detail.ValidFrom = &t
	}
	if validUntil.Valid {
		t := validUntil.Time
		detail.ValidUntilAt = &t
	}

	assignRows, assignErr := s.db.QueryContext(ctx, `
		SELECT id, qualification_id, project_ref, status, released_at
		FROM qualification_assignments
		WHERE tenant_id=$1 AND qualification_id=$2
		ORDER BY created_at DESC, id DESC
		LIMIT 200
	`, tenantID, id)
	if assignErr != nil {
		if !isMissingRelationErr(assignErr, "qualification_assignments") {
			return nil, assignErr
		}
		return detail, nil
	}
	defer assignRows.Close()

	for assignRows.Next() {
		item := &QualificationAssignmentBrief{}
		var releasedAt sql.NullTime
		if err := assignRows.Scan(&item.ID, &item.QualificationID, &item.ProjectRef, &item.Status, &releasedAt); err != nil {
			return nil, err
		}
		if releasedAt.Valid {
			t := releasedAt.Time
			item.ReleasedAt = &t
		}
		detail.Assignments = append(detail.Assignments, item)
	}
	if err := assignRows.Err(); err != nil {
		return nil, err
	}

	return detail, nil
}

func (s *PGStore) getEngineeringStandardDetail(ctx context.Context, tenantID int, id int64) (*EngineeringStandardDetail, error) {
	hasDeleted, err := s.hasColumn(ctx, "drawings", "deleted")
	if err != nil {
		return nil, err
	}
	drawingNoExpr, statusExpr, err := s.buildDrawingDisplayExpressions(ctx)
	if err != nil {
		return nil, err
	}
	whereClause := "d.tenant_id=$1 AND d.id=$2"
	if hasDeleted {
		whereClause += " AND d.deleted=FALSE"
	}

	detail := &EngineeringStandardDetail{
		Attachments: make([]*DrawingAttachmentDetail, 0),
	}
	err = s.db.QueryRowContext(ctx, `
		SELECT d.id,
		       `+drawingNoExpr+`,
		       COALESCE(d.major, ''),
		       `+statusExpr+`,
		       COALESCE(d.project_ref, ''),
		       COALESCE(d.contract_id, 0),
		       COALESCE(att.cnt, 0),
		       d.updated_at,
		       COALESCE(d.reviewer, ''),
		       COALESCE(d.remarks, ''),
		       d.created_at
		FROM drawings d
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::int AS cnt
			FROM drawing_attachments da
			WHERE da.drawing_id = d.id
		) att ON TRUE
		WHERE `+whereClause+`
	`, tenantID, id).Scan(
		&detail.Item.ID,
		&detail.Item.DrawingNo,
		&detail.Item.Major,
		&detail.Item.Status,
		&detail.Item.ProjectRef,
		&detail.Item.ContractID,
		&detail.Item.AttachmentCount,
		&detail.Item.UpdatedAt,
		&detail.Reviewer,
		&detail.Remarks,
		&detail.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	attRows, attErr := s.db.QueryContext(ctx, `
		SELECT id, COALESCE(source_table, ''), COALESCE(name, ''), COALESCE(url, ''),
		       COALESCE(version, ''), COALESCE(state, 0), approve_date, updated_at
		FROM drawing_attachments
		WHERE tenant_id=$1 AND drawing_id=$2
		ORDER BY updated_at DESC, id DESC
		LIMIT 500
	`, tenantID, id)
	if attErr != nil {
		if !isMissingRelationErr(attErr, "drawing_attachments") {
			return nil, attErr
		}
		return detail, nil
	}
	defer attRows.Close()

	for attRows.Next() {
		item := &DrawingAttachmentDetail{}
		var approveDate sql.NullTime
		if err := attRows.Scan(
			&item.ID,
			&item.SourceTable,
			&item.Name,
			&item.URL,
			&item.Version,
			&item.State,
			&approveDate,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if approveDate.Valid {
			t := approveDate.Time
			item.ApproveDate = &t
		}
		detail.Attachments = append(detail.Attachments, item)
	}
	if err := attRows.Err(); err != nil {
		return nil, err
	}

	return detail, nil
}

func (s *PGStore) getRegulationLibraryDetail(ctx context.Context, tenantID int, id int64) (*RegulationLibraryDetail, error) {
	detail := &RegulationLibraryDetail{
		Versions: make([]*RegulationVersionDetail, 0),
	}
	err := s.db.QueryRowContext(ctx, `
		SELECT id, COALESCE(doc_no, ''), title, COALESCE(category, ''), COALESCE(publisher, ''), status, updated_at,
		       COALESCE(doc_type, ''), COALESCE(jurisdiction, ''), COALESCE(keywords, ''), COALESCE(summary, ''),
		       COALESCE(source_url, ''), COALESCE(ref, '')
		FROM regulation_documents
		WHERE tenant_id=$1 AND id=$2 AND deleted=FALSE
	`, tenantID, id).Scan(
		&detail.Item.ID,
		&detail.Item.DocNo,
		&detail.Item.Title,
		&detail.Item.Category,
		&detail.Item.Publisher,
		&detail.Item.Status,
		&detail.Item.UpdatedAt,
		&detail.DocType,
		&detail.Jurisdiction,
		&detail.Keywords,
		&detail.Summary,
		&detail.SourceURL,
		&detail.Ref,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	rows, verErr := s.db.QueryContext(ctx, `
		SELECT id, version_no, effective_from, effective_to, published_at,
		       COALESCE(content_hash, ''), COALESCE(attachment_url, ''), COALESCE(source_note, '')
		FROM regulation_versions
		WHERE tenant_id=$1 AND document_id=$2
		ORDER BY version_no DESC, id DESC
	`, tenantID, id)
	if verErr != nil {
		if !isMissingRelationErr(verErr, "regulation_versions") {
			return nil, verErr
		}
		return detail, nil
	}
	defer rows.Close()

	for rows.Next() {
		item := &RegulationVersionDetail{}
		var effFrom, effTo, pubAt sql.NullTime
		if err := rows.Scan(
			&item.ID,
			&item.VersionNo,
			&effFrom,
			&effTo,
			&pubAt,
			&item.ContentHash,
			&item.AttachmentURL,
			&item.SourceNote,
		); err != nil {
			return nil, err
		}
		if effFrom.Valid {
			t := effFrom.Time
			item.EffectiveFrom = &t
		}
		if effTo.Valid {
			t := effTo.Time
			item.EffectiveTo = &t
		}
		if pubAt.Valid {
			t := pubAt.Time
			item.PublishedAt = &t
		}
		detail.Versions = append(detail.Versions, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return detail, nil
}

func (s *PGStore) listQualificationLibrary(ctx context.Context, tenantID, limit, offset int) ([]*QualificationLibraryItem, int, error) {
	var total int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM qualifications
		WHERE tenant_id=$1 AND deleted=FALSE
	`, tenantID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, qual_type, holder_name, COALESCE(executor_ref, ''), status,
		       COALESCE(TO_CHAR(valid_until,'YYYY-MM-DD'), ''), updated_at
		FROM qualifications
		WHERE tenant_id=$1 AND deleted=FALSE
		ORDER BY updated_at DESC, id DESC
		LIMIT $2 OFFSET $3
	`, tenantID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items := make([]*QualificationLibraryItem, 0, limit)
	for rows.Next() {
		item := &QualificationLibraryItem{}
		if err := rows.Scan(
			&item.ID,
			&item.QualType,
			&item.HolderName,
			&item.ExecutorRef,
			&item.Status,
			&item.ValidUntil,
			&item.UpdatedAt,
		); err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (s *PGStore) listEngineeringStandards(ctx context.Context, tenantID, limit, offset int) ([]*EngineeringStandardItem, int, error) {
	hasDeleted, err := s.hasColumn(ctx, "drawings", "deleted")
	if err != nil {
		return nil, 0, err
	}
	hasDrawingNo, err := s.hasColumn(ctx, "drawings", "drawing_no")
	if err != nil {
		return nil, 0, err
	}
	hasStatus, err := s.hasColumn(ctx, "drawings", "status")
	if err != nil {
		return nil, 0, err
	}
	hasState, err := s.hasColumn(ctx, "drawings", "state")
	if err != nil {
		return nil, 0, err
	}

	whereClause := "d.tenant_id=$1"
	if hasDeleted {
		whereClause += " AND d.deleted=FALSE"
	}

	drawingNoExpr := "COALESCE(NULLIF(d.num,''), 'drawing-' || d.id::text)"
	if hasDrawingNo {
		drawingNoExpr = "COALESCE(NULLIF(d.drawing_no,''), NULLIF(d.num,''), 'drawing-' || d.id::text)"
	}

	statusExpr := "'UNKNOWN'"
	switch {
	case hasStatus && hasState:
		statusExpr = "COALESCE(NULLIF(d.status,''), NULLIF(d.state,''), 'UNKNOWN')"
	case hasStatus:
		statusExpr = "COALESCE(NULLIF(d.status,''), 'UNKNOWN')"
	case hasState:
		statusExpr = "COALESCE(NULLIF(d.state,''), 'UNKNOWN')"
	}

	var total int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM drawings d
		WHERE `+whereClause,
		tenantID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT d.id,
		       `+drawingNoExpr+`,
		       COALESCE(d.major, ''),
		       `+statusExpr+`,
		       COALESCE(d.project_ref, ''),
		       COALESCE(d.contract_id, 0),
		       COALESCE(att.cnt, 0),
		       d.updated_at
		FROM drawings d
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::int AS cnt
			FROM drawing_attachments da
			WHERE da.drawing_id = d.id
		) att ON TRUE
		WHERE `+whereClause+`
		ORDER BY d.updated_at DESC, d.id DESC
		LIMIT $2 OFFSET $3
	`, tenantID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items := make([]*EngineeringStandardItem, 0, limit)
	for rows.Next() {
		item := &EngineeringStandardItem{}
		if err := rows.Scan(
			&item.ID,
			&item.DrawingNo,
			&item.Major,
			&item.Status,
			&item.ProjectRef,
			&item.ContractID,
			&item.AttachmentCount,
			&item.UpdatedAt,
		); err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (s *PGStore) listRegulations(ctx context.Context, tenantID, limit, offset int) ([]*RegulationLibraryItem, int, error) {
	var total int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM regulation_documents
		WHERE tenant_id=$1 AND deleted=FALSE
	`, tenantID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, COALESCE(doc_no, ''), title, COALESCE(category, ''), COALESCE(publisher, ''), status, updated_at
		FROM regulation_documents
		WHERE tenant_id=$1 AND deleted=FALSE
		ORDER BY updated_at DESC, id DESC
		LIMIT $2 OFFSET $3
	`, tenantID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items := make([]*RegulationLibraryItem, 0, limit)
	for rows.Next() {
		item := &RegulationLibraryItem{}
		if err := rows.Scan(
			&item.ID,
			&item.DocNo,
			&item.Title,
			&item.Category,
			&item.Publisher,
			&item.Status,
			&item.UpdatedAt,
		); err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

type relationGraphBuilder struct {
	nodeMap map[string]*LibraryRelationNode
	edgeSet map[string]struct{}
	nodes   []*LibraryRelationNode
	edges   []*LibraryRelationEdge
}

func newRelationGraphBuilder() *relationGraphBuilder {
	return &relationGraphBuilder{
		nodeMap: make(map[string]*LibraryRelationNode),
		edgeSet: make(map[string]struct{}),
		nodes:   make([]*LibraryRelationNode, 0, 16),
		edges:   make([]*LibraryRelationEdge, 0, 24),
	}
}

func (b *relationGraphBuilder) addNode(node *LibraryRelationNode) {
	if node == nil {
		return
	}
	nodeRef := strings.TrimSpace(node.NodeRef)
	if nodeRef == "" {
		return
	}
	if exists, ok := b.nodeMap[nodeRef]; ok {
		if strings.TrimSpace(exists.Label) == "" && strings.TrimSpace(node.Label) != "" {
			exists.Label = strings.TrimSpace(node.Label)
		}
		if strings.TrimSpace(exists.Status) == "" && strings.TrimSpace(node.Status) != "" {
			exists.Status = strings.TrimSpace(node.Status)
		}
		if strings.TrimSpace(exists.Ref) == "" && strings.TrimSpace(node.Ref) != "" {
			exists.Ref = strings.TrimSpace(node.Ref)
		}
		if exists.ID == 0 && node.ID > 0 {
			exists.ID = node.ID
		}
		if strings.TrimSpace(exists.LibraryType) == "" && strings.TrimSpace(node.LibraryType) != "" {
			exists.LibraryType = strings.TrimSpace(node.LibraryType)
		}
		return
	}
	copied := *node
	copied.NodeRef = nodeRef
	copied.NodeType = strings.TrimSpace(copied.NodeType)
	copied.LibraryType = strings.TrimSpace(copied.LibraryType)
	copied.Ref = strings.TrimSpace(copied.Ref)
	copied.Label = strings.TrimSpace(copied.Label)
	copied.Status = strings.TrimSpace(copied.Status)
	if copied.Label == "" {
		copied.Label = nodeRef
	}
	b.nodeMap[nodeRef] = &copied
	b.nodes = append(b.nodes, &copied)
}

func (b *relationGraphBuilder) addEdge(from, to, relation string) {
	from = strings.TrimSpace(from)
	to = strings.TrimSpace(to)
	relation = strings.TrimSpace(relation)
	if from == "" || to == "" || relation == "" {
		return
	}
	key := from + "|" + to + "|" + relation
	if _, ok := b.edgeSet[key]; ok {
		return
	}
	b.edgeSet[key] = struct{}{}
	b.edges = append(b.edges, &LibraryRelationEdge{
		From:     from,
		To:       to,
		Relation: relation,
	})
}

func (b *relationGraphBuilder) build(rootType string, rootID int64) *LibraryRelationGraph {
	return &LibraryRelationGraph{
		RootType:  strings.TrimSpace(rootType),
		RootID:    rootID,
		Nodes:     b.nodes,
		Edges:     b.edges,
		UpdatedAt: time.Now().UTC(),
	}
}

func (s *PGStore) appendProjectDrawingRelations(ctx context.Context, tenantID int, builder *relationGraphBuilder, projectRefs []string, limit int) error {
	if builder == nil || len(projectRefs) == 0 {
		return nil
	}
	hasDrawings, err := s.hasColumn(ctx, "drawings", "id")
	if err != nil {
		return err
	}
	if !hasDrawings {
		return nil
	}
	hasDeleted, err := s.hasColumn(ctx, "drawings", "deleted")
	if err != nil {
		return err
	}
	drawingNoExpr, statusExpr, err := s.buildDrawingDisplayExpressions(ctx)
	if err != nil {
		return err
	}

	seen := make(map[string]struct{}, len(projectRefs))
	dedupProjectRefs := make([]string, 0, len(projectRefs))
	for _, projectRef := range projectRefs {
		projectRef = strings.TrimSpace(projectRef)
		if projectRef == "" {
			continue
		}
		if _, ok := seen[projectRef]; ok {
			continue
		}
		seen[projectRef] = struct{}{}
		dedupProjectRefs = append(dedupProjectRefs, projectRef)
	}
	if len(dedupProjectRefs) == 0 {
		return nil
	}

	limit = normalizePageLimit(limit)
	args := make([]any, 0, len(dedupProjectRefs)+2)
	args = append(args, tenantID)
	holders := make([]string, 0, len(dedupProjectRefs))
	for i, projectRef := range dedupProjectRefs {
		args = append(args, projectRef)
		holders = append(holders, fmt.Sprintf("$%d", i+2))
	}
	args = append(args, limit)
	limitArg := fmt.Sprintf("$%d", len(args))

	whereClause := "d.tenant_id=$1 AND d.project_ref IN (" + strings.Join(holders, ",") + ")"
	if hasDeleted {
		whereClause += " AND d.deleted=FALSE"
	}

	rows, qErr := s.db.QueryContext(ctx, `
		SELECT COALESCE(d.project_ref, ''), d.id, `+drawingNoExpr+`, `+statusExpr+`
		FROM drawings d
		WHERE `+whereClause+`
		ORDER BY d.updated_at DESC, d.id DESC
		LIMIT `+limitArg, args...)
	if qErr != nil {
		return qErr
	}
	defer rows.Close()

	for rows.Next() {
		var projectRef, drawingNo, status string
		var drawingID int64
		if scanErr := rows.Scan(&projectRef, &drawingID, &drawingNo, &status); scanErr != nil {
			return scanErr
		}
		projectRef = strings.TrimSpace(projectRef)
		if projectRef == "" {
			continue
		}
		projectNodeRef := "project:" + projectRef
		builder.addNode(&LibraryRelationNode{
			NodeRef:  projectNodeRef,
			NodeType: "project",
			Ref:      projectRef,
			Label:    projectRef,
		})
		standardNodeRef := fmt.Sprintf("standard:%d", drawingID)
		builder.addNode(&LibraryRelationNode{
			NodeRef:     standardNodeRef,
			NodeType:    "standard",
			LibraryType: "standard",
			ID:          drawingID,
			Label:       strings.TrimSpace(drawingNo),
			Status:      strings.TrimSpace(status),
			Ref:         projectRef,
		})
		builder.addEdge(projectNodeRef, standardNodeRef, "contains_standard")
	}
	return rows.Err()
}

func (s *PGStore) projectLabelByRef(ctx context.Context, tenantID int, projectRef string) string {
	projectRef = strings.TrimSpace(projectRef)
	if projectRef == "" {
		return ""
	}
	var name string
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(name, '')
		FROM project_nodes
		WHERE tenant_id=$1 AND ref=$2
		ORDER BY updated_at DESC, id DESC
		LIMIT 1
	`, tenantID, projectRef).Scan(&name)
	if err == nil {
		if v := strings.TrimSpace(name); v != "" {
			return v
		}
	}
	return projectRef
}

func (s *PGStore) executorLabelByRef(ctx context.Context, tenantID int, executorRef string) string {
	executorRef = strings.TrimSpace(executorRef)
	if executorRef == "" {
		return ""
	}
	var name string
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(name, '')
		FROM employees
		WHERE tenant_id=$1 AND executor_ref=$2
		ORDER BY updated_at DESC, id DESC
		LIMIT 1
	`, tenantID, executorRef).Scan(&name)
	if err == nil {
		if v := strings.TrimSpace(name); v != "" {
			return v + " (" + executorRef + ")"
		}
	}
	return executorRef
}

func normalizeThreeLibrariesQuery(q ThreeLibrariesQuery) ThreeLibrariesQuery {
	q.QualificationLimit = normalizePageLimit(q.QualificationLimit)
	q.StandardLimit = normalizePageLimit(q.StandardLimit)
	q.RegulationLimit = normalizePageLimit(q.RegulationLimit)
	q.QualificationOffset = normalizePageOffset(q.QualificationOffset)
	q.StandardOffset = normalizePageOffset(q.StandardOffset)
	q.RegulationOffset = normalizePageOffset(q.RegulationOffset)
	return q
}

func normalizeLibrarySearchQuery(q LibrarySearchQuery) LibrarySearchQuery {
	q.Limit = normalizePageLimit(q.Limit)
	q.Offset = normalizePageOffset(q.Offset)
	q.Keyword = strings.TrimSpace(q.Keyword)
	q.Type = normalizeLibraryType(q.Type)
	q.Status = strings.TrimSpace(q.Status)
	return q
}

func normalizeLibraryRelationsQuery(q LibraryRelationsQuery) LibraryRelationsQuery {
	q.Limit = normalizePageLimit(q.Limit)
	return q
}

func normalizeLibraryType(raw string) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	switch v {
	case "qualification", "qualifications", "qual", "certificate":
		return "qualification"
	case "standard", "standards", "engineering-standard", "engineering_standards", "drawing", "drawings":
		return "standard"
	case "regulation", "regulations", "law", "laws":
		return "regulation"
	default:
		return v
	}
}

func (s *PGStore) buildDrawingDisplayExpressions(ctx context.Context) (string, string, error) {
	hasDrawingNo, err := s.hasColumn(ctx, "drawings", "drawing_no")
	if err != nil {
		return "", "", err
	}
	hasStatus, err := s.hasColumn(ctx, "drawings", "status")
	if err != nil {
		return "", "", err
	}
	hasState, err := s.hasColumn(ctx, "drawings", "state")
	if err != nil {
		return "", "", err
	}

	drawingNoExpr := "COALESCE(NULLIF(d.num,''), 'drawing-' || d.id::text)"
	if hasDrawingNo {
		drawingNoExpr = "COALESCE(NULLIF(d.drawing_no,''), NULLIF(d.num,''), 'drawing-' || d.id::text)"
	}
	statusExpr := "'UNKNOWN'"
	switch {
	case hasStatus && hasState:
		statusExpr = "COALESCE(NULLIF(d.status,''), NULLIF(d.state,''), 'UNKNOWN')"
	case hasStatus:
		statusExpr = "COALESCE(NULLIF(d.status,''), 'UNKNOWN')"
	case hasState:
		statusExpr = "COALESCE(NULLIF(d.state,''), 'UNKNOWN')"
	}
	return drawingNoExpr, statusExpr, nil
}

func normalizePageLimit(raw int) int {
	if raw <= 0 {
		return 20
	}
	if raw > 200 {
		return 200
	}
	return raw
}

func normalizePageOffset(raw int) int {
	if raw < 0 {
		return 0
	}
	return raw
}

func isMissingRelationErr(err error, tableNames ...string) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "does not exist") {
		return false
	}
	for _, name := range tableNames {
		if strings.Contains(msg, strings.ToLower(name)) {
			return true
		}
	}
	return false
}

func (s *PGStore) hasColumn(ctx context.Context, tableName, columnName string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = $1
			  AND column_name = $2
		)
	`, tableName, columnName).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
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
