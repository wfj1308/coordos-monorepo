package report

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strconv"
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
	ExecutorRef         string
	IncludeHistory      bool
	ValidOn             *time.Time
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
	Versions    []*StandardVersionDetail   `json:"versions"`
}

type StandardVersionDetail struct {
	ID            int64     `json:"id"`
	VersionNo     int       `json:"version_no"`
	Status        string    `json:"status"`
	ReviewCertRef string    `json:"review_cert_ref"`
	FileHash      string    `json:"file_hash"`
	ProofHash     string    `json:"proof_hash"`
	PublisherRef  string    `json:"publisher_ref"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
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
	Regulations        []*RegulationLibraryItem        `json:"regulations"`
	QualificationCount int                             `json:"qualification_count"`
	AssignmentCount    int                             `json:"assignment_count"`
	DrawingCount       int                             `json:"drawing_count"`
	RegulationCount    int                             `json:"regulation_count"`
	UpdatedAt          time.Time                       `json:"updated_at"`
}

type LibrarySearchQuery struct {
	Keyword        string
	Type           string
	Status         string
	UpdatedFrom    *time.Time
	UpdatedTo      *time.Time
	HasExecutor    *bool
	Limit          int
	Offset         int
	ExecutorRef    string
	IncludeHistory bool
	ValidOn        *time.Time
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

type LibraryChangesQuery struct {
	Limit          int
	Offset         int
	From           *time.Time
	To             *time.Time
	ExecutorRef    string
	IncludeHistory bool
	ValidOn        *time.Time
}

type LibraryChangeItem struct {
	Type      string         `json:"type"`
	ID        int64          `json:"id"`
	EventType string         `json:"event_type"`
	Source    string         `json:"source"`
	Summary   string         `json:"summary"`
	ChangedAt time.Time      `json:"changed_at"`
	Payload   map[string]any `json:"payload,omitempty"`
}

type LibraryChangesResult struct {
	Total     int                  `json:"total"`
	Limit     int                  `json:"limit"`
	Offset    int                  `json:"offset"`
	Items     []*LibraryChangeItem `json:"items"`
	UpdatedAt time.Time            `json:"updated_at"`
}

type LibraryVersionDiffQuery struct {
	FromVersionID int64
	ToVersionID   int64
	ExecutorRef   string
}

type LibraryVersionDiffVersion struct {
	ID        int64          `json:"id"`
	VersionNo int            `json:"version_no"`
	ChangedAt time.Time      `json:"changed_at"`
	Data      map[string]any `json:"data"`
}

type LibraryVersionDiffResult struct {
	Type          string                     `json:"type"`
	LibraryID     int64                      `json:"library_id"`
	FromVersion   *LibraryVersionDiffVersion `json:"from_version,omitempty"`
	ToVersion     *LibraryVersionDiffVersion `json:"to_version,omitempty"`
	ChangedFields []string                   `json:"changed_fields"`
	Summary       string                     `json:"summary"`
	UpdatedAt     time.Time                  `json:"updated_at"`
}

type LibraryRelationsQuery struct {
	Limit          int
	ExecutorRef    string
	IncludeHistory bool
	ValidOn        *time.Time
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

type LibraryQualityCheck struct {
	Code     string           `json:"code"`
	Name     string           `json:"name"`
	Severity string           `json:"severity"`
	Status   string           `json:"status"`
	Count    int              `json:"count"`
	Message  string           `json:"message"`
	Samples  []map[string]any `json:"samples"`
}

type LibrariesQualityGate struct {
	Status        string                 `json:"status"`
	TotalChecks   int                    `json:"total_checks"`
	FailedChecks  int                    `json:"failed_checks"`
	WarningChecks int                    `json:"warning_checks"`
	Checks        []*LibraryQualityCheck `json:"checks"`
	UpdatedAt     time.Time              `json:"updated_at"`
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
	GetLibraryDetail(ctx context.Context, tenantID int, libraryType string, id int64, executorRef string, includeHistory bool, validOn *time.Time) (any, error)
	GetExecutorCredentialVault(ctx context.Context, tenantID int, executorRef string, drawingLimit int) (*ExecutorCredentialVault, error)
	SearchLibraries(ctx context.Context, tenantID int, query LibrarySearchQuery) (*LibrarySearchResult, error)
	GetLibraryChanges(ctx context.Context, tenantID int, libraryType string, id int64, query LibraryChangesQuery) (*LibraryChangesResult, error)
	GetLibraryVersionDiff(ctx context.Context, tenantID int, libraryType string, id int64, query LibraryVersionDiffQuery) (*LibraryVersionDiffResult, error)
	GetLibraryRelations(ctx context.Context, tenantID int, libraryType string, id int64, query LibraryRelationsQuery) (*LibraryRelationGraph, error)
	GetLibrariesQualityGate(ctx context.Context, tenantID int, sampleLimit int) (*LibrariesQualityGate, error)
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

func (s *Service) GetLibraryDetail(ctx context.Context, libraryType string, id int64, executorRef string, includeHistory bool, validOn *time.Time) (any, error) {
	return s.store.GetLibraryDetail(ctx, s.tenantID, libraryType, id, executorRef, includeHistory, validOn)
}

func (s *Service) GetExecutorCredentialVault(ctx context.Context, executorRef string, drawingLimit int) (*ExecutorCredentialVault, error) {
	return s.store.GetExecutorCredentialVault(ctx, s.tenantID, executorRef, drawingLimit)
}

func (s *Service) SearchLibraries(ctx context.Context, query LibrarySearchQuery) (*LibrarySearchResult, error) {
	return s.store.SearchLibraries(ctx, s.tenantID, query)
}

func (s *Service) GetLibraryChanges(ctx context.Context, libraryType string, id int64, query LibraryChangesQuery) (*LibraryChangesResult, error) {
	return s.store.GetLibraryChanges(ctx, s.tenantID, libraryType, id, query)
}

func (s *Service) GetLibraryVersionDiff(ctx context.Context, libraryType string, id int64, query LibraryVersionDiffQuery) (*LibraryVersionDiffResult, error) {
	return s.store.GetLibraryVersionDiff(ctx, s.tenantID, libraryType, id, query)
}

func (s *Service) GetLibraryRelations(ctx context.Context, libraryType string, id int64, query LibraryRelationsQuery) (*LibraryRelationGraph, error) {
	return s.store.GetLibraryRelations(ctx, s.tenantID, libraryType, id, query)
}

func (s *Service) GetLibrariesQualityGate(ctx context.Context, sampleLimit int) (*LibrariesQualityGate, error) {
	return s.store.GetLibrariesQualityGate(ctx, s.tenantID, sampleLimit)
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

	quals, qualTotal, err := s.listQualificationLibrary(ctx, tenantID, query.QualificationLimit, query.QualificationOffset, query.ExecutorRef)
	if err != nil {
		if !isMissingRelationErr(err, "qualifications") {
			return nil, err
		}
	} else {
		out.Qualifications.Items = quals
		out.Qualifications.Total = qualTotal
	}

	standards, standardTotal, err := s.listEngineeringStandards(
		ctx,
		tenantID,
		query.StandardLimit,
		query.StandardOffset,
		query.ExecutorRef,
		query.IncludeHistory,
		query.ValidOn,
	)
	if err != nil {
		if !isMissingRelationErr(err, "drawings", "drawing_attachments") {
			return nil, err
		}
	} else {
		out.EngineeringStandard.Items = standards
		out.EngineeringStandard.Total = standardTotal
	}

	regulations, regulationTotal, err := s.listRegulations(
		ctx,
		tenantID,
		query.RegulationLimit,
		query.RegulationOffset,
		query.ExecutorRef,
		query.IncludeHistory,
		query.ValidOn,
	)
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

	args := make([]any, 0, 12)
	args = append(args, tenantID)
	validOnArg := 0
	if !query.IncludeHistory {
		effectiveAt := time.Now().UTC()
		if query.ValidOn != nil {
			effectiveAt = query.ValidOn.UTC()
		}
		args = append(args, effectiveAt)
		validOnArg = len(args)
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
		if !query.IncludeHistory {
			hasDrawingVersions, verErr := s.hasColumn(ctx, "drawing_versions", "id")
			if verErr != nil {
				return nil, verErr
			}
			hasDrawingVersionRows := false
			if hasDrawingVersions {
				hasDrawingVersionRows, verErr = s.hasTenantRows(ctx, "drawing_versions", tenantID)
				if verErr != nil {
					return nil, verErr
				}
			}
			if hasDrawingVersions && hasDrawingVersionRows && validOnArg > 0 {
				drawWhere += fmt.Sprintf(` AND EXISTS (
					SELECT 1
					FROM drawing_versions dv
					WHERE dv.tenant_id = d.tenant_id
					  AND dv.drawing_no = `+drawingNoExpr+`
					  AND COALESCE(dv.status, '') <> 'REVOKED'
					  AND dv.created_at <= $%d
				)`, validOnArg)
			}
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
		regWhere := "r.tenant_id = $1 AND COALESCE(r.deleted, FALSE) = FALSE"
		if !query.IncludeHistory {
			hasRegVersions, verErr := s.hasColumn(ctx, "regulation_versions", "id")
			if verErr != nil {
				return nil, verErr
			}
			if hasRegVersions {
				regWhere += fmt.Sprintf(` AND EXISTS (
					SELECT 1
					FROM regulation_versions rv
					WHERE rv.tenant_id = r.tenant_id
					  AND rv.document_id = r.id
					  AND COALESCE(rv.effective_from, '-infinity'::timestamptz) <= $%d
					  AND COALESCE(rv.effective_to, 'infinity'::timestamptz) >= $%d
				)`, validOnArg, validOnArg)
			}
		}

		regProjectExpr := "''::text"
		hasRegProjectRef, regProjErr := s.hasColumn(ctx, "regulation_documents", "project_ref")
		if regProjErr != nil {
			return nil, regProjErr
		}
		if hasRegProjectRef {
			regProjectExpr = "COALESCE(r.project_ref, '')"
		}

		regExecutorExpr := "''::text"
		regJoin := ""
		hasRegExecutorRef, regExecErr := s.hasColumn(ctx, "regulation_documents", "executor_ref")
		if regExecErr != nil {
			return nil, regExecErr
		}
		if hasRegExecutorRef {
			regExecutorExpr = "COALESCE(r.executor_ref, '')"
		} else if hasRegProjectRef && hasQualifications && hasQualificationAssignments {
			regJoin = `LEFT JOIN LATERAL (
				SELECT COALESCE(q.executor_ref, '') AS executor_ref
				FROM qualification_assignments a
				JOIN qualifications q
				  ON q.id = a.qualification_id
				 AND q.tenant_id = a.tenant_id
				 AND COALESCE(q.deleted, FALSE) = FALSE
				WHERE a.tenant_id = r.tenant_id
				  AND a.project_ref = r.project_ref
				  AND COALESCE(q.executor_ref, '') <> ''
				ORDER BY a.created_at DESC, a.id DESC
				LIMIT 1
			) rex ON TRUE`
			regExecutorExpr = "COALESCE(rex.executor_ref, '')"
		}

		parts = append(parts, `
			SELECT 'regulation'::text AS type,
			       r.id,
			       COALESCE(r.title, '') AS title,
			       COALESCE(r.doc_no, '') AS subtitle,
			       COALESCE(r.status, '') AS status,
			       r.updated_at,
			       `+regExecutorExpr+` AS executor_ref,
			       `+regProjectExpr+` AS project_ref,
			       (`+regExecutorExpr+` <> '') AS has_executor
			FROM regulation_documents r
			`+regJoin+`
			WHERE `+regWhere)
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
	if execRef := strings.TrimSpace(query.ExecutorRef); execRef != "" {
		args = append(args, execRef)
		where = append(where, fmt.Sprintf("x.executor_ref = $%d", len(args)))
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

func (s *PGStore) GetLibraryChanges(ctx context.Context, tenantID int, libraryType string, id int64, query LibraryChangesQuery) (*LibraryChangesResult, error) {
	query = normalizeLibraryChangesQuery(query)
	normalizedType := normalizeLibraryType(libraryType)
	if query.ExecutorRef != "" {
		allowed, err := s.isLibraryVisibleToExecutor(ctx, tenantID, normalizedType, id, query.ExecutorRef, query.IncludeHistory, query.ValidOn)
		if err != nil {
			return nil, err
		}
		if !allowed {
			return nil, nil
		}
	}
	if normalizedType == "regulation" && !query.IncludeHistory {
		ok, err := s.regulationIsEffectiveOn(ctx, tenantID, id, query.ValidOn)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
	}

	var (
		items []*LibraryChangeItem
		found bool
		err   error
	)
	switch normalizedType {
	case "qualification":
		items, found, err = s.collectQualificationChanges(ctx, tenantID, id, query)
	case "standard":
		items, found, err = s.collectStandardChanges(ctx, tenantID, id, query)
	case "regulation":
		items, found, err = s.collectRegulationChanges(ctx, tenantID, id, query)
	default:
		return nil, fmt.Errorf("unsupported library type: %s", libraryType)
	}
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	filtered := make([]*LibraryChangeItem, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		changedAt := item.ChangedAt.UTC()
		if query.From != nil && changedAt.Before(query.From.UTC()) {
			continue
		}
		if query.To != nil && changedAt.After(query.To.UTC()) {
			continue
		}
		item.ChangedAt = changedAt
		item.Type = normalizeLibraryType(item.Type)
		item.EventType = strings.TrimSpace(item.EventType)
		item.Source = strings.TrimSpace(item.Source)
		item.Summary = strings.TrimSpace(item.Summary)
		if item.Type == "" {
			item.Type = normalizedType
		}
		if item.EventType == "" {
			item.EventType = "updated"
		}
		if item.Source == "" {
			item.Source = item.Type
		}
		filtered = append(filtered, item)
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].ChangedAt.Equal(filtered[j].ChangedAt) {
			if filtered[i].ID == filtered[j].ID {
				return filtered[i].EventType < filtered[j].EventType
			}
			return filtered[i].ID > filtered[j].ID
		}
		return filtered[i].ChangedAt.After(filtered[j].ChangedAt)
	})

	out := &LibraryChangesResult{
		Total:     len(filtered),
		Limit:     query.Limit,
		Offset:    query.Offset,
		Items:     make([]*LibraryChangeItem, 0),
		UpdatedAt: time.Now().UTC(),
	}
	if out.Total == 0 || out.Offset >= out.Total {
		return out, nil
	}

	end := out.Offset + out.Limit
	if end > out.Total {
		end = out.Total
	}
	out.Items = append(out.Items, filtered[out.Offset:end]...)
	return out, nil
}

func (s *PGStore) GetLibraryVersionDiff(ctx context.Context, tenantID int, libraryType string, id int64, query LibraryVersionDiffQuery) (*LibraryVersionDiffResult, error) {
	query = normalizeLibraryVersionDiffQuery(query)
	normalizedType := normalizeLibraryType(libraryType)
	if query.ExecutorRef != "" {
		allowed, err := s.isLibraryVisibleToExecutor(ctx, tenantID, normalizedType, id, query.ExecutorRef, true, nil)
		if err != nil {
			return nil, err
		}
		if !allowed {
			return nil, nil
		}
	}

	var (
		fromVersion *LibraryVersionDiffVersion
		toVersion   *LibraryVersionDiffVersion
		found       bool
		err         error
	)
	switch normalizedType {
	case "standard":
		fromVersion, toVersion, found, err = s.getStandardVersionDiff(ctx, tenantID, id, query.FromVersionID, query.ToVersionID)
	case "regulation":
		fromVersion, toVersion, found, err = s.getRegulationVersionDiff(ctx, tenantID, id, query.FromVersionID, query.ToVersionID)
	default:
		return nil, fmt.Errorf("unsupported library type for diff: %s", libraryType)
	}
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	changedFields := make([]string, 0, 12)
	summary := ""
	switch {
	case toVersion == nil:
		summary = "no versions found"
	case fromVersion == nil:
		summary = fmt.Sprintf("only one version available: v%d", toVersion.VersionNo)
		for key := range toVersion.Data {
			changedFields = append(changedFields, key)
		}
	default:
		changedFields = diffChangedFields(fromVersion.Data, toVersion.Data)
		if len(changedFields) == 0 {
			summary = fmt.Sprintf("v%d -> v%d, no field changes", fromVersion.VersionNo, toVersion.VersionNo)
		} else {
			summary = fmt.Sprintf("v%d -> v%d, %d fields changed", fromVersion.VersionNo, toVersion.VersionNo, len(changedFields))
		}
	}
	sort.Strings(changedFields)

	return &LibraryVersionDiffResult{
		Type:          normalizedType,
		LibraryID:     id,
		FromVersion:   fromVersion,
		ToVersion:     toVersion,
		ChangedFields: changedFields,
		Summary:       summary,
		UpdatedAt:     time.Now().UTC(),
	}, nil
}

func (s *PGStore) GetLibraryRelations(ctx context.Context, tenantID int, libraryType string, id int64, query LibraryRelationsQuery) (*LibraryRelationGraph, error) {
	query = normalizeLibraryRelationsQuery(query)
	normalizedType := normalizeLibraryType(libraryType)
	if query.ExecutorRef != "" {
		allowed, err := s.isLibraryVisibleToExecutor(ctx, tenantID, normalizedType, id, query.ExecutorRef, query.IncludeHistory, query.ValidOn)
		if err != nil {
			return nil, err
		}
		if !allowed {
			return nil, nil
		}
	}
	switch normalizedType {
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
	if err := s.appendProjectRegulationRelations(ctx, tenantID, builder, projectRefs, query.IncludeHistory, query.ValidOn, query.Limit*2); err != nil {
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
	versions, hasVersionTable, verErr := s.listDrawingVersions(ctx, tenantID, drawingNo, query.IncludeHistory, query.ValidOn, query.Limit)
	if verErr != nil {
		return nil, verErr
	}
	if !query.IncludeHistory && hasVersionTable && len(versions) == 0 {
		return nil, nil
	}
	for _, ver := range versions {
		if ver == nil {
			continue
		}
		verNodeRef := fmt.Sprintf("standard_version:%d", ver.ID)
		verLabel := fmt.Sprintf("v%d", ver.VersionNo)
		if s := strings.TrimSpace(ver.Status); s != "" {
			verLabel += " / " + s
		}
		builder.addNode(&LibraryRelationNode{
			NodeRef:  verNodeRef,
			NodeType: "standard_version",
			ID:       ver.ID,
			Label:    verLabel,
			Status:   strings.TrimSpace(ver.Status),
		})
		builder.addEdge(rootRef, verNodeRef, "has_version")
	}

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
		if err := s.appendProjectRegulationRelations(ctx, tenantID, builder, []string{projectRef}, query.IncludeHistory, query.ValidOn, query.Limit); err != nil {
			return nil, err
		}
	}
	return builder.build("standard", id), nil
}

func (s *PGStore) getRegulationRelations(ctx context.Context, tenantID int, id int64, query LibraryRelationsQuery) (*LibraryRelationGraph, error) {
	hasRegProjectRef, err := s.hasColumn(ctx, "regulation_documents", "project_ref")
	if err != nil {
		return nil, err
	}

	var title, docNo, status, category, projectRef string
	regQuery := `
		SELECT COALESCE(title, ''), COALESCE(doc_no, ''), COALESCE(status, ''), COALESCE(category, '')`
	if hasRegProjectRef {
		regQuery += `, COALESCE(project_ref, '')`
	}
	regQuery += `
		FROM regulation_documents
		WHERE tenant_id=$1 AND id=$2 AND COALESCE(deleted, FALSE)=FALSE
	`

	if hasRegProjectRef {
		err = s.db.QueryRowContext(ctx, regQuery, tenantID, id).Scan(&title, &docNo, &status, &category, &projectRef)
	} else {
		err = s.db.QueryRowContext(ctx, regQuery, tenantID, id).Scan(&title, &docNo, &status, &category)
	}
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !query.IncludeHistory {
		ok, checkErr := s.regulationIsEffectiveOn(ctx, tenantID, id, query.ValidOn)
		if checkErr != nil {
			return nil, checkErr
		}
		if !ok {
			return nil, nil
		}
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
		builder.addEdge(projectNodeRef, rootRef, "applies_regulation")

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
			for rows.Next() {
				var qualID int64
				var qualType, holderName, executorRef, qualStatus string
				if scanErr := rows.Scan(&qualID, &qualType, &holderName, &executorRef, &qualStatus); scanErr != nil {
					return nil, scanErr
				}
				qualNodeRef := fmt.Sprintf("qualification:%d", qualID)
				qualLabel := strings.TrimSpace(qualType)
				if qualLabel == "" {
					qualLabel = fmt.Sprintf("qualification-%d", qualID)
				}
				if holder := strings.TrimSpace(holderName); holder != "" {
					qualLabel += " / " + holder
				}
				builder.addNode(&LibraryRelationNode{
					NodeRef:     qualNodeRef,
					NodeType:    "qualification",
					LibraryType: "qualification",
					ID:          qualID,
					Label:       qualLabel,
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

	hasVersions, err := s.hasColumn(ctx, "regulation_versions", "id")
	if err != nil {
		return nil, err
	}
	if hasVersions {
		versionWhere := "tenant_id=$1 AND document_id=$2"
		queryArgs := []any{tenantID, id}
		limit := query.Limit
		if !query.IncludeHistory {
			effectiveAt := time.Now().UTC()
			if query.ValidOn != nil {
				effectiveAt = query.ValidOn.UTC()
			}
			queryArgs = append(queryArgs, effectiveAt)
			argIdx := len(queryArgs)
			versionWhere += fmt.Sprintf(" AND COALESCE(effective_from, '-infinity'::timestamptz) <= $%d AND COALESCE(effective_to, 'infinity'::timestamptz) >= $%d", argIdx, argIdx)
			limit = 1
		}
		queryArgs = append(queryArgs, limit)
		limitArg := len(queryArgs)
		rows, qErr := s.db.QueryContext(ctx, `
			SELECT id, version_no, effective_from, effective_to
			FROM regulation_versions
			WHERE `+versionWhere+`
			ORDER BY version_no DESC, id DESC
			LIMIT $`+strconv.Itoa(limitArg)+`
		`, queryArgs...)
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

func (s *PGStore) GetLibraryDetail(ctx context.Context, tenantID int, libraryType string, id int64, executorRef string, includeHistory bool, validOn *time.Time) (any, error) {
	normalizedType := normalizeLibraryType(libraryType)
	executorRef = strings.TrimSpace(executorRef)
	if executorRef != "" {
		allowed, err := s.isLibraryVisibleToExecutor(ctx, tenantID, normalizedType, id, executorRef, includeHistory, validOn)
		if err != nil {
			return nil, err
		}
		if !allowed {
			return nil, nil
		}
	}
	if normalizedType == "regulation" && !includeHistory {
		ok, err := s.regulationIsEffectiveOn(ctx, tenantID, id, validOn)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
	}
	switch normalizedType {
	case "qualification":
		return s.getQualificationLibraryDetail(ctx, tenantID, id)
	case "standard":
		return s.getEngineeringStandardDetail(ctx, tenantID, id, includeHistory, validOn)
	case "regulation":
		return s.getRegulationLibraryDetail(ctx, tenantID, id, includeHistory, validOn)
	default:
		return nil, fmt.Errorf("unsupported library type: %s", libraryType)
	}
}

func (s *PGStore) GetLibrariesQualityGate(ctx context.Context, tenantID int, sampleLimit int) (*LibrariesQualityGate, error) {
	sampleLimit = normalizePageLimit(sampleLimit)
	out := &LibrariesQualityGate{
		Status:    "GREEN",
		Checks:    make([]*LibraryQualityCheck, 0, 8),
		UpdatedAt: time.Now().UTC(),
	}

	appendCheck := func(check *LibraryQualityCheck) {
		if check == nil {
			return
		}
		check.Code = strings.TrimSpace(check.Code)
		check.Name = strings.TrimSpace(check.Name)
		check.Severity = strings.ToUpper(strings.TrimSpace(check.Severity))
		check.Status = strings.ToUpper(strings.TrimSpace(check.Status))
		check.Message = strings.TrimSpace(check.Message)
		if check.Name == "" {
			check.Name = check.Code
		}
		if check.Severity == "" {
			check.Severity = "ERROR"
		}
		if check.Status == "" {
			check.Status = "PASS"
		}
		if check.Samples == nil {
			check.Samples = make([]map[string]any, 0)
		}
		out.Checks = append(out.Checks, check)
		if check.Status == "FAIL" {
			if check.Severity == "WARN" {
				out.WarningChecks++
			} else {
				out.FailedChecks++
			}
		}
	}

	tableExists := func(tableName string) (bool, error) {
		return s.hasColumn(ctx, tableName, "id")
	}

	hasAssignments, err := tableExists("qualification_assignments")
	if err != nil {
		return nil, err
	}
	hasQualifications, err := tableExists("qualifications")
	if err != nil {
		return nil, err
	}
	hasProjects, err := s.hasColumn(ctx, "project_nodes", "ref")
	if err != nil {
		return nil, err
	}
	hasRegDocs, err := tableExists("regulation_documents")
	if err != nil {
		return nil, err
	}
	hasRegVersions, err := tableExists("regulation_versions")
	if err != nil {
		return nil, err
	}

	qualDeletedExpr := ""
	if hasQualifications {
		hasQualDeleted, qErr := s.hasColumn(ctx, "qualifications", "deleted")
		if qErr != nil {
			return nil, qErr
		}
		if hasQualDeleted {
			qualDeletedExpr = " AND COALESCE(q.deleted, FALSE)=FALSE"
		}
	}

	regDeletedExpr := ""
	if hasRegDocs {
		hasRegDeleted, rErr := s.hasColumn(ctx, "regulation_documents", "deleted")
		if rErr != nil {
			return nil, rErr
		}
		if hasRegDeleted {
			regDeletedExpr = " AND COALESCE(deleted, FALSE)=FALSE"
		}
	}

	projectDeletedExpr := ""
	if hasProjects {
		hasProjectDeleted, pErr := s.hasColumn(ctx, "project_nodes", "deleted")
		if pErr != nil {
			return nil, pErr
		}
		if hasProjectDeleted {
			projectDeletedExpr = " AND COALESCE(p.deleted, FALSE)=FALSE"
		}
	}

	if !hasAssignments || !hasQualifications {
		appendCheck(&LibraryQualityCheck{
			Code:     "ORPHAN_ASSIGNMENT_QUALIFICATION",
			Name:     "孤儿关联(资质)",
			Severity: "ERROR",
			Status:   "SKIP",
			Message:  "qualification_assignments 或 qualifications 表缺失，跳过检查",
		})
	} else {
		var count int
		err = s.db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM qualification_assignments a
			LEFT JOIN qualifications q
			  ON q.id = a.qualification_id
			 AND q.tenant_id = a.tenant_id`+qualDeletedExpr+`
			WHERE a.tenant_id=$1
			  AND q.id IS NULL
		`, tenantID).Scan(&count)
		if err != nil {
			return nil, err
		}
		check := &LibraryQualityCheck{
			Code:     "ORPHAN_ASSIGNMENT_QUALIFICATION",
			Name:     "孤儿关联(资质)",
			Severity: "ERROR",
			Status:   "PASS",
			Count:    count,
			Message:  "qualification_assignments.qualification_id 未匹配有效资质",
			Samples:  make([]map[string]any, 0),
		}
		if count > 0 {
			check.Status = "FAIL"
			rows, qErr := s.db.QueryContext(ctx, `
				SELECT a.id, a.qualification_id, COALESCE(a.project_ref, ''), COALESCE(a.status, '')
				FROM qualification_assignments a
				LEFT JOIN qualifications q
				  ON q.id = a.qualification_id
				 AND q.tenant_id = a.tenant_id`+qualDeletedExpr+`
				WHERE a.tenant_id=$1
				  AND q.id IS NULL
				ORDER BY a.id DESC
				LIMIT $2
			`, tenantID, sampleLimit)
			if qErr != nil {
				return nil, qErr
			}
			defer rows.Close()
			for rows.Next() {
				var rowID, qualificationID int64
				var projectRef, status string
				if scanErr := rows.Scan(&rowID, &qualificationID, &projectRef, &status); scanErr != nil {
					return nil, scanErr
				}
				check.Samples = append(check.Samples, map[string]any{
					"id":               rowID,
					"qualification_id": qualificationID,
					"library_type":     "qualification",
					"library_id":       qualificationID,
					"project_ref":      projectRef,
					"status":           status,
				})
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				return nil, rowsErr
			}
		}
		appendCheck(check)
	}

	if !hasAssignments || !hasProjects {
		appendCheck(&LibraryQualityCheck{
			Code:     "ORPHAN_ASSIGNMENT_PROJECT",
			Name:     "孤儿关联(项目)",
			Severity: "ERROR",
			Status:   "SKIP",
			Message:  "qualification_assignments 或 project_nodes 表缺失，跳过检查",
		})
	} else {
		var count int
		err = s.db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM qualification_assignments a
			LEFT JOIN project_nodes p
			  ON p.tenant_id = a.tenant_id
			 AND p.ref = a.project_ref`+projectDeletedExpr+`
			WHERE a.tenant_id=$1
			  AND COALESCE(a.project_ref, '') <> ''
			  AND p.ref IS NULL
		`, tenantID).Scan(&count)
		if err != nil {
			return nil, err
		}
		check := &LibraryQualityCheck{
			Code:     "ORPHAN_ASSIGNMENT_PROJECT",
			Name:     "孤儿关联(项目)",
			Severity: "ERROR",
			Status:   "PASS",
			Count:    count,
			Message:  "qualification_assignments.project_ref 未匹配有效项目",
			Samples:  make([]map[string]any, 0),
		}
		if count > 0 {
			check.Status = "FAIL"
			rows, qErr := s.db.QueryContext(ctx, `
				SELECT a.id, a.qualification_id, COALESCE(a.project_ref, ''), COALESCE(a.status, '')
				FROM qualification_assignments a
				LEFT JOIN project_nodes p
				  ON p.tenant_id = a.tenant_id
				 AND p.ref = a.project_ref`+projectDeletedExpr+`
				WHERE a.tenant_id=$1
				  AND COALESCE(a.project_ref, '') <> ''
				  AND p.ref IS NULL
				ORDER BY a.id DESC
				LIMIT $2
			`, tenantID, sampleLimit)
			if qErr != nil {
				return nil, qErr
			}
			defer rows.Close()
			for rows.Next() {
				var rowID, qualificationID int64
				var projectRef, status string
				if scanErr := rows.Scan(&rowID, &qualificationID, &projectRef, &status); scanErr != nil {
					return nil, scanErr
				}
				check.Samples = append(check.Samples, map[string]any{
					"id":               rowID,
					"qualification_id": qualificationID,
					"library_type":     "qualification",
					"library_id":       qualificationID,
					"project_ref":      projectRef,
					"status":           status,
				})
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				return nil, rowsErr
			}
		}
		appendCheck(check)
	}

	if !hasRegDocs {
		appendCheck(&LibraryQualityCheck{
			Code:     "DUPLICATE_REGULATION_DOC_NO",
			Name:     "重复文号",
			Severity: "ERROR",
			Status:   "SKIP",
			Message:  "regulation_documents 表缺失，跳过检查",
		})
	} else {
		var count int
		err = s.db.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(dup.cnt), 0)::int
			FROM (
				SELECT COUNT(*)::int AS cnt
				FROM regulation_documents
				WHERE tenant_id=$1`+regDeletedExpr+`
				  AND COALESCE(BTRIM(doc_no), '') <> ''
				GROUP BY COALESCE(doc_no, '')
				HAVING COUNT(*) > 1
			) dup
		`, tenantID).Scan(&count)
		if err != nil {
			return nil, err
		}
		check := &LibraryQualityCheck{
			Code:     "DUPLICATE_REGULATION_DOC_NO",
			Name:     "重复文号",
			Severity: "ERROR",
			Status:   "PASS",
			Count:    count,
			Message:  "法规库存在重复 doc_no",
			Samples:  make([]map[string]any, 0),
		}
		if count > 0 {
			check.Status = "FAIL"
			rows, qErr := s.db.QueryContext(ctx, `
				SELECT COALESCE(doc_no, ''), COUNT(*)::int AS cnt, MAX(id)::bigint AS sample_id
				FROM regulation_documents
				WHERE tenant_id=$1`+regDeletedExpr+`
				  AND COALESCE(BTRIM(doc_no), '') <> ''
				GROUP BY COALESCE(doc_no, '')
				HAVING COUNT(*) > 1
				ORDER BY cnt DESC, COALESCE(doc_no, '') ASC
				LIMIT $2
			`, tenantID, sampleLimit)
			if qErr != nil {
				return nil, qErr
			}
			defer rows.Close()
			for rows.Next() {
				var docNo string
				var dupCount int
				var sampleID int64
				if scanErr := rows.Scan(&docNo, &dupCount, &sampleID); scanErr != nil {
					return nil, scanErr
				}
				check.Samples = append(check.Samples, map[string]any{
					"doc_no":       docNo,
					"count":        dupCount,
					"library_type": "regulation",
					"library_id":   sampleID,
				})
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				return nil, rowsErr
			}
		}
		appendCheck(check)
	}

	if !hasQualifications {
		appendCheck(&LibraryQualityCheck{
			Code:     "EMPTY_EXECUTOR_REF",
			Name:     "空执行体编码",
			Severity: "ERROR",
			Status:   "SKIP",
			Message:  "qualifications 表缺失，跳过检查",
		})
	} else {
		qualWhere := "tenant_id=$1"
		if qualDeletedExpr != "" {
			qualWhere += " AND COALESCE(deleted, FALSE)=FALSE"
		}
		var count int
		err = s.db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM qualifications
			WHERE `+qualWhere+`
			  AND COALESCE(BTRIM(executor_ref), '') = ''
		`, tenantID).Scan(&count)
		if err != nil {
			return nil, err
		}
		check := &LibraryQualityCheck{
			Code:     "EMPTY_EXECUTOR_REF",
			Name:     "空执行体编码",
			Severity: "ERROR",
			Status:   "PASS",
			Count:    count,
			Message:  "资质记录 executor_ref 为空",
			Samples:  make([]map[string]any, 0),
		}
		if count > 0 {
			check.Status = "FAIL"
			rows, qErr := s.db.QueryContext(ctx, `
				SELECT id, COALESCE(qual_type, ''), COALESCE(holder_name, ''), COALESCE(status, '')
				FROM qualifications
				WHERE `+qualWhere+`
				  AND COALESCE(BTRIM(executor_ref), '') = ''
				ORDER BY updated_at DESC, id DESC
				LIMIT $2
			`, tenantID, sampleLimit)
			if qErr != nil {
				return nil, qErr
			}
			defer rows.Close()
			for rows.Next() {
				var id int64
				var qualType, holderName, status string
				if scanErr := rows.Scan(&id, &qualType, &holderName, &status); scanErr != nil {
					return nil, scanErr
				}
				check.Samples = append(check.Samples, map[string]any{
					"id":           id,
					"qual_type":    qualType,
					"holder_name":  holderName,
					"status":       status,
					"library_type": "qualification",
					"library_id":   id,
				})
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				return nil, rowsErr
			}
		}
		appendCheck(check)
	}

	if !hasRegDocs || !hasRegVersions {
		appendCheck(&LibraryQualityCheck{
			Code:     "REGULATION_WITHOUT_VERSION",
			Name:     "法规无版本",
			Severity: "ERROR",
			Status:   "SKIP",
			Message:  "regulation_documents 或 regulation_versions 表缺失，跳过检查",
		})
	} else {
		var count int
		err = s.db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM (
				SELECT d.id
				FROM regulation_documents d
				LEFT JOIN regulation_versions v
				  ON v.tenant_id=d.tenant_id
				 AND v.document_id=d.id
				WHERE d.tenant_id=$1`+strings.ReplaceAll(regDeletedExpr, "deleted", "d.deleted")+`
				GROUP BY d.id
				HAVING COUNT(v.id)=0
			) x
		`, tenantID).Scan(&count)
		if err != nil {
			return nil, err
		}
		check := &LibraryQualityCheck{
			Code:     "REGULATION_WITHOUT_VERSION",
			Name:     "法规无版本",
			Severity: "ERROR",
			Status:   "PASS",
			Count:    count,
			Message:  "法规文档缺少 regulation_versions 版本记录",
			Samples:  make([]map[string]any, 0),
		}
		if count > 0 {
			check.Status = "FAIL"
			rows, qErr := s.db.QueryContext(ctx, `
				SELECT d.id, COALESCE(d.doc_no, ''), COALESCE(d.title, '')
				FROM regulation_documents d
				LEFT JOIN regulation_versions v
				  ON v.tenant_id=d.tenant_id
				 AND v.document_id=d.id
				WHERE d.tenant_id=$1`+strings.ReplaceAll(regDeletedExpr, "deleted", "d.deleted")+`
				GROUP BY d.id, d.doc_no, d.title
				HAVING COUNT(v.id)=0
				ORDER BY d.id DESC
				LIMIT $2
			`, tenantID, sampleLimit)
			if qErr != nil {
				return nil, qErr
			}
			defer rows.Close()
			for rows.Next() {
				var id int64
				var docNo, title string
				if scanErr := rows.Scan(&id, &docNo, &title); scanErr != nil {
					return nil, scanErr
				}
				check.Samples = append(check.Samples, map[string]any{
					"id":           id,
					"doc_no":       docNo,
					"title":        title,
					"library_type": "regulation",
					"library_id":   id,
				})
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				return nil, rowsErr
			}
		}
		appendCheck(check)
	}

	out.TotalChecks = len(out.Checks)
	switch {
	case out.FailedChecks > 0:
		out.Status = "RED"
	case out.WarningChecks > 0:
		out.Status = "YELLOW"
	default:
		out.Status = "GREEN"
	}
	return out, nil
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
		Regulations:    make([]*RegulationLibraryItem, 0),
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

	if len(projectRefSet) > 0 {
		projectRefs := make([]string, 0, len(projectRefSet))
		for ref := range projectRefSet {
			projectRefs = append(projectRefs, ref)
		}

		hasRegDocs, regErr := s.hasColumn(ctx, "regulation_documents", "id")
		if regErr != nil {
			return nil, regErr
		}
		hasRegProjectRef := false
		if hasRegDocs {
			hasRegProjectRef, regErr = s.hasColumn(ctx, "regulation_documents", "project_ref")
			if regErr != nil {
				return nil, regErr
			}
		}
		if hasRegDocs && hasRegProjectRef {
			hasRegDeleted, delErr := s.hasColumn(ctx, "regulation_documents", "deleted")
			if delErr != nil {
				return nil, delErr
			}
			hasRegVersions, verErr := s.hasColumn(ctx, "regulation_versions", "id")
			if verErr != nil {
				return nil, verErr
			}

			args := make([]any, 0, len(projectRefs)+3)
			args = append(args, tenantID)
			holders := make([]string, 0, len(projectRefs))
			for i, ref := range projectRefs {
				args = append(args, ref)
				holders = append(holders, fmt.Sprintf("$%d", i+2))
			}
			whereClause := "r.tenant_id=$1 AND r.project_ref IN (" + strings.Join(holders, ",") + ")"
			if hasRegDeleted {
				whereClause += " AND COALESCE(r.deleted, FALSE)=FALSE"
			}
			if hasRegVersions {
				args = append(args, time.Now().UTC())
				validArg := len(args)
				whereClause += fmt.Sprintf(` AND EXISTS (
					SELECT 1
					FROM regulation_versions rv
					WHERE rv.tenant_id=r.tenant_id
					  AND rv.document_id=r.id
					  AND COALESCE(rv.effective_from, '-infinity'::timestamptz) <= $%d
					  AND COALESCE(rv.effective_to, 'infinity'::timestamptz) >= $%d
				)`, validArg, validArg)
			}
			args = append(args, drawingLimit)
			limitArg := len(args)
			regRows, qErr := s.db.QueryContext(ctx, `
				SELECT r.id, COALESCE(r.doc_no, ''), COALESCE(r.title, ''), COALESCE(r.category, ''), COALESCE(r.publisher, ''),
				       COALESCE(r.status, ''), r.updated_at
				FROM regulation_documents r
				WHERE `+whereClause+`
				ORDER BY r.updated_at DESC, r.id DESC
				LIMIT $`+strconv.Itoa(limitArg), args...)
			if qErr != nil {
				if !isMissingRelationErr(qErr, "regulation_documents", "regulation_versions") {
					return nil, qErr
				}
			} else {
				defer regRows.Close()
				for regRows.Next() {
					item := &RegulationLibraryItem{}
					if scanErr := regRows.Scan(
						&item.ID,
						&item.DocNo,
						&item.Title,
						&item.Category,
						&item.Publisher,
						&item.Status,
						&item.UpdatedAt,
					); scanErr != nil {
						return nil, scanErr
					}
					out.Regulations = append(out.Regulations, item)
				}
				if regRowsErr := regRows.Err(); regRowsErr != nil {
					return nil, regRowsErr
				}
			}
		}
	}
	if len(out.Regulations) == 0 {
		hasRegDocs, regErr := s.hasColumn(ctx, "regulation_documents", "id")
		if regErr != nil {
			return nil, regErr
		}
		if hasRegDocs {
			hasRegDeleted, delErr := s.hasColumn(ctx, "regulation_documents", "deleted")
			if delErr != nil {
				return nil, delErr
			}
			hasRegVersions, verErr := s.hasColumn(ctx, "regulation_versions", "id")
			if verErr != nil {
				return nil, verErr
			}
			args := []any{tenantID}
			whereClause := "r.tenant_id=$1"
			if hasRegDeleted {
				whereClause += " AND COALESCE(r.deleted, FALSE)=FALSE"
			}
			if hasRegVersions {
				args = append(args, time.Now().UTC())
				validArg := len(args)
				whereClause += fmt.Sprintf(` AND EXISTS (
					SELECT 1
					FROM regulation_versions rv
					WHERE rv.tenant_id=r.tenant_id
					  AND rv.document_id=r.id
					  AND COALESCE(rv.effective_from, '-infinity'::timestamptz) <= $%d
					  AND COALESCE(rv.effective_to, 'infinity'::timestamptz) >= $%d
				)`, validArg, validArg)
			}
			args = append(args, drawingLimit)
			limitArg := len(args)
			rows, qErr := s.db.QueryContext(ctx, `
				SELECT r.id, COALESCE(r.doc_no, ''), COALESCE(r.title, ''), COALESCE(r.category, ''), COALESCE(r.publisher, ''),
				       COALESCE(r.status, ''), r.updated_at
				FROM regulation_documents r
				WHERE `+whereClause+`
				ORDER BY r.updated_at DESC, r.id DESC
				LIMIT $`+strconv.Itoa(limitArg), args...)
			if qErr != nil {
				if !isMissingRelationErr(qErr, "regulation_documents", "regulation_versions") {
					return nil, qErr
				}
			} else {
				defer rows.Close()
				for rows.Next() {
					item := &RegulationLibraryItem{}
					if scanErr := rows.Scan(
						&item.ID,
						&item.DocNo,
						&item.Title,
						&item.Category,
						&item.Publisher,
						&item.Status,
						&item.UpdatedAt,
					); scanErr != nil {
						return nil, scanErr
					}
					out.Regulations = append(out.Regulations, item)
				}
				if rowsErr := rows.Err(); rowsErr != nil {
					return nil, rowsErr
				}
			}
		}
	}
	out.RegulationCount = len(out.Regulations)

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

func (s *PGStore) getEngineeringStandardDetail(ctx context.Context, tenantID int, id int64, includeHistory bool, validOn *time.Time) (*EngineeringStandardDetail, error) {
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
		Versions:    make([]*StandardVersionDetail, 0),
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

	versions, hasVersionTable, verErr := s.listDrawingVersions(ctx, tenantID, detail.Item.DrawingNo, includeHistory, validOn, 200)
	if verErr != nil {
		return nil, verErr
	}
	if !includeHistory && hasVersionTable && len(versions) == 0 {
		return nil, nil
	}
	detail.Versions = versions

	return detail, nil
}

func (s *PGStore) getRegulationLibraryDetail(ctx context.Context, tenantID int, id int64, includeHistory bool, validOn *time.Time) (*RegulationLibraryDetail, error) {
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

	versionWhere := "tenant_id=$1 AND document_id=$2"
	verArgs := []any{tenantID, id}
	if !includeHistory {
		effectiveAt := time.Now().UTC()
		if validOn != nil {
			effectiveAt = validOn.UTC()
		}
		verArgs = append(verArgs, effectiveAt)
		argIdx := len(verArgs)
		versionWhere += fmt.Sprintf(" AND COALESCE(effective_from, '-infinity'::timestamptz) <= $%d AND COALESCE(effective_to, 'infinity'::timestamptz) >= $%d", argIdx, argIdx)
	}
	verSQL := `
		SELECT id, version_no, effective_from, effective_to, published_at,
		       COALESCE(content_hash, ''), COALESCE(attachment_url, ''), COALESCE(source_note, '')
		FROM regulation_versions
		WHERE ` + versionWhere + `
		ORDER BY version_no DESC, id DESC`
	if !includeHistory {
		verSQL += " LIMIT 1"
	}
	rows, verErr := s.db.QueryContext(ctx, verSQL, verArgs...)
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

func (s *PGStore) listQualificationLibrary(ctx context.Context, tenantID, limit, offset int, executorRef string) ([]*QualificationLibraryItem, int, error) {
	executorRef = strings.TrimSpace(executorRef)
	whereClause := "tenant_id=$1 AND deleted=FALSE"
	countArgs := []any{tenantID}
	if executorRef != "" {
		whereClause += " AND COALESCE(BTRIM(executor_ref), '') = $2"
		countArgs = append(countArgs, executorRef)
	}

	var total int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM qualifications
		WHERE `+whereClause+`
	`, countArgs...).Scan(&total); err != nil {
		return nil, 0, err
	}

	listArgs := append([]any{}, countArgs...)
	listArgs = append(listArgs, limit, offset)
	limitArg := len(countArgs) + 1
	offsetArg := len(countArgs) + 2
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, qual_type, holder_name, COALESCE(executor_ref, ''), status,
		       COALESCE(TO_CHAR(valid_until,'YYYY-MM-DD'), ''), updated_at
		FROM qualifications
		WHERE `+whereClause+`
		ORDER BY updated_at DESC, id DESC
		LIMIT $`+strconv.Itoa(limitArg)+` OFFSET $`+strconv.Itoa(offsetArg)+`
	`, listArgs...)
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

func (s *PGStore) listEngineeringStandards(ctx context.Context, tenantID, limit, offset int, executorRef string, includeHistory bool, validOn *time.Time) ([]*EngineeringStandardItem, int, error) {
	executorRef = strings.TrimSpace(executorRef)
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

	drawingNoExpr := "COALESCE(NULLIF(d.num,''), 'drawing-' || d.id::text)"
	if hasDrawingNo {
		drawingNoExpr = "COALESCE(NULLIF(d.drawing_no,''), NULLIF(d.num,''), 'drawing-' || d.id::text)"
	}

	whereClause := "d.tenant_id=$1"
	if hasDeleted {
		whereClause += " AND d.deleted=FALSE"
	}
	whereArgs := []any{tenantID}
	hasDrawingVersions, err := s.hasColumn(ctx, "drawing_versions", "id")
	if err != nil {
		return nil, 0, err
	}
	hasDrawingVersionRows := false
	if hasDrawingVersions {
		hasDrawingVersionRows, err = s.hasTenantRows(ctx, "drawing_versions", tenantID)
		if err != nil {
			return nil, 0, err
		}
	}
	if !includeHistory && hasDrawingVersions && hasDrawingVersionRows {
		effectiveAt := time.Now().UTC()
		if validOn != nil {
			effectiveAt = validOn.UTC()
		}
		whereArgs = append(whereArgs, effectiveAt)
		argIdx := len(whereArgs)
		whereClause += fmt.Sprintf(` AND EXISTS (
			SELECT 1
			FROM drawing_versions dv
			WHERE dv.tenant_id=d.tenant_id
			  AND dv.drawing_no=`+drawingNoExpr+`
			  AND COALESCE(dv.status, '') <> 'REVOKED'
			  AND dv.created_at <= $%d
		)`, argIdx)
	}
	if executorRef != "" {
		hasAssignments, assignErr := s.hasColumn(ctx, "qualification_assignments", "id")
		if assignErr != nil {
			return nil, 0, assignErr
		}
		hasQualifications, qualErr := s.hasColumn(ctx, "qualifications", "id")
		if qualErr != nil {
			return nil, 0, qualErr
		}
		if !hasAssignments || !hasQualifications {
			return make([]*EngineeringStandardItem, 0), 0, nil
		}
		whereArgs = append(whereArgs, executorRef)
		execArgIdx := len(whereArgs)
		whereClause += fmt.Sprintf(` AND EXISTS (
			SELECT 1
			FROM qualification_assignments a
			JOIN qualifications q
			  ON q.id = a.qualification_id
			 AND q.tenant_id = a.tenant_id
			 AND COALESCE(q.deleted, FALSE)=FALSE
			WHERE a.tenant_id = d.tenant_id
			  AND a.project_ref = d.project_ref
			  AND COALESCE(BTRIM(q.executor_ref), '') = $%d
		)`, execArgIdx)
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
		whereArgs...).Scan(&total); err != nil {
		return nil, 0, err
	}

	listArgs := append([]any{}, whereArgs...)
	listArgs = append(listArgs, limit, offset)
	limitArg := len(whereArgs) + 1
	offsetArg := len(whereArgs) + 2
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
		LIMIT $`+strconv.Itoa(limitArg)+` OFFSET $`+strconv.Itoa(offsetArg)+`
	`, listArgs...)
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

func (s *PGStore) listRegulations(ctx context.Context, tenantID, limit, offset int, executorRef string, includeHistory bool, validOn *time.Time) ([]*RegulationLibraryItem, int, error) {
	executorRef = strings.TrimSpace(executorRef)
	whereClause := "r.tenant_id=$1 AND r.deleted=FALSE"
	args := []any{tenantID}
	if executorRef != "" {
		hasRegExecutorRef, err := s.hasColumn(ctx, "regulation_documents", "executor_ref")
		if err != nil {
			return nil, 0, err
		}
		if hasRegExecutorRef {
			whereClause += " AND COALESCE(BTRIM(r.executor_ref), '') = $2"
			args = append(args, executorRef)
		} else {
			hasRegProjectRef, colErr := s.hasColumn(ctx, "regulation_documents", "project_ref")
			if colErr != nil {
				return nil, 0, colErr
			}
			hasAssignments, assignErr := s.hasColumn(ctx, "qualification_assignments", "id")
			if assignErr != nil {
				return nil, 0, assignErr
			}
			hasQualifications, qualErr := s.hasColumn(ctx, "qualifications", "id")
			if qualErr != nil {
				return nil, 0, qualErr
			}
			if !hasRegProjectRef || !hasAssignments || !hasQualifications {
				return make([]*RegulationLibraryItem, 0), 0, nil
			}
			whereClause += ` AND EXISTS (
				SELECT 1
				FROM qualification_assignments a
				JOIN qualifications q
				  ON q.id = a.qualification_id
				 AND q.tenant_id = a.tenant_id
				 AND COALESCE(q.deleted, FALSE)=FALSE
				WHERE a.tenant_id = r.tenant_id
				  AND a.project_ref = r.project_ref
				  AND COALESCE(BTRIM(q.executor_ref), '') = $2
			)`
			args = append(args, executorRef)
		}
	}
	if !includeHistory {
		hasRegVersions, err := s.hasColumn(ctx, "regulation_versions", "id")
		if err != nil {
			return nil, 0, err
		}
		if hasRegVersions {
			effectiveAt := time.Now().UTC()
			if validOn != nil {
				effectiveAt = validOn.UTC()
			}
			args = append(args, effectiveAt)
			argIdx := len(args)
			whereClause += fmt.Sprintf(` AND EXISTS (
				SELECT 1
				FROM regulation_versions rv
				WHERE rv.tenant_id = r.tenant_id
				  AND rv.document_id = r.id
				  AND COALESCE(rv.effective_from, '-infinity'::timestamptz) <= $%d
				  AND COALESCE(rv.effective_to, 'infinity'::timestamptz) >= $%d
			)`, argIdx, argIdx)
		}
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM regulation_documents r
		WHERE `+whereClause+`
	`, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	listArgs := append([]any{}, args...)
	listArgs = append(listArgs, limit, offset)
	limitArg := len(args) + 1
	offsetArg := len(args) + 2
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, COALESCE(doc_no, ''), title, COALESCE(category, ''), COALESCE(publisher, ''), status, updated_at
		FROM regulation_documents r
		WHERE `+whereClause+`
		ORDER BY updated_at DESC, id DESC
		LIMIT $`+strconv.Itoa(limitArg)+` OFFSET $`+strconv.Itoa(offsetArg)+`
	`, listArgs...)
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

func (s *PGStore) collectQualificationChanges(ctx context.Context, tenantID int, id int64, query LibraryChangesQuery) ([]*LibraryChangeItem, bool, error) {
	var qualType, holderName, status, executorRef, certNo string
	var updatedAt time.Time
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(qual_type, ''), COALESCE(holder_name, ''), COALESCE(status, ''),
		       COALESCE(executor_ref, ''), COALESCE(cert_no, ''), updated_at
		FROM qualifications
		WHERE tenant_id=$1 AND id=$2 AND COALESCE(deleted, FALSE)=FALSE
	`, tenantID, id).Scan(&qualType, &holderName, &status, &executorRef, &certNo, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	out := make([]*LibraryChangeItem, 0, 32)
	summaryParts := make([]string, 0, 3)
	if v := strings.TrimSpace(qualType); v != "" {
		summaryParts = append(summaryParts, v)
	}
	if v := strings.TrimSpace(holderName); v != "" {
		summaryParts = append(summaryParts, v)
	}
	if v := strings.TrimSpace(status); v != "" {
		summaryParts = append(summaryParts, "status="+v)
	}
	out = append(out, &LibraryChangeItem{
		Type:      "qualification",
		ID:        id,
		EventType: "qualification.updated",
		Source:    "qualifications",
		Summary:   strings.Join(summaryParts, " / "),
		ChangedAt: updatedAt.UTC(),
		Payload: map[string]any{
			"id":           id,
			"qual_type":    qualType,
			"holder_name":  holderName,
			"status":       status,
			"executor_ref": executorRef,
			"cert_no":      certNo,
		},
	})

	hasAssignments, err := s.hasColumn(ctx, "qualification_assignments", "id")
	if err != nil {
		return nil, false, err
	}
	if !hasAssignments {
		return out, true, nil
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, COALESCE(project_ref, ''), COALESCE(status, ''), updated_at, released_at
		FROM qualification_assignments
		WHERE tenant_id=$1 AND qualification_id=$2
		ORDER BY updated_at DESC, id DESC
		LIMIT 500
	`, tenantID, id)
	if err != nil {
		if isMissingRelationErr(err, "qualification_assignments") {
			return out, true, nil
		}
		return nil, false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			assignID   int64
			projectRef string
			assignStat string
			assignAt   time.Time
			releasedAt sql.NullTime
		)
		if scanErr := rows.Scan(&assignID, &projectRef, &assignStat, &assignAt, &releasedAt); scanErr != nil {
			return nil, false, scanErr
		}
		eventType := "qualification.assignment.updated"
		if strings.EqualFold(assignStat, "ACTIVE") {
			eventType = "qualification.assignment.active"
		}
		if strings.EqualFold(assignStat, "RELEASED") || releasedAt.Valid {
			eventType = "qualification.assignment.released"
		}
		changedAt := assignAt.UTC()
		if releasedAt.Valid && releasedAt.Time.After(changedAt) {
			changedAt = releasedAt.Time.UTC()
		}
		out = append(out, &LibraryChangeItem{
			Type:      "qualification",
			ID:        assignID,
			EventType: eventType,
			Source:    "qualification_assignments",
			Summary:   fmt.Sprintf("project=%s / status=%s", strings.TrimSpace(projectRef), strings.TrimSpace(assignStat)),
			ChangedAt: changedAt,
			Payload: map[string]any{
				"assignment_id":    assignID,
				"qualification_id": id,
				"project_ref":      projectRef,
				"status":           assignStat,
				"released_at": func() *time.Time {
					if !releasedAt.Valid {
						return nil
					}
					t := releasedAt.Time.UTC()
					return &t
				}(),
			},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	return out, true, nil
}

func (s *PGStore) collectStandardChanges(ctx context.Context, tenantID int, id int64, query LibraryChangesQuery) ([]*LibraryChangeItem, bool, error) {
	hasDeleted, err := s.hasColumn(ctx, "drawings", "deleted")
	if err != nil {
		return nil, false, err
	}
	drawingNoExpr, statusExpr, err := s.buildDrawingDisplayExpressions(ctx)
	if err != nil {
		return nil, false, err
	}
	whereClause := "d.tenant_id=$1 AND d.id=$2"
	if hasDeleted {
		whereClause += " AND d.deleted=FALSE"
	}

	var (
		drawingNo  string
		major      string
		status     string
		projectRef string
		updatedAt  time.Time
	)
	err = s.db.QueryRowContext(ctx, `
		SELECT `+drawingNoExpr+`, COALESCE(d.major, ''), `+statusExpr+`, COALESCE(d.project_ref, ''), d.updated_at
		FROM drawings d
		WHERE `+whereClause, tenantID, id).Scan(&drawingNo, &major, &status, &projectRef, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	versions, hasVersionTable, verErr := s.listDrawingVersions(ctx, tenantID, drawingNo, query.IncludeHistory, query.ValidOn, 500)
	if verErr != nil {
		return nil, false, verErr
	}
	if !query.IncludeHistory && hasVersionTable && len(versions) == 0 {
		return nil, false, nil
	}

	out := make([]*LibraryChangeItem, 0, 64)
	out = append(out, &LibraryChangeItem{
		Type:      "standard",
		ID:        id,
		EventType: "standard.updated",
		Source:    "drawings",
		Summary: strings.TrimSpace(strings.Join([]string{
			strings.TrimSpace(drawingNo),
			strings.TrimSpace(major),
			"status=" + strings.TrimSpace(status),
		}, " / ")),
		ChangedAt: updatedAt.UTC(),
		Payload: map[string]any{
			"id":          id,
			"drawing_no":  drawingNo,
			"major":       major,
			"status":      status,
			"project_ref": projectRef,
		},
	})

	hasAttachments, err := s.hasColumn(ctx, "drawing_attachments", "id")
	if err != nil {
		return nil, false, err
	}
	if hasAttachments {
		rows, qErr := s.db.QueryContext(ctx, `
			SELECT id, COALESCE(source_table, ''), COALESCE(name, ''), COALESCE(url, ''),
			       COALESCE(version, ''), COALESCE(state, 0), approve_date, updated_at
			FROM drawing_attachments
			WHERE tenant_id=$1 AND drawing_id=$2
			ORDER BY updated_at DESC, id DESC
			LIMIT 500
		`, tenantID, id)
		if qErr != nil {
			if !isMissingRelationErr(qErr, "drawing_attachments") {
				return nil, false, qErr
			}
		} else {
			defer rows.Close()
			for rows.Next() {
				var (
					attID       int64
					sourceTable string
					name        string
					url         string
					version     string
					state       int
					approveDate sql.NullTime
					attUpdated  time.Time
				)
				if scanErr := rows.Scan(&attID, &sourceTable, &name, &url, &version, &state, &approveDate, &attUpdated); scanErr != nil {
					return nil, false, scanErr
				}
				changedAt := attUpdated.UTC()
				if approveDate.Valid && approveDate.Time.After(changedAt) {
					changedAt = approveDate.Time.UTC()
				}
				out = append(out, &LibraryChangeItem{
					Type:      "standard",
					ID:        attID,
					EventType: "standard.attachment.updated",
					Source:    "drawing_attachments",
					Summary:   fmt.Sprintf("attachment=%s / version=%s / state=%d", strings.TrimSpace(name), strings.TrimSpace(version), state),
					ChangedAt: changedAt,
					Payload: map[string]any{
						"attachment_id": attID,
						"drawing_id":    id,
						"source_table":  sourceTable,
						"name":          name,
						"url":           url,
						"version":       version,
						"state":         state,
					},
				})
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				return nil, false, rowsErr
			}
		}
	}

	for _, ver := range versions {
		if ver == nil {
			continue
		}
		changedAt := ver.UpdatedAt.UTC()
		if changedAt.IsZero() {
			changedAt = ver.CreatedAt.UTC()
		}
		out = append(out, &LibraryChangeItem{
			Type:      "standard",
			ID:        ver.ID,
			EventType: "standard.version.updated",
			Source:    "drawing_versions",
			Summary:   fmt.Sprintf("v%d / status=%s", ver.VersionNo, strings.TrimSpace(ver.Status)),
			ChangedAt: changedAt,
			Payload: map[string]any{
				"version_id":      ver.ID,
				"drawing_no":      drawingNo,
				"version_no":      ver.VersionNo,
				"status":          ver.Status,
				"review_cert_ref": ver.ReviewCertRef,
				"file_hash":       ver.FileHash,
				"proof_hash":      ver.ProofHash,
				"publisher_ref":   ver.PublisherRef,
			},
		})
	}
	return out, true, nil
}

func (s *PGStore) collectRegulationChanges(ctx context.Context, tenantID int, id int64, query LibraryChangesQuery) ([]*LibraryChangeItem, bool, error) {
	var docNo, title, status, category, publisher string
	var updatedAt time.Time
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(doc_no, ''), COALESCE(title, ''), COALESCE(status, ''),
		       COALESCE(category, ''), COALESCE(publisher, ''), updated_at
		FROM regulation_documents
		WHERE tenant_id=$1 AND id=$2 AND COALESCE(deleted, FALSE)=FALSE
	`, tenantID, id).Scan(&docNo, &title, &status, &category, &publisher, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	out := make([]*LibraryChangeItem, 0, 48)
	out = append(out, &LibraryChangeItem{
		Type:      "regulation",
		ID:        id,
		EventType: "regulation.updated",
		Source:    "regulation_documents",
		Summary: strings.TrimSpace(strings.Join([]string{
			strings.TrimSpace(title),
			strings.TrimSpace(docNo),
			"status=" + strings.TrimSpace(status),
		}, " / ")),
		ChangedAt: updatedAt.UTC(),
		Payload: map[string]any{
			"id":        id,
			"doc_no":    docNo,
			"title":     title,
			"status":    status,
			"category":  category,
			"publisher": publisher,
		},
	})

	hasVersions, err := s.hasColumn(ctx, "regulation_versions", "id")
	if err != nil {
		return nil, false, err
	}
	if !hasVersions {
		return out, true, nil
	}

	whereClause := "tenant_id=$1 AND document_id=$2"
	args := []any{tenantID, id}
	limit := 500
	if !query.IncludeHistory {
		effectiveAt := time.Now().UTC()
		if query.ValidOn != nil {
			effectiveAt = query.ValidOn.UTC()
		}
		args = append(args, effectiveAt)
		argIdx := len(args)
		whereClause += fmt.Sprintf(" AND COALESCE(effective_from, '-infinity'::timestamptz) <= $%d AND COALESCE(effective_to, 'infinity'::timestamptz) >= $%d", argIdx, argIdx)
		limit = 1
	}
	args = append(args, limit)
	limitArg := len(args)
	rows, qErr := s.db.QueryContext(ctx, `
		SELECT id, version_no, effective_from, effective_to, published_at,
		       COALESCE(content_hash, ''), COALESCE(attachment_url, ''), COALESCE(source_note, ''), updated_at
		FROM regulation_versions
		WHERE `+whereClause+`
		ORDER BY version_no DESC, id DESC
		LIMIT $`+strconv.Itoa(limitArg), args...)
	if qErr != nil {
		if isMissingRelationErr(qErr, "regulation_versions") {
			return out, true, nil
		}
		return nil, false, qErr
	}
	defer rows.Close()

	versionCount := 0
	for rows.Next() {
		var (
			versionID     int64
			versionNo     int
			effectiveFrom sql.NullTime
			effectiveTo   sql.NullTime
			publishedAt   sql.NullTime
			contentHash   string
			attachmentURL string
			sourceNote    string
			versionAt     time.Time
		)
		if scanErr := rows.Scan(
			&versionID,
			&versionNo,
			&effectiveFrom,
			&effectiveTo,
			&publishedAt,
			&contentHash,
			&attachmentURL,
			&sourceNote,
			&versionAt,
		); scanErr != nil {
			return nil, false, scanErr
		}
		changedAt := versionAt.UTC()
		if publishedAt.Valid && publishedAt.Time.After(changedAt) {
			changedAt = publishedAt.Time.UTC()
		}
		out = append(out, &LibraryChangeItem{
			Type:      "regulation",
			ID:        versionID,
			EventType: "regulation.version.updated",
			Source:    "regulation_versions",
			Summary:   fmt.Sprintf("v%d / %s", versionNo, strings.TrimSpace(sourceNote)),
			ChangedAt: changedAt,
			Payload: map[string]any{
				"version_id":  versionID,
				"document_id": id,
				"version_no":  versionNo,
				"effective_from": func() *time.Time {
					if !effectiveFrom.Valid {
						return nil
					}
					t := effectiveFrom.Time.UTC()
					return &t
				}(),
				"effective_to": func() *time.Time {
					if !effectiveTo.Valid {
						return nil
					}
					t := effectiveTo.Time.UTC()
					return &t
				}(),
				"published_at": func() *time.Time {
					if !publishedAt.Valid {
						return nil
					}
					t := publishedAt.Time.UTC()
					return &t
				}(),
				"content_hash":   contentHash,
				"attachment_url": attachmentURL,
				"source_note":    sourceNote,
			},
		})
		versionCount++
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, false, rowsErr
	}
	if !query.IncludeHistory && hasVersions && versionCount == 0 {
		return nil, false, nil
	}
	return out, true, nil
}

func (s *PGStore) getStandardVersionDiff(
	ctx context.Context,
	tenantID int,
	libraryID int64,
	fromVersionID int64,
	toVersionID int64,
) (*LibraryVersionDiffVersion, *LibraryVersionDiffVersion, bool, error) {
	hasDrawings, err := s.hasColumn(ctx, "drawings", "id")
	if err != nil {
		return nil, nil, false, err
	}
	if !hasDrawings {
		return nil, nil, false, nil
	}
	hasDeleted, err := s.hasColumn(ctx, "drawings", "deleted")
	if err != nil {
		return nil, nil, false, err
	}
	drawingNoExpr, _, err := s.buildDrawingDisplayExpressions(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	whereClause := "tenant_id=$1 AND id=$2"
	if hasDeleted {
		whereClause += " AND deleted=FALSE"
	}
	var drawingNo string
	err = s.db.QueryRowContext(ctx, `
		SELECT `+drawingNoExpr+`
		FROM drawings d
		WHERE `+strings.ReplaceAll(whereClause, "tenant_id", "d.tenant_id"), tenantID, libraryID).Scan(&drawingNo)
	if err == sql.ErrNoRows {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	drawingNo = strings.TrimSpace(drawingNo)
	if drawingNo == "" {
		return nil, nil, false, nil
	}

	hasVersions, err := s.hasColumn(ctx, "drawing_versions", "id")
	if err != nil {
		return nil, nil, false, err
	}
	if !hasVersions {
		return nil, nil, false, nil
	}

	loadOne := func(versionID int64) (*LibraryVersionDiffVersion, error) {
		item := &LibraryVersionDiffVersion{Data: make(map[string]any)}
		var (
			status        string
			reviewCertRef string
			fileHash      string
			proofHash     string
			publisherRef  string
			createdAt     time.Time
			updatedAt     time.Time
		)
		err := s.db.QueryRowContext(ctx, `
			SELECT id, version_no, COALESCE(status, ''), COALESCE(review_cert_ref, ''), COALESCE(file_hash, ''),
			       COALESCE(proof_hash, ''), COALESCE(publisher_ref, ''), created_at, updated_at
			FROM drawing_versions
			WHERE tenant_id=$1 AND drawing_no=$2 AND id=$3
			LIMIT 1
		`, tenantID, drawingNo, versionID).Scan(
			&item.ID,
			&item.VersionNo,
			&status,
			&reviewCertRef,
			&fileHash,
			&proofHash,
			&publisherRef,
			&createdAt,
			&updatedAt,
		)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		item.ChangedAt = updatedAt.UTC()
		item.Data["status"] = status
		item.Data["review_cert_ref"] = reviewCertRef
		item.Data["file_hash"] = fileHash
		item.Data["proof_hash"] = proofHash
		item.Data["publisher_ref"] = publisherRef
		item.Data["created_at"] = createdAt.UTC().Format(time.RFC3339)
		item.Data["updated_at"] = updatedAt.UTC().Format(time.RFC3339)
		return item, nil
	}

	if toVersionID > 0 {
		toVersion, toErr := loadOne(toVersionID)
		if toErr != nil {
			return nil, nil, false, toErr
		}
		if toVersion == nil {
			return nil, nil, false, nil
		}
		var fromVersion *LibraryVersionDiffVersion
		if fromVersionID > 0 {
			fromVersion, toErr = loadOne(fromVersionID)
			if toErr != nil {
				return nil, nil, false, toErr
			}
			if fromVersion == nil {
				return nil, nil, false, nil
			}
		}
		return fromVersion, toVersion, true, nil
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, version_no, COALESCE(status, ''), COALESCE(review_cert_ref, ''), COALESCE(file_hash, ''),
		       COALESCE(proof_hash, ''), COALESCE(publisher_ref, ''), created_at, updated_at
		FROM drawing_versions
		WHERE tenant_id=$1 AND drawing_no=$2
		ORDER BY version_no DESC, id DESC
		LIMIT 2
	`, tenantID, drawingNo)
	if err != nil {
		return nil, nil, false, err
	}
	defer rows.Close()

	versions := make([]*LibraryVersionDiffVersion, 0, 2)
	for rows.Next() {
		item := &LibraryVersionDiffVersion{Data: make(map[string]any)}
		var (
			status        string
			reviewCertRef string
			fileHash      string
			proofHash     string
			publisherRef  string
			createdAt     time.Time
			updatedAt     time.Time
		)
		if scanErr := rows.Scan(
			&item.ID,
			&item.VersionNo,
			&status,
			&reviewCertRef,
			&fileHash,
			&proofHash,
			&publisherRef,
			&createdAt,
			&updatedAt,
		); scanErr != nil {
			return nil, nil, false, scanErr
		}
		item.ChangedAt = updatedAt.UTC()
		item.Data["status"] = status
		item.Data["review_cert_ref"] = reviewCertRef
		item.Data["file_hash"] = fileHash
		item.Data["proof_hash"] = proofHash
		item.Data["publisher_ref"] = publisherRef
		item.Data["created_at"] = createdAt.UTC().Format(time.RFC3339)
		item.Data["updated_at"] = updatedAt.UTC().Format(time.RFC3339)
		versions = append(versions, item)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, false, err
	}
	if len(versions) == 0 {
		return nil, nil, false, nil
	}
	if len(versions) == 1 {
		return nil, versions[0], true, nil
	}
	return versions[1], versions[0], true, nil
}

func (s *PGStore) getRegulationVersionDiff(
	ctx context.Context,
	tenantID int,
	libraryID int64,
	fromVersionID int64,
	toVersionID int64,
) (*LibraryVersionDiffVersion, *LibraryVersionDiffVersion, bool, error) {
	hasRegDocs, err := s.hasColumn(ctx, "regulation_documents", "id")
	if err != nil {
		return nil, nil, false, err
	}
	if !hasRegDocs {
		return nil, nil, false, nil
	}
	var docExists bool
	err = s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM regulation_documents
			WHERE tenant_id=$1 AND id=$2 AND COALESCE(deleted, FALSE)=FALSE
		)
	`, tenantID, libraryID).Scan(&docExists)
	if err != nil {
		return nil, nil, false, err
	}
	if !docExists {
		return nil, nil, false, nil
	}

	hasVersions, err := s.hasColumn(ctx, "regulation_versions", "id")
	if err != nil {
		return nil, nil, false, err
	}
	if !hasVersions {
		return nil, nil, false, nil
	}

	loadOne := func(versionID int64) (*LibraryVersionDiffVersion, error) {
		item := &LibraryVersionDiffVersion{Data: make(map[string]any)}
		var (
			effectiveFrom sql.NullTime
			effectiveTo   sql.NullTime
			publishedAt   sql.NullTime
			contentHash   string
			attachmentURL string
			sourceNote    string
			updatedAt     time.Time
		)
		err := s.db.QueryRowContext(ctx, `
			SELECT id, version_no, effective_from, effective_to, published_at,
			       COALESCE(content_hash, ''), COALESCE(attachment_url, ''), COALESCE(source_note, ''), updated_at
			FROM regulation_versions
			WHERE tenant_id=$1 AND document_id=$2 AND id=$3
			LIMIT 1
		`, tenantID, libraryID, versionID).Scan(
			&item.ID,
			&item.VersionNo,
			&effectiveFrom,
			&effectiveTo,
			&publishedAt,
			&contentHash,
			&attachmentURL,
			&sourceNote,
			&updatedAt,
		)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		item.ChangedAt = updatedAt.UTC()
		item.Data["effective_from"] = formatNullTimeRFC3339(effectiveFrom)
		item.Data["effective_to"] = formatNullTimeRFC3339(effectiveTo)
		item.Data["published_at"] = formatNullTimeRFC3339(publishedAt)
		item.Data["content_hash"] = contentHash
		item.Data["attachment_url"] = attachmentURL
		item.Data["source_note"] = sourceNote
		item.Data["updated_at"] = updatedAt.UTC().Format(time.RFC3339)
		return item, nil
	}

	if toVersionID > 0 {
		toVersion, toErr := loadOne(toVersionID)
		if toErr != nil {
			return nil, nil, false, toErr
		}
		if toVersion == nil {
			return nil, nil, false, nil
		}
		var fromVersion *LibraryVersionDiffVersion
		if fromVersionID > 0 {
			fromVersion, toErr = loadOne(fromVersionID)
			if toErr != nil {
				return nil, nil, false, toErr
			}
			if fromVersion == nil {
				return nil, nil, false, nil
			}
		}
		return fromVersion, toVersion, true, nil
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, version_no, effective_from, effective_to, published_at,
		       COALESCE(content_hash, ''), COALESCE(attachment_url, ''), COALESCE(source_note, ''), updated_at
		FROM regulation_versions
		WHERE tenant_id=$1 AND document_id=$2
		ORDER BY version_no DESC, id DESC
		LIMIT 2
	`, tenantID, libraryID)
	if err != nil {
		return nil, nil, false, err
	}
	defer rows.Close()

	versions := make([]*LibraryVersionDiffVersion, 0, 2)
	for rows.Next() {
		item := &LibraryVersionDiffVersion{Data: make(map[string]any)}
		var (
			effectiveFrom sql.NullTime
			effectiveTo   sql.NullTime
			publishedAt   sql.NullTime
			contentHash   string
			attachmentURL string
			sourceNote    string
			updatedAt     time.Time
		)
		if scanErr := rows.Scan(
			&item.ID,
			&item.VersionNo,
			&effectiveFrom,
			&effectiveTo,
			&publishedAt,
			&contentHash,
			&attachmentURL,
			&sourceNote,
			&updatedAt,
		); scanErr != nil {
			return nil, nil, false, scanErr
		}
		item.ChangedAt = updatedAt.UTC()
		item.Data["effective_from"] = formatNullTimeRFC3339(effectiveFrom)
		item.Data["effective_to"] = formatNullTimeRFC3339(effectiveTo)
		item.Data["published_at"] = formatNullTimeRFC3339(publishedAt)
		item.Data["content_hash"] = contentHash
		item.Data["attachment_url"] = attachmentURL
		item.Data["source_note"] = sourceNote
		item.Data["updated_at"] = updatedAt.UTC().Format(time.RFC3339)
		versions = append(versions, item)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, false, err
	}
	if len(versions) == 0 {
		return nil, nil, false, nil
	}
	if len(versions) == 1 {
		return nil, versions[0], true, nil
	}
	return versions[1], versions[0], true, nil
}

func formatNullTimeRFC3339(v sql.NullTime) string {
	if !v.Valid {
		return ""
	}
	return v.Time.UTC().Format(time.RFC3339)
}

func diffChangedFields(fromData map[string]any, toData map[string]any) []string {
	changed := make([]string, 0, 16)
	keys := make(map[string]struct{}, len(fromData)+len(toData))
	for key := range fromData {
		keys[key] = struct{}{}
	}
	for key := range toData {
		keys[key] = struct{}{}
	}
	for key := range keys {
		if !reflect.DeepEqual(fromData[key], toData[key]) {
			changed = append(changed, key)
		}
	}
	sort.Strings(changed)
	return changed
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

func (s *PGStore) appendProjectRegulationRelations(
	ctx context.Context,
	tenantID int,
	builder *relationGraphBuilder,
	projectRefs []string,
	includeHistory bool,
	validOn *time.Time,
	limit int,
) error {
	if builder == nil || len(projectRefs) == 0 {
		return nil
	}
	hasRegDocs, err := s.hasColumn(ctx, "regulation_documents", "id")
	if err != nil {
		return err
	}
	if !hasRegDocs {
		return nil
	}
	hasRegProjectRef, err := s.hasColumn(ctx, "regulation_documents", "project_ref")
	if err != nil {
		return err
	}
	hasRegDeleted, err := s.hasColumn(ctx, "regulation_documents", "deleted")
	if err != nil {
		return err
	}
	hasRegVersions, err := s.hasColumn(ctx, "regulation_versions", "id")
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
	appliedCount := 0
	if hasRegProjectRef {
		args := make([]any, 0, len(dedupProjectRefs)+3)
		args = append(args, tenantID)
		holders := make([]string, 0, len(dedupProjectRefs))
		for i, projectRef := range dedupProjectRefs {
			args = append(args, projectRef)
			holders = append(holders, fmt.Sprintf("$%d", i+2))
		}

		whereClause := "r.tenant_id=$1 AND r.project_ref IN (" + strings.Join(holders, ",") + ")"
		if hasRegDeleted {
			whereClause += " AND COALESCE(r.deleted, FALSE)=FALSE"
		}
		if !includeHistory && hasRegVersions {
			effectiveAt := time.Now().UTC()
			if validOn != nil {
				effectiveAt = validOn.UTC()
			}
			args = append(args, effectiveAt)
			argIdx := len(args)
			whereClause += fmt.Sprintf(` AND EXISTS (
				SELECT 1
				FROM regulation_versions rv
				WHERE rv.tenant_id=r.tenant_id
				  AND rv.document_id=r.id
				  AND COALESCE(rv.effective_from, '-infinity'::timestamptz) <= $%d
				  AND COALESCE(rv.effective_to, 'infinity'::timestamptz) >= $%d
			)`, argIdx, argIdx)
		}
		args = append(args, limit)
		limitArg := fmt.Sprintf("$%d", len(args))

		rows, qErr := s.db.QueryContext(ctx, `
			SELECT COALESCE(r.project_ref, ''), r.id, COALESCE(r.title, ''), COALESCE(r.doc_no, ''), COALESCE(r.status, '')
			FROM regulation_documents r
			WHERE `+whereClause+`
			ORDER BY r.updated_at DESC, r.id DESC
			LIMIT `+limitArg, args...)
		if qErr != nil {
			return qErr
		}
		defer rows.Close()

		for rows.Next() {
			var (
				projectRef string
				regID      int64
				title      string
				docNo      string
				status     string
			)
			if scanErr := rows.Scan(&projectRef, &regID, &title, &docNo, &status); scanErr != nil {
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
				Label:    s.projectLabelByRef(ctx, tenantID, projectRef),
			})
			regNodeRef := fmt.Sprintf("regulation:%d", regID)
			label := strings.TrimSpace(title)
			if label == "" {
				label = fmt.Sprintf("regulation-%d", regID)
			}
			if v := strings.TrimSpace(docNo); v != "" {
				label += " / " + v
			}
			builder.addNode(&LibraryRelationNode{
				NodeRef:     regNodeRef,
				NodeType:    "regulation",
				LibraryType: "regulation",
				ID:          regID,
				Label:       label,
				Status:      strings.TrimSpace(status),
				Ref:         projectRef,
			})
			builder.addEdge(projectNodeRef, regNodeRef, "applies_regulation")
			appliedCount++
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return rowsErr
		}
	}

	if appliedCount > 0 {
		return nil
	}

	fallbackLimit := limit
	if fallbackLimit > 10 {
		fallbackLimit = 10
	}
	if fallbackLimit <= 0 {
		fallbackLimit = 5
	}
	fallbackArgs := []any{tenantID}
	fallbackWhere := "r.tenant_id=$1"
	if hasRegDeleted {
		fallbackWhere += " AND COALESCE(r.deleted, FALSE)=FALSE"
	}
	if !includeHistory && hasRegVersions {
		effectiveAt := time.Now().UTC()
		if validOn != nil {
			effectiveAt = validOn.UTC()
		}
		fallbackArgs = append(fallbackArgs, effectiveAt)
		argIdx := len(fallbackArgs)
		fallbackWhere += fmt.Sprintf(` AND EXISTS (
			SELECT 1
			FROM regulation_versions rv
			WHERE rv.tenant_id=r.tenant_id
			  AND rv.document_id=r.id
			  AND COALESCE(rv.effective_from, '-infinity'::timestamptz) <= $%d
			  AND COALESCE(rv.effective_to, 'infinity'::timestamptz) >= $%d
		)`, argIdx, argIdx)
	}
	fallbackArgs = append(fallbackArgs, fallbackLimit)
	fallbackLimitArg := len(fallbackArgs)
	rows, qErr := s.db.QueryContext(ctx, `
		SELECT r.id, COALESCE(r.title, ''), COALESCE(r.doc_no, ''), COALESCE(r.status, '')
		FROM regulation_documents r
		WHERE `+fallbackWhere+`
		ORDER BY r.updated_at DESC, r.id DESC
		LIMIT $`+strconv.Itoa(fallbackLimitArg), fallbackArgs...)
	if qErr != nil {
		return qErr
	}
	defer rows.Close()

	fallbackRegs := make([]*RegulationLibraryItem, 0, fallbackLimit)
	for rows.Next() {
		item := &RegulationLibraryItem{}
		if scanErr := rows.Scan(&item.ID, &item.Title, &item.DocNo, &item.Status); scanErr != nil {
			return scanErr
		}
		fallbackRegs = append(fallbackRegs, item)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return rowsErr
	}
	for _, projectRef := range dedupProjectRefs {
		projectNodeRef := "project:" + projectRef
		builder.addNode(&LibraryRelationNode{
			NodeRef:  projectNodeRef,
			NodeType: "project",
			Ref:      projectRef,
			Label:    s.projectLabelByRef(ctx, tenantID, projectRef),
		})
		for _, reg := range fallbackRegs {
			if reg == nil {
				continue
			}
			regNodeRef := fmt.Sprintf("regulation:%d", reg.ID)
			label := strings.TrimSpace(reg.Title)
			if label == "" {
				label = fmt.Sprintf("regulation-%d", reg.ID)
			}
			if no := strings.TrimSpace(reg.DocNo); no != "" {
				label += " / " + no
			}
			builder.addNode(&LibraryRelationNode{
				NodeRef:     regNodeRef,
				NodeType:    "regulation",
				LibraryType: "regulation",
				ID:          reg.ID,
				Label:       label,
				Status:      strings.TrimSpace(reg.Status),
				Ref:         projectRef,
			})
			builder.addEdge(projectNodeRef, regNodeRef, "references_regulation")
		}
	}
	return nil
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

func (s *PGStore) listDrawingVersions(
	ctx context.Context,
	tenantID int,
	drawingNo string,
	includeHistory bool,
	validOn *time.Time,
	limit int,
) ([]*StandardVersionDetail, bool, error) {
	drawingNo = strings.TrimSpace(drawingNo)
	out := make([]*StandardVersionDetail, 0)
	hasVersions, err := s.hasColumn(ctx, "drawing_versions", "id")
	if err != nil {
		return nil, false, err
	}
	if !hasVersions {
		return out, false, nil
	}
	hasVersionRows, err := s.hasTenantRows(ctx, "drawing_versions", tenantID)
	if err != nil {
		return nil, false, err
	}
	if !hasVersionRows {
		return out, false, nil
	}
	if drawingNo == "" {
		return out, true, nil
	}
	if limit <= 0 {
		limit = 200
	}

	whereClause := "tenant_id=$1 AND drawing_no=$2"
	args := []any{tenantID, drawingNo}
	if !includeHistory {
		effectiveAt := time.Now().UTC()
		if validOn != nil {
			effectiveAt = validOn.UTC()
		}
		args = append(args, effectiveAt)
		idx := len(args)
		whereClause += fmt.Sprintf(" AND COALESCE(status, '') <> 'REVOKED' AND created_at <= $%d", idx)
	}

	sqlText := `
		SELECT id, version_no, COALESCE(status, ''), COALESCE(review_cert_ref, ''),
		       COALESCE(file_hash, ''), COALESCE(proof_hash, ''), COALESCE(publisher_ref, ''),
		       created_at, updated_at
		FROM drawing_versions
		WHERE ` + whereClause + `
		ORDER BY version_no DESC, id DESC`
	if includeHistory {
		args = append(args, limit)
		sqlText += fmt.Sprintf(" LIMIT $%d", len(args))
	} else {
		sqlText += " LIMIT 1"
	}

	rows, err := s.db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	for rows.Next() {
		item := &StandardVersionDetail{}
		if scanErr := rows.Scan(
			&item.ID,
			&item.VersionNo,
			&item.Status,
			&item.ReviewCertRef,
			&item.FileHash,
			&item.ProofHash,
			&item.PublisherRef,
			&item.CreatedAt,
			&item.UpdatedAt,
		); scanErr != nil {
			return nil, true, scanErr
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, true, err
	}
	return out, true, nil
}

func (s *PGStore) isLibraryVisibleToExecutor(ctx context.Context, tenantID int, libraryType string, id int64, executorRef string, includeHistory bool, validOn *time.Time) (bool, error) {
	executorRef = strings.TrimSpace(executorRef)
	if id <= 0 || executorRef == "" {
		return false, nil
	}

	switch normalizeLibraryType(libraryType) {
	case "qualification":
		var exists bool
		err := s.db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM qualifications q
				WHERE q.tenant_id=$1
				  AND q.id=$2
				  AND COALESCE(q.deleted, FALSE)=FALSE
				  AND COALESCE(BTRIM(q.executor_ref), '')=$3
			)
		`, tenantID, id, executorRef).Scan(&exists)
		return exists, err
	case "standard":
		hasAssignments, err := s.hasColumn(ctx, "qualification_assignments", "id")
		if err != nil {
			return false, err
		}
		hasQualifications, err := s.hasColumn(ctx, "qualifications", "id")
		if err != nil {
			return false, err
		}
		if !hasAssignments || !hasQualifications {
			return false, nil
		}
		hasDeleted, err := s.hasColumn(ctx, "drawings", "deleted")
		if err != nil {
			return false, err
		}
		whereClause := "d.tenant_id=$1 AND d.id=$2"
		if hasDeleted {
			whereClause += " AND d.deleted=FALSE"
		}
		var exists bool
		err = s.db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM drawings d
				WHERE `+whereClause+`
				  AND EXISTS (
					SELECT 1
					FROM qualification_assignments a
					JOIN qualifications q
					  ON q.id=a.qualification_id
					 AND q.tenant_id=a.tenant_id
					 AND COALESCE(q.deleted, FALSE)=FALSE
					WHERE a.tenant_id=d.tenant_id
					  AND a.project_ref=d.project_ref
					  AND COALESCE(BTRIM(q.executor_ref), '')=$3
				  )
			)
		`, tenantID, id, executorRef).Scan(&exists)
		return exists, err
	case "regulation":
		hasRegExecutorRef, err := s.hasColumn(ctx, "regulation_documents", "executor_ref")
		if err != nil {
			return false, err
		}
		if hasRegExecutorRef {
			var exists bool
			err = s.db.QueryRowContext(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM regulation_documents r
					WHERE r.tenant_id=$1
					  AND r.id=$2
					  AND COALESCE(r.deleted, FALSE)=FALSE
					  AND COALESCE(BTRIM(r.executor_ref), '')=$3
				)
			`, tenantID, id, executorRef).Scan(&exists)
			if err != nil || !exists || includeHistory {
				return exists, err
			}
			return s.regulationIsEffectiveOn(ctx, tenantID, id, validOn)
		}

		hasRegProjectRef, err := s.hasColumn(ctx, "regulation_documents", "project_ref")
		if err != nil {
			return false, err
		}
		hasAssignments, err := s.hasColumn(ctx, "qualification_assignments", "id")
		if err != nil {
			return false, err
		}
		hasQualifications, err := s.hasColumn(ctx, "qualifications", "id")
		if err != nil {
			return false, err
		}
		if !hasRegProjectRef || !hasAssignments || !hasQualifications {
			return false, nil
		}
		var exists bool
		err = s.db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM regulation_documents r
				WHERE r.tenant_id=$1
				  AND r.id=$2
				  AND COALESCE(r.deleted, FALSE)=FALSE
				  AND EXISTS (
					SELECT 1
					FROM qualification_assignments a
					JOIN qualifications q
					  ON q.id=a.qualification_id
					 AND q.tenant_id=a.tenant_id
					 AND COALESCE(q.deleted, FALSE)=FALSE
					WHERE a.tenant_id=r.tenant_id
					  AND a.project_ref=r.project_ref
					  AND COALESCE(BTRIM(q.executor_ref), '')=$3
				  )
			)
		`, tenantID, id, executorRef).Scan(&exists)
		if err != nil || !exists || includeHistory {
			return exists, err
		}
		return s.regulationIsEffectiveOn(ctx, tenantID, id, validOn)
	default:
		return false, fmt.Errorf("unsupported library type: %s", libraryType)
	}
}

func (s *PGStore) regulationIsEffectiveOn(ctx context.Context, tenantID int, documentID int64, validOn *time.Time) (bool, error) {
	if documentID <= 0 {
		return false, nil
	}
	hasVersions, err := s.hasColumn(ctx, "regulation_versions", "id")
	if err != nil {
		return false, err
	}
	if !hasVersions {
		return true, nil
	}
	effectiveAt := time.Now().UTC()
	if validOn != nil {
		effectiveAt = validOn.UTC()
	}
	var exists bool
	err = s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM regulation_versions rv
			WHERE rv.tenant_id=$1
			  AND rv.document_id=$2
			  AND COALESCE(rv.effective_from, '-infinity'::timestamptz) <= $3
			  AND COALESCE(rv.effective_to, 'infinity'::timestamptz) >= $3
		)
	`, tenantID, documentID, effectiveAt).Scan(&exists)
	return exists, err
}

func normalizeThreeLibrariesQuery(q ThreeLibrariesQuery) ThreeLibrariesQuery {
	q.QualificationLimit = normalizePageLimit(q.QualificationLimit)
	q.StandardLimit = normalizePageLimit(q.StandardLimit)
	q.RegulationLimit = normalizePageLimit(q.RegulationLimit)
	q.QualificationOffset = normalizePageOffset(q.QualificationOffset)
	q.StandardOffset = normalizePageOffset(q.StandardOffset)
	q.RegulationOffset = normalizePageOffset(q.RegulationOffset)
	q.ExecutorRef = strings.TrimSpace(q.ExecutorRef)
	if q.ValidOn != nil {
		t := q.ValidOn.UTC()
		q.ValidOn = &t
	}
	return q
}

func normalizeLibrarySearchQuery(q LibrarySearchQuery) LibrarySearchQuery {
	q.Limit = normalizePageLimit(q.Limit)
	q.Offset = normalizePageOffset(q.Offset)
	q.Keyword = strings.TrimSpace(q.Keyword)
	q.Type = normalizeLibraryType(q.Type)
	q.Status = strings.TrimSpace(q.Status)
	q.ExecutorRef = strings.TrimSpace(q.ExecutorRef)
	if q.ValidOn != nil {
		t := q.ValidOn.UTC()
		q.ValidOn = &t
	}
	return q
}

func normalizeLibraryChangesQuery(q LibraryChangesQuery) LibraryChangesQuery {
	q.Limit = normalizePageLimit(q.Limit)
	q.Offset = normalizePageOffset(q.Offset)
	q.ExecutorRef = strings.TrimSpace(q.ExecutorRef)
	if q.ValidOn != nil {
		t := q.ValidOn.UTC()
		q.ValidOn = &t
	}
	if q.From != nil {
		t := q.From.UTC()
		q.From = &t
	}
	if q.To != nil {
		t := q.To.UTC()
		q.To = &t
	}
	return q
}

func normalizeLibraryVersionDiffQuery(q LibraryVersionDiffQuery) LibraryVersionDiffQuery {
	q.FromVersionID = normalizePositiveInt64(q.FromVersionID)
	q.ToVersionID = normalizePositiveInt64(q.ToVersionID)
	q.ExecutorRef = strings.TrimSpace(q.ExecutorRef)
	return q
}

func normalizeLibraryRelationsQuery(q LibraryRelationsQuery) LibraryRelationsQuery {
	q.Limit = normalizePageLimit(q.Limit)
	q.ExecutorRef = strings.TrimSpace(q.ExecutorRef)
	if q.ValidOn != nil {
		t := q.ValidOn.UTC()
		q.ValidOn = &t
	}
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

func normalizePositiveInt64(raw int64) int64 {
	if raw <= 0 {
		return 0
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

func (s *PGStore) hasTenantRows(ctx context.Context, tableName string, tenantID int) (bool, error) {
	table := strings.TrimSpace(tableName)
	switch table {
	case "drawing_versions", "regulation_versions":
	default:
		return false, fmt.Errorf("unsupported table for tenant row check: %s", tableName)
	}
	var exists bool
	err := s.db.QueryRowContext(
		ctx,
		fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE tenant_id=$1 LIMIT 1)", table),
		tenantID,
	).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
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
