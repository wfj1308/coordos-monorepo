package register

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

type Service struct {
	db       *sql.DB
	tenantID int
	client   *http.Client
}

func NewService(db *sql.DB, tenantID int) *Service {
	return &Service{
		db:       db,
		tenantID: tenantID,
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

type RegisterOrgInput struct {
	ShortCode      string                  `json:"short_code"`
	NamespaceRef   string                  `json:"namespace_ref"`
	ParentRef      string                  `json:"parent_ref"`
	CompanyName    string                  `json:"company_name"`
	CreditCode     string                  `json:"credit_code"`
	CertNo         string                  `json:"cert_no"`
	CertValidUntil string                  `json:"cert_valid_until"`
	RegCapital     int64                   `json:"reg_capital"`
	LegalRep       string                  `json:"legal_rep"`
	TechDirector   string                  `json:"tech_director"`
	Address        string                  `json:"address"`
	EstablishedAt  string                  `json:"established_at"`
	InheritedRules []string                `json:"inherited_rules"`
	OrgType        string                  `json:"org_type"`
	Qualifications []OrgQualificationInput `json:"qualifications"`
}

type OrgQualificationInput struct {
	QualType     string   `json:"qual_type"`
	ResourceType string   `json:"resource_type"`
	Name         string   `json:"name"`
	Scope        []string `json:"scope"`
	Grade        string   `json:"grade"`
	Industry     string   `json:"industry"`
	IssuedBy     string   `json:"issued_by"`
	VerifyURL    string   `json:"verify_url"`
	RuleBinding  []string `json:"rule_binding"`
	GenesisRef   string   `json:"genesis_ref"`
}

type RegisterOrgResult struct {
	CompanyID            int64    `json:"company_id"`
	NamespaceRef         string   `json:"namespace_ref"`
	ShortCode            string   `json:"short_code"`
	OrgType              string   `json:"org_type"`
	QualificationGenesis []string `json:"qualification_genesis"`
	RightGenesis         []string `json:"right_genesis"`
	OwnedGenesis         []string `json:"owned_genesis"`
}

type ImportEngineersOptions struct {
	DefaultValidUntil string `json:"default_valid_until"`
}

type ImportEngineerFailure struct {
	Row     int    `json:"row"`
	Name    string `json:"name,omitempty"`
	IDCard  string `json:"id_card,omitempty"`
	CertNo  string `json:"cert_no,omitempty"`
	Reason  string `json:"reason"`
	Payload string `json:"payload,omitempty"`
}

type StatsRefreshFailure struct {
	ExecutorRef string `json:"executor_ref"`
	Reason      string `json:"reason"`
}

type ImportEngineersResult struct {
	NamespaceRef        string                  `json:"namespace_ref"`
	TotalRows           int                     `json:"total_rows"`
	SuccessCount        int                     `json:"success_count"`
	FailureCount        int                     `json:"failure_count"`
	Failures            []ImportEngineerFailure `json:"failures,omitempty"`
	ExecutorRefs        []string                `json:"executor_refs,omitempty"`
	StatsRefreshed      int                     `json:"stats_refreshed"`
	StatsRefreshFailure []StatsRefreshFailure   `json:"stats_refresh_failures,omitempty"`
}

type ImportExecutorsOptions struct {
	DefaultValidUntil         string `json:"default_valid_until"`
	DefaultMaxConcurrentTasks int    `json:"default_max_concurrent_tasks"`
}

type ImportExecutorFailure struct {
	Row      int    `json:"row"`
	Name     string `json:"name,omitempty"`
	IDCard   string `json:"id_card,omitempty"`
	RoleCode string `json:"role_code,omitempty"`
	Reason   string `json:"reason"`
	Payload  string `json:"payload,omitempty"`
}

type ImportExecutorsResult struct {
	NamespaceRef        string                  `json:"namespace_ref"`
	TotalRows           int                     `json:"total_rows"`
	SuccessCount        int                     `json:"success_count"`
	FailureCount        int                     `json:"failure_count"`
	Failures            []ImportExecutorFailure `json:"failures,omitempty"`
	ExecutorRefs        []string                `json:"executor_refs,omitempty"`
	RoleCounts          map[string]int          `json:"role_counts,omitempty"`
	StatsRefreshed      int                     `json:"stats_refreshed"`
	StatsRefreshFailure []StatsRefreshFailure   `json:"stats_refresh_failures,omitempty"`
}

type ExtractCertInput struct {
	FileName    string
	ContentType string
	FileBytes   []byte
}

type ExtractedQualification struct {
	Name        string   `json:"name"`
	QualType    string   `json:"qual_type"`
	ResourceKey string   `json:"resource_key"`
	Scope       []string `json:"scope"`
	Grade       string   `json:"grade"`
	Industry    string   `json:"industry"`
}

type ExtractCertResult struct {
	CompanyName        string                   `json:"company_name"`
	CreditCode         string                   `json:"credit_code"`
	CertNo             string                   `json:"cert_no"`
	CertValidUntil     string                   `json:"cert_valid_until"`
	LegalRep           string                   `json:"legal_rep"`
	TechDirector       string                   `json:"tech_director"`
	Address            string                   `json:"address"`
	ExtractedFromModel bool                     `json:"extracted_from_model"`
	NeedsManualReview  bool                     `json:"needs_manual_review"`
	Qualifications     []ExtractedQualification `json:"qualifications"`
	RawText            string                   `json:"raw_text,omitempty"`
}

type qualTemplate struct {
	AliasWords   []string
	ResourceType string
	Name         string
	Suffix       string
	Scope        []string
	Grade        string
	Industry     string
	RuleBinding  []string
}

var defaultQualTemplates = []qualTemplate{
	{
		AliasWords:   []string{"HIGHWAY", "PUBLIC ROAD", "公路"},
		ResourceType: "QUAL_HIGHWAY_INDUSTRY_A",
		Name:         "Highway Industry Grade A",
		Suffix:       "qual/highway_a",
		Scope:        []string{"Road", "Long Span Bridge", "Long Tunnel", "Traffic Engineering"},
		Grade:        "A",
		Industry:     "HIGHWAY",
		RuleBinding:  []string{"RULE-001", "RULE-002"},
	},
	{
		AliasWords:   []string{"MUNICIPAL", "市政"},
		ResourceType: "QUAL_MUNICIPAL_INDUSTRY_A",
		Name:         "Municipal Industry Grade A",
		Suffix:       "qual/municipal_a",
		Scope:        []string{"Drainage", "Town Gas", "Road", "Bridge"},
		Grade:        "A",
		Industry:     "MUNICIPAL",
		RuleBinding:  []string{"RULE-001", "RULE-002"},
	},
	{
		AliasWords:   []string{"ARCH", "ARCHITECTURE", "建筑"},
		ResourceType: "QUAL_ARCH_COMPREHENSIVE_A",
		Name:         "Architecture Industry Grade A",
		Suffix:       "qual/arch_a",
		Scope:        []string{"Architecture", "Decoration", "Curtain Wall", "Light Steel", "Building Intelligence", "Lighting", "Fire Protection"},
		Grade:        "A",
		Industry:     "ARCHITECTURE",
		RuleBinding:  []string{"RULE-001", "RULE-002"},
	},
	{
		AliasWords:   []string{"LANDSCAPE", "风景园林"},
		ResourceType: "QUAL_LANDSCAPE_SPECIAL_A",
		Name:         "Landscape Design Special Grade A",
		Suffix:       "qual/landscape_a",
		Scope:        []string{"Landscape Design"},
		Grade:        "A",
		Industry:     "LANDSCAPE",
		RuleBinding:  []string{"RULE-001", "RULE-002"},
	},
	{
		AliasWords:   []string{"WATER", "WATERWORKS", "水利"},
		ResourceType: "QUAL_WATER_INDUSTRY_B",
		Name:         "Water Industry Grade B",
		Suffix:       "qual/water_b",
		Scope:        []string{"Water Engineering"},
		Grade:        "B",
		Industry:     "WATER",
		RuleBinding:  []string{"RULE-001"},
	},
}

type executorRoleDef struct {
	Code                 string
	Name                 string
	DefaultSpecialty     string
	DefaultMaxConcurrent int
	DefaultSPURefs       []string
	Aliases              []string
}

var executorRoleDefs = []executorRoleDef{
	{
		Code:                 "ROLE_TAX_FILER",
		Name:                 "Tax Filer",
		DefaultSpecialty:     "Tax filing",
		DefaultMaxConcurrent: 8,
		DefaultSPURefs: []string{
			"v://zhongbei/spu/finance/invoice@v1",
			"v://zhongbei/spu/finance/collection@v1",
			"v://zhongbei/spu/finance/tax_return@v1",
		},
		Aliases: []string{"TAX_FILER", "TAX"},
	},
	{
		Code:                 "ROLE_ACCOUNTANT",
		Name:                 "Accountant",
		DefaultSpecialty:     "Accounting",
		DefaultMaxConcurrent: 6,
		DefaultSPURefs: []string{
			"v://zhongbei/spu/finance/invoice@v1",
			"v://zhongbei/spu/finance/collection@v1",
		},
		Aliases: []string{"ACCOUNTANT"},
	},
	{
		Code:                 "ROLE_CASHIER",
		Name:                 "Cashier",
		DefaultSpecialty:     "Cash operations",
		DefaultMaxConcurrent: 6,
		DefaultSPURefs: []string{
			"v://zhongbei/spu/finance/collection@v1",
		},
		Aliases: []string{"CASHIER"},
	},
	{
		Code:                 "ROLE_CONTRACT_ADMIN",
		Name:                 "Contract Administrator",
		DefaultSpecialty:     "Contract ledger",
		DefaultMaxConcurrent: 8,
		DefaultSPURefs: []string{
			"v://zhongbei/spu/contract/review@v1",
			"v://zhongbei/spu/contract/sign@v1",
			"v://zhongbei/spu/contract/archive@v1",
		},
		Aliases: []string{"CONTRACT_ADMIN"},
	},
	{
		Code:                 "ROLE_PROJECT_MANAGER",
		Name:                 "Project Manager",
		DefaultSpecialty:     "Project orchestration",
		DefaultMaxConcurrent: 5,
		Aliases:              []string{"PROJECT_MANAGER"},
	},
	{
		Code:                 "ROLE_QUALITY_MANAGER",
		Name:                 "Quality Manager",
		DefaultSpecialty:     "Quality control",
		DefaultMaxConcurrent: 5,
		Aliases:              []string{"QUALITY_MANAGER"},
	},
	{
		Code:                 "ROLE_SCHEDULE_MANAGER",
		Name:                 "Schedule Manager",
		DefaultSpecialty:     "Schedule control",
		DefaultMaxConcurrent: 6,
		Aliases:              []string{"SCHEDULE_MANAGER"},
	},
	{
		Code:                 "ROLE_CAD_DRAFTER",
		Name:                 "CAD Drafter",
		DefaultSpecialty:     "Drawing",
		DefaultMaxConcurrent: 10,
		Aliases:              []string{"CAD_DRAFTER"},
	},
	{
		Code:                 "ROLE_DOC_CONTROLLER",
		Name:                 "Document Controller",
		DefaultSpecialty:     "Document archive",
		DefaultMaxConcurrent: 10,
		Aliases:              []string{"DOC_CONTROLLER"},
	},
	{
		Code:                 "ROLE_EXTERNAL_REVIEW_COORDINATOR",
		Name:                 "External Review Coordinator",
		DefaultSpecialty:     "External review",
		DefaultMaxConcurrent: 7,
		Aliases:              []string{"EXTERNAL_REVIEW_COORDINATOR"},
	},
	{
		Code:                 "ROLE_MARKET_MANAGER",
		Name:                 "Market Manager",
		DefaultSpecialty:     "Client development",
		DefaultMaxConcurrent: 6,
		DefaultSPURefs: []string{
			"v://zhongbei/spu/bid/preparation@v1",
			"v://zhongbei/spu/bid/submission@v1",
			"v://zhongbei/spu/bid/award@v1",
		},
		Aliases: []string{"MARKET_MANAGER"},
	},
	{
		Code:                 "ROLE_BID_SPECIALIST",
		Name:                 "Bid Specialist",
		DefaultSpecialty:     "Bid package",
		DefaultMaxConcurrent: 8,
		DefaultSPURefs: []string{
			"v://zhongbei/spu/bid/preparation@v1",
			"v://zhongbei/spu/bid/submission@v1",
		},
		Aliases: []string{"BID_SPECIALIST"},
	},
}

func (s *Service) RegisterOrg(ctx context.Context, in RegisterOrgInput) (*RegisterOrgResult, error) {
	if s.db == nil {
		return nil, fmt.Errorf("db is nil")
	}

	nsRef, shortCode, err := resolveNamespaceRef(in.NamespaceRef, in.ShortCode)
	if err != nil {
		return nil, err
	}
	parentRef := strings.TrimSpace(in.ParentRef)
	orgType := strings.ToUpper(strings.TrimSpace(in.OrgType))

	autoParentRef := extractParentRef(shortCode)
	autoDepth := computeDepth(shortCode)

	if parentRef == "" && autoParentRef != "" {
		parentRef = autoParentRef
	}

	if orgType == "" {
		if autoDepth > 1 {
			orgType = "BRANCH"
		} else {
			orgType = "HEAD_OFFICE"
		}
	}
	if orgType != "HEAD_OFFICE" && orgType != "BRANCH" {
		return nil, fmt.Errorf("org_type must be HEAD_OFFICE or BRANCH")
	}

	companyName := strings.TrimSpace(in.CompanyName)
	if companyName == "" {
		return nil, fmt.Errorf("company_name is required")
	}
	creditCode := strings.ToUpper(strings.TrimSpace(in.CreditCode))
	if creditCode == "" {
		return nil, fmt.Errorf("credit_code is required")
	}
	certNo := strings.TrimSpace(in.CertNo)
	if certNo == "" {
		return nil, fmt.Errorf("cert_no is required")
	}
	certValidUntil, err := parseDatePtr(in.CertValidUntil)
	if err != nil {
		return nil, fmt.Errorf("invalid cert_valid_until: %w", err)
	}
	establishedAt, err := parseDatePtr(in.EstablishedAt)
	if err != nil {
		return nil, fmt.Errorf("invalid established_at: %w", err)
	}
	qualInputs := normalizeOrgQualifications(in.Qualifications)
	if len(qualInputs) == 0 {
		return nil, fmt.Errorf("qualifications is required")
	}
	inheritedRules := normalizeStringList(in.InheritedRules)
	if len(inheritedRules) == 0 {
		inheritedRules = []string{"RULE-001", "RULE-002", "RULE-003", "RULE-004", "RULE-005"}
	}
	now := time.Now().UTC()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	parentRefPtr := emptyToNil(parentRef)
	depth := autoDepth
	path := nsRef
	if parentRefPtr != nil {
		var parentDepth int
		var parentPath string
		err := tx.QueryRowContext(ctx, `
			SELECT depth, path FROM namespaces WHERE tenant_id=$1 AND ref=$2 LIMIT 1
		`, s.tenantID, *parentRefPtr).Scan(&parentDepth, &parentPath)
		if err == sql.ErrNoRows {
			depth = autoDepth
			path = nsRef
		} else if err == nil {
			depth = parentDepth + 1
			path = strings.TrimRight(parentPath, "/") + "/" + extractSlug(shortCode)
		}
	}

	var companyID int64
	companyType := 1
	if orgType == "BRANCH" {
		companyType = 2
	}
	err = tx.QueryRowContext(ctx, `
		INSERT INTO companies (
			name, short_name, company_type, credit_code, reg_capital, legal_rep, tech_director,
			address, established_at, cert_no, cert_valid_until, genesis_ref,
			tenant_id, deleted, created_at, updated_at, migrate_status
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,FALSE,$14,$15,'PENDING'
		)
		ON CONFLICT (credit_code) DO UPDATE SET
			name=EXCLUDED.name,
			short_name=EXCLUDED.short_name,
			company_type=EXCLUDED.company_type,
			reg_capital=EXCLUDED.reg_capital,
			legal_rep=EXCLUDED.legal_rep,
			tech_director=EXCLUDED.tech_director,
			address=EXCLUDED.address,
			established_at=EXCLUDED.established_at,
			cert_no=EXCLUDED.cert_no,
			cert_valid_until=EXCLUDED.cert_valid_until,
			genesis_ref=EXCLUDED.genesis_ref,
			tenant_id=EXCLUDED.tenant_id,
			updated_at=EXCLUDED.updated_at
		RETURNING id
	`,
		companyName,
		shortCode,
		companyType,
		creditCode,
		in.RegCapital,
		emptyToNull(in.LegalRep),
		emptyToNull(in.TechDirector),
		emptyToNull(in.Address),
		establishedAt,
		certNo,
		certValidUntil,
		nsRef,
		s.tenantID,
		now,
		now,
	).Scan(&companyID)
	if err != nil {
		return nil, fmt.Errorf("upsert company failed: %w", err)
	}

	qualGenesis := make([]string, 0, len(qualInputs))
	for _, q := range qualInputs {
		tpl := matchQualTemplate(q.ResourceType, q.QualType, q.Name)
		resourceType := strings.TrimSpace(q.ResourceType)
		if resourceType == "" && tpl != nil {
			resourceType = tpl.ResourceType
		}
		if resourceType == "" {
			resourceType = "QUAL_CUSTOM"
		}

		scope := normalizeStringList(q.Scope)
		if len(scope) == 0 && tpl != nil {
			scope = append(scope, tpl.Scope...)
		}
		name := strings.TrimSpace(q.Name)
		if name == "" && tpl != nil {
			name = tpl.Name
		}
		if name == "" {
			name = resourceType
		}
		grade := strings.TrimSpace(q.Grade)
		if grade == "" && tpl != nil {
			grade = tpl.Grade
		}
		industry := strings.TrimSpace(q.Industry)
		if industry == "" && tpl != nil {
			industry = tpl.Industry
		}
		issuedBy := strings.TrimSpace(q.IssuedBy)
		if issuedBy == "" {
			issuedBy = "住房和城乡建设部"
		}
		verifyURL := strings.TrimSpace(q.VerifyURL)
		if verifyURL == "" {
			verifyURL = "https://jzsc.mohurd.gov.cn/data/company?q=" + certNo
		}
		ruleBinding := normalizeStringList(q.RuleBinding)
		if len(ruleBinding) == 0 && tpl != nil {
			ruleBinding = append(ruleBinding, tpl.RuleBinding...)
		}
		if len(ruleBinding) == 0 {
			ruleBinding = []string{"RULE-001", "RULE-002"}
		}
		genesisRef := resolveGenesisRef(nsRef, q.GenesisRef, tpl, resourceType)
		constraint, err := json.Marshal(map[string]any{
			"cert_no":               certNo,
			"credit_code":           creditCode,
			"issued_by":             issuedBy,
			"valid_until":           formatDatePtr(certValidUntil),
			"scope":                 scope,
			"grade":                 grade,
			"industry":              industry,
			"require_reg_engineer":  true,
			"require_tech_director": true,
			"verifiable_url":        verifyURL,
			"rule_binding":          ruleBinding,
		})
		if err != nil {
			return nil, err
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO genesis_utxos (
				ref, resource_type, name, total_amount, available_amount, unit,
				constraints, status, tenant_id, created_at, updated_at
			) VALUES (
				$1,$2,$3,999999,999999,'UNIT',$4::jsonb,'ACTIVE',$5,$6,$7
			)
			ON CONFLICT (ref) DO UPDATE SET
				resource_type=EXCLUDED.resource_type,
				name=EXCLUDED.name,
                total_amount=EXCLUDED.total_amount,
                available_amount=EXCLUDED.available_amount,
                unit=EXCLUDED.unit,
                constraints=EXCLUDED.constraints,
                status='ACTIVE',
                updated_at=EXCLUDED.updated_at
		`, genesisRef, resourceType, name, string(constraint), s.tenantID, now, now)
		if err != nil {
			return nil, fmt.Errorf("upsert genesis qualification %s failed: %w", genesisRef, err)
		}
		qualGenesis = append(qualGenesis, genesisRef)
	}

	type rightDef struct {
		Suffix string
		Type   string
		Name   string
		Rules  []string
		Scope  []string
	}
	rightDefs := []rightDef{
		{Suffix: "review_stamp", Type: "RIGHT_REVIEW_STAMP", Name: "Review and stamp right", Rules: []string{"RULE-003"}, Scope: []string{"REVIEW", "STAMP"}},
		{Suffix: "publish", Type: "RIGHT_PUBLISH", Name: "Publish right", Rules: []string{"RULE-004"}, Scope: []string{"PUBLISH"}},
		{Suffix: "invoice", Type: "RIGHT_INVOICE", Name: "Invoice right", Rules: []string{"RULE-005"}, Scope: []string{"INVOICE"}},
	}
	rightRefs := make([]string, 0, len(rightDefs))
	for _, item := range rightDefs {
		ref := fmt.Sprintf("%s/genesis/right/%s", nsRef, item.Suffix)
		payload, _ := json.Marshal(map[string]any{
			"rule_binding":      item.Rules,
			"owner_namespace":   nsRef,
			"scope":             item.Scope,
			"registered_by_api": true,
		})
		_, err = tx.ExecContext(ctx, `
			INSERT INTO genesis_utxos (
				ref, resource_type, name, total_amount, available_amount, unit,
				constraints, status, tenant_id, created_at, updated_at
			) VALUES (
				$1,$2,$3,1,1,'RIGHT',$4::jsonb,'ACTIVE',$5,$6,$7
			)
			ON CONFLICT (ref) DO UPDATE SET
				resource_type=EXCLUDED.resource_type,
				name=EXCLUDED.name,
				total_amount=EXCLUDED.total_amount,
				available_amount=EXCLUDED.available_amount,
				unit=EXCLUDED.unit,
				constraints=EXCLUDED.constraints,
				status='ACTIVE',
				updated_at=EXCLUDED.updated_at
		`, ref, item.Type, item.Name, string(payload), s.tenantID, now, now)
		if err != nil {
			return nil, fmt.Errorf("upsert genesis right %s failed: %w", ref, err)
		}
		rightRefs = append(rightRefs, ref)
	}

	ownedGenesis := mergeStrings(qualGenesis, rightRefs)
	routePolicy, _ := json.Marshal(map[string]string{
		"policy":       "self",
		"review_stamp": "self_only",
		"invoice":      "self_only",
	})
	_, err = tx.ExecContext(ctx, `
		INSERT INTO namespaces (
			ref, parent_ref, name, inherited_rules, owned_genesis, tenant_id,
			created_at, updated_at, short_code, org_type, depth, path,
			accessible_genesis, manage_fee_rate, route_policy, status
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,0,$14::jsonb,'ACTIVE'
		)
		ON CONFLICT (ref) DO UPDATE SET
			parent_ref=EXCLUDED.parent_ref,
			name=EXCLUDED.name,
			inherited_rules=EXCLUDED.inherited_rules,
			owned_genesis=EXCLUDED.owned_genesis,
			tenant_id=EXCLUDED.tenant_id,
			updated_at=EXCLUDED.updated_at,
			short_code=EXCLUDED.short_code,
			org_type=EXCLUDED.org_type,
			depth=EXCLUDED.depth,
			path=EXCLUDED.path,
			accessible_genesis=EXCLUDED.accessible_genesis,
			manage_fee_rate=EXCLUDED.manage_fee_rate,
			route_policy=EXCLUDED.route_policy,
			status='ACTIVE'
	`,
		nsRef,
		parentRefPtr,
		companyName,
		pq.Array(inheritedRules),
		pq.Array(ownedGenesis),
		s.tenantID,
		now,
		now,
		shortCode,
		orgType,
		depth,
		path,
		pq.Array(qualGenesis),
		string(routePolicy),
	)
	if err != nil {
		return nil, fmt.Errorf("upsert namespace failed: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE companies SET genesis_ref=$1, updated_at=$2 WHERE id=$3
	`, nsRef, now, companyID); err != nil {
		return nil, fmt.Errorf("update company genesis_ref failed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &RegisterOrgResult{
		CompanyID:            companyID,
		NamespaceRef:         nsRef,
		ShortCode:            shortCode,
		OrgType:              orgType,
		QualificationGenesis: qualGenesis,
		RightGenesis:         rightRefs,
		OwnedGenesis:         ownedGenesis,
	}, nil
}

func (s *Service) ImportEngineersCSV(ctx context.Context, namespace string, r io.Reader, opts ImportEngineersOptions) (*ImportEngineersResult, error) {
	if s.db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return s.ImportEngineersFile(ctx, namespace, "engineers.csv", "text/csv", raw, opts)
}

func (s *Service) ImportEngineersFile(
	ctx context.Context,
	namespace string,
	fileName string,
	contentType string,
	fileBytes []byte,
	opts ImportEngineersOptions,
) (*ImportEngineersResult, error) {
	if s.db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	nsRef, shortCode, err := resolveNamespaceRef(namespace, "")
	if err != nil {
		return nil, err
	}
	defaultValidUntil := strings.TrimSpace(opts.DefaultValidUntil)
	if defaultValidUntil == "" {
		defaultValidUntil = "2029-12-31"
	}
	if _, err := parseDatePtr(defaultValidUntil); err != nil {
		return nil, fmt.Errorf("invalid default_valid_until")
	}
	if len(fileBytes) == 0 {
		return nil, fmt.Errorf("empty file")
	}

	rows, err := parseEngineerRowsFromFile(fileName, contentType, fileBytes)
	if err != nil {
		return nil, err
	}
	return s.importEngineerRows(ctx, nsRef, shortCode, defaultValidUntil, rows)
}

func (s *Service) importEngineerRows(
	ctx context.Context,
	nsRef string,
	shortCode string,
	defaultValidUntil string,
	rows [][]string,
) (*ImportEngineersResult, error) {
	res := &ImportEngineersResult{
		NamespaceRef:        nsRef,
		TotalRows:           len(rows),
		Failures:            make([]ImportEngineerFailure, 0),
		ExecutorRefs:        make([]string, 0),
		StatsRefreshFailure: make([]StatsRefreshFailure, 0),
	}
	seenExecutor := make(map[string]struct{})
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	for idx, row := range rows {
		parsed, skip, err := parseEngineerRow(row)
		if skip {
			continue
		}
		if err != nil {
			res.Failures = append(res.Failures, ImportEngineerFailure{
				Row:     idx + 1,
				Payload: strings.Join(row, "|"),
				Reason:  err.Error(),
			})
			continue
		}
		qualType, err := mapQualType(parsed.RegisterType, parsed.Specialty)
		if err != nil {
			res.Failures = append(res.Failures, ImportEngineerFailure{
				Row:    idx + 1,
				Name:   parsed.Name,
				IDCard: parsed.IDCard,
				CertNo: parsed.CertNo,
				Reason: err.Error(),
			})
			continue
		}
		executorRef := buildExecutorRef(shortCode, parsed.Name, parsed.IDCard)
		var finalExecutorRef string
		err = tx.QueryRowContext(ctx, `
			INSERT INTO employees (
				name, id_card, executor_ref, company_ref, tenant_id, deleted, created_at, updated_at, migrate_status
			) VALUES (
				$1,$2,$3,$4,$5,FALSE,$6,$7,'PENDING'
			)
			ON CONFLICT (id_card) DO UPDATE SET
				name=EXCLUDED.name,
				company_ref=EXCLUDED.company_ref,
				tenant_id=EXCLUDED.tenant_id,
				executor_ref=COALESCE(NULLIF(employees.executor_ref,''), EXCLUDED.executor_ref),
				updated_at=EXCLUDED.updated_at
			RETURNING executor_ref
		`, parsed.Name, parsed.IDCard, executorRef, nsRef, s.tenantID, now, now).Scan(&finalExecutorRef)
		if err != nil {
			res.Failures = append(res.Failures, ImportEngineerFailure{
				Row:    idx + 1,
				Name:   parsed.Name,
				IDCard: parsed.IDCard,
				CertNo: parsed.CertNo,
				Reason: "upsert employee failed: " + err.Error(),
			})
			continue
		}
		if strings.TrimSpace(finalExecutorRef) == "" {
			finalExecutorRef = executorRef
		}

		maxConcurrent := maxProjectsByQualType(qualType)
		_, err = tx.ExecContext(ctx, `
			INSERT INTO qualifications (
				executor_ref, qual_type, cert_no, specialty, holder_type, status,
				max_concurrent_projects, valid_until, tenant_id, deleted, created_at, updated_at
			) VALUES (
				$1,$2,$3,$4,'PERSON','VALID',$5,$6,$7,FALSE,$8,$9
			)
			ON CONFLICT (cert_no) DO UPDATE SET
				executor_ref=EXCLUDED.executor_ref,
				qual_type=EXCLUDED.qual_type,
				specialty=EXCLUDED.specialty,
				holder_type=EXCLUDED.holder_type,
				status=EXCLUDED.status,
				max_concurrent_projects=EXCLUDED.max_concurrent_projects,
				valid_until=EXCLUDED.valid_until,
				tenant_id=EXCLUDED.tenant_id,
				deleted=FALSE,
				updated_at=EXCLUDED.updated_at
		`, finalExecutorRef, qualType, parsed.CertNo, parsed.Specialty, maxConcurrent, defaultValidUntil, s.tenantID, now, now)
		if err != nil {
			res.Failures = append(res.Failures, ImportEngineerFailure{
				Row:    idx + 1,
				Name:   parsed.Name,
				IDCard: parsed.IDCard,
				CertNo: parsed.CertNo,
				Reason: "upsert qualification failed: " + err.Error(),
			})
			continue
		}

		personGenesisRef := fmt.Sprintf("%s/genesis/person/%s/%s/%s",
			nsRef,
			strings.ToLower(cleanIDCard(parsed.IDCard)),
			strings.ToLower(strings.TrimPrefix(qualType, "REG_")),
			sanitizeToken(parsed.CertNo),
		)
		payload, _ := json.Marshal(map[string]any{
			"name":         parsed.Name,
			"id_card":      parsed.IDCard,
			"cert_no":      parsed.CertNo,
			"qual_type":    qualType,
			"specialty":    parsed.Specialty,
			"valid_until":  defaultValidUntil,
			"executor_ref": finalExecutorRef,
		})
		_, err = tx.ExecContext(ctx, `
			INSERT INTO genesis_utxos (
				ref, resource_type, name, total_amount, available_amount, unit,
				constraints, status, tenant_id, created_at, updated_at
			) VALUES (
				$1,'PERSON_QUAL_GENESIS',$2,1,1,'CERT',$3::jsonb,'ACTIVE',$4,$5,$6
			)
			ON CONFLICT (ref) DO UPDATE SET
				name=EXCLUDED.name,
				constraints=EXCLUDED.constraints,
				status='ACTIVE',
				updated_at=EXCLUDED.updated_at
		`, personGenesisRef, parsed.Name+"-"+qualType, string(payload), s.tenantID, now, now)
		if err != nil {
			res.Failures = append(res.Failures, ImportEngineerFailure{
				Row:    idx + 1,
				Name:   parsed.Name,
				IDCard: parsed.IDCard,
				CertNo: parsed.CertNo,
				Reason: "upsert person genesis failed: " + err.Error(),
			})
			continue
		}

		res.SuccessCount++
		if _, ok := seenExecutor[finalExecutorRef]; !ok {
			seenExecutor[finalExecutorRef] = struct{}{}
			res.ExecutorRefs = append(res.ExecutorRefs, finalExecutorRef)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	res.FailureCount = len(res.Failures)
	return res, nil
}

func (s *Service) ImportExecutorsCSV(ctx context.Context, namespace string, r io.Reader, opts ImportExecutorsOptions) (*ImportExecutorsResult, error) {
	if s.db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return s.ImportExecutorsFile(ctx, namespace, "executors.csv", "text/csv", raw, opts)
}

func (s *Service) ImportExecutorsFile(
	ctx context.Context,
	namespace string,
	fileName string,
	contentType string,
	fileBytes []byte,
	opts ImportExecutorsOptions,
) (*ImportExecutorsResult, error) {
	if s.db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	nsRef, shortCode, err := resolveNamespaceRef(namespace, "")
	if err != nil {
		return nil, err
	}
	defaultValidUntil := strings.TrimSpace(opts.DefaultValidUntil)
	if defaultValidUntil == "" {
		defaultValidUntil = "2029-12-31"
	}
	if _, err := parseDatePtr(defaultValidUntil); err != nil {
		return nil, fmt.Errorf("invalid default_valid_until")
	}
	if len(fileBytes) == 0 {
		return nil, fmt.Errorf("empty file")
	}

	rows, err := parseEngineerRowsFromFile(fileName, contentType, fileBytes)
	if err != nil {
		return nil, err
	}
	return s.importExecutorRows(ctx, nsRef, shortCode, defaultValidUntil, opts.DefaultMaxConcurrentTasks, rows)
}

func (s *Service) importExecutorRows(
	ctx context.Context,
	nsRef string,
	shortCode string,
	defaultValidUntil string,
	defaultMaxConcurrent int,
	rows [][]string,
) (*ImportExecutorsResult, error) {
	res := &ImportExecutorsResult{
		NamespaceRef:        nsRef,
		TotalRows:           len(rows),
		Failures:            make([]ImportExecutorFailure, 0),
		ExecutorRefs:        make([]string, 0),
		RoleCounts:          make(map[string]int),
		StatsRefreshFailure: make([]StatsRefreshFailure, 0),
	}
	if defaultMaxConcurrent <= 0 {
		defaultMaxConcurrent = 5
	}

	seenExecutor := make(map[string]struct{})
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	for idx, row := range rows {
		parsed, skip, err := parseExecutorRow(row)
		if skip {
			continue
		}
		if err != nil {
			res.Failures = append(res.Failures, ImportExecutorFailure{
				Row:     idx + 1,
				Payload: strings.Join(row, "|"),
				Reason:  err.Error(),
			})
			continue
		}

		roleDef, err := resolveExecutorRole(parsed.Role)
		if err != nil {
			res.Failures = append(res.Failures, ImportExecutorFailure{
				Row:      idx + 1,
				Name:     parsed.Name,
				IDCard:   parsed.IDCard,
				RoleCode: parsed.Role,
				Reason:   err.Error(),
			})
			continue
		}

		position := parsed.Position
		if strings.TrimSpace(position) == "" {
			position = roleDef.Name
		}
		specialty := parsed.Specialty
		if strings.TrimSpace(specialty) == "" {
			specialty = roleDef.DefaultSpecialty
		}
		maxConcurrent := parsed.MaxConcurrent
		if maxConcurrent <= 0 {
			if roleDef.DefaultMaxConcurrent > 0 {
				maxConcurrent = roleDef.DefaultMaxConcurrent
			} else {
				maxConcurrent = defaultMaxConcurrent
			}
		}
		validUntil := parsed.ValidUntil
		if strings.TrimSpace(validUntil) == "" {
			validUntil = defaultValidUntil
		}
		if _, err := parseDatePtr(validUntil); err != nil {
			res.Failures = append(res.Failures, ImportExecutorFailure{
				Row:      idx + 1,
				Name:     parsed.Name,
				IDCard:   parsed.IDCard,
				RoleCode: roleDef.Code,
				Reason:   "invalid valid_until: " + err.Error(),
			})
			continue
		}

		executorRef := buildStaffExecutorRef(shortCode, parsed.Name, parsed.IDCard)
		var finalExecutorRef string
		err = tx.QueryRowContext(ctx, `
			INSERT INTO employees (
				name, id_card, executor_ref, company_ref, position, tenant_id, deleted, created_at, updated_at, migrate_status
			) VALUES (
				$1,$2,$3,$4,$5,$6,FALSE,$7,$8,'PENDING'
			)
			ON CONFLICT (id_card) DO UPDATE SET
				name=EXCLUDED.name,
				company_ref=EXCLUDED.company_ref,
				position=COALESCE(NULLIF(EXCLUDED.position,''), employees.position),
				tenant_id=EXCLUDED.tenant_id,
				executor_ref=COALESCE(NULLIF(employees.executor_ref,''), EXCLUDED.executor_ref),
				updated_at=EXCLUDED.updated_at
			RETURNING executor_ref
		`, parsed.Name, parsed.IDCard, executorRef, nsRef, position, s.tenantID, now, now).Scan(&finalExecutorRef)
		if err != nil {
			res.Failures = append(res.Failures, ImportExecutorFailure{
				Row:      idx + 1,
				Name:     parsed.Name,
				IDCard:   parsed.IDCard,
				RoleCode: roleDef.Code,
				Reason:   "upsert employee failed: " + err.Error(),
			})
			continue
		}
		if strings.TrimSpace(finalExecutorRef) == "" {
			finalExecutorRef = executorRef
		}

		certNo := strings.TrimSpace(parsed.CertNo)
		if certNo == "" {
			certNo = buildInternalRoleCertNo(roleDef.Code, parsed.IDCard)
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO qualifications (
				executor_ref, qual_type, cert_no, specialty, holder_type, status,
				max_concurrent_projects, valid_until, tenant_id, deleted, created_at, updated_at
			) VALUES (
				$1,$2,$3,$4,'PERSON','VALID',$5,$6,$7,FALSE,$8,$9
			)
			ON CONFLICT (cert_no) DO UPDATE SET
				executor_ref=EXCLUDED.executor_ref,
				qual_type=EXCLUDED.qual_type,
				specialty=EXCLUDED.specialty,
				holder_type=EXCLUDED.holder_type,
				status=EXCLUDED.status,
				max_concurrent_projects=EXCLUDED.max_concurrent_projects,
				valid_until=EXCLUDED.valid_until,
				tenant_id=EXCLUDED.tenant_id,
				deleted=FALSE,
				updated_at=EXCLUDED.updated_at
		`, finalExecutorRef, roleDef.Code, certNo, specialty, maxConcurrent, validUntil, s.tenantID, now, now)
		if err != nil {
			res.Failures = append(res.Failures, ImportExecutorFailure{
				Row:      idx + 1,
				Name:     parsed.Name,
				IDCard:   parsed.IDCard,
				RoleCode: roleDef.Code,
				Reason:   "upsert role qualification failed: " + err.Error(),
			})
			continue
		}

		roleSPURefs := normalizeStringList(rewriteSPURefsNamespace(roleDef.DefaultSPURefs, shortCode))
		personGenesisRef := fmt.Sprintf("%s/genesis/staff/%s/%s",
			nsRef,
			strings.ToLower(cleanIDCard(parsed.IDCard)),
			strings.ToLower(strings.TrimPrefix(roleDef.Code, "ROLE_")),
		)
		payload, _ := json.Marshal(map[string]any{
			"name":           parsed.Name,
			"id_card":        parsed.IDCard,
			"role_code":      roleDef.Code,
			"role_name":      roleDef.Name,
			"position":       position,
			"specialty":      specialty,
			"cert_no":        certNo,
			"valid_until":    validUntil,
			"executor_ref":   finalExecutorRef,
			"spu_refs":       roleSPURefs,
			"capability_src": "INTERNAL_ROLE",
		})
		_, err = tx.ExecContext(ctx, `
			INSERT INTO genesis_utxos (
				ref, resource_type, name, total_amount, available_amount, unit,
				constraints, status, tenant_id, created_at, updated_at
			) VALUES (
				$1,'PERSON_ROLE_GENESIS',$2,1,1,'ROLE',$3::jsonb,'ACTIVE',$4,$5,$6
			)
			ON CONFLICT (ref) DO UPDATE SET
				name=EXCLUDED.name,
				constraints=EXCLUDED.constraints,
				status='ACTIVE',
				updated_at=EXCLUDED.updated_at
		`, personGenesisRef, parsed.Name+"-"+roleDef.Code, string(payload), s.tenantID, now, now)
		if err != nil {
			res.Failures = append(res.Failures, ImportExecutorFailure{
				Row:      idx + 1,
				Name:     parsed.Name,
				IDCard:   parsed.IDCard,
				RoleCode: roleDef.Code,
				Reason:   "upsert staff genesis failed: " + err.Error(),
			})
			continue
		}

		res.SuccessCount++
		res.RoleCounts[roleDef.Code]++
		if _, ok := seenExecutor[finalExecutorRef]; !ok {
			seenExecutor[finalExecutorRef] = struct{}{}
			res.ExecutorRefs = append(res.ExecutorRefs, finalExecutorRef)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	res.FailureCount = len(res.Failures)
	return res, nil
}

func (s *Service) ExtractCert(ctx context.Context, in ExtractCertInput) (*ExtractCertResult, error) {
	if len(in.FileBytes) == 0 {
		return nil, fmt.Errorf("empty file")
	}
	apiKey := strings.TrimSpace(os.Getenv("CLAUDE_API_KEY"))
	if apiKey == "" {
		fallback := extractCertByRegex(string(in.FileBytes))
		fallback.NeedsManualReview = true
		fallback.ExtractedFromModel = false
		return fallback, nil
	}

prompt := `请从资质证书文件中提取结构化信息，并严格返回 JSON，不要返回任何额外文本。JSON 字段：{
  "company_name":"",
  "credit_code":"",
  "cert_no":"",
  "cert_valid_until":"YYYY-MM-DD",
  "legal_rep":"",
  "tech_director":"",
  "address":"",
  "qualifications":[
    {
      "name":"",
      "qual_type":"",
      "resource_key":"",
      "scope":[""],
      "grade":"",
      "industry":""
    }
  ]
}
qual_type 仅使用这些值：
QUAL_HIGHWAY_INDUSTRY_A, QUAL_MUNICIPAL_INDUSTRY_A, QUAL_ARCH_COMPREHENSIVE_A, QUAL_LANDSCAPE_SPECIAL_A, QUAL_WATER_INDUSTRY_B, QUAL_CUSTOM`

	mediaType := strings.TrimSpace(in.ContentType)
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}
	modelResp, err := s.callClaudeExtract(ctx, apiKey, in.FileName, mediaType, in.FileBytes, prompt)
	if err != nil {
		fallback := extractCertByRegex(string(in.FileBytes))
		fallback.RawText = err.Error()
		fallback.NeedsManualReview = true
		fallback.ExtractedFromModel = false
		return fallback, nil
	}
	parsed := extractCertByJSON(modelResp)
	parsed.RawText = modelResp
	parsed.ExtractedFromModel = true
	parsed.NeedsManualReview = parsed.CertNo == "" || len(parsed.Qualifications) == 0
	return parsed, nil
}

func (s *Service) callClaudeExtract(ctx context.Context, apiKey, fileName, mediaType string, fileBytes []byte, prompt string) (string, error) {
	type source struct {
		Type      string `json:"type"`
		MediaType string `json:"media_type,omitempty"`
		Data      string `json:"data,omitempty"`
	}
	type contentItem struct {
		Type   string `json:"type"`
		Text   string `json:"text,omitempty"`
		Source source `json:"source,omitempty"`
	}
	fileItemType := "document"
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(mediaType)), "image/") {
		fileItemType = "image"
	}
	reqPayload := map[string]any{
		"model":      "claude-3-5-sonnet-latest",
		"max_tokens": 2048,
		"messages": []map[string]any{
			{
				"role": "user",
				"content": []contentItem{
					{Type: "text", Text: prompt},
					{
						Type: fileItemType,
						Source: source{
							Type:      "base64",
							MediaType: mediaType,
							Data:      base64.StdEncoding.EncodeToString(fileBytes),
						},
					},
					{Type: "text", Text: "文件： " + fileName},
				},
			},
		},
	}
	body, _ := json.Marshal(reqPayload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.anthropic.com/v1/messages", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= http.StatusBadRequest {
		return "", fmt.Errorf("claude api error: %s", strings.TrimSpace(string(respBody)))
	}
	var parsed struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return "", fmt.Errorf("decode claude response failed: %w", err)
	}
	var builder strings.Builder
	for _, item := range parsed.Content {
		if item.Type != "text" {
			continue
		}
		if builder.Len() > 0 {
			builder.WriteString("\n")
		}
		builder.WriteString(item.Text)
	}
	if strings.TrimSpace(builder.String()) == "" {
		return "", fmt.Errorf("claude returned empty text")
	}
	return builder.String(), nil
}

func extractCertByJSON(raw string) *ExtractCertResult {
	result := &ExtractCertResult{
		Qualifications: make([]ExtractedQualification, 0),
	}
	jsonPart := findFirstJSONObject(raw)
	if jsonPart == "" {
		return result
	}
	var payload struct {
		CompanyName    string `json:"company_name"`
		CreditCode     string `json:"credit_code"`
		CertNo         string `json:"cert_no"`
		CertValidUntil string `json:"cert_valid_until"`
		LegalRep       string `json:"legal_rep"`
		TechDirector   string `json:"tech_director"`
		Address        string `json:"address"`
		Qualifications []struct {
			Name        string   `json:"name"`
			QualType    string   `json:"qual_type"`
			ResourceKey string   `json:"resource_key"`
			Scope       []string `json:"scope"`
			Grade       string   `json:"grade"`
			Industry    string   `json:"industry"`
		} `json:"qualifications"`
	}
	if err := json.Unmarshal([]byte(jsonPart), &payload); err != nil {
		return result
	}
	result.CompanyName = strings.TrimSpace(payload.CompanyName)
	result.CreditCode = strings.TrimSpace(payload.CreditCode)
	result.CertNo = strings.TrimSpace(payload.CertNo)
	result.CertValidUntil = strings.TrimSpace(payload.CertValidUntil)
	result.LegalRep = strings.TrimSpace(payload.LegalRep)
	result.TechDirector = strings.TrimSpace(payload.TechDirector)
	result.Address = strings.TrimSpace(payload.Address)
	for _, q := range payload.Qualifications {
		qq := ExtractedQualification{
			Name:        strings.TrimSpace(q.Name),
			QualType:    strings.TrimSpace(q.QualType),
			ResourceKey: strings.TrimSpace(q.ResourceKey),
			Scope:       normalizeStringList(q.Scope),
			Grade:       strings.TrimSpace(q.Grade),
			Industry:    strings.TrimSpace(q.Industry),
		}
		if qq.QualType == "" && qq.ResourceKey != "" {
			qq.QualType = qq.ResourceKey
		}
		result.Qualifications = append(result.Qualifications, qq)
	}
	return result
}

func extractCertByRegex(raw string) *ExtractCertResult {
	res := &ExtractCertResult{
		Qualifications: make([]ExtractedQualification, 0),
	}
	raw = strings.ReplaceAll(raw, "\r", "\n")
	lines := strings.Split(raw, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if res.CompanyName == "" && (strings.Contains(line, "公司") || strings.Contains(strings.ToLower(line), "design")) {
			res.CompanyName = line
		}
	}
	certRe := regexp.MustCompile(`[A-Z]\d{9,}-\d+/\d+`)
	if m := certRe.FindString(raw); m != "" {
		res.CertNo = m
	}
	codeRe := regexp.MustCompile(`[0-9A-Z]{18}`)
	if m := codeRe.FindString(raw); m != "" {
		res.CreditCode = m
	}
	dateRe := regexp.MustCompile(`20\d{2}[-./年]\d{1,2}[-./月]\d{1,2}`)
	if m := dateRe.FindString(raw); m != "" {
		res.CertValidUntil = normalizeDateString(m)
	}
	for _, tpl := range defaultQualTemplates {
		for _, alias := range tpl.AliasWords {
			if strings.Contains(strings.ToUpper(raw), strings.ToUpper(alias)) || strings.Contains(raw, alias) {
				res.Qualifications = append(res.Qualifications, ExtractedQualification{
					Name:        tpl.Name,
					QualType:    tpl.ResourceType,
					ResourceKey: tpl.ResourceType,
					Scope:       slices.Clone(tpl.Scope),
					Grade:       tpl.Grade,
					Industry:    tpl.Industry,
				})
				break
			}
		}
	}
	if len(res.Qualifications) == 0 {
		for _, tpl := range defaultQualTemplates {
			res.Qualifications = append(res.Qualifications, ExtractedQualification{
				Name:        tpl.Name,
				QualType:    tpl.ResourceType,
				ResourceKey: tpl.ResourceType,
				Scope:       slices.Clone(tpl.Scope),
				Grade:       tpl.Grade,
				Industry:    tpl.Industry,
			})
		}
	}
	return res
}

type parsedEngineerRow struct {
	Name         string
	IDCard       string
	RegisterType string
	Specialty    string
	CertNo       string
}

type parsedExecutorRow struct {
	Name          string
	IDCard        string
	Role          string
	Specialty     string
	Position      string
	MaxConcurrent int
	CertNo        string
	ValidUntil    string
}

func parseExecutorRow(row []string) (*parsedExecutorRow, bool, error) {
	trimmed := make([]string, 0, len(row))
	for _, cell := range row {
		trimmed = append(trimmed, strings.TrimSpace(cell))
	}
	nonEmpty := 0
	for _, c := range trimmed {
		if c != "" {
			nonEmpty++
		}
	}
	if nonEmpty == 0 {
		return nil, true, nil
	}

	joinedUpper := strings.ToUpper(strings.Join(trimmed, "|"))
	if strings.Contains(joinedUpper, "姓名") ||
		strings.Contains(joinedUpper, "身份证") ||
		strings.Contains(joinedUpper, "角色") ||
		strings.Contains(joinedUpper, "岗位") ||
		strings.Contains(joinedUpper, "ROLE") {
		return nil, true, nil
	}

	if len(trimmed) < 3 {
		return nil, false, fmt.Errorf("row columns not enough")
	}
	start := 0
	if isIndexCell(trimmed[0]) && len(trimmed) >= 4 {
		start = 1
	}
	if len(trimmed)-start < 3 {
		return nil, false, fmt.Errorf("row columns not enough")
	}

	out := &parsedExecutorRow{
		Name:   strings.TrimSpace(trimmed[start]),
		IDCard: cleanIDCard(trimmed[start+1]),
		Role:   strings.TrimSpace(trimmed[start+2]),
	}
	if len(trimmed)-start >= 4 {
		out.Specialty = strings.TrimSpace(trimmed[start+3])
	}
	if len(trimmed)-start >= 5 {
		out.Position = strings.TrimSpace(trimmed[start+4])
	}
	if len(trimmed)-start >= 6 {
		maxRaw := strings.TrimSpace(trimmed[start+5])
		if maxRaw != "" {
			n, err := strconv.Atoi(maxRaw)
			if err != nil {
				return nil, false, fmt.Errorf("invalid max_concurrent_projects")
			}
			out.MaxConcurrent = n
		}
	}
	if len(trimmed)-start >= 7 {
		out.CertNo = strings.TrimSpace(trimmed[start+6])
	}
	if len(trimmed)-start >= 8 {
		out.ValidUntil = strings.TrimSpace(trimmed[start+7])
	}

	if out.Name == "" || out.IDCard == "" || out.Role == "" {
		return nil, false, fmt.Errorf("name/id_card/role is required")
	}
	if !isLikelyIDCard(out.IDCard) {
		return nil, false, fmt.Errorf("invalid id_card")
	}
	return out, false, nil
}

func resolveExecutorRole(raw string) (executorRoleDef, error) {
	normalized := normalizeRoleKey(raw)
	if normalized == "" {
		return executorRoleDef{}, fmt.Errorf("role is required")
	}
	for _, role := range executorRoleDefs {
		if normalizeRoleKey(role.Code) == normalized {
			return role, nil
		}
		for _, alias := range role.Aliases {
			if normalizeRoleKey(alias) == normalized {
				return role, nil
			}
		}
	}
	return executorRoleDef{}, fmt.Errorf("unsupported role: %s", raw)
}

func normalizeRoleKey(raw string) string {
	raw = strings.TrimSpace(strings.ToUpper(raw))
	if raw == "" {
		return ""
	}
	replacer := strings.NewReplacer(
		"-", "",
		"_", "",
		" ", "",
		"（", "",
		"）", "",
		"(", "",
		")", "",
		"/", "",
		"\\", "",
	)
	return replacer.Replace(raw)
}

func parseEngineerRow(row []string) (*parsedEngineerRow, bool, error) {
	trimmed := make([]string, 0, len(row))
	for _, cell := range row {
		trimmed = append(trimmed, strings.TrimSpace(cell))
	}
	nonEmpty := 0
	for _, c := range trimmed {
		if c != "" {
			nonEmpty++
		}
	}
	if nonEmpty == 0 {
		return nil, true, nil
	}

	joinedUpper := strings.ToUpper(strings.Join(trimmed, "|"))
	if strings.Contains(joinedUpper, "姓名") ||
		strings.Contains(joinedUpper, "身份证") ||
		strings.Contains(joinedUpper, "注册类别") ||
		strings.Contains(joinedUpper, "REGISTER") {
		return nil, true, nil
	}

	if len(trimmed) < 4 {
		return nil, false, fmt.Errorf("row columns not enough")
	}
	start := 0
	if isIndexCell(trimmed[0]) && len(trimmed) >= 5 {
		start = 1
	}
	if len(trimmed)-start < 4 {
		return nil, false, fmt.Errorf("row columns not enough")
	}

	name := strings.TrimSpace(trimmed[start])
	idCard := cleanIDCard(trimmed[start+1])
	registerType := strings.TrimSpace(trimmed[start+2])
	specialty := ""
	certNoIdx := start + 3
	if len(trimmed)-start >= 5 {
		specialty = strings.TrimSpace(trimmed[start+3])
		certNoIdx = start + 4
	}
	if certNoIdx >= len(trimmed) {
		return nil, false, fmt.Errorf("missing cert_no")
	}
	certNo := strings.TrimSpace(trimmed[certNoIdx])

	if name == "" || idCard == "" || registerType == "" || certNo == "" {
		return nil, false, fmt.Errorf("name/id_card/register_type/cert_no is required")
	}
	if !isLikelyIDCard(idCard) {
		return nil, false, fmt.Errorf("invalid id_card")
	}
	return &parsedEngineerRow{
		Name:         name,
		IDCard:       idCard,
		RegisterType: registerType,
		Specialty:    specialty,
		CertNo:       certNo,
	}, false, nil
}

func mapQualType(registerType, specialty string) (string, error) {
	combined := strings.ToUpper(strings.TrimSpace(registerType) + " " + strings.TrimSpace(specialty))
	switch {
	case strings.Contains(combined, "REG_STRUCTURE_2"), strings.Contains(combined, "二级注册结构工程师"), strings.Contains(combined, "二级结构"):
		return "REG_STRUCTURE_2", nil
	case strings.Contains(combined, "一级注册结构工程师"), strings.Contains(combined, "REG_STRUCTURE"), strings.Contains(combined, "一级结构"):
		return "REG_STRUCTURE", nil
	case strings.Contains(combined, "REG_ARCH"), strings.Contains(combined, "一级注册建筑师"), strings.Contains(combined, "建筑师"):
		return "REG_ARCH", nil
	case strings.Contains(combined, "REG_CIVIL_GEOTEC"), strings.Contains(combined, "注册土木工程师（岩土"), strings.Contains(combined, "岩土"):
		return "REG_CIVIL_GEOTEC", nil
	case strings.Contains(combined, "REG_CIVIL_WATER"), strings.Contains(combined, "注册土木工程师（水利"), strings.Contains(combined, "水利水电"):
		return "REG_CIVIL_WATER", nil
	case strings.Contains(combined, "REG_COST"), strings.Contains(combined, "一级注册造价工程师"), strings.Contains(combined, "造价"):
		return "REG_COST", nil
	case strings.Contains(combined, "REG_ELECTRIC_POWER"), strings.Contains(combined, "注册电气工程师（供配电"), strings.Contains(combined, "供配电"):
		return "REG_ELECTRIC_POWER", nil
	case strings.Contains(combined, "REG_ELECTRIC_TRANS"), strings.Contains(combined, "注册电气工程师（发输变电"), strings.Contains(combined, "发输变电"):
		return "REG_ELECTRIC_TRANS", nil
	case strings.Contains(combined, "REG_MEP_POWER"), strings.Contains(combined, "注册公用设备工程师（动力"), strings.Contains(combined, "动力"):
		return "REG_MEP_POWER", nil
	case strings.Contains(combined, "REG_MEP_WATER"), strings.Contains(combined, "注册公用设备工程师（给水排水"), strings.Contains(combined, "给水排水"):
		return "REG_MEP_WATER", nil
	case strings.Contains(combined, "REG_MEP_HVAC"), strings.Contains(combined, "注册公用设备工程师（暖通空调"), strings.Contains(combined, "暖通空调"):
		return "REG_MEP_HVAC", nil
	default:
		return "", fmt.Errorf("unsupported register_type: %s", registerType)
	}
}

func maxProjectsByQualType(qualType string) int {
	switch strings.ToUpper(strings.TrimSpace(qualType)) {
	case "REG_COST":
		return 5
	case "REG_STRUCTURE_2":
		return 2
	default:
		return 3
	}
}

func resolveNamespaceRef(namespaceRef, shortCode string) (string, string, error) {
	ns := strings.TrimSpace(namespaceRef)
	code := strings.TrimSpace(shortCode)
	if ns != "" && strings.HasPrefix(ns, "v://") {
		withoutPrefix := strings.TrimPrefix(ns, "v://")
		if idx := strings.Index(withoutPrefix, "/"); idx >= 0 {
			code = withoutPrefix[:idx]
		} else {
			code = withoutPrefix
		}
		code = sanitizeShortCode(code)
		if code == "" {
			return "", "", fmt.Errorf("invalid namespace_ref")
		}
		return "v://" + code, code, nil
	}
	if ns != "" && !strings.HasPrefix(ns, "v://") && code == "" {
		code = ns
	}
	code = sanitizeShortCode(code)
	if code == "" {
		return "", "", fmt.Errorf("short_code is required")
	}
	return "v://" + code, code, nil
}

func sanitizeShortCode(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return ""
	}
	raw = strings.TrimPrefix(raw, "v://")
	if idx := strings.Index(raw, "/"); idx >= 0 {
		raw = raw[:idx]
	}
	var b strings.Builder
	prevSep := false
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			prevSep = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			prevSep = false
		case r == '.':
			if b.Len() == 0 || prevSep {
				continue
			}
			b.WriteRune('.')
			prevSep = true
		case r == '-' || r == '_':
			if b.Len() == 0 || prevSep {
				continue
			}
			b.WriteRune('-')
			prevSep = true
		}
	}
	return strings.Trim(b.String(), "-.")
}

func extractSlug(shortCode string) string {
	parts := strings.Split(shortCode, ".")
	if len(parts) == 0 {
		return shortCode
	}
	return parts[len(parts)-1]
}

func extractParentRef(shortCode string) string {
	shortCode = strings.TrimSpace(shortCode)
	if shortCode == "" {
		return ""
	}
	parts := strings.Split(shortCode, ".")
	if len(parts) <= 1 {
		return "v://coordos"
	}
	parentCode := strings.Join(parts[:len(parts)-1], ".")
	return "v://" + parentCode
}

func computeDepth(shortCode string) int {
	if shortCode == "" {
		return 0
	}
	return strings.Count(shortCode, ".") + 1
}

func matchQualTemplate(resourceType, qualType, name string) *qualTemplate {
	keywords := []string{
		strings.ToUpper(strings.TrimSpace(resourceType)),
		strings.ToUpper(strings.TrimSpace(qualType)),
		strings.ToUpper(strings.TrimSpace(name)),
	}
	for _, tpl := range defaultQualTemplates {
		for _, word := range tpl.AliasWords {
			word = strings.ToUpper(word)
			for _, keyword := range keywords {
				if keyword != "" && strings.Contains(keyword, word) {
					return &tpl
				}
			}
		}
		if strings.EqualFold(strings.TrimSpace(resourceType), strings.TrimSpace(tpl.ResourceType)) {
			return &tpl
		}
	}
	return nil
}

func normalizeOrgQualifications(input []OrgQualificationInput) []OrgQualificationInput {
	if len(input) == 0 {
		return nil
	}
	out := make([]OrgQualificationInput, 0, len(input))
	for _, item := range input {
		if strings.TrimSpace(item.ResourceType) == "" &&
			strings.TrimSpace(item.QualType) == "" &&
			strings.TrimSpace(item.Name) == "" {
			continue
		}
		item.Scope = normalizeStringList(item.Scope)
		item.RuleBinding = normalizeStringList(item.RuleBinding)
		out = append(out, item)
	}
	return out
}

func resolveGenesisRef(nsRef, raw string, tpl *qualTemplate, resourceType string) string {
	raw = strings.TrimSpace(raw)
	if raw != "" {
		if strings.HasPrefix(raw, "v://") {
			return raw
		}
		if strings.HasPrefix(raw, "genesis/") {
			return nsRef + "/" + raw
		}
		return nsRef + "/genesis/" + strings.TrimPrefix(raw, "/")
	}
	if tpl != nil && tpl.Suffix != "" {
		return nsRef + "/genesis/" + tpl.Suffix
	}
	return nsRef + "/genesis/qual/" + sanitizeToken(resourceType)
}

func normalizeStringList(raw []string) []string {
	if len(raw) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(raw))
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func mergeStrings(parts ...[]string) []string {
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	seen := make(map[string]struct{}, total)
	out := make([]string, 0, total)
	for _, p := range parts {
		for _, v := range p {
			v = strings.TrimSpace(v)
			if v == "" {
				continue
			}
			if _, ok := seen[v]; ok {
				continue
			}
			seen[v] = struct{}{}
			out = append(out, v)
		}
	}
	return out
}

func parseDatePtr(raw string) (*time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	raw = normalizeDateString(raw)
	t, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func normalizeDateString(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.ReplaceAll(raw, "年", "-")
	raw = strings.ReplaceAll(raw, "月", "-")
	raw = strings.ReplaceAll(raw, "日", "")
	raw = strings.ReplaceAll(raw, ".", "-")
	raw = strings.ReplaceAll(raw, "/", "-")
	parts := strings.Split(raw, "-")
	if len(parts) != 3 {
		return raw
	}
	year := strings.TrimSpace(parts[0])
	month := leftPad2(strings.TrimSpace(parts[1]))
	day := leftPad2(strings.TrimSpace(parts[2]))
	return year + "-" + month + "-" + day
}

func leftPad2(v string) string {
	if len(v) >= 2 {
		return v
	}
	return "0" + v
}

func formatDatePtr(v *time.Time) string {
	if v == nil {
		return ""
	}
	return v.Format("2006-01-02")
}

func emptyToNull(v string) any {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil
	}
	return v
}

func emptyToNil(v string) *string {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil
	}
	return &v
}

func findFirstJSONObject(raw string) string {
	start := strings.Index(raw, "{")
	end := strings.LastIndex(raw, "}")
	if start < 0 || end <= start {
		return ""
	}
	return raw[start : end+1]
}

func parseEngineerRowsFromFile(fileName, contentType string, fileBytes []byte) ([][]string, error) {
	fileBytes = bytes.TrimPrefix(fileBytes, []byte{0xEF, 0xBB, 0xBF})
	ext := strings.ToLower(strings.TrimSpace(filepath.Ext(fileName)))
	ct := strings.ToLower(strings.TrimSpace(contentType))

	switch {
	case ext == ".xlsx", strings.Contains(ct, "spreadsheetml"), looksLikeZip(fileBytes):
		rows, err := parseXLSXRows(fileBytes)
		if err != nil {
			if ext == ".xlsx" || strings.Contains(ct, "spreadsheetml") {
				return nil, fmt.Errorf("parse xlsx failed: %w", err)
			}
		} else {
			return rows, nil
		}
	}

	rows, err := parseCSVRows(fileBytes)
	if err != nil {
		return nil, fmt.Errorf("parse csv failed: %w", err)
	}
	return rows, nil
}

func parseCSVRows(raw []byte) ([][]string, error) {
	delimiter := detectCSVDelimiter(raw)
	reader := csv.NewReader(bytes.NewReader(raw))
	reader.Comma = delimiter
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1
	return reader.ReadAll()
}

type xlsxWorksheet struct {
	SheetData struct {
		Rows []xlsxRow `xml:"row"`
	} `xml:"sheetData"`
}

type xlsxRow struct {
	Cells []xlsxCell `xml:"c"`
}

type xlsxCell struct {
	Ref      string `xml:"r,attr"`
	Type     string `xml:"t,attr"`
	Value    string `xml:"v"`
	InlineIs struct {
		Text string `xml:"t"`
	} `xml:"is"`
}

type xlsxSharedStrings struct {
	Items []xlsxSI `xml:"si"`
}

type xlsxSI struct {
	Text string `xml:"t"`
	Runs []struct {
		Text string `xml:"t"`
	} `xml:"r"`
}

type xlsxWorkbook struct {
	Sheets []xlsxWorkbookSheet `xml:"sheets>sheet"`
}

type xlsxWorkbookSheet struct {
	Name string `xml:"name,attr"`
	RID  string `xml:"id,attr"`
}

type xlsxWorkbookRels struct {
	Relationships []xlsxRelationship `xml:"Relationship"`
}

type xlsxRelationship struct {
	ID     string `xml:"Id,attr"`
	Target string `xml:"Target,attr"`
	Type   string `xml:"Type,attr"`
}

func parseXLSXRows(raw []byte) ([][]string, error) {
	zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return nil, err
	}
	files := make(map[string]*zip.File, len(zr.File))
	for _, f := range zr.File {
		files[f.Name] = f
	}

	sharedStrings := make([]string, 0)
	if f := files["xl/sharedStrings.xml"]; f != nil {
		data, err := readZipFile(f)
		if err != nil {
			return nil, err
		}
		var sst xlsxSharedStrings
		if err := xml.Unmarshal(data, &sst); err == nil {
			for _, it := range sst.Items {
				txt := strings.TrimSpace(it.Text)
				if txt == "" && len(it.Runs) > 0 {
					var b strings.Builder
					for _, run := range it.Runs {
						b.WriteString(run.Text)
					}
					txt = b.String()
				}
				sharedStrings = append(sharedStrings, txt)
			}
		}
	}

	sheetPath := "xl/worksheets/sheet1.xml"
	if f := files["xl/workbook.xml"]; f != nil {
		wbData, err := readZipFile(f)
		if err != nil {
			return nil, err
		}
		var wb xlsxWorkbook
		if err := xml.Unmarshal(wbData, &wb); err == nil && len(wb.Sheets) > 0 {
			if relFile := files["xl/_rels/workbook.xml.rels"]; relFile != nil {
				relData, err := readZipFile(relFile)
				if err != nil {
					return nil, err
				}
				var rels xlsxWorkbookRels
				if err := xml.Unmarshal(relData, &rels); err == nil {
					relMap := map[string]string{}
					for _, rel := range rels.Relationships {
						target := strings.TrimSpace(rel.Target)
						if target == "" {
							continue
						}
						if !strings.HasPrefix(target, "xl/") {
							target = "xl/" + strings.TrimPrefix(target, "/")
						}
						target = strings.ReplaceAll(target, "\\", "/")
						relMap[strings.TrimSpace(rel.ID)] = target
					}
					first := wb.Sheets[0]
					if target, ok := relMap[strings.TrimSpace(first.RID)]; ok {
						sheetPath = target
					}
				}
			}
		}
	}
	sheetFile := files[sheetPath]
	if sheetFile == nil {
		return nil, fmt.Errorf("worksheet not found")
	}
	sheetData, err := readZipFile(sheetFile)
	if err != nil {
		return nil, err
	}

	var ws xlsxWorksheet
	if err := xml.Unmarshal(sheetData, &ws); err != nil {
		return nil, err
	}
	rows := make([][]string, 0, len(ws.SheetData.Rows))
	for _, row := range ws.SheetData.Rows {
		maxCol := 0
		cols := make(map[int]string, len(row.Cells))
		for _, c := range row.Cells {
			col := columnIndexFromCellRef(c.Ref)
			if col < 0 {
				col = maxCol
			}
			val := strings.TrimSpace(c.Value)
			switch strings.TrimSpace(c.Type) {
			case "s":
				idx, err := strconv.Atoi(val)
				if err == nil && idx >= 0 && idx < len(sharedStrings) {
					val = sharedStrings[idx]
				}
			case "inlineStr":
				val = strings.TrimSpace(c.InlineIs.Text)
			case "", "n":
				val = normalizeSpreadsheetNumericValue(val)
			}
			cols[col] = val
			if col > maxCol {
				maxCol = col
			}
		}
		if len(cols) == 0 {
			rows = append(rows, []string{})
			continue
		}
		record := make([]string, maxCol+1)
		for i := 0; i <= maxCol; i++ {
			record[i] = cols[i]
		}
		rows = append(rows, record)
	}
	return rows, nil
}

func readZipFile(f *zip.File) ([]byte, error) {
	rc, err := f.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func looksLikeZip(raw []byte) bool {
	return len(raw) > 4 && raw[0] == 'P' && raw[1] == 'K'
}

func columnIndexFromCellRef(ref string) int {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return -1
	}
	col := 0
	seen := false
	for _, r := range ref {
		if r >= 'A' && r <= 'Z' {
			col = col*26 + int(r-'A'+1)
			seen = true
			continue
		}
		if r >= 'a' && r <= 'z' {
			col = col*26 + int(r-'a'+1)
			seen = true
			continue
		}
		break
	}
	if !seen {
		return -1
	}
	return col - 1
}

func normalizeSpreadsheetNumericValue(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return raw
	}
	lower := strings.ToLower(raw)
	if !strings.Contains(lower, "e") && !strings.HasSuffix(raw, ".0") {
		return raw
	}
	f, _, err := big.ParseFloat(raw, 10, 256, big.ToNearestEven)
	if err != nil {
		return raw
	}
	i, _ := f.Int(nil)
	if i == nil {
		return raw
	}
	return i.String()
}

func detectCSVDelimiter(raw []byte) rune {
	if len(raw) == 0 {
		return ','
	}
	firstLine := string(raw)
	if idx := strings.IndexByte(firstLine, '\n'); idx > 0 {
		firstLine = firstLine[:idx]
	}
	candidates := []rune{',', ';', '\t'}
	best := ','
	bestCount := -1
	for _, c := range candidates {
		count := strings.Count(firstLine, string(c))
		if count > bestCount {
			best = c
			bestCount = count
		}
	}
	return best
}

func isIndexCell(v string) bool {
	v = strings.TrimSpace(v)
	if v == "" {
		return false
	}
	_, err := strconv.Atoi(v)
	return err == nil
}

func cleanIDCard(v string) string {
	v = strings.TrimSpace(strings.ToUpper(v))
	return strings.ReplaceAll(v, " ", "")
}

func isLikelyIDCard(v string) bool {
	if len(v) != 15 && len(v) != 18 {
		return false
	}
	for i, r := range v {
		if r >= '0' && r <= '9' {
			continue
		}
		if i == len(v)-1 && r == 'X' {
			continue
		}
		return false
	}
	return true
}

func buildExecutorRef(shortCode, name, idCard string) string {
	normalizedID := sanitizeIDForRef(idCard)
	if normalizedID == "" {
		normalizedID = sanitizeToken(name)
	}
	if normalizedID == "" {
		normalizedID = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	slug := extractSlug(shortCode)
	return fmt.Sprintf("v://%s/executor/person/%s@v1", shortCode, slug+"_"+normalizedID)
}

func buildStaffExecutorRef(shortCode, name, idCard string) string {
	normalizedID := sanitizeIDForRef(idCard)
	if normalizedID == "" {
		normalizedID = sanitizeToken(name)
	}
	if normalizedID == "" {
		normalizedID = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	slug := extractSlug(shortCode)
	return fmt.Sprintf("v://%s/executor/staff/%s@v1", shortCode, slug+"_"+normalizedID)
}

func buildInternalRoleCertNo(roleCode, idCard string) string {
	id := sanitizeIDForRef(idCard)
	if len(id) > 8 {
		id = id[len(id)-8:]
	}
	if id == "" {
		id = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	role := strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(roleCode)), "ROLE_")
	role = sanitizeToken(role)
	return "INT-" + strings.ToUpper(role) + "-" + strings.ToUpper(id)
}

func sanitizeIDForRef(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func sanitizeToken(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return "na"
	}
	var b strings.Builder
	for _, r := range v {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return strings.Trim(b.String(), "_")
}

func rewriteSPURefsNamespace(spuRefs []string, shortCode string) []string {
	tenant := sanitizeShortCode(shortCode)
	if tenant == "" || len(spuRefs) == 0 {
		return spuRefs
	}
	out := make([]string, 0, len(spuRefs))
	for _, ref := range spuRefs {
		ref = strings.TrimSpace(ref)
		if !strings.HasPrefix(ref, "v://") {
			out = append(out, ref)
			continue
		}
		rest := strings.TrimPrefix(ref, "v://")
		slash := strings.Index(rest, "/")
		if slash <= 0 || slash+1 >= len(rest) {
			out = append(out, ref)
			continue
		}
		out = append(out, "v://"+tenant+"/"+rest[slash+1:])
	}
	return out
}
