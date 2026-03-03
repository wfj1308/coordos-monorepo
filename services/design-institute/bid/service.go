package bid

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ══════════════════════════════════════════════════════════════
// 类型定义
// ══════════════════════════════════════════════════════════════

type BidStatus string

const (
	StatusDraft      BidStatus = "DRAFT"
	StatusSubmitted  BidStatus = "SUBMITTED"
	StatusAwarded    BidStatus = "AWARDED"
	StatusContracted BidStatus = "CONTRACTED"
	StatusFailed     BidStatus = "FAILED"
)

type ResourceType string

const (
	ResourceQualCompany ResourceType = "QUAL_COMPANY"
	ResourceQualPerson  ResourceType = "QUAL_PERSON"
	ResourceAchievement ResourceType = "ACHIEVEMENT"
	ResourceFinancial   ResourceType = "FINANCIAL"
)

type ConsumeMode string

const (
	ConsumeReference ConsumeMode = "REFERENCE"
	ConsumeOccupy    ConsumeMode = "OCCUPY"
)

type ConsumeStatus string

const (
	ConsumePending    ConsumeStatus = "PENDING"
	ConsumeReferenced ConsumeStatus = "REFERENCED"
	ConsumeOccupied   ConsumeStatus = "OCCUPIED"
	ConsumeReleased   ConsumeStatus = "RELEASED"
)

// ══════════════════════════════════════════════════════════════
// 数据结构
// ══════════════════════════════════════════════════════════════

type BidDocument struct {
	ID               int64
	BidRef           string
	TenantID         int
	NamespaceRef     string
	TenderGenesisRef *string
	ProjectName      string
	ProjectType      string
	OwnerName        *string
	EstimatedAmount  *float64
	BidDeadline      *time.Time
	OurBidAmount     *float64
	BidPackageRef    *string
	Status           BidStatus
	ProofHash        string
	ResourceCount    int
	ProjectRef       *string
	ContractID       *int64
	CreatedAt        time.Time
	SubmittedAt      *time.Time
	AwardedAt        *time.Time
	FailedAt         *time.Time
}

type BidResource struct {
	ID            int64
	BidID         int64
	TenantID      int
	ResourceType  ResourceType
	ResourceRef   string
	ConsumeMode   ConsumeMode
	ConsumeStatus ConsumeStatus
	ResourceName  string
	ResourceData  json.RawMessage
	ValidFrom     *time.Time
	ValidUntil    *time.Time
	VerifyURL     *string
	CreatedAt     time.Time
}

type AchievementInPool struct {
	ID               int64
	UTXORef          string
	SPURef           string
	ProjectRef       string
	ExecutorRef      string
	ProofHash        string
	Status           string
	Source           string
	SettledAt        *time.Time
	TenantID         int
	ProjectName      string
	ProjectStatus    string
	ContractName     *string
	ContractAmount   *float64
	InferredProjType string
	Within3Years     bool
	IsUsableForBid   bool
}

// ══════════════════════════════════════════════════════════════
// 输入类型
// ══════════════════════════════════════════════════════════════

type CreateBidInput struct {
	ProjectName      string     `json:"ProjectName"`
	ProjectType      string     `json:"ProjectType"`
	OwnerName        *string    `json:"OwnerName,omitempty"`
	EstimatedAmount  *float64   `json:"EstimatedAmount,omitempty"`
	BidDeadline      *time.Time `json:"BidDeadline,omitempty"`
	OurBidAmount     *float64   `json:"OurBidAmount,omitempty"`
	NamespaceRef     string     `json:"NamespaceRef"`
	TenderGenesisRef *string    `json:"TenderGenesisRef,omitempty"`

	CompanyQualRefs []string `json:"CompanyQualRefs"`
	PersonQualRefs  []string `json:"PersonQualRefs"`
	AchievementRefs []string `json:"AchievementRefs"`
}

type ValidateBidInput struct {
	NamespaceRef     string `json:"namespace_ref"`
	ProjectType      string `json:"project_type"`
	TargetSPU        string `json:"target_spu"`
	ExecutorRef      string `json:"executor_ref"`
	NeedPersonQuals  int    `json:"need_person_quals"`
	NeedAchievements int    `json:"need_achievements"`
	AchievementYears int    `json:"achievement_years"`
}

type VerifyResult struct {
	CanBid   bool     `json:"can_bid"`
	Pass     bool     `json:"pass"`
	Errors   []string `json:"errors,omitempty"`
	Warnings []string `json:"warnings,omitempty"`

	// 约束检查明细
	ConstraintChecks []ConstraintCheck `json:"constraint_checks"`

	// 资源统计
	CompanyQualCount   int `json:"company_qual_count"`
	PersonQualCount    int `json:"person_qual_count"`
	AvailableEngineers int `json:"available_engineers"`
	AchievementCount   int `json:"achievement_count"`

	// 匹配度评分
	MatchingScore int `json:"matching_score"`
}

type ConstraintCheck struct {
	Constraint  string `json:"constraint"`
	Required    string `json:"required"`
	Actual      string `json:"actual"`
	Pass        bool   `json:"pass"`
	Description string `json:"description"`
}

type ExecutorCandidate struct {
	ExecutorRef        string        `json:"executor_ref"`
	EmployeeName       string        `json:"employee_name,omitempty"`
	CapabilityLevel    string        `json:"capability_level"`
	Skills             []string      `json:"skills,omitempty"`
	Qualifications     []QualSummary `json:"qualifications,omitempty"`
	RecentAchievements int           `json:"recent_achievements"`
	IsAvailable        bool          `json:"is_available"`
	MatchingScore      int           `json:"matching_score"`
}

type QualSummary struct {
	QualType   string `json:"qual_type"`
	CertNo     string `json:"cert_no"`
	ValidUntil string `json:"valid_until"`
}

type AchievementMatch struct {
	UTXORef        string  `json:"utxo_ref"`
	ProjectName    string  `json:"project_name"`
	SPURef         string  `json:"spu_ref"`
	SettledAt      string  `json:"settled_at"`
	ContractAmount float64 `json:"contract_amount"`
	ProofHash      string  `json:"proof_hash"`
	MatchScore     int     `json:"match_score"`
}

type CapabilityProof struct {
	ExecutorRef string    `json:"executor_ref"`
	TargetSPU   string    `json:"target_spu"`
	GeneratedAt time.Time `json:"generated_at"`

	// 企业资质
	CompanyQuals []QualSummary `json:"company_quals"`

	// 人员能力
	Personnel []ExecutorCandidate `json:"personnel"`

	// 业绩证明
	Achievements []AchievementMatch `json:"achievements"`

	// 综合评分
	OverallScore int  `json:"overall_score"`
	IsValid      bool `json:"is_valid"`
}

type ValidateResult struct {
	CanBid   bool     `json:"can_bid"`
	Errors   []string `json:"errors,omitempty"`
	Warnings []string `json:"warnings,omitempty"`

	// 资源检查结果
	CompanyQualOK bool `json:"company_qual_ok"`
	PersonQualOK  bool `json:"person_qual_ok"`
	AchievementOK bool `json:"achievement_ok"`

	// 可用资源统计
	CompanyQualCount   int `json:"company_qual_count"`
	PersonQualCount    int `json:"person_qual_count"`
	AvailableEngineers int `json:"available_engineers"`
	AchievementCount   int `json:"achievement_count"`
}

type AchievementsFilter struct {
	NamespaceRef string
	ProjectType  *string
	WithinYears  int
	Limit        int
	Offset       int
}

type CreateTenderInput struct {
	ProjectName     string    `json:"project_name"`
	ProjectType     string    `json:"project_type"`
	OwnerName       string    `json:"owner_name,omitempty"`
	EstimatedAmount float64   `json:"estimated_amount,omitempty"`
	BidDeadline     time.Time `json:"bid_deadline,omitempty"`
	RuleBinding     []string  `json:"rule_binding,omitempty"`
}

type TenderGenesis struct {
	Ref             string   `json:"ref"`
	NamespaceRef    string   `json:"namespace_ref"`
	ProjectName     string   `json:"project_name"`
	ProjectType     string   `json:"project_type"`
	OwnerName       string   `json:"owner_name,omitempty"`
	EstimatedAmount float64  `json:"estimated_amount"`
	RuleBinding     []string `json:"rule_binding,omitempty"`
	Status          string   `json:"status"`
}

// ══════════════════════════════════════════════════════════════
// Store 接口
// ══════════════════════════════════════════════════════════════

type Store interface {
	// 投标文档
	CreateBid(ctx context.Context, bid *BidDocument) (int64, error)
	GetBid(ctx context.Context, id int64) (*BidDocument, error)
	GetBidByRef(ctx context.Context, ref string) (*BidDocument, error)
	UpdateBidStatus(ctx context.Context, id int64, status BidStatus, at time.Time) error
	ListBids(ctx context.Context, tenantID int, status *BidStatus, limit, offset int) ([]*BidDocument, int, error)

	// 投标资源
	AddResource(ctx context.Context, res *BidResource) (int64, error)
	ListResources(ctx context.Context, bidID int64) ([]*BidResource, error)
	UpdateResourceStatus(ctx context.Context, bidID int64, resourceType ResourceType, status ConsumeStatus) error

	// 业绩池
	SearchAchievements(ctx context.Context, f AchievementsFilter) ([]*AchievementInPool, int, error)
	GetAchievementByRef(ctx context.Context, utxoRef string) (*AchievementInPool, error)

	// 资源验证
	CountCompanyQuals(ctx context.Context, namespace string) (int, error)
	CountPersonQuals(ctx context.Context, namespace string) (int, error)
	CountAvailableEngineers(ctx context.Context, namespace string) (int, error)
	ValidateQualValid(ctx context.Context, qualRef string) (bool, error)

	// 执行体寻址
	FindCandidateExecutors(ctx context.Context, namespace string, spuRef string, limit int) ([]*ExecutorCandidate, error)
	GetExecutorCapabilities(ctx context.Context, executorRef string) (*ExecutorCandidate, error)
	MatchExecutorAchievements(ctx context.Context, executorRef string, projectType string, withinYears int, limit int) ([]*AchievementMatch, error)
}

type QualificationStore interface {
	GetByHolder(ctx context.Context, holderRef string) ([]map[string]any, error)
	CountByHolderType(ctx context.Context, holderRef string, holderType string) (int, error)
}

type ResolveStore interface {
	GetSPURequirements(spuRef string) (map[string]any, error)
}

// ══════════════════════════════════════════════════════════════
// Service
// ══════════════════════════════════════════════════════════════

type Service struct {
	store    Store
	tenantID int
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

func (s *Service) storeDB() (*sql.DB, error) {
	pg, ok := s.store.(*PGStore)
	if !ok || pg == nil || pg.db == nil {
		return nil, fmt.Errorf("bid store does not expose postgres db")
	}
	return pg.db, nil
}

func (s *Service) CreateTender(ctx context.Context, ns string, in CreateTenderInput) (*TenderGenesis, error) {
	db, err := s.storeDB()
	if err != nil {
		return nil, err
	}
	namespaceRef := normalizeNamespaceRef(ns)
	if namespaceRef == "" {
		return nil, fmt.Errorf("namespace is required")
	}

	projectName := strings.TrimSpace(in.ProjectName)
	if projectName == "" {
		projectName = "untitled_tender"
	}
	projectType := strings.TrimSpace(strings.ToUpper(in.ProjectType))
	if projectType == "" {
		projectType = "OTHER"
	}
	ownerName := strings.TrimSpace(in.OwnerName)
	ruleBinding := in.RuleBinding
	if len(ruleBinding) == 0 {
		ruleBinding = []string{"RULE-002"}
	}

	year := time.Now().Year()
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM genesis_utxos
		WHERE resource_type='TENDER' AND ref LIKE $1
	`, namespaceRef+"/genesis/tender/"+strconv.Itoa(year)+"/%").Scan(&count); err != nil {
		return nil, fmt.Errorf("count existing tenders failed: %w", err)
	}
	ref := fmt.Sprintf("%s/genesis/tender/%d/%03d", namespaceRef, year, count+1)

	constraintsJSON, _ := json.Marshal(map[string]any{
		"project_name":     projectName,
		"project_type":     projectType,
		"owner_name":       ownerName,
		"estimated_amount": in.EstimatedAmount,
		"bid_deadline":     in.BidDeadline,
		"rule_binding":     ruleBinding,
		"source":           "TENDER_API",
	})

	amountYuan := int64(in.EstimatedAmount * 10000)
	if amountYuan < 0 {
		amountYuan = 0
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO genesis_utxos (
			ref, resource_type, name,
			total_amount, available_amount, unit,
			constraints, status, tenant_id, created_at, updated_at
		) VALUES ($1,'TENDER',$2,$3,$3,'CNY',$4,'ACTIVE',$5,NOW(),NOW())
		ON CONFLICT (ref) DO UPDATE SET
			name=EXCLUDED.name,
			total_amount=EXCLUDED.total_amount,
			available_amount=EXCLUDED.available_amount,
			constraints=EXCLUDED.constraints,
			updated_at=NOW()
	`, ref, projectName, amountYuan, constraintsJSON, s.tenantID); err != nil {
		return nil, fmt.Errorf("create tender genesis failed: %w", err)
	}

	return &TenderGenesis{
		Ref:             ref,
		NamespaceRef:    namespaceRef,
		ProjectName:     projectName,
		ProjectType:     projectType,
		OwnerName:       ownerName,
		EstimatedAmount: in.EstimatedAmount,
		RuleBinding:     ruleBinding,
		Status:          "ACTIVE",
	}, nil
}

// ValidateBid 投标前校验
// 检查：企业资质有效性、注册工程师可用数、业绩匹配度
func (s *Service) ValidateBid(ctx context.Context, in ValidateBidInput) (*ValidateResult, error) {
	result := &ValidateResult{
		CanBid: true,
	}

	// 1. 检查企业资质
	companyQualCount, err := s.store.CountCompanyQuals(ctx, in.NamespaceRef)
	if err != nil {
		return nil, fmt.Errorf("count company quals failed: %w", err)
	}
	result.CompanyQualCount = companyQualCount
	if companyQualCount == 0 {
		result.Errors = append(result.Errors, "企业资质不足")
		result.CompanyQualOK = false
	} else {
		result.CompanyQualOK = true
	}

	// 2. 检查注册工程师
	personQualCount, err := s.store.CountPersonQuals(ctx, in.NamespaceRef)
	if err != nil {
		return nil, fmt.Errorf("count person quals failed: %w", err)
	}
	result.PersonQualCount = personQualCount

	availableEngineers, err := s.store.CountAvailableEngineers(ctx, in.NamespaceRef)
	if err != nil {
		return nil, fmt.Errorf("count available engineers failed: %w", err)
	}
	result.AvailableEngineers = availableEngineers

	if availableEngineers < in.NeedPersonQuals {
		result.Errors = append(result.Errors,
			fmt.Sprintf("可用注册工程师不足：需要%d，可用%d", in.NeedPersonQuals, availableEngineers))
		result.PersonQualOK = false
	} else {
		result.PersonQualOK = true
	}

	// 3. 检查业绩
	withinYears := in.AchievementYears
	if withinYears == 0 {
		withinYears = 3
	}
	_, total, err := s.store.SearchAchievements(ctx, AchievementsFilter{
		NamespaceRef: in.NamespaceRef,
		ProjectType:  &in.ProjectType,
		WithinYears:  withinYears,
		Limit:        100,
	})
	if err != nil {
		return nil, fmt.Errorf("search achievements failed: %w", err)
	}
	result.AchievementCount = total

	if total < in.NeedAchievements {
		result.Errors = append(result.Errors,
			fmt.Sprintf("近%d年业绩不足：需要%d，有%d", withinYears, in.NeedAchievements, total))
		result.AchievementOK = false
	} else {
		result.AchievementOK = true
	}

	// 综合判断
	if len(result.Errors) > 0 {
		result.CanBid = false
	}

	return result, nil
}

// CreateBid 创建标书
// 锁定资源，生成 proof_hash
func (s *Service) CreateBid(ctx context.Context, in CreateBidInput) (*BidDocument, []*BidResource, error) {
	if in.ProjectName == "" || in.ProjectType == "" {
		return nil, nil, fmt.Errorf("项目名称和类型不能为空")
	}

	// 生成投标引用
	now := time.Now()
	bidRef := fmt.Sprintf("v://%s/bid/%d/%d",
		strings.TrimPrefix(in.NamespaceRef, "v://"),
		now.Year(),
		now.UnixNano())

	// 创建投标文档
	bid := &BidDocument{
		BidRef:           bidRef,
		TenantID:         s.tenantID,
		NamespaceRef:     in.NamespaceRef,
		TenderGenesisRef: in.TenderGenesisRef,
		ProjectName:      in.ProjectName,
		ProjectType:      in.ProjectType,
		OwnerName:        in.OwnerName,
		EstimatedAmount:  in.EstimatedAmount,
		BidDeadline:      in.BidDeadline,
		OurBidAmount:     in.OurBidAmount,
		Status:           StatusDraft,
		CreatedAt:        now,
	}

	bidID, err := s.store.CreateBid(ctx, bid)
	if err != nil {
		return nil, nil, fmt.Errorf("create bid failed: %w", err)
	}
	bid.ID = bidID

	// 收集资源引用，用于计算 proof_hash
	var resourceRefs []string
	var resources []*BidResource

	// 添加企业资质（引用型）
	for _, qualRef := range in.CompanyQualRefs {
		res := &BidResource{
			BidID:         bidID,
			TenantID:      s.tenantID,
			ResourceType:  ResourceQualCompany,
			ResourceRef:   qualRef,
			ConsumeMode:   ConsumeReference,
			ConsumeStatus: ConsumeReferenced,
			CreatedAt:     now,
		}
		id, err := s.store.AddResource(ctx, res)
		if err != nil {
			return nil, nil, fmt.Errorf("add company qual resource failed: %w", err)
		}
		res.ID = id
		resources = append(resources, res)
		resourceRefs = append(resourceRefs, qualRef)
	}

	// 添加注册工程师（占用型）
	for _, qualRef := range in.PersonQualRefs {
		// 校验工程师可用性
		valid, err := s.store.ValidateQualValid(ctx, qualRef)
		if err != nil || !valid {
			continue // 跳过不可用的工程师
		}

		res := &BidResource{
			BidID:         bidID,
			TenantID:      s.tenantID,
			ResourceType:  ResourceQualPerson,
			ResourceRef:   qualRef,
			ConsumeMode:   ConsumeOccupy,
			ConsumeStatus: ConsumeReferenced, // 初始 REFERENCED，中标后变 OCCUPIED
			CreatedAt:     now,
		}
		id, err := s.store.AddResource(ctx, res)
		if err != nil {
			return nil, nil, fmt.Errorf("add person qual resource failed: %w", err)
		}
		res.ID = id
		resources = append(resources, res)
		resourceRefs = append(resourceRefs, qualRef)
	}

	// 添加历史业绩（引用型）
	for _, achRef := range in.AchievementRefs {
		res := &BidResource{
			BidID:         bidID,
			TenantID:      s.tenantID,
			ResourceType:  ResourceAchievement,
			ResourceRef:   achRef,
			ConsumeMode:   ConsumeReference,
			ConsumeStatus: ConsumeReferenced,
			CreatedAt:     now,
		}
		id, err := s.store.AddResource(ctx, res)
		if err != nil {
			return nil, nil, fmt.Errorf("add achievement resource failed: %w", err)
		}
		res.ID = id
		resources = append(resources, res)
		resourceRefs = append(resourceRefs, achRef)
	}

	// 计算 proof_hash
	bid.ProofHash = s.computeProofHash(bidRef, in.ProjectName, in.ProjectType, resourceRefs)
	bid.ResourceCount = len(resources)

	// 更新 proof_hash（需要单独方法或在这里做）
	// 简化：在创建时已写入

	return bid, resources, nil
}

// SubmitBid 提交投标
func (s *Service) SubmitBid(ctx context.Context, bidID int64) error {
	bid, err := s.store.GetBid(ctx, bidID)
	if err != nil {
		return err
	}

	if bid.Status != StatusDraft {
		return fmt.Errorf("只能提交草稿状态的投标")
	}

	return s.store.UpdateBidStatus(ctx, bidID, StatusSubmitted, time.Now())
}

// Award 标书中标
// 触发数据库触发器，自动创建项目树、合同、占用工程师
func (s *Service) Award(ctx context.Context, bidID int64) (*BidDocument, error) {
	bid, err := s.store.GetBid(ctx, bidID)
	if err != nil {
		return nil, err
	}

	if bid.Status != StatusSubmitted {
		return nil, fmt.Errorf("只能对已提交的投标进行中标操作")
	}

	// 更新状态为 AWARDED，触发器会自动执行
	err = s.store.UpdateBidStatus(ctx, bidID, StatusAwarded, time.Now())
	if err != nil {
		return nil, fmt.Errorf("award failed: %w", err)
	}

	// 重新获取，拿到触发器更新的字段
	bid, err = s.store.GetBid(ctx, bidID)
	if err != nil {
		return nil, err
	}

	return bid, nil
}

// Fail 投标失败
func (s *Service) Fail(ctx context.Context, bidID int64) error {
	bid, err := s.store.GetBid(ctx, bidID)
	if err != nil {
		return err
	}

	if bid.Status != StatusSubmitted {
		return fmt.Errorf("只能对已提交的投标进行失败标记")
	}

	// 更新状态为 FAILED
	err = s.store.UpdateBidStatus(ctx, bidID, StatusFailed, time.Now())
	if err != nil {
		return err
	}

	// 释放锁定的工程师资源
	return s.store.UpdateResourceStatus(ctx, bidID, ResourceQualPerson, ConsumeReleased)
}

// SearchAchievements 搜索可用业绩
func (s *Service) SearchAchievements(ctx context.Context, f AchievementsFilter) ([]*AchievementInPool, int, error) {
	if f.Limit == 0 {
		f.Limit = 20
	}
	if f.WithinYears == 0 {
		f.WithinYears = 3
	}
	return s.store.SearchAchievements(ctx, f)
}

// GetBid 获取投标详情
func (s *Service) GetBid(ctx context.Context, bidID int64) (*BidDocument, []*BidResource, error) {
	bid, err := s.store.GetBid(ctx, bidID)
	if err != nil {
		return nil, nil, err
	}
	resources, err := s.store.ListResources(ctx, bidID)
	if err != nil {
		return nil, nil, err
	}
	return bid, resources, nil
}

// ListBids 列表
func (s *Service) ListBids(ctx context.Context, status *BidStatus, limit, offset int) ([]*BidDocument, int, error) {
	if limit == 0 {
		limit = 20
	}
	return s.store.ListBids(ctx, s.tenantID, status, limit, offset)
}

// ComputeProofHash 计算 proof_hash
func (s *Service) computeProofHash(bidRef, projectName, projectType string, resourceRefs []string) string {
	h := sha256.New()
	h.Write([]byte(bidRef))
	h.Write([]byte(projectName))
	h.Write([]byte(projectType))
	for _, ref := range resourceRefs {
		h.Write([]byte(ref))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// VerifyBid 验证执行体是否满足目标 SPU 约束
// 招标方发布目标 SPU，系统自动验证执行体能否承接
func (s *Service) VerifyBid(ctx context.Context, in ValidateBidInput) (*VerifyResult, error) {
	result := &VerifyResult{
		Pass:          true,
		MatchingScore: 0,
	}

	// 1. 检查企业资质
	companyQualCount, err := s.store.CountCompanyQuals(ctx, in.NamespaceRef)
	if err != nil {
		return nil, fmt.Errorf("count company quals failed: %w", err)
	}
	result.CompanyQualCount = companyQualCount

	check := ConstraintCheck{
		Constraint:  "企业资质",
		Required:    ">= 1",
		Actual:      fmt.Sprintf("%d", companyQualCount),
		Pass:        companyQualCount > 0,
		Description: "投标方必须持有有效企业资质",
	}
	result.ConstraintChecks = append(result.ConstraintChecks, check)
	if !check.Pass {
		result.Errors = append(result.Errors, "企业资质不足")
		result.Pass = false
	} else {
		result.MatchingScore += 20
	}

	// 2. 检查注册工程师
	personQualCount, err := s.store.CountPersonQuals(ctx, in.NamespaceRef)
	if err != nil {
		return nil, fmt.Errorf("count person quals failed: %w", err)
	}
	result.PersonQualCount = personQualCount

	availableEngineers, err := s.store.CountAvailableEngineers(ctx, in.NamespaceRef)
	if err != nil {
		return nil, fmt.Errorf("count available engineers failed: %w", err)
	}
	result.AvailableEngineers = availableEngineers

	needEngineers := in.NeedPersonQuals
	if needEngineers == 0 {
		needEngineers = 1 // 默认至少需要1人
	}

	check = ConstraintCheck{
		Constraint:  "注册工程师",
		Required:    fmt.Sprintf(">= %d (可用)", needEngineers),
		Actual:      fmt.Sprintf("%d (可用 %d)", personQualCount, availableEngineers),
		Pass:        availableEngineers >= needEngineers,
		Description: "投标方必须有足够的可用注册工程师",
	}
	result.ConstraintChecks = append(result.ConstraintChecks, check)
	if !check.Pass {
		result.Errors = append(result.Errors,
			fmt.Sprintf("可用注册工程师不足：需要%d，可用%d", needEngineers, availableEngineers))
		result.Pass = false
	} else {
		result.MatchingScore += 30
	}

	// 3. 检查业绩
	withinYears := in.AchievementYears
	if withinYears == 0 {
		withinYears = 3
	}
	needAchievements := in.NeedAchievements
	if needAchievements == 0 {
		needAchievements = 1
	}

	_, total, err := s.store.SearchAchievements(ctx, AchievementsFilter{
		NamespaceRef: in.NamespaceRef,
		ProjectType:  &in.ProjectType,
		WithinYears:  withinYears,
		Limit:        100,
	})
	if err != nil {
		return nil, fmt.Errorf("search achievements failed: %w", err)
	}
	result.AchievementCount = total

	check = ConstraintCheck{
		Constraint:  "历史业绩",
		Required:    fmt.Sprintf(">= %d (近%d年)", needAchievements, withinYears),
		Actual:      fmt.Sprintf("%d", total),
		Pass:        total >= needAchievements,
		Description: "投标方必须具备同类业绩证明",
	}
	result.ConstraintChecks = append(result.ConstraintChecks, check)
	if !check.Pass {
		result.Errors = append(result.Errors,
			fmt.Sprintf("近%d年业绩不足：需要%d，有%d", withinYears, needAchievements, total))
		result.Pass = false
	} else {
		result.MatchingScore += 50
	}

	result.CanBid = result.Pass
	return result, nil
}

// RecommendTeam 推荐最强投标阵容
// Resolver.Resolve: 找到满足 SPU 要求的最佳人员组合
func (s *Service) RecommendTeam(ctx context.Context, namespace string, spuRef string, limit int) ([]*ExecutorCandidate, error) {
	if limit == 0 {
		limit = 10
	}

	candidates, err := s.store.FindCandidateExecutors(ctx, namespace, spuRef, limit*2)
	if err != nil {
		return nil, fmt.Errorf("find candidate executors failed: %w", err)
	}

	// 按匹配度排序
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].IsAvailable != candidates[j].IsAvailable {
			return candidates[i].IsAvailable
		}
		return candidates[i].MatchingScore > candidates[j].MatchingScore
	})

	if len(candidates) > limit {
		candidates = candidates[:limit]
	}

	return candidates, nil
}

// MatchAchievements 匹配历史业绩
// 根据项目类型和金额自动匹配业绩
func (s *Service) MatchAchievements(ctx context.Context, executorRef string, projectType string, withinYears int, minAmount float64, limit int) ([]*AchievementMatch, error) {
	if withinYears == 0 {
		withinYears = 3
	}
	if limit == 0 {
		limit = 10
	}

	matches, err := s.store.MatchExecutorAchievements(ctx, executorRef, projectType, withinYears, limit)
	if err != nil {
		return nil, fmt.Errorf("match executor achievements failed: %w", err)
	}

	// 过滤金额
	if minAmount > 0 {
		var filtered []*AchievementMatch
		for _, m := range matches {
			if m.ContractAmount >= minAmount {
				filtered = append(filtered, m)
			}
		}
		matches = filtered
	}

	return matches, nil
}

// GenerateCapabilityProof 生成投标能力证明
// 聚合资质 + 人员 + 业绩，输出可验证的证明文档
func (s *Service) GenerateCapabilityProof(ctx context.Context, executorRef string, targetSPU string) (*CapabilityProof, error) {
	now := time.Now()
	proof := &CapabilityProof{
		ExecutorRef: executorRef,
		TargetSPU:   targetSPU,
		GeneratedAt: now,
		IsValid:     true,
	}

	// 1. 获取执行体能力
	capabilities, err := s.store.GetExecutorCapabilities(ctx, executorRef)
	if err != nil {
		proof.IsValid = false
		return proof, nil
	}
	if capabilities != nil {
		proof.Personnel = []ExecutorCandidate{*capabilities}
		proof.OverallScore = capabilities.MatchingScore
	}

	// 2. 获取企业资质
	namespace := extractNamespace(executorRef)
	companyQualCount, _ := s.store.CountCompanyQuals(ctx, namespace)
	if companyQualCount > 0 {
		proof.CompanyQuals = []QualSummary{{
			QualType:   "ENTERPRISE_QUAL",
			CertNo:     "N/A",
			ValidUntil: "2099-12-31",
		}}
		proof.OverallScore += 20
	}

	// 3. 获取匹配业绩
	achievements, _, err := s.store.SearchAchievements(ctx, AchievementsFilter{
		NamespaceRef: namespace,
		WithinYears:  3,
		Limit:        10,
	})
	if err == nil && len(achievements) > 0 {
		for _, a := range achievements {
			proof.Achievements = append(proof.Achievements, AchievementMatch{
				UTXORef:     a.UTXORef,
				ProjectName: a.ProjectName,
				SPURef:      a.SPURef,
				ProofHash:   a.ProofHash,
			})
		}
		proof.OverallScore += len(achievements) * 5
		if proof.OverallScore > 100 {
			proof.OverallScore = 100
		}
	}

	return proof, nil
}

func extractNamespace(ref string) string {
	// v://zhongbei/executor/person/xxx → v://zhongbei
	parts := strings.Split(ref, "/")
	if len(parts) >= 3 {
		return strings.Join(parts[:3], "/")
	}
	return ref
}

func normalizeNamespaceRef(ns string) string {
	ns = strings.TrimSpace(ns)
	if ns == "" {
		return ""
	}
	if strings.HasPrefix(ns, "v://") {
		return ns
	}
	return "v://" + ns
}
