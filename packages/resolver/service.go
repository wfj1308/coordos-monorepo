package resolver

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ── SPU 资质要求映射表 ────────────────────────────────────────
// 把 SPU ref 和 Action 映射到具体的资质要求
// 这是 SPU 规格 JSON 里 executor_requirement 的代码化
// 后续可以从 JSON 动态加载，现在先硬编码保证可运行

type spuRequirement struct {
	NeedCertTypes  []CertType
	HeadOfficeOnly bool
	MinExperience  int // 年
	Description    string
}

var spuRequirements = map[string]spuRequirement{
	// 审图合格证：RULE-002 核心，必须总院注册结构师
	"v://zhongbei/spu/bridge/review_certificate@v1": {
		NeedCertTypes:  []CertType{CertRegStruct},
		HeadOfficeOnly: true,
		MinExperience:  10,
		Description:    "审图合格证须由总院注册结构工程师签发（RULE-002）",
	},
	// 桩基施工图：注册结构师即可，不限总院
	"v://zhongbei/spu/bridge/pile_foundation_drawing@v1": {
		NeedCertTypes: []CertType{CertRegStruct},
		Description:   "桩基施工图设计须持注册结构工程师证书",
	},
	// 承台施工图
	"v://zhongbei/spu/bridge/pile_cap_drawing@v1": {
		NeedCertTypes: []CertType{CertRegStruct},
		Description:   "承台施工图须注册结构工程师",
	},
	// 墩柱钢筋图
	"v://zhongbei/spu/bridge/pier_rebar_drawing@v1": {
		NeedCertTypes: []CertType{CertRegStruct},
		Description:   "墩柱钢筋图须注册结构工程师",
	},
	// 上部结构图
	"v://zhongbei/spu/bridge/superstructure_drawing@v1": {
		NeedCertTypes: []CertType{CertRegStruct},
		Description:   "上部结构图须注册结构工程师",
	},
	// 结算凭证：需要总院开票权
	"v://zhongbei/spu/bridge/settlement_cert@v1": {
		NeedCertTypes:  []CertType{RightInvoice, RightHeadOffice},
		HeadOfficeOnly: true,
		Description:    "结算凭证须总院开票权（RULE-002，RULE-005）",
	},
}

// Action → 资质要求映射（不关联具体 SPU 时使用）
var actionRequirements = map[Action]spuRequirement{
	ActionIssueReviewCert: {
		NeedCertTypes:  []CertType{CertRegStruct},
		HeadOfficeOnly: true,
		Description:    "审图签发须总院注册结构工程师",
	},
	ActionIssueInvoice: {
		NeedCertTypes:  []CertType{RightInvoice},
		HeadOfficeOnly: true,
		Description:    "开票须总院开票权授权",
	},
	ActionIssueDelivery: {
		NeedCertTypes:  []CertType{RightHeadOffice},
		HeadOfficeOnly: true,
		Description:    "交付签发须总院身份",
	},
}

// ── Service ──────────────────────────────────────────────────

type Service struct {
	store         Store
	tenantID      int
	headOfficeRef string // 总院的 executor_ref 前缀，用于 HeadOfficeOnly 校验
}

func NewService(store Store, tenantID int, headOfficeRef string) *Service {
	return &Service{
		store:         store,
		tenantID:      tenantID,
		headOfficeRef: headOfficeRef,
	}
}

// ── Verify：校验执行体是否合规 ────────────────────────────────

func (s *Service) Verify(ctx context.Context, in VerifyInput) (*VerifyResult, error) {
	if in.ValidOn.IsZero() {
		in.ValidOn = time.Now()
	}

	result := &VerifyResult{Pass: true}

	// 获取该执行体的有效证书
	creds, err := s.store.GetCredentials(ctx, in.ExecutorRef, in.ValidOn)
	if err != nil {
		return nil, fmt.Errorf("查询证书失败: %w", err)
	}
	credMap := indexCreds(creds)

	// 确定本次需要校验的资质要求
	req := s.resolveRequirement(in.SPURef, in.Action)

	// ① 校验 HeadOfficeOnly
	if req.HeadOfficeOnly {
		if !s.isHeadOffice(in.ExecutorRef) {
			result.AddFail(
				"总院执行体身份（RULE-002）",
				fmt.Sprintf("执行体 %s 不属于总院（%s），无法签发", in.ExecutorRef, s.headOfficeRef),
			)
		} else {
			// 查总院身份凭证
			if cred, ok := credMap[RightHeadOffice]; ok {
				result.AddPass("总院执行体身份（RULE-002）", cred)
			} else {
				// 虽然 ref 前缀匹配，但没有显式的 RIGHT_HEAD_OFFICE 证书
				// 也算通过（前缀匹配即可）
				result.AddPass("总院执行体身份（RULE-002）", nil)
			}
		}
	}

	// ② 校验证书类型要求
	for _, certType := range req.NeedCertTypes {
		cred, ok := credMap[certType]
		if !ok {
			result.AddFail(
				fmt.Sprintf("需要证书：%s", certTypeLabel(certType)),
				fmt.Sprintf("执行体 %s 未持有有效的 %s 证书", in.ExecutorRef, certTypeLabel(certType)),
			)
		} else {
			result.AddPass(fmt.Sprintf("需要证书：%s", certTypeLabel(certType)), cred)
		}
	}

	// ③ 生成摘要
	if result.Pass {
		result.Summary = fmt.Sprintf("✓ %s 具备执行 %s 的资质", in.ExecutorRef, in.Action)
	} else {
		var fails []string
		for _, r := range result.Reasons {
			if !r.Pass {
				fails = append(fails, r.FailReason)
			}
		}
		result.Summary = "✗ 资质校验不通过：" + strings.Join(fails, "；")
	}

	return result, nil
}

// ── Resolve：寻找候选执行体 ───────────────────────────────────

func (s *Service) Resolve(ctx context.Context, in ResolveInput) ([]*Candidate, error) {
	if in.ValidOn.IsZero() {
		in.ValidOn = time.Now()
	}
	if in.Limit == 0 {
		in.Limit = 10
	}

	// 确定资质要求
	req := s.resolveRequirement(in.SPURef, in.Action)

	// 合并显式传入的证书要求
	needTypes := append(req.NeedCertTypes, in.NeedCertTypes...)
	if in.HeadOfficeOnly {
		req.HeadOfficeOnly = true
	}

	// 如果有明确的证书要求，先按证书类型缩小范围
	var candidateRefs []string
	if len(needTypes) > 0 {
		// 取第一个最稀缺的证书类型的持证人
		primaryType := needTypes[0]
		creds, err := s.store.GetCredentialsByType(ctx, in.TenantID, primaryType, in.ValidOn)
		if err != nil {
			return nil, err
		}
		seen := map[string]bool{}
		for _, c := range creds {
			if !seen[c.HolderRef] {
				seen[c.HolderRef] = true
				candidateRefs = append(candidateRefs, c.HolderRef)
			}
		}
	} else {
		// 没有证书要求，从全体执行体里找
		refs, err := s.store.ListExecutorsByTenant(ctx, in.TenantID)
		if err != nil {
			return nil, err
		}
		candidateRefs = refs
	}

	// 对每个候选执行体做完整校验
	var candidates []*Candidate
	for _, ref := range candidateRefs {
		if req.HeadOfficeOnly && !s.isHeadOffice(ref) {
			continue
		}

		// 查证书
		creds, err := s.store.GetCredentials(ctx, ref, in.ValidOn)
		if err != nil {
			continue
		}
		credMap := indexCreds(creds)

		// 检查所有要求的证书是否都满足
		allMatch := true
		var matched []Credential
		for _, certType := range needTypes {
			cred, ok := credMap[certType]
			if !ok {
				allMatch = false
				break
			}
			matched = append(matched, *cred)
		}
		if !allMatch {
			continue
		}

		// 查在建项目（承接余量）
		activeProjects, err := s.store.GetActiveProjects(ctx, ref)
		if err != nil {
			activeProjects = nil
		}
		limit := projectLimit(creds)
		capacityOK := len(activeProjects) < limit

		// 计算匹配分
		score := s.scoreCandidate(matched, len(activeProjects), limit)

		name, _ := s.store.GetExecutorName(ctx, ref)

		candidates = append(candidates, &Candidate{
			ExecutorRef:    ref,
			Name:           name,
			MatchedCreds:   matched,
			ActiveProjects: len(activeProjects),
			CapacityOK:     capacityOK,
			Score:          score,
		})

		if len(candidates) >= in.Limit {
			break
		}
	}

	// 按分数排序
	sortCandidates(candidates)
	return candidates, nil
}

// ── Occupied：查询执行体资源占用 ─────────────────────────────

func (s *Service) Occupied(ctx context.Context, executorRef string) (*OccupiedState, error) {
	projects, err := s.store.GetActiveProjects(ctx, executorRef)
	if err != nil {
		return nil, fmt.Errorf("查询占用状态失败: %w", err)
	}

	// 读证书确定上限
	creds, err := s.store.GetCredentials(ctx, executorRef, time.Now())
	if err != nil {
		creds = nil
	}
	limit := projectLimit(creds)

	return &OccupiedState{
		ExecutorRef:    executorRef,
		ActiveProjects: len(projects),
		ProjectLimit:   limit,
		Available:      len(projects) < limit,
		Projects:       projects,
	}, nil
}

// ── 内部工具函数 ──────────────────────────────────────────────

// resolveRequirement 从 SPURef 或 Action 确定资质要求
func (s *Service) resolveRequirement(spuRef string, action Action) spuRequirement {
	if spuRef != "" {
		if req, ok := spuRequirements[spuRef]; ok {
			return req
		}
	}
	if action != "" {
		if req, ok := actionRequirements[action]; ok {
			return req
		}
	}
	return spuRequirement{} // 无要求
}

// isHeadOffice 判断执行体是否属于总院
func (s *Service) isHeadOffice(executorRef string) bool {
	return strings.HasPrefix(executorRef, s.headOfficeRef)
}

// indexCreds 把证书列表按类型建索引（同类取第一个，即有效期最远的）
func indexCreds(creds []*Credential) map[CertType]*Credential {
	m := map[CertType]*Credential{}
	for _, c := range creds {
		if _, exists := m[c.CertType]; !exists {
			m[c.CertType] = c
		}
	}
	return m
}

// projectLimit 根据证书推算同期项目数上限
func projectLimit(creds []*Credential) int {
	// 按照建设部规定：注册工程师同期执业项目通常≤5
	// 有总工程师职务的可适当放宽
	for _, c := range creds {
		if c.CertType == CertChiefEng {
			return 8
		}
	}
	return 5
}

// scoreCandidate 计算候选执行体的推荐分（0-1）
func (s *Service) scoreCandidate(matched []Credential, active, limit int) float64 {
	if limit == 0 {
		return 0
	}
	// 剩余容量越多，分越高
	capacityScore := float64(limit-active) / float64(limit)
	if capacityScore < 0 {
		capacityScore = 0
	}
	// 证书数量加分
	certScore := float64(len(matched)) * 0.1
	if certScore > 0.3 {
		certScore = 0.3
	}
	return capacityScore*0.7 + certScore
}

// sortCandidates 简单冒泡排序（候选数通常很小）
func sortCandidates(cs []*Candidate) {
	for i := 0; i < len(cs); i++ {
		for j := i + 1; j < len(cs); j++ {
			if cs[j].Score > cs[i].Score {
				cs[i], cs[j] = cs[j], cs[i]
			}
		}
	}
}

// certTypeLabel 证书类型的中文标签
func certTypeLabel(ct CertType) string {
	labels := map[CertType]string{
		CertRegStruct:    "注册结构工程师",
		CertRegArch:      "注册建筑师",
		CertRegElec:      "注册电气工程师",
		CertRegGeo:       "注册岩土工程师",
		CertRegCivil:     "注册土木工程师",
		CertSeniorEng:    "高级工程师",
		CertChiefEng:     "总工程师",
		CertComprehA:     "工程设计综合甲级",
		CertIndustryA:    "行业甲级",
		CertIndustryB:    "行业乙级",
		CertSpecialA:     "专项甲级",
		RightReviewStamp: "审图盖章权",
		RightInvoice:     "开票权",
		RightHeadOffice:  "总院身份",
	}
	if l, ok := labels[ct]; ok {
		return l
	}
	return string(ct)
}
