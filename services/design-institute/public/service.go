// ============================================================
//  public/service.go
//  对外寻址接口——ERP底座的外部暴露层
//
//  三个寻址入口（只读，不暴露内部敏感数据）：
//
//  ① 产品寻址（Product Address）
//     客户输入需求关键词，返回匹配的SPU规格
//     + 历史成交摘要 + 可验证业绩
//
//  ② 执行体寻址（Executor Address）
//     给定SPU和约束，返回能承接的执行体摘要
//     能力等级、通过率、当前可用性（不暴露姓名和证书号）
//
//  ③ 能力声明（Capability Declaration）
//     给定命名空间，返回该组织的能力快照
//     资质、人员、业绩——每条附proof_hash，可独立验证
//
//  访问控制：
//    - 无需认证（公开只读）
//    - 不返回个人隐私信息（证书号、联系方式、薪资）
//    - 业绩附proof_hash，访问者可自行验证真实性
//    - Rate limiting（实现层控制，这里不做）
// ============================================================

package public

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ── 对外数据结构（脱敏版本）────────────────────────────────────

// ProductSpec 产品规格（对外版）
// 来自 specs/spu/ 目录，展示客户能看的内容
type ProductSpec struct {
	SPURef      string   `json:"spu_ref"`
	Name        string   `json:"name"`
	Domain      string   `json:"domain"`
	Kind        string   `json:"kind"`  // DRAWING/RECORD/REPORT/CERT
	Stage       string   `json:"stage"` // 设计/施工/竣工/结算
	Description string   `json:"description"`
	Tags        []string `json:"tags"`

	// 交付物说明
	Deliverables []string `json:"deliverables"`

	// 历史统计（来自 achievement_utxos）
	HistoryStats ProductStats `json:"history_stats"`
}

type ProductStats struct {
	TotalDelivered int     `json:"total_delivered"` // 历史交付次数
	AvgPassRate    float64 `json:"avg_pass_rate"`   // 平均通过率
	RecentCount    int     `json:"recent_count"`    // 近1年交付次数
}

// ExecutorSummary 执行体摘要（对外版，脱敏）
// 不暴露姓名、证书号、联系方式
type ExecutorSummary struct {
	// 用角色描述代替姓名
	Role            string   `json:"role"`             // "注册结构工程师·高级"
	CapabilityGrade string   `json:"capability_grade"` // SENIOR/EXPERT/MASTER
	CapabilityLevel float64  `json:"capability_level"`
	SpecialtySPUs   []string `json:"specialty_spus"` // 擅长的SPU类型
	YearlyPassRate  float64  `json:"yearly_pass_rate"`
	Available       bool     `json:"available"`       // 当前是否可接单
	RecentProjects  int      `json:"recent_projects"` // 近1年项目数
}

// AchievementSummary 业绩摘要（对外版）
// 附proof_hash，访客可独立验证
type AchievementSummary struct {
	ProjectName      string `json:"project_name"`
	SPUName          string `json:"spu_name"`
	ContractAmtRange string `json:"contract_amt_range"` // "100万-500万"（不暴露精确金额）
	CompletedYear    int    `json:"completed_year"`
	ProjectType      string `json:"project_type"`
	Province         string `json:"province"`

	// 可验证性
	UTXORef    string `json:"utxo_ref"`
	ProofHash  string `json:"proof_hash"`
	Verifiable bool   `json:"verifiable"`  // 是否有proof_hash
	Source     string `json:"source"`      // MANUAL/TRIP_DERIVED
	TrustLevel int    `json:"trust_level"` // 1-5星
}

// OrgCapabilityDecl 组织能力声明（对外版）
type OrgCapabilityDecl struct {
	OrgName    string    `json:"org_name"`
	OrgRef     string    `json:"org_ref"`
	DeclaredAt time.Time `json:"declared_at"`

	// 资质（来自 qualifications）
	Qualifications []QualDecl `json:"qualifications"`

	// 人员能力分布（脱敏，只显示分布不显示个人）
	PersonnelStats PersonnelStats `json:"personnel_stats"`

	// 可承接的产品类型
	ProductCapacity []ProductCapacity `json:"product_capacity"`

	// 近三年业绩摘要
	RecentAchievements []AchievementSummary `json:"recent_achievements"`

	// 可验证声明
	VerificationNote string `json:"verification_note"`
}

type QualDecl struct {
	QualType   string `json:"qual_type"`
	QualName   string `json:"qual_name"`
	CertNo     string `json:"cert_no"` // 企业资质证书号（公开信息）
	IssuedBy   string `json:"issued_by"`
	ValidUntil string `json:"valid_until"`
	Status     string `json:"status"`
	Verifiable string `json:"verifiable"` // 核查方式说明
}

type PersonnelStats struct {
	RegisteredEngineers int            `json:"registered_engineers"` // 注册工程师总数
	ByType              map[string]int `json:"by_type"`              // 按类型分布
	ByGrade             map[string]int `json:"by_grade"`             // 按能力等级分布
	AvgCapabilityLevel  float64        `json:"avg_capability_level"`
}

type ProductCapacity struct {
	SPURef      string  `json:"spu_ref"`
	SPUName     string  `json:"spu_name"`
	Headcount   int     `json:"headcount"`    // 能执行的人数
	AvgLevel    float64 `json:"avg_level"`    // 平均能力等级
	RecentCount int     `json:"recent_count"` // 近1年执行次数
}

// AddressResult 寻址结果——三类寻址的统一返回格式
type AddressResult struct {
	Query       string    `json:"query"`
	QueryType   string    `json:"query_type"` // PRODUCT/EXECUTOR/CAPABILITY
	MatchCount  int       `json:"match_count"`
	Results     any       `json:"results"`
	GeneratedAt time.Time `json:"generated_at"`
}

// ── Service ───────────────────────────────────────────────────

type Service struct {
	db       *sql.DB
	specsDir string // specs/spu/ 目录路径
	tenantID int
}

func NewService(db *sql.DB, specsDir string, tenantID int) *Service {
	return &Service{db: db, specsDir: specsDir, tenantID: tenantID}
}

// ── ① 产品寻址 ────────────────────────────────────────────────

func (s *Service) AddressProduct(ctx context.Context, keyword string) (*AddressResult, error) {
	var specs []*ProductSpec

	// 从 specs/spu/ 目录扫描 JSON 文件
	err := filepath.WalkDir(s.specsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		if strings.Contains(path, "catalog") {
			return nil // 跳过目录文件
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		var raw map[string]any
		if err := json.Unmarshal(b, &raw); err != nil {
			return nil
		}

		name := strVal(raw["name"])
		spuRef := strVal(raw["spu_ref"])
		desc := strVal(raw["description"])

		// 关键词匹配
		kw := strings.ToLower(keyword)
		if kw != "" && !strings.Contains(strings.ToLower(name), kw) &&
			!strings.Contains(strings.ToLower(desc), kw) &&
			!strings.Contains(strings.ToLower(spuRef), kw) {
			return nil
		}

		spec := &ProductSpec{
			SPURef:      spuRef,
			Name:        name,
			Kind:        strVal(raw["kind"]),
			Stage:       strVal(raw["stage"]),
			Description: desc,
		}

		// 交付物
		if pu, ok := raw["product_unit"].(map[string]any); ok {
			if dels, ok := pu["deliverables"].([]any); ok {
				for _, d := range dels {
					if dm, ok := d.(map[string]any); ok {
						spec.Deliverables = append(spec.Deliverables, strVal(dm["name"]))
					}
				}
			}
		}

		// 历史统计（来自 achievement_utxos）
		spec.HistoryStats = s.getProductStats(ctx, spuRef)

		specs = append(specs, spec)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("扫描SPU规格失败: %w", err)
	}

	return &AddressResult{
		Query:       keyword,
		QueryType:   "PRODUCT",
		MatchCount:  len(specs),
		Results:     specs,
		GeneratedAt: time.Now(),
	}, nil
}

func (s *Service) getProductStats(ctx context.Context, spuRef string) ProductStats {
	var stats ProductStats
	oneYearAgo := time.Now().AddDate(-1, 0, 0)

	s.db.QueryRowContext(ctx, `
		SELECT
		  COUNT(*),
		  ROUND(COUNT(*) FILTER (WHERE status='SETTLED')::numeric /
		        NULLIF(COUNT(*),0), 4),
		  COUNT(*) FILTER (WHERE ingested_at >= $2)
		FROM achievement_utxos
		WHERE spu_ref=$1 AND tenant_id=$3
	`, spuRef, oneYearAgo, s.tenantID).Scan(
		&stats.TotalDelivered,
		&stats.AvgPassRate,
		&stats.RecentCount,
	)
	return stats
}

// ── ② 执行体寻址 ──────────────────────────────────────────────

func (s *Service) AddressExecutor(ctx context.Context, spuRef string, minGrade string) (*AddressResult, error) {
	// 从 executor_stats 查满足条件的执行体
	// 对外脱敏：不暴露 executor_ref，用角色+等级描述代替
	rows, err := s.db.QueryContext(ctx, `
		SELECT es.capability_grade, es.capability_level,
		       es.specialty_spus::text, es.yearly_pass_rate,
		       es.active_projects, es.project_quota,
		       es.yearly_utxos,
		       -- 从 qualifications 取最高级证书类型
		       COALESCE((
		         SELECT qual_type FROM qualifications q
		         WHERE q.executor_ref = es.executor_ref
		           AND q.status IN ('VALID','EXPIRE_SOON')
		           AND q.deleted = FALSE
		           AND q.qual_type LIKE 'REG_%'
		         ORDER BY q.valid_until DESC LIMIT 1
		       ), 'ENGINEER') as cert_type
		FROM executor_stats es
		WHERE es.tenant_id=$1
		  AND es.active_projects < es.project_quota
		  AND ($2 = '' OR es.capability_grade >= $2)
		  AND ($3 = '' OR es.specialty_spus @> ARRAY[$3]::text[])
		ORDER BY es.capability_level DESC
		LIMIT 20
	`, s.tenantID, minGrade, spuRef)
	if err != nil {
		return nil, fmt.Errorf("查询执行体失败: %w", err)
	}
	defer rows.Close()

	var summaries []*ExecutorSummary
	for rows.Next() {
		var grade, spusStr, certType string
		var level, passRate float64
		var active, quota, yearlyCount int

		rows.Scan(&grade, &level, &spusStr, &passRate,
			&active, &quota, &yearlyCount, &certType)

		summaries = append(summaries, &ExecutorSummary{
			Role:            certLabel(certType) + "·" + gradeLabel(grade),
			CapabilityGrade: grade,
			CapabilityLevel: level,
			SpecialtySPUs:   parseArr(spusStr),
			YearlyPassRate:  passRate,
			Available:       active < quota,
			RecentProjects:  yearlyCount,
		})
	}

	return &AddressResult{
		Query:       spuRef,
		QueryType:   "EXECUTOR",
		MatchCount:  len(summaries),
		Results:     summaries,
		GeneratedAt: time.Now(),
	}, nil
}

// ── ③ 能力声明（组织对外声明）────────────────────────────────

func (s *Service) DeclareCapability(ctx context.Context, nsRef string) (*OrgCapabilityDecl, error) {
	decl := &OrgCapabilityDecl{
		OrgRef:           nsRef,
		DeclaredAt:       time.Now(),
		VerificationNote: "所有业绩UTXO附proof_hash，可通过 GET /public/v1/verify/{utxo_ref} 独立验证",
	}

	// 组织名称
	s.db.QueryRowContext(ctx, `SELECT name FROM namespaces WHERE ref=$1`, nsRef).Scan(&decl.OrgName)
	if decl.OrgName == "" {
		// fallback：从 companies 表查
		s.db.QueryRowContext(ctx, `SELECT name FROM companies WHERE company_type=1 AND tenant_id=$1 LIMIT 1`,
			s.tenantID).Scan(&decl.OrgName)
	}

	// ─ 企业资质（公开信息）──────────────────────────────────
	qualRows, _ := s.db.QueryContext(ctx, `
		SELECT qual_type, cert_no, COALESCE(issued_by,''),
		       COALESCE(TO_CHAR(valid_until,'YYYY-MM-DD'),'长期'),
		       status, COALESCE(level,'')
		FROM qualifications
		WHERE holder_type='COMPANY' AND tenant_id=$1
		  AND status IN ('VALID','EXPIRE_SOON')
		  AND deleted=FALSE
		ORDER BY qual_type
	`, s.tenantID)
	if qualRows != nil {
		defer qualRows.Close()
		for qualRows.Next() {
			var qt, certNo, issuedBy, validUntil, status, level string
			qualRows.Scan(&qt, &certNo, &issuedBy, &validUntil, &status, &level)
			decl.Qualifications = append(decl.Qualifications, QualDecl{
				QualType:   qt,
				QualName:   qualTypeName(qt),
				CertNo:     certNo,
				IssuedBy:   issuedBy,
				ValidUntil: validUntil,
				Status:     status,
				Verifiable: "住房和城乡建设部官网可核查，证书编号：" + certNo,
			})
		}
	}

	// ─ 人员能力分布（脱敏）──────────────────────────────────
	statsRows, _ := s.db.QueryContext(ctx, `
		SELECT es.capability_grade, es.capability_level,
		       COALESCE((
		         SELECT qual_type FROM qualifications q
		         WHERE q.executor_ref=es.executor_ref
		           AND q.status IN ('VALID','EXPIRE_SOON')
		           AND q.qual_type LIKE 'REG_%' AND q.deleted=FALSE
		         LIMIT 1
		       ),'ENGINEER') as cert_type
		FROM executor_stats es
		WHERE es.tenant_id=$1
	`, s.tenantID)

	byType := map[string]int{}
	byGrade := map[string]int{}
	var totalLevel float64
	var totalCount int

	if statsRows != nil {
		defer statsRows.Close()
		for statsRows.Next() {
			var grade, certType string
			var level float64
			statsRows.Scan(&grade, &level, &certType)
			byType[certLabel(certType)]++
			byGrade[grade]++
			totalLevel += level
			totalCount++
		}
	}

	// 也从 qualifications 表直接统计注册工程师数
	var regCount int
	s.db.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT executor_ref) FROM qualifications
		WHERE tenant_id=$1 AND holder_type='PERSON'
		  AND qual_type LIKE 'REG_%'
		  AND status IN ('VALID','EXPIRE_SOON') AND deleted=FALSE
	`, s.tenantID).Scan(&regCount)

	avgLevel := 0.0
	if totalCount > 0 {
		avgLevel = totalLevel / float64(totalCount)
	}

	decl.PersonnelStats = PersonnelStats{
		RegisteredEngineers: regCount,
		ByType:              byType,
		ByGrade:             byGrade,
		AvgCapabilityLevel:  avgLevel,
	}

	// ─ 产品承接能力（来自 executor_stats.specialty_spus）────
	spuCapRows, _ := s.db.QueryContext(ctx, `
		SELECT spu_ref, COUNT(*) as headcount,
		       AVG(capability_level) as avg_level
		FROM (
		  SELECT es.capability_level,
		         unnest(es.specialty_spus) as spu_ref
		  FROM executor_stats es WHERE es.tenant_id=$1
		) t
		GROUP BY spu_ref
		ORDER BY headcount DESC
		LIMIT 15
	`, s.tenantID)
	if spuCapRows != nil {
		defer spuCapRows.Close()
		for spuCapRows.Next() {
			var spuRef string
			var headcount int
			var avgLvl float64
			spuCapRows.Scan(&spuRef, &headcount, &avgLvl)

			// 近1年执行次数
			var recentCount int
			s.db.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM achievement_utxos
				WHERE spu_ref=$1 AND tenant_id=$2
				  AND ingested_at >= NOW() - INTERVAL '1 year'
				  AND status='SETTLED'
			`, spuRef, s.tenantID).Scan(&recentCount)

			decl.ProductCapacity = append(decl.ProductCapacity, ProductCapacity{
				SPURef:      spuRef,
				SPUName:     spuShortName(spuRef),
				Headcount:   headcount,
				AvgLevel:    avgLvl,
				RecentCount: recentCount,
			})
		}
	}

	// ─ 近三年业绩摘要（脱敏，附proof_hash）─────────────────
	threeYearsAgo := time.Now().AddDate(-3, 0, 0)
	achRows, _ := s.db.QueryContext(ctx, `
		SELECT a.utxo_ref, a.spu_ref, a.proof_hash,
		       COALESCE(c.contract_name, pn.name,
		           a.payload->>'project_name', '工程项目') as proj_name,
		       COALESCE(c.contract_balance,
		           (a.payload->>'contract_amount')::numeric * 10000, 0) as amount,
		       COALESCE(
		           (a.payload->>'completed_year')::int,
		           EXTRACT(YEAR FROM a.ingested_at)::int) as yr,
		       COALESCE(c.signing_subject, a.payload->>'owner_name', '') as owner,
		       COALESCE(a.payload->>'source', a.source, 'MANUAL') as src
		FROM achievement_utxos a
		LEFT JOIN contracts c ON c.id = a.contract_id
		LEFT JOIN project_nodes pn ON pn.ref = a.project_ref
		WHERE a.tenant_id=$1
		  AND a.status='SETTLED'
		  AND a.ingested_at >= $2
		ORDER BY a.ingested_at DESC
		LIMIT 20
	`, s.tenantID, threeYearsAgo)

	if achRows != nil {
		defer achRows.Close()
		for achRows.Next() {
			var utxoRef, spuRef, proofHash, projName, owner, src string
			var amount float64
			var yr int
			achRows.Scan(&utxoRef, &spuRef, &proofHash, &projName, &amount, &yr, &owner, &src)
			trust := 2
			if src == "TRIP_DERIVED" {
				trust = 5
			}

			decl.RecentAchievements = append(decl.RecentAchievements, AchievementSummary{
				ProjectName:      projName,
				SPUName:          spuShortName(spuRef),
				ContractAmtRange: amtRange(amount),
				CompletedYear:    yr,
				ProjectType:      inferType(projName),
				Province:         inferProvince(projName),
				UTXORef:          utxoRef,
				ProofHash:        proofHash,
				Verifiable:       proofHash != "",
				Source:           src,
				TrustLevel:       trust,
			})
		}
	}

	return decl, nil
}

// ── UTXO 独立验证 ─────────────────────────────────────────────

type VerifyResult struct {
	UTXORef    string    `json:"utxo_ref"`
	Exists     bool      `json:"exists"`
	ProofHash  string    `json:"proof_hash"`
	Status     string    `json:"status"`
	SPURef     string    `json:"spu_ref"`
	IngestedAt time.Time `json:"ingested_at"`
	// 验证说明
	VerifyNote string `json:"verify_note"`
}

func (s *Service) VerifyUTXO(ctx context.Context, utxoRef string) (*VerifyResult, error) {
	r := &VerifyResult{UTXORef: utxoRef}
	utxoRef = strings.TrimSpace(utxoRef)
	canonicalRef := utxoRef
	if canonicalRef != "" {
		var mapped string
		err := s.db.QueryRowContext(ctx, `
			SELECT canonical_ref
			FROM ref_aliases
			WHERE tenant_id=$1
			  AND alias_ref=$2
			  AND status='ACTIVE'
			ORDER BY id DESC
			LIMIT 1
		`, s.tenantID, canonicalRef).Scan(&mapped)
		if err == nil && strings.TrimSpace(mapped) != "" {
			canonicalRef = strings.TrimSpace(mapped)
		}
	}

	err := s.db.QueryRowContext(ctx, `
		SELECT utxo_ref, proof_hash, status, spu_ref, ingested_at
		FROM achievement_utxos WHERE utxo_ref=$1 OR utxo_ref=$2
		LIMIT 1
	`, utxoRef, canonicalRef).Scan(
		&r.UTXORef, &r.ProofHash, &r.Status, &r.SPURef, &r.IngestedAt,
	)
	if err == sql.ErrNoRows {
		r.Exists = false
		r.VerifyNote = "该UTXO不存在或已被撤销"
		return r, nil
	}
	if err != nil {
		return nil, err
	}

	r.Exists = true
	r.VerifyNote = fmt.Sprintf(
		"UTXO存在，proof_hash=%s，状态=%s。可将原始业绩数据计算SHA-256后与proof_hash对比验证。",
		r.ProofHash[:16]+"...", r.Status,
	)
	return r, nil
}

// ── 工具函数 ──────────────────────────────────────────────────

func certLabel(qualType string) string {
	labels := map[string]string{
		"REG_STRUCTURE":   "注册结构工程师",
		"REG_ARCH":        "注册建筑师",
		"REG_CIVIL":       "注册岩土工程师",
		"REG_COST":        "注册造价工程师",
		"REG_SURVEY":      "注册测量师",
		"REG_ELECTRIC":    "注册电气工程师",
		"REG_MECH":        "注册机械工程师",
		"REG_SAFETY":      "注册安全工程师",
		"SENIOR_ENGINEER": "高级工程师",
		"ENGINEER":        "工程师",
	}
	if l, ok := labels[qualType]; ok {
		return l
	}
	return "工程师"
}

func gradeLabel(grade string) string {
	labels := map[string]string{
		"JUNIOR":   "初级",
		"STANDARD": "标准",
		"SENIOR":   "高级",
		"EXPERT":   "专家",
		"MASTER":   "首席",
	}
	if l, ok := labels[grade]; ok {
		return l
	}
	return grade
}

func qualTypeName(qt string) string {
	names := map[string]string{
		"COMPREHENSIVE_A": "工程设计综合甲级",
		"INDUSTRY_A":      "工程设计行业甲级",
		"INDUSTRY_B":      "工程设计行业乙级",
		"SPECIAL_A":       "工程设计专项甲级",
	}
	if n, ok := names[qt]; ok {
		return n
	}
	return qt
}

func spuShortName(spuRef string) string {
	parts := strings.Split(spuRef, "/")
	if len(parts) > 0 {
		last := parts[len(parts)-1]
		if idx := strings.Index(last, "@"); idx > 0 {
			last = last[:idx]
		}
		// 下划线转中文友好名
		names := map[string]string{
			"pile_foundation_drawing":  "桩基施工图",
			"pile_cap_drawing":         "承台施工图",
			"pier_rebar_drawing":       "墩柱配筋图",
			"superstructure_drawing":   "上部结构施工图",
			"concealed_acceptance":     "隐蔽工程验收",
			"prestress_record":         "预应力张拉记录",
			"concrete_strength_report": "混凝土强度检测",
			"coordinate_proof":         "实测坐标证明",
			"review_certificate":       "审图合格证",
			"settlement_cert":          "结算凭证",
		}
		if n, ok := names[last]; ok {
			return n
		}
		return last
	}
	return spuRef
}

func amtRange(amt float64) string {
	switch {
	case amt <= 0:
		return "未披露"
	case amt < 500000:
		return "50万以下"
	case amt < 1000000:
		return "50万-100万"
	case amt < 5000000:
		return "100万-500万"
	case amt < 10000000:
		return "500万-1000万"
	default:
		return "1000万以上"
	}
}

func inferType(name string) string {
	switch {
	case contains(name, "桥") || contains(name, "立交"):
		return "桥梁工程"
	case contains(name, "隧"):
		return "隧道工程"
	case contains(name, "高速") || contains(name, "公路") || contains(name, "路"):
		return "公路工程"
	case contains(name, "市政"):
		return "市政工程"
	default:
		return "工程设计"
	}
}

func inferProvince(name string) string {
	provinces := []string{"陕西", "甘肃", "新疆", "四川", "重庆", "湖北", "河南", "山西", "内蒙古"}
	for _, p := range provinces {
		if contains(name, p) {
			return p
		}
	}
	return ""
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}

func strVal(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func parseArr(s string) []string {
	s = strings.TrimPrefix(s, "{")
	s = strings.TrimSuffix(s, "}")
	if s == "" {
		return []string{}
	}
	var result []string
	for _, p := range strings.Split(s, ",") {
		p = strings.Trim(p, `"`)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
