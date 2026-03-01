// ============================================================
// achievementprofile/service.go
// 业绩档案管理
//
// 和 achievement 服务的区别：
//   - achievement/service.go  → 协议层，UTXO记录，机器写入，存证用
//   - achievementprofile       → 业务层，业绩档案，人工维护，申报/投标用
//
// 两者关联：
//   profile.utxo_ref → achievement_utxos.utxo_ref（可追溯到存证源头）
//   profile 可以从 utxo 自动生成，也可以手动录入历史项目
//
// 业绩统计维度：
//   - 公司业绩：招投标时提交"类似项目业绩"
//   - 个人业绩：注册证书申报/继续教育学时
// ============================================================

package achievementprofile

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ── 业绩类型 ─────────────────────────────────────────────

type ProjectType string

const (
	ProjHighway    ProjectType = "HIGHWAY"    // 公路
	ProjBridge     ProjectType = "BRIDGE"     // 桥梁
	ProjTunnel     ProjectType = "TUNNEL"     // 隧道
	ProjBuilding   ProjectType = "BUILDING"   // 房建
	ProjMunicipal  ProjectType = "MUNICIPAL"  // 市政
	ProjRailway    ProjectType = "RAILWAY"    // 轨道交通
	ProjWater      ProjectType = "WATER"      // 水利
	ProjEnviro     ProjectType = "ENVIRO"     // 环保
	ProjIndustrial ProjectType = "INDUSTRIAL" // 工业
	ProjOther      ProjectType = "OTHER"
)

type ProfileStatus string

const (
	ProfileDraft     ProfileStatus = "DRAFT"     // 草稿，待完善
	ProfileComplete  ProfileStatus = "COMPLETE"  // 资料完整，可用于申报
	ProfileSubmitted ProfileStatus = "SUBMITTED" // 已提交申报
)

// ── 核心数据结构 ─────────────────────────────────────────

// AchievementProfile 业绩档案
// 一个项目一条记录，包含申报所需的全部字段
type AchievementProfile struct {
	ID int64

	// ── 项目基本信息 ──────────────────────────────────────
	ProjectName  string      // 项目名称
	ProjectType  ProjectType // 项目类型
	BuildingUnit string      // 建设单位
	Location     string      // 项目地点（省市）
	StartDate    *time.Time  // 项目开始时间
	EndDate      *time.Time  // 项目完成时间（出审图合格证时间）

	// ── 我方承担内容 ──────────────────────────────────────
	OurScope       string  // 我方承担的设计内容
	ContractAmount float64 // 合同金额（元）
	OurAmount      float64 // 我方实际承担金额

	// ── 规模指标（按项目类型填对应字段）─────────────────
	ScaleMetrics json.RawMessage // 灵活的规模指标
	// 示例（桥梁）：{"span": "120m", "width": "26m", "bridge_type": "预应力混凝土连续梁"}
	// 示例（道路）：{"length": "12.5km", "grade": "一级公路", "design_speed": "100km/h"}
	// 示例（房建）：{"area": "85000m2", "height": "168m", "floors": 42}

	// ── 参与人员 ─────────────────────────────────────────
	Personnel []*ProfilePersonnel // 项目人员配置

	// ── 佐证材料 ─────────────────────────────────────────
	Attachments []*ProfileAttachment

	// ── 关联系统数据 ──────────────────────────────────────
	ContractID *int64  // → contracts.id
	ProjectRef *string // → project_nodes.ref
	UTXORef    *string // → achievement_utxos.utxo_ref（存证源头）

	// ── 状态 ─────────────────────────────────────────────
	Status    ProfileStatus
	CompanyID int    // 归属公司（公司业绩）
	Source    string // UTXO_AUTO（自动生成）/ MANUAL（手动录入）
	Note      string
	TenantID  int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ProfilePersonnel 项目人员配置
type ProfilePersonnel struct {
	ID           int64
	ProfileID    int64
	EmployeeID   *int64  // → employees.id
	EmployeeName string  // 冗余
	ExecutorRef  *string // → v://zhongbei/executor/person/{id}
	Role         string  // 项目负责人 / 专业负责人 / 审核人 / 审定人 / 设计人
	Specialty    string  // 专业（桥梁/道路/结构...）
	QualType     string  // 持有证书类型（展示用）
	CertNo       string  // 证书编号（展示用）
}

// ProfileAttachment 业绩佐证材料
type ProfileAttachment struct {
	ID        int64
	ProfileID int64
	Kind      string  // CONTRACT（合同）/ REVIEW_CERT（审图合格证）/ COMPLETION（竣工验收）/ OTHER
	Name      string  // 文件名称
	URL       string  // 文件地址
	UTXORef   *string // 如果来自 UTXO 存证，关联 utxo_ref（proof_hash 可验证）
	Note      string
}

// ── 查询与统计 ────────────────────────────────────────────

type ProfileFilter struct {
	CompanyID   *int
	EmployeeID  *int64
	ProjectType *ProjectType
	Status      *ProfileStatus
	YearFrom    *int
	YearTo      *int
	Keyword     string // 项目名称/建设单位模糊搜索
	TenantID    int
	Limit       int
	Offset      int
}

// BiddingPackage 投标业绩包（按要求筛选后的业绩集合）
type BiddingPackage struct {
	QueryDesc   string // 筛选条件描述
	Profiles    []*AchievementProfile
	TotalAmount float64 // 合计合同金额
	GeneratedAt time.Time
}

// PersonalProfile 个人业绩汇总（注册证书申报用）
type PersonalProfile struct {
	EmployeeID   int64
	EmployeeName string
	ExecutorRef  string
	// 按项目类型分组
	ByType map[ProjectType][]*AchievementProfile
	// 汇总
	TotalProjects int
	TotalAmount   float64
	// 最近5年（注册证书继续教育要求）
	Last5Years []*AchievementProfile
}

// ── 输入类型 ─────────────────────────────────────────────

type CreateInput struct {
	ProjectName    string
	ProjectType    ProjectType
	BuildingUnit   string
	Location       string
	StartDate      *time.Time
	EndDate        *time.Time
	OurScope       string
	ContractAmount float64
	OurAmount      float64
	ScaleMetrics   json.RawMessage
	Personnel      []*ProfilePersonnel
	Attachments    []*ProfileAttachment
	ContractID     *int64
	ProjectRef     *string
	UTXORef        *string
	CompanyID      int
	Note           string
}

// ── Store 接口 ────────────────────────────────────────────

type Store interface {
	Create(ctx context.Context, p *AchievementProfile) (int64, error)
	Get(ctx context.Context, id int64) (*AchievementProfile, error)
	Update(ctx context.Context, p *AchievementProfile) error
	List(ctx context.Context, f ProfileFilter) ([]*AchievementProfile, int, error)
	ListByEmployee(ctx context.Context, employeeID int64, tenantID int) ([]*AchievementProfile, error)
	GetByUTXORef(ctx context.Context, utxoRef string) (*AchievementProfile, error)
	AddPersonnel(ctx context.Context, pp *ProfilePersonnel) (int64, error)
	AddAttachment(ctx context.Context, att *ProfileAttachment) (int64, error)
	// 统计
	SumAmountByCompany(ctx context.Context, companyID int, tenantID int) (float64, error)
	CountByType(ctx context.Context, companyID int, tenantID int) (map[ProjectType]int, error)
}

// ── Service ──────────────────────────────────────────────

type Service struct {
	store    Store
	tenantID int
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

func (s *Service) Create(ctx context.Context, in CreateInput) (*AchievementProfile, error) {
	if in.ProjectName == "" {
		return nil, fmt.Errorf("项目名称不能为空")
	}
	if in.CompanyID == 0 {
		return nil, fmt.Errorf("必须指定归属公司")
	}

	now := time.Now()
	source := "MANUAL"
	if in.UTXORef != nil {
		source = "UTXO_AUTO"
	}

	p := &AchievementProfile{
		ProjectName:    in.ProjectName,
		ProjectType:    in.ProjectType,
		BuildingUnit:   in.BuildingUnit,
		Location:       in.Location,
		StartDate:      in.StartDate,
		EndDate:        in.EndDate,
		OurScope:       in.OurScope,
		ContractAmount: in.ContractAmount,
		OurAmount:      in.OurAmount,
		ScaleMetrics:   in.ScaleMetrics,
		Personnel:      in.Personnel,
		Attachments:    in.Attachments,
		ContractID:     in.ContractID,
		ProjectRef:     in.ProjectRef,
		UTXORef:        in.UTXORef,
		Status:         s.calcStatus(in),
		CompanyID:      in.CompanyID,
		Source:         source,
		Note:           in.Note,
		TenantID:       s.tenantID,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	id, err := s.store.Create(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("创建业绩档案失败: %w", err)
	}
	p.ID = id
	return p, nil
}

func (s *Service) Get(ctx context.Context, id int64) (*AchievementProfile, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) List(ctx context.Context, f ProfileFilter) ([]*AchievementProfile, int, error) {
	f.TenantID = s.tenantID
	if f.Limit == 0 {
		f.Limit = 20
	}
	return s.store.List(ctx, f)
}

func (s *Service) AddPersonnel(ctx context.Context, pp *ProfilePersonnel) (*ProfilePersonnel, error) {
	if pp.ProfileID == 0 || pp.EmployeeName == "" || pp.Role == "" {
		return nil, fmt.Errorf("ProfileID、EmployeeName、Role 不能为空")
	}
	id, err := s.store.AddPersonnel(ctx, pp)
	if err != nil {
		return nil, err
	}
	pp.ID = id
	return pp, nil
}

func (s *Service) AddAttachment(ctx context.Context, att *ProfileAttachment) (*ProfileAttachment, error) {
	if att.ProfileID == 0 || att.URL == "" {
		return nil, fmt.Errorf("ProfileID 和 URL 不能为空")
	}
	id, err := s.store.AddAttachment(ctx, att)
	if err != nil {
		return nil, err
	}
	att.ID = id
	return att, nil
}

// GetPersonalProfile 个人业绩汇总（注册证书申报用）
func (s *Service) GetPersonalProfile(ctx context.Context, employeeID int64) (*PersonalProfile, error) {
	profiles, err := s.store.ListByEmployee(ctx, employeeID, s.tenantID)
	if err != nil {
		return nil, err
	}

	pp := &PersonalProfile{
		EmployeeID: employeeID,
		ByType:     make(map[ProjectType][]*AchievementProfile),
	}

	fiveYearsAgo := time.Now().AddDate(-5, 0, 0)
	for _, p := range profiles {
		pp.TotalProjects++
		pp.TotalAmount += p.OurAmount
		pp.ByType[p.ProjectType] = append(pp.ByType[p.ProjectType], p)
		if p.EndDate != nil && p.EndDate.After(fiveYearsAgo) {
			pp.Last5Years = append(pp.Last5Years, p)
		}
	}
	return pp, nil
}

// BuildBiddingPackage 生成投标业绩包
// 按招标要求筛选符合条件的业绩（项目类型、规模、时间段）
func (s *Service) BuildBiddingPackage(ctx context.Context, f ProfileFilter, desc string) (*BiddingPackage, error) {
	f.TenantID = s.tenantID
	f.Limit = 100 // 投标通常不超过100条

	profiles, _, err := s.store.List(ctx, f)
	if err != nil {
		return nil, err
	}

	var total float64
	for _, p := range profiles {
		total += p.OurAmount
	}

	return &BiddingPackage{
		QueryDesc:   desc,
		Profiles:    profiles,
		TotalAmount: total,
		GeneratedAt: time.Now(),
	}, nil
}

// AutoGenerateFromUTXO 从 achievement_utxo 自动生成业绩档案
// 当 SPU 产出 review_certificate UTXO 时调用（审图合格证 = 项目完成）
func (s *Service) AutoGenerateFromUTXO(ctx context.Context, utxoRef, projectRef string, contractID int64, payload json.RawMessage) (*AchievementProfile, error) {
	// 检查是否已存在
	existing, _ := s.store.GetByUTXORef(ctx, utxoRef)
	if existing != nil {
		return existing, nil
	}

	// 从 payload 提取项目信息
	var meta struct {
		ProjectName  string `json:"project_name"`
		BuildingUnit string `json:"building_unit"`
		Location     string `json:"location"`
		CompanyID    int    `json:"company_id"`
	}
	if payload != nil {
		json.Unmarshal(payload, &meta)
	}

	now := time.Now()
	return s.Create(ctx, CreateInput{
		ProjectName:  meta.ProjectName,
		BuildingUnit: meta.BuildingUnit,
		Location:     meta.Location,
		EndDate:      &now, // 审图合格证时间 = 项目完成时间
		ContractID:   &contractID,
		ProjectRef:   &projectRef,
		UTXORef:      &utxoRef,
		CompanyID:    meta.CompanyID,
	})
	// source is set inside Create based on UTXORef presence
}

// calcStatus 根据录入完整性判断状态
func (s *Service) calcStatus(in CreateInput) ProfileStatus {
	missing := []string{}
	if in.ProjectName == "" {
		missing = append(missing, "项目名称")
	}
	if in.BuildingUnit == "" {
		missing = append(missing, "建设单位")
	}
	if in.OurScope == "" {
		missing = append(missing, "承担内容")
	}
	if in.ContractAmount == 0 {
		missing = append(missing, "合同金额")
	}
	if in.EndDate == nil {
		missing = append(missing, "完成时间")
	}
	if len(missing) > 0 {
		return ProfileDraft
	}
	return ProfileComplete
}

// ── PGStore ──────────────────────────────────────────────

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) Create(ctx context.Context, p *AchievementProfile) (int64, error) {
	metrics := p.ScaleMetrics
	if metrics == nil {
		metrics = json.RawMessage("{}")
	}
	row := s.db.QueryRowContext(ctx, `
		INSERT INTO achievement_profiles (
			project_name, project_type, building_unit, location,
			start_date, end_date, our_scope,
			contract_amount, our_amount, scale_metrics,
			contract_id, project_ref, utxo_ref,
			status, company_id, source, note,
			tenant_id, created_at, updated_at
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
			$11,$12,$13,$14,$15,$16,$17,$18,$19,$20
		) RETURNING id`,
		p.ProjectName, p.ProjectType, p.BuildingUnit, p.Location,
		p.StartDate, p.EndDate, p.OurScope,
		p.ContractAmount, p.OurAmount, metrics,
		p.ContractID, p.ProjectRef, p.UTXORef,
		p.Status, p.CompanyID, p.Source, p.Note,
		p.TenantID, p.CreatedAt, p.UpdatedAt,
	)
	var id int64
	return id, row.Scan(&id)
}

func (s *PGStore) Get(ctx context.Context, id int64) (*AchievementProfile, error) {
	row := s.db.QueryRowContext(ctx, profileSelectSQL+" WHERE p.id=$1 AND p.deleted=FALSE", id)
	p, err := scanProfile(row)
	if err != nil {
		return nil, err
	}
	// 加载人员和附件
	p.Personnel, _ = s.listPersonnel(ctx, id)
	p.Attachments, _ = s.listAttachments(ctx, id)
	return p, nil
}

func (s *PGStore) Update(ctx context.Context, p *AchievementProfile) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE achievement_profiles SET
			project_name=$2, project_type=$3, building_unit=$4, location=$5,
			start_date=$6, end_date=$7, our_scope=$8,
			contract_amount=$9, our_amount=$10, scale_metrics=$11,
			status=$12, note=$13, updated_at=NOW()
		WHERE id=$1`,
		p.ID, p.ProjectName, p.ProjectType, p.BuildingUnit, p.Location,
		p.StartDate, p.EndDate, p.OurScope,
		p.ContractAmount, p.OurAmount, p.ScaleMetrics,
		p.Status, p.Note,
	)
	return err
}

func (s *PGStore) List(ctx context.Context, f ProfileFilter) ([]*AchievementProfile, int, error) {
	where, args := buildProfileWhere(f)

	var total int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM achievement_profiles p "+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	i := len(args) + 1
	args = append(args, f.Limit, f.Offset)
	rows, err := s.db.QueryContext(ctx,
		profileSelectSQL+" "+where+
			fmt.Sprintf(" ORDER BY p.end_date DESC NULLS LAST LIMIT $%d OFFSET $%d", i, i+1),
		args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var list []*AchievementProfile
	for rows.Next() {
		p, err := scanProfileRow(rows)
		if err == nil {
			list = append(list, p)
		}
	}
	return list, total, nil
}

func (s *PGStore) ListByEmployee(ctx context.Context, employeeID int64, tenantID int) ([]*AchievementProfile, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT `+profileColumns+`
		FROM achievement_profiles p
		JOIN achievement_profile_personnel pp ON pp.profile_id = p.id
		WHERE pp.employee_id = $1 AND p.tenant_id = $2 AND p.deleted = FALSE
		ORDER BY p.end_date DESC NULLS LAST`,
		employeeID, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []*AchievementProfile
	for rows.Next() {
		if p, err := scanProfileRow(rows); err == nil {
			list = append(list, p)
		}
	}
	return list, nil
}

func (s *PGStore) GetByUTXORef(ctx context.Context, utxoRef string) (*AchievementProfile, error) {
	row := s.db.QueryRowContext(ctx,
		profileSelectSQL+" WHERE p.utxo_ref=$1 AND p.deleted=FALSE", utxoRef)
	return scanProfile(row)
}

func (s *PGStore) AddPersonnel(ctx context.Context, pp *ProfilePersonnel) (int64, error) {
	row := s.db.QueryRowContext(ctx, `
		INSERT INTO achievement_profile_personnel
			(profile_id, employee_id, employee_name, executor_ref, role, specialty, qual_type, cert_no)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		RETURNING id`,
		pp.ProfileID, pp.EmployeeID, pp.EmployeeName, pp.ExecutorRef,
		pp.Role, pp.Specialty, pp.QualType, pp.CertNo,
	)
	var id int64
	return id, row.Scan(&id)
}

func (s *PGStore) AddAttachment(ctx context.Context, att *ProfileAttachment) (int64, error) {
	row := s.db.QueryRowContext(ctx, `
		INSERT INTO achievement_profile_attachments
			(profile_id, kind, name, url, utxo_ref, note)
		VALUES ($1,$2,$3,$4,$5,$6)
		RETURNING id`,
		att.ProfileID, att.Kind, att.Name, att.URL, att.UTXORef, att.Note,
	)
	var id int64
	return id, row.Scan(&id)
}

func (s *PGStore) SumAmountByCompany(ctx context.Context, companyID int, tenantID int) (float64, error) {
	var sum float64
	err := s.db.QueryRowContext(ctx,
		"SELECT COALESCE(SUM(our_amount),0) FROM achievement_profiles WHERE company_id=$1 AND tenant_id=$2 AND deleted=FALSE AND status='COMPLETE'",
		companyID, tenantID).Scan(&sum)
	return sum, err
}

func (s *PGStore) CountByType(ctx context.Context, companyID int, tenantID int) (map[ProjectType]int, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT project_type, COUNT(*) FROM achievement_profiles WHERE company_id=$1 AND tenant_id=$2 AND deleted=FALSE GROUP BY project_type",
		companyID, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[ProjectType]int)
	for rows.Next() {
		var pt ProjectType
		var cnt int
		if err := rows.Scan(&pt, &cnt); err == nil {
			result[pt] = cnt
		}
	}
	return result, nil
}

func (s *PGStore) listPersonnel(ctx context.Context, profileID int64) ([]*ProfilePersonnel, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, profile_id, employee_id, employee_name, executor_ref, role, specialty, qual_type, cert_no FROM achievement_profile_personnel WHERE profile_id=$1",
		profileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []*ProfilePersonnel
	for rows.Next() {
		pp := &ProfilePersonnel{}
		if err := rows.Scan(&pp.ID, &pp.ProfileID, &pp.EmployeeID, &pp.EmployeeName, &pp.ExecutorRef,
			&pp.Role, &pp.Specialty, &pp.QualType, &pp.CertNo); err == nil {
			list = append(list, pp)
		}
	}
	return list, nil
}

func (s *PGStore) listAttachments(ctx context.Context, profileID int64) ([]*ProfileAttachment, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, profile_id, kind, name, url, utxo_ref, note FROM achievement_profile_attachments WHERE profile_id=$1",
		profileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []*ProfileAttachment
	for rows.Next() {
		att := &ProfileAttachment{}
		if err := rows.Scan(&att.ID, &att.ProfileID, &att.Kind, &att.Name,
			&att.URL, &att.UTXORef, &att.Note); err == nil {
			list = append(list, att)
		}
	}
	return list, nil
}

const profileColumns = `
	p.id, p.project_name, p.project_type, p.building_unit, p.location,
	p.start_date, p.end_date, p.our_scope,
	p.contract_amount, p.our_amount, p.scale_metrics,
	p.contract_id, p.project_ref, p.utxo_ref,
	p.status, p.company_id, p.source, p.note,
	p.tenant_id, p.created_at, p.updated_at`

const profileSelectSQL = "SELECT " + profileColumns + " FROM achievement_profiles p"

func scanProfile(row *sql.Row) (*AchievementProfile, error) {
	p := &AchievementProfile{}
	err := row.Scan(
		&p.ID, &p.ProjectName, &p.ProjectType, &p.BuildingUnit, &p.Location,
		&p.StartDate, &p.EndDate, &p.OurScope,
		&p.ContractAmount, &p.OurAmount, &p.ScaleMetrics,
		&p.ContractID, &p.ProjectRef, &p.UTXORef,
		&p.Status, &p.CompanyID, &p.Source, &p.Note,
		&p.TenantID, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func scanProfileRow(rows *sql.Rows) (*AchievementProfile, error) {
	p := &AchievementProfile{}
	err := rows.Scan(
		&p.ID, &p.ProjectName, &p.ProjectType, &p.BuildingUnit, &p.Location,
		&p.StartDate, &p.EndDate, &p.OurScope,
		&p.ContractAmount, &p.OurAmount, &p.ScaleMetrics,
		&p.ContractID, &p.ProjectRef, &p.UTXORef,
		&p.Status, &p.CompanyID, &p.Source, &p.Note,
		&p.TenantID, &p.CreatedAt, &p.UpdatedAt,
	)
	return p, err
}

func buildProfileWhere(f ProfileFilter) (string, []any) {
	conds := []string{"p.deleted=FALSE", fmt.Sprintf("p.tenant_id=$%d", 1)}
	args := []any{f.TenantID}
	i := 2

	if f.CompanyID != nil {
		conds = append(conds, fmt.Sprintf("p.company_id=$%d", i))
		args = append(args, *f.CompanyID)
		i++
	}
	if f.ProjectType != nil {
		conds = append(conds, fmt.Sprintf("p.project_type=$%d", i))
		args = append(args, *f.ProjectType)
		i++
	}
	if f.Status != nil {
		conds = append(conds, fmt.Sprintf("p.status=$%d", i))
		args = append(args, *f.Status)
		i++
	}
	if f.YearFrom != nil {
		conds = append(conds, fmt.Sprintf("EXTRACT(YEAR FROM p.end_date) >= $%d", i))
		args = append(args, *f.YearFrom)
		i++
	}
	if f.YearTo != nil {
		conds = append(conds, fmt.Sprintf("EXTRACT(YEAR FROM p.end_date) <= $%d", i))
		args = append(args, *f.YearTo)
		i++
	}
	if f.Keyword != "" {
		conds = append(conds, fmt.Sprintf("(p.project_name ILIKE $%d OR p.building_unit ILIKE $%d)", i, i))
		args = append(args, "%"+f.Keyword+"%")
		i++
	}

	return "WHERE " + strings.Join(conds, " AND "), args
}
