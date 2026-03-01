// ============================================================
// qualification/service.go
// 资质证书管理
//
// 两类持有者：公司（综合甲级/行业甲级乙级）+ 个人（注册工程师）
// 关联点：
//   - RULE-002 校验时从这里查执行体是否持有有效证书
//   - achievement-profile 导出时附上证书信息
//   - 招投标时统计有效注册师人数/类型
// ============================================================

package qualification

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ── 资质类型 ──────────────────────────────────────────────

type HolderType string

const (
	HolderCompany HolderType = "COMPANY" // 公司资质
	HolderPerson  HolderType = "PERSON"  // 个人注册证书
)

// QualType 资质/证书类型
type QualType string

const (
	// 公司资质
	QualComprehensiveA QualType = "COMPREHENSIVE_A" // 工程设计综合甲级
	QualIndustryA      QualType = "INDUSTRY_A"      // 行业甲级
	QualIndustryB      QualType = "INDUSTRY_B"      // 行业乙级
	QualSpecialA       QualType = "SPECIAL_A"       // 专项甲级
	QualSpecialB       QualType = "SPECIAL_B"       // 专项乙级

	// 个人注册证书
	QualRegArch        QualType = "REG_ARCH"        // 注册建筑师
	QualRegStructure   QualType = "REG_STRUCTURE"   // 注册结构工程师
	QualRegCivil       QualType = "REG_CIVIL"       // 注册土木工程师
	QualRegElectric    QualType = "REG_ELECTRIC"    // 注册电气工程师
	QualRegMech        QualType = "REG_MECH"        // 注册机械工程师
	QualRegCost        QualType = "REG_COST"        // 注册造价工程师
	QualRegSafety      QualType = "REG_SAFETY"      // 注册安全工程师
	QualRegSurvey      QualType = "REG_SURVEY"      // 注册测量师
	QualSeniorEngineer QualType = "SENIOR_ENGINEER" // 高级工程师（职称）
	QualEngineer       QualType = "ENGINEER"        // 工程师（职称）
)

type CertStatus string

const (
	StatusValid      CertStatus = "VALID"       // 有效
	StatusExpired    CertStatus = "EXPIRED"     // 已过期
	StatusExpireSoon CertStatus = "EXPIRE_SOON" // 即将到期（90天内）
	StatusApplying   CertStatus = "APPLYING"    // 申报中
	StatusRevoked    CertStatus = "REVOKED"     // 已注销
)

// ── 核心数据结构 ─────────────────────────────────────────

type Qualification struct {
	ID            int64
	HolderType    HolderType // COMPANY / PERSON
	HolderID      int64      // company.id 或 employee.id
	HolderName    string     // 冗余，方便查询展示
	ExecutorRef   string     // v://zhongbei/executor/... 关联协议层
	QualType      QualType
	CertNo        string     // 证书编号
	IssuedBy      string     // 发证机关
	IssuedAt      *time.Time // 发证日期
	ValidFrom     *time.Time // 有效期开始
	ValidUntil    *time.Time // 有效期截止（nil = 长期有效）
	Status        CertStatus
	Specialty     string // 专业方向（e.g. 岩土、桥梁、道路）
	Level         string // 等级（一级/二级，甲级/乙级）
	Scope         string // 业务范围描述
	AttachmentURL string // 证书扫描件 URL
	Note          string
	TenantID      int
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// ExpiryWarning 到期预警
type ExpiryWarning struct {
	QualID      int64
	HolderName  string
	QualType    QualType
	CertNo      string
	ValidUntil  time.Time
	DaysLeft    int
	ExecutorRef string
}

// QualSummary 资质统计（用于资质申报/招投标）
type QualSummary struct {
	// 公司维度
	CompanyQuals []*Qualification
	// 个人维度（按类型分组）
	PersonsByType map[QualType][]*Qualification
	// 汇总数字
	TotalValidCerts int
	TotalRegistered int // 有效注册证书数
	ExpiringSoon    int // 90天内到期数
}

// ── 输入类型 ─────────────────────────────────────────────

type CreateInput struct {
	HolderType    HolderType
	HolderID      int64
	ExecutorRef   string
	QualType      QualType
	CertNo        string
	IssuedBy      string
	IssuedAt      *time.Time
	ValidFrom     *time.Time
	ValidUntil    *time.Time // nil = 长期有效
	Specialty     string
	Level         string
	Scope         string
	AttachmentURL string
	Note          string
}

type UpdateInput struct {
	CertNo        *string
	IssuedBy      *string
	IssuedAt      *time.Time
	ValidFrom     *time.Time
	ValidUntil    *time.Time
	Status        *CertStatus
	Specialty     *string
	Level         *string
	Scope         *string
	AttachmentURL *string
	Note          *string
}

type Filter struct {
	HolderType *HolderType
	HolderID   *int64
	QualType   *QualType
	Status     *CertStatus
	CompanyID  *int // 查某公司及其员工的所有证书
	TenantID   int
	Limit      int
	Offset     int
}

// ── Store 接口 ───────────────────────────────────────────

type Store interface {
	Create(ctx context.Context, q *Qualification) (int64, error)
	Get(ctx context.Context, id int64) (*Qualification, error)
	Update(ctx context.Context, id int64, in UpdateInput) error
	Revoke(ctx context.Context, id int64, reason string) error
	List(ctx context.Context, f Filter) ([]*Qualification, int, error)
	ListByExecutorRef(ctx context.Context, executorRef string) ([]*Qualification, error)
	ListExpiring(ctx context.Context, withinDays int, tenantID int) ([]*Qualification, error)
	SummaryByCompany(ctx context.Context, companyID int, tenantID int) (*QualSummary, error)
	// RULE-002 校验用：查执行体是否有指定类型的有效证书
	CheckValid(ctx context.Context, executorRef string, qualType QualType) (bool, error)
}

// ── Service ──────────────────────────────────────────────

type Service struct {
	store    Store
	tenantID int
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

func (s *Service) Create(ctx context.Context, in CreateInput) (*Qualification, error) {
	if in.HolderID == 0 {
		return nil, fmt.Errorf("HolderID 不能为空")
	}
	if in.QualType == "" {
		return nil, fmt.Errorf("QualType 不能为空")
	}
	if in.CertNo == "" {
		return nil, fmt.Errorf("证书编号不能为空")
	}

	now := time.Now()
	status := StatusValid
	if in.ValidUntil != nil {
		status = s.calcStatus(*in.ValidUntil)
	}

	q := &Qualification{
		HolderType:    in.HolderType,
		HolderID:      in.HolderID,
		ExecutorRef:   in.ExecutorRef,
		QualType:      in.QualType,
		CertNo:        in.CertNo,
		IssuedBy:      in.IssuedBy,
		IssuedAt:      in.IssuedAt,
		ValidFrom:     in.ValidFrom,
		ValidUntil:    in.ValidUntil,
		Status:        status,
		Specialty:     in.Specialty,
		Level:         in.Level,
		Scope:         in.Scope,
		AttachmentURL: in.AttachmentURL,
		Note:          in.Note,
		TenantID:      s.tenantID,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	id, err := s.store.Create(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("创建资质证书失败: %w", err)
	}
	q.ID = id
	return q, nil
}

func (s *Service) Get(ctx context.Context, id int64) (*Qualification, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) Update(ctx context.Context, id int64, in UpdateInput) error {
	// 如果更新了有效期，重新计算状态
	if in.ValidUntil != nil && in.Status == nil {
		status := s.calcStatus(*in.ValidUntil)
		in.Status = &status
	}
	return s.store.Update(ctx, id, in)
}

func (s *Service) Revoke(ctx context.Context, id int64, reason string) error {
	return s.store.Revoke(ctx, id, reason)
}

func (s *Service) List(ctx context.Context, f Filter) ([]*Qualification, int, error) {
	f.TenantID = s.tenantID
	if f.Limit == 0 {
		f.Limit = 20
	}
	return s.store.List(ctx, f)
}

func (s *Service) ListByExecutorRef(ctx context.Context, executorRef string) ([]*Qualification, error) {
	return s.store.ListByExecutorRef(ctx, executorRef)
}

// GetExpiryWarnings 获取即将到期的证书列表（默认90天）
func (s *Service) GetExpiryWarnings(ctx context.Context, withinDays int) ([]*ExpiryWarning, error) {
	if withinDays <= 0 {
		withinDays = 90
	}
	quals, err := s.store.ListExpiring(ctx, withinDays, s.tenantID)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	warnings := make([]*ExpiryWarning, 0, len(quals))
	for _, q := range quals {
		if q.ValidUntil == nil {
			continue
		}
		daysLeft := int(q.ValidUntil.Sub(now).Hours() / 24)
		warnings = append(warnings, &ExpiryWarning{
			QualID:      q.ID,
			HolderName:  q.HolderName,
			QualType:    q.QualType,
			CertNo:      q.CertNo,
			ValidUntil:  *q.ValidUntil,
			DaysLeft:    daysLeft,
			ExecutorRef: q.ExecutorRef,
		})
	}
	return warnings, nil
}

// SummaryByCompany 公司资质统计（用于资质申报）
func (s *Service) SummaryByCompany(ctx context.Context, companyID int) (*QualSummary, error) {
	return s.store.SummaryByCompany(ctx, companyID, s.tenantID)
}

// CheckValidForRule002 RULE-002 校验：执行体是否持有有效审图资质
// 审图资质要求：持有综合甲级或相关行业甲级，且在有效期内
func (s *Service) CheckValidForRule002(ctx context.Context, executorRef string) (bool, error) {
	// 综合甲级满足 RULE-002
	ok, err := s.store.CheckValid(ctx, executorRef, QualComprehensiveA)
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}
	// 行业甲级也满足
	return s.store.CheckValid(ctx, executorRef, QualIndustryA)
}

// RefreshStatuses 批量刷新证书状态（定时任务调用）
func (s *Service) RefreshStatuses(ctx context.Context) (int, error) {
	f := Filter{TenantID: s.tenantID, Limit: 1000}
	quals, _, err := s.store.List(ctx, f)
	if err != nil {
		return 0, err
	}

	updated := 0
	for _, q := range quals {
		if q.ValidUntil == nil || q.Status == StatusRevoked {
			continue
		}
		newStatus := s.calcStatus(*q.ValidUntil)
		if newStatus != q.Status {
			if err := s.store.Update(ctx, q.ID, UpdateInput{Status: &newStatus}); err != nil {
				continue
			}
			updated++
		}
	}
	return updated, nil
}

func (s *Service) calcStatus(validUntil time.Time) CertStatus {
	now := time.Now()
	if validUntil.Before(now) {
		return StatusExpired
	}
	if validUntil.Before(now.Add(90 * 24 * time.Hour)) {
		return StatusExpireSoon
	}
	return StatusValid
}

// ── PGStore ──────────────────────────────────────────────

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) Create(ctx context.Context, q *Qualification) (int64, error) {
	row := s.db.QueryRowContext(ctx, `
		INSERT INTO qualifications (
			holder_type, holder_id, holder_name, executor_ref,
			qual_type, cert_no, issued_by, issued_at,
			valid_from, valid_until, status,
			specialty, level, scope, attachment_url, note,
			tenant_id, created_at, updated_at
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19
		) RETURNING id`,
		q.HolderType, q.HolderID, q.HolderName, q.ExecutorRef,
		q.QualType, q.CertNo, q.IssuedBy, q.IssuedAt,
		q.ValidFrom, q.ValidUntil, q.Status,
		q.Specialty, q.Level, q.Scope, q.AttachmentURL, q.Note,
		q.TenantID, q.CreatedAt, q.UpdatedAt,
	)
	var id int64
	return id, row.Scan(&id)
}

func (s *PGStore) Get(ctx context.Context, id int64) (*Qualification, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+qualColumns+` FROM qualifications q WHERE id=$1 AND deleted=FALSE`, id)
	return scanQual(row)
}

func (s *PGStore) Update(ctx context.Context, id int64, in UpdateInput) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE qualifications SET
			cert_no        = COALESCE($2, cert_no),
			issued_by      = COALESCE($3, issued_by),
			issued_at      = COALESCE($4, issued_at),
			valid_from     = COALESCE($5, valid_from),
			valid_until    = COALESCE($6, valid_until),
			status         = COALESCE($7, status),
			specialty      = COALESCE($8, specialty),
			level          = COALESCE($9, level),
			scope          = COALESCE($10, scope),
			attachment_url = COALESCE($11, attachment_url),
			note           = COALESCE($12, note),
			updated_at     = NOW()
		WHERE id=$1`,
		id,
		in.CertNo, in.IssuedBy, in.IssuedAt,
		in.ValidFrom, in.ValidUntil, in.Status,
		in.Specialty, in.Level, in.Scope,
		in.AttachmentURL, in.Note,
	)
	return err
}

func (s *PGStore) Revoke(ctx context.Context, id int64, reason string) error {
	status := StatusRevoked
	return s.Update(ctx, id, UpdateInput{Status: &status, Note: &reason})
}

func (s *PGStore) List(ctx context.Context, f Filter) ([]*Qualification, int, error) {
	where := "WHERE q.deleted=FALSE AND q.tenant_id=$1"
	args := []any{f.TenantID}
	i := 2
	if f.HolderType != nil {
		where += fmt.Sprintf(" AND q.holder_type=$%d", i)
		args = append(args, *f.HolderType)
		i++
	}
	if f.HolderID != nil {
		where += fmt.Sprintf(" AND q.holder_id=$%d", i)
		args = append(args, *f.HolderID)
		i++
	}
	if f.QualType != nil {
		where += fmt.Sprintf(" AND q.qual_type=$%d", i)
		args = append(args, *f.QualType)
		i++
	}
	if f.Status != nil {
		where += fmt.Sprintf(" AND q.status=$%d", i)
		args = append(args, *f.Status)
		i++
	}

	var total int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM qualifications q "+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	args = append(args, f.Limit, f.Offset)
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+qualColumns+" FROM qualifications q "+where+
			fmt.Sprintf(" ORDER BY q.valid_until ASC NULLS LAST LIMIT $%d OFFSET $%d", i, i+1),
		args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	return scanQuals(rows), total, nil
}

func (s *PGStore) ListByExecutorRef(ctx context.Context, executorRef string) ([]*Qualification, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+qualColumns+" FROM qualifications q WHERE executor_ref=$1 AND deleted=FALSE ORDER BY valid_until ASC NULLS LAST",
		executorRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanQuals(rows), nil
}

func (s *PGStore) ListExpiring(ctx context.Context, withinDays int, tenantID int) ([]*Qualification, error) {
	deadline := time.Now().Add(time.Duration(withinDays) * 24 * time.Hour)
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+qualColumns+" FROM qualifications q WHERE tenant_id=$1 AND deleted=FALSE AND status!='REVOKED' AND valid_until IS NOT NULL AND valid_until <= $2 ORDER BY valid_until ASC",
		tenantID, deadline)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanQuals(rows), nil
}

func (s *PGStore) SummaryByCompany(ctx context.Context, companyID int, tenantID int) (*QualSummary, error) {
	// 公司自身资质
	companyType := HolderCompany
	companyHolderID := int64(companyID)
	companyQuals, _, err := s.List(ctx, Filter{
		HolderType: &companyType,
		HolderID:   &companyHolderID,
		TenantID:   tenantID,
		Limit:      100,
	})
	if err != nil {
		return nil, err
	}

	// 该公司员工的个人证书
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+qualColumns+` FROM qualifications q
		JOIN employees e ON e.id = q.holder_id AND q.holder_type = 'PERSON'
		WHERE e.company_id = $1 AND q.tenant_id = $2 AND q.deleted = FALSE
		ORDER BY q.qual_type, q.valid_until ASC NULLS LAST`,
		companyID, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	personQuals := scanQuals(rows)

	summary := &QualSummary{
		CompanyQuals:  companyQuals,
		PersonsByType: make(map[QualType][]*Qualification),
	}

	for range companyQuals {
		summary.TotalValidCerts++
	}
	for _, q := range personQuals {
		summary.PersonsByType[q.QualType] = append(summary.PersonsByType[q.QualType], q)
		if q.Status == StatusValid {
			summary.TotalValidCerts++
			summary.TotalRegistered++
		}
		if q.Status == StatusExpireSoon {
			summary.ExpiringSoon++
		}
	}
	return summary, nil
}

func (s *PGStore) CheckValid(ctx context.Context, executorRef string, qualType QualType) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM qualifications
		WHERE executor_ref=$1
		  AND qual_type=$2
		  AND status IN ('VALID','EXPIRE_SOON')
		  AND deleted=FALSE
		  AND (valid_from IS NULL OR valid_from <= NOW())
		  AND (valid_until IS NULL OR valid_until > NOW())`,
		executorRef, qualType).Scan(&count)
	return count > 0, err
}

const qualColumns = `
	q.id, q.holder_type, q.holder_id, q.holder_name, q.executor_ref,
	q.qual_type, q.cert_no, q.issued_by, q.issued_at,
	q.valid_from, q.valid_until, q.status,
	q.specialty, q.level, q.scope, q.attachment_url, q.note,
	q.tenant_id, q.created_at, q.updated_at`

func scanQual(row *sql.Row) (*Qualification, error) {
	q := &Qualification{}
	err := row.Scan(
		&q.ID, &q.HolderType, &q.HolderID, &q.HolderName, &q.ExecutorRef,
		&q.QualType, &q.CertNo, &q.IssuedBy, &q.IssuedAt,
		&q.ValidFrom, &q.ValidUntil, &q.Status,
		&q.Specialty, &q.Level, &q.Scope, &q.AttachmentURL, &q.Note,
		&q.TenantID, &q.CreatedAt, &q.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return q, nil
}

func scanQuals(rows *sql.Rows) []*Qualification {
	var list []*Qualification
	for rows.Next() {
		q := &Qualification{}
		if err := rows.Scan(
			&q.ID, &q.HolderType, &q.HolderID, &q.HolderName, &q.ExecutorRef,
			&q.QualType, &q.CertNo, &q.IssuedBy, &q.IssuedAt,
			&q.ValidFrom, &q.ValidUntil, &q.Status,
			&q.Specialty, &q.Level, &q.Scope, &q.AttachmentURL, &q.Note,
			&q.TenantID, &q.CreatedAt, &q.UpdatedAt,
		); err == nil {
			list = append(list, q)
		}
	}
	return list
}
