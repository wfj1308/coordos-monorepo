package review_publish

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ══════════════════════════════════════════════════════════════
// 审图与出版中心 - 设计院最值钱的两个权力节点
//
// 审图中心：总院审图章（公章）- 带约束的资源批次
// 出版中心：图纸出版权 - 管理图纸合法性状态
//
// 审图在出版之前，系统强制保证，不是靠人工流程规定
// ══════════════════════════════════════════════════════════════

type ReviewStampStatus string

const (
	ReviewStampActive    ReviewStampStatus = "ACTIVE"
	ReviewStampExhausted ReviewStampStatus = "EXHAUSTED"
	ReviewStampFrozen    ReviewStampStatus = "FROZEN"
)

type DrawingStatus string

const (
	DrawingDraft      DrawingStatus = "DRAFT"
	DrawingReviewing  DrawingStatus = "REVIEWING"
	DrawingSealed     DrawingStatus = "SEALED"
	DrawingPublished  DrawingStatus = "PUBLISHED"
	DrawingSuperseded DrawingStatus = "SUPERSEDED"
)

// ══════════════════════════════════════════════════════════════
// 审图章资源批次
// ══════════════════════════════════════════════════════════════

type ReviewStampBatch struct {
	ID          int64  `json:"id"`
	Ref         string `json:"ref"`
	BatchSource string `json:"batch_source"` // HEAD_OFFICE
	Quantity    int    `json:"quantity"`     // 1个章
	Remaining   int    `json:"remaining"`

	// 约束三维度
	Constraints ReviewStampConstraints `json:"constraints"`

	// 消耗记录
	ConsumedBy []ReviewStampConsumption `json:"consumed_by"`

	Status    ReviewStampStatus `json:"status"`
	TenantID  int               `json:"tenant_id"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

type ReviewStampConstraints struct {
	Time     TimeConstraints          `json:"time"`
	Quantity QuantityConstraints      `json:"quantity"`
	Scope    ScopeConstraints         `json:"scope"`
	Process  ReviewProcessConstraints `json:"process"` // 审图特有
}

type ReviewProcessConstraints struct {
	RequireUTXO              string `json:"require_utxo"`                // REVIEW_COMMENTS
	RequireResolutionRate    int    `json:"require_resolution_rate"`     // 100%
	RequireChiefEngineerSign bool   `json:"require_chief_engineer_sign"` // 必须总工签字
	RequireHeadOfficeProject bool   `json:"require_head_office_project"` // 项目归属总院
}

type TimeConstraints struct {
	ValidFrom  *time.Time `json:"valid_from"`
	ValidUntil *time.Time `json:"valid_until"`
}

type QuantityConstraints struct {
	MaxConcurrent int `json:"max_concurrent"`
	MaxPerProject int `json:"max_per_project"`
}

type ScopeConstraints struct {
	ProjectTypes      []string `json:"project_types"`
	Regions           []string `json:"regions"`
	MinContractAmount float64  `json:"min_contract_amount"`
}

type ReviewStampConsumption struct {
	ProjectRef       string    `json:"project_ref"`
	ConsumedAt       time.Time `json:"consumed_at"`
	ExecutorRef      string    `json:"executor_ref"`       // 主审工程师
	ChiefEngineerRef string    `json:"chief_engineer_ref"` // 总工
	ResolutionRate   int       `json:"resolution_rate"`    // 意见处理率
	UTXORef          string    `json:"utxo_ref"`           // 审图合格证 UTXO
	ProofHash        string    `json:"proof_hash"`
}

// ══════════════════════════════════════════════════════════════
// 图纸版本链
// ══════════════════════════════════════════════════════════════

type Drawing struct {
	ID            int64         `json:"id"`
	DrawingNo     string        `json:"drawing_no"`      // 图纸编号（唯一）
	Version       int           `json:"version"`         // 版本号
	PrevVersionID *int64        `json:"prev_version_id"` // 前一版本
	Status        DrawingStatus `json:"status"`

	// 审图证引用
	ReviewCertUTXORef string     `json:"review_cert_utxo_ref"`
	ReviewCertID      *int64     `json:"review_cert_id"`
	SealedAt          *time.Time `json:"sealed_at"`
	SealedBy          string     `json:"sealed_by"`

	// 出版记录
	PublishedAt *time.Time `json:"published_at"`
	PublishedBy string     `json:"published_by"`
	ProofHash   string     `json:"proof_hash"`

	ProjectRef string    `json:"project_ref"`
	TenantID   int       `json:"tenant_id"`
	CreatedAt  time.Time `json:"created_at"`
}

// ══════════════════════════════════════════════════════════════
// 审图验证输入
// ══════════════════════════════════════════════════════════════

type VerifyReviewInput struct {
	ProjectRef         string `json:"project_ref"`
	ExecutorRef        string `json:"executor_ref"`         // 主审工程师
	ChiefEngineerRef   string `json:"chief_engineer_ref"`   // 总工
	ReviewCommentsUTXO string `json:"review_comments_utxo"` // 审图意见 UTXO
	ResolutionRate     int    `json:"resolution_rate"`      // 意见处理率
}

type VerifyReviewResult struct {
	CanSeal  bool     `json:"can_seal"`
	Errors   []string `json:"errors"`
	Warnings []string `json:"warnings"`

	// 约束检查明细
	ExecutorValid       bool `json:"executor_valid"`
	ResolutionRateOK    bool `json:"resolution_rate_ok"`
	ChiefEngineerSigned bool `json:"chief_engineer_signed"`
	HeadOfficeProject   bool `json:"head_office_project"`
}

type VerifyPublishInput struct {
	DrawingNo      string `json:"drawing_no"`
	Version        int    `json:"version"`
	PrevVersionID  *int64 `json:"prev_version_id"`
	ReviewCertUTXO string `json:"review_cert_utxo"`
	PublisherRef   string `json:"publisher_ref"`
}

type VerifyPublishResult struct {
	CanPublish bool     `json:"can_publish"`
	Errors     []string `json:"errors"`

	ReviewCertValid       bool `json:"review_cert_valid"`
	DrawingNoUnique       bool `json:"drawing_no_unique"`
	PrevVersionSuperseded bool `json:"prev_version_superseded"`
	PublisherCertValid    bool `json:"publisher_cert_valid"`
}

// ══════════════════════════════════════════════════════════════
// Store 接口
// ══════════════════════════════════════════════════════════════

type Store interface {
	// 审图章批次
	GetReviewStampBatch(ctx context.Context, ref string) (*ReviewStampBatch, error)
	UpdateBatchConsumption(ctx context.Context, ref string, consumption ReviewStampConsumption) error

	// 图纸
	GetDrawing(ctx context.Context, id int64) (*Drawing, error)
	GetDrawingByNoVersion(ctx context.Context, tenantID int, drawingNo string, version int) (*Drawing, error)
	CreateDrawing(ctx context.Context, d *Drawing) (int64, error)
	UpdateDrawingStatus(ctx context.Context, id int64, status DrawingStatus, at time.Time) error

	// 验证查询
	CountActiveReviewStamps(ctx context.Context, projectRef string) (int, error)
	CheckDrawingNoUnique(ctx context.Context, tenantID int, drawingNo string, excludeID int64) (bool, error)
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

func (s *Service) safeCtx(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (s *Service) storeDB() (*sql.DB, bool) {
	pg, ok := s.store.(*PGStore)
	if !ok || pg == nil || pg.db == nil {
		return nil, false
	}
	return pg.db, true
}

// VerifyReview 验证审图约束
// 审图章每次签发必须满足所有约束
func (s *Service) VerifyReview(ctx context.Context, in VerifyReviewInput) (*VerifyReviewResult, error) {
	ctx = s.safeCtx(ctx)
	result := &VerifyReviewResult{CanSeal: true}

	// ① 主审工程师资质有效
	executorValid, err := s.checkExecutorQualification(ctx, in.ExecutorRef)
	if err != nil {
		return nil, err
	}
	result.ExecutorValid = executorValid
	if !executorValid {
		result.Errors = append(result.Errors, "主审工程师资质无效或已过期")
		result.CanSeal = false
	}

	// ② 审图意见处理率 = 100%
	if in.ResolutionRate < 100 {
		result.ResolutionRateOK = false
		result.Errors = append(result.Errors, fmt.Sprintf("审图意见处理率不足：当前%d%%，要求100%%", in.ResolutionRate))
		result.CanSeal = false
	} else {
		result.ResolutionRateOK = true
	}

	// ③ 总工程师签字
	if in.ChiefEngineerRef == "" {
		result.ChiefEngineerSigned = false
		result.Errors = append(result.Errors, "缺少总工程师签字")
		result.CanSeal = false
	} else {
		// 验证总工资格
		chiefValid, _ := s.checkChiefEngineerQualification(ctx, in.ChiefEngineerRef)
		result.ChiefEngineerSigned = chiefValid
		if !chiefValid {
			result.Errors = append(result.Errors, "总工程师资格无效")
			result.CanSeal = false
		}
	}

	// ④ 项目归属总院合同体系
	headOfficeProject, err := s.checkHeadOfficeProject(ctx, in.ProjectRef)
	if err != nil {
		return nil, err
	}
	result.HeadOfficeProject = headOfficeProject
	if !headOfficeProject {
		result.Errors = append(result.Errors, "项目未归属总院合同体系")
		result.CanSeal = false
	}

	return result, nil
}

// SealReview 盖审图章
// 消耗审图章批次，产出审图合格证 UTXO
func (s *Service) SealReview(ctx context.Context, batchRef string, in VerifyReviewInput) (*ReviewStampConsumption, error) {
	ctx = s.safeCtx(ctx)
	// 先验证
	result, err := s.VerifyReview(ctx, in)
	if err != nil {
		return nil, err
	}
	if !result.CanSeal {
		return nil, fmt.Errorf("审图约束验证失败：%v", result.Errors)
	}

	now := time.Now()
	consumption := ReviewStampConsumption{
		ProjectRef:       in.ProjectRef,
		ConsumedAt:       now,
		ExecutorRef:      in.ExecutorRef,
		ChiefEngineerRef: in.ChiefEngineerRef,
		ResolutionRate:   in.ResolutionRate,
		ProofHash:        s.computeReviewProofHash(in, now),
	}

	// 更新批次消耗记录
	if err := s.store.UpdateBatchConsumption(ctx, batchRef, consumption); err != nil {
		return nil, fmt.Errorf("更新审图章消耗记录失败：%w", err)
	}
	utxoRef, err := s.createReviewCertUTXO(ctx, in, consumption)
	if err != nil {
		return nil, fmt.Errorf("create review certificate utxo failed: %w", err)
	}
	consumption.UTXORef = utxoRef

	return &consumption, nil
}

// VerifyPublish 验证出版约束
// 图纸出版必须满足：有审图合格证、编号唯一、前一版本已作废
func (s *Service) VerifyPublish(ctx context.Context, in VerifyPublishInput) (*VerifyPublishResult, error) {
	ctx = s.safeCtx(ctx)
	result := &VerifyPublishResult{CanPublish: true}

	// ① 图纸版本有审图合格证 UTXO
	if in.ReviewCertUTXO == "" {
		result.ReviewCertValid = false
		result.Errors = append(result.Errors, "缺少审图合格证 UTXO")
		result.CanPublish = false
	} else {
		reviewCertValid, _ := s.checkReviewCertValid(ctx, in.ReviewCertUTXO)
		result.ReviewCertValid = reviewCertValid
		if !reviewCertValid {
			result.Errors = append(result.Errors, "审图合格证无效")
			result.CanPublish = false
		}
	}

	// ② 图纸编号唯一
	drawingNoUnique, err := s.store.CheckDrawingNoUnique(ctx, s.tenantID, in.DrawingNo, 0)
	if err != nil {
		return nil, err
	}
	result.DrawingNoUnique = drawingNoUnique
	if !drawingNoUnique {
		result.Errors = append(result.Errors, "图纸编号已被占用")
		result.CanPublish = false
	}

	// ③ 前一版本已标记 SUPERSEDED
	if in.PrevVersionID != nil {
		prevDrawing, err := s.store.GetDrawing(ctx, *in.PrevVersionID)
		if err == nil && prevDrawing != nil {
			result.PrevVersionSuperseded = (prevDrawing.Status == DrawingSuperseded)
			if !result.PrevVersionSuperseded {
				result.Errors = append(result.Errors, "前一版本未标记作废")
				result.CanPublish = false
			}
		}
	} else {
		result.PrevVersionSuperseded = true // 没有前一版本
	}

	// ④ 签发人证书有效
	publisherCertValid, _ := s.checkPublisherQualification(ctx, in.PublisherRef)
	result.PublisherCertValid = publisherCertValid
	if !publisherCertValid {
		result.Errors = append(result.Errors, "出版签发人证书无效")
		result.CanPublish = false
	}

	return result, nil
}

// PublishDrawing 出版图纸
// 更新图纸状态为 PUBLISHED，前一版本标记为 SUPERSEDED
func (s *Service) PublishDrawing(ctx context.Context, drawingID int64, publisherRef string) error {
	ctx = s.safeCtx(ctx)
	// 获取图纸
	drawing, err := s.store.GetDrawing(ctx, drawingID)
	if err != nil {
		return err
	}

	// 验证出版约束
	result, err := s.VerifyPublish(ctx, VerifyPublishInput{
		DrawingNo:      drawing.DrawingNo,
		Version:        drawing.Version,
		PrevVersionID:  drawing.PrevVersionID,
		ReviewCertUTXO: drawing.ReviewCertUTXORef,
		PublisherRef:   publisherRef,
	})
	if err != nil {
		return err
	}
	if !result.CanPublish {
		return fmt.Errorf("出版约束验证失败：%v", result.Errors)
	}

	now := time.Now()

	// 更新当前图纸为已出版
	if err := s.store.UpdateDrawingStatus(ctx, drawingID, DrawingPublished, now); err != nil {
		return err
	}

	// 将前一版本标记为作废
	if drawing.PrevVersionID != nil {
		if err := s.store.UpdateDrawingStatus(ctx, *drawing.PrevVersionID, DrawingSuperseded, now); err != nil {
			return err
		}
	}

	return nil
}

// ══════════════════════════════════════════════════════════════
// 辅助方法
// ══════════════════════════════════════════════════════════════

func (s *Service) checkExecutorQualification(ctx context.Context, executorRef string) (bool, error) {
	executorRef = strings.TrimSpace(executorRef)
	if executorRef == "" {
		return false, nil
	}
	db, ok := s.storeDB()
	if !ok {
		return true, nil
	}
	var count int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM qualifications
		WHERE executor_ref = $1
		  AND holder_type = 'PERSON'
		  AND status = 'VALID'
		  AND COALESCE(deleted, FALSE) = FALSE
		  AND (valid_until IS NULL OR valid_until >= CURRENT_DATE)
	`, executorRef).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *Service) checkChiefEngineerQualification(ctx context.Context, chiefEngineerRef string) (bool, error) {
	chiefEngineerRef = strings.TrimSpace(chiefEngineerRef)
	if chiefEngineerRef == "" {
		return false, nil
	}
	// 当前版本先复用执行体资质有效性检查。
	return s.checkExecutorQualification(ctx, chiefEngineerRef)
}

func (s *Service) checkHeadOfficeProject(ctx context.Context, projectRef string) (bool, error) {
	projectRef = strings.TrimSpace(projectRef)
	if projectRef == "" {
		return false, nil
	}
	db, ok := s.storeDB()
	if !ok {
		return true, nil
	}
	var count int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM project_nodes
		WHERE ref = $1 AND tenant_id = $2
	`, projectRef, s.tenantID).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *Service) checkReviewCertValid(ctx context.Context, utxoRef string) (bool, error) {
	utxoRef = strings.TrimSpace(utxoRef)
	if utxoRef == "" {
		return false, nil
	}
	db, ok := s.storeDB()
	if !ok {
		return true, nil
	}
	var count int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM achievement_utxos
		WHERE tenant_id = $1
		  AND utxo_ref = $2
		  AND spu_ref ILIKE '%review_certificate%'
		  AND status IN ('PENDING','SETTLED')
	`, s.tenantID, utxoRef).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *Service) checkPublisherQualification(ctx context.Context, publisherRef string) (bool, error) {
	publisherRef = strings.TrimSpace(publisherRef)
	if publisherRef == "" {
		return false, nil
	}
	db, ok := s.storeDB()
	if !ok {
		return true, nil
	}
	var rightCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM rights
		WHERE tenant_id = $1
		  AND holder_ref = $2
		  AND status = 'ACTIVE'
		  AND (right_type = 'PUBLISH' OR right_type = 'RIGHT_PUBLISH')
	`, s.tenantID, publisherRef).Scan(&rightCount); err == nil && rightCount > 0 {
		return true, nil
	}
	return s.checkExecutorQualification(ctx, publisherRef)
}

func (s *Service) computeReviewProofHash(in VerifyReviewInput, at time.Time) string {
	h := sha256.New()
	h.Write([]byte(strings.TrimSpace(in.ProjectRef)))
	h.Write([]byte("|"))
	h.Write([]byte(strings.TrimSpace(in.ExecutorRef)))
	h.Write([]byte("|"))
	h.Write([]byte(strings.TrimSpace(in.ChiefEngineerRef)))
	h.Write([]byte("|"))
	h.Write([]byte(strings.TrimSpace(in.ReviewCommentsUTXO)))
	h.Write([]byte("|"))
	h.Write([]byte(fmt.Sprintf("%d", in.ResolutionRate)))
	h.Write([]byte("|"))
	h.Write([]byte(at.UTC().Format(time.RFC3339Nano)))
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

func (s *Service) createReviewCertUTXO(ctx context.Context, in VerifyReviewInput, consumption ReviewStampConsumption) (string, error) {
	db, ok := s.storeDB()
	if !ok {
		return "", nil
	}
	ns := namespaceFromProjectRef(strings.TrimSpace(in.ProjectRef))
	utxoRef := fmt.Sprintf("v://%s/utxo/review/%d", ns, consumption.ConsumedAt.UnixNano())
	payload, _ := json.Marshal(map[string]any{
		"project_ref":           strings.TrimSpace(in.ProjectRef),
		"executor_ref":          strings.TrimSpace(in.ExecutorRef),
		"chief_engineer_ref":    strings.TrimSpace(in.ChiefEngineerRef),
		"review_comments_utxo":  strings.TrimSpace(in.ReviewCommentsUTXO),
		"resolution_rate":       in.ResolutionRate,
		"review_proof_hash":     consumption.ProofHash,
		"review_cert_generated": true,
	})
	_, err := db.ExecContext(ctx, `
		INSERT INTO achievement_utxos (
			utxo_ref, spu_ref, project_ref, executor_ref,
			payload, proof_hash, status, source, tenant_id, ingested_at
		) VALUES (
			$1, $2, $3, $4,
			$5::jsonb, $6, 'PENDING', 'MANUAL', $7, NOW()
		)
		ON CONFLICT (utxo_ref) DO NOTHING
	`, utxoRef, "v://coordos/spu/review/review_certificate@v1", strings.TrimSpace(in.ProjectRef), strings.TrimSpace(in.ExecutorRef), string(payload), consumption.ProofHash, s.tenantID)
	if err != nil {
		return "", err
	}
	return utxoRef, nil
}

func namespaceFromProjectRef(projectRef string) string {
	ref := strings.TrimSpace(strings.ToLower(projectRef))
	if strings.HasPrefix(ref, "v://cn.zhongbei/") {
		return "cn.zhongbei"
	}
	if strings.HasPrefix(ref, "v://zhongbei/") || strings.HasPrefix(ref, "v://10000/") {
		return "cn.zhongbei"
	}
	if strings.HasPrefix(ref, "v://") {
		without := strings.TrimPrefix(ref, "v://")
		if idx := strings.IndexByte(without, '/'); idx > 0 {
			return strings.TrimSpace(without[:idx])
		}
	}
	return "cn.zhongbei"
}

// ══════════════════════════════════════════════════════════════
// PGStore
// ══════════════════════════════════════════════════════════════

type PGStore struct {
	db *sql.DB
}

func NewPGStore(db *sql.DB) Store {
	return &PGStore{db: db}
}

func (s *PGStore) GetReviewStampBatch(ctx context.Context, ref string) (*ReviewStampBatch, error) {
	b := &ReviewStampBatch{}
	var constraintsRaw []byte
	var consumedByRaw []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT id, ref, COALESCE(batch_source,'INTERNAL'), COALESCE(quantity,0), COALESCE(remaining,0),
		       COALESCE(constraints, '{}'::jsonb), COALESCE(consumed_by, '[]'::jsonb),
		       status, created_at, updated_at
		FROM genesis_utxos
		WHERE ref = $1 AND resource_type = 'RIGHT_REVIEW_STAMP'
	`, ref).Scan(
		&b.ID, &b.Ref, &b.BatchSource, &b.Quantity, &b.Remaining,
		&constraintsRaw, &consumedByRaw,
		&b.Status, &b.CreatedAt, &b.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal(constraintsRaw, &b.Constraints)
	_ = json.Unmarshal(consumedByRaw, &b.ConsumedBy)
	return b, nil
}

func (s *PGStore) UpdateBatchConsumption(ctx context.Context, ref string, consumption ReviewStampConsumption) error {
	consumptionJSON, _ := json.Marshal(consumption)
	result, err := s.db.ExecContext(ctx, `
		UPDATE genesis_utxos
		SET consumed_by = COALESCE(consumed_by, '[]'::jsonb) || jsonb_build_array($1::jsonb),
		    remaining = GREATEST(COALESCE(remaining, 0) - 1, 0),
		    status = CASE
		        WHEN GREATEST(COALESCE(remaining, 0) - 1, 0) = 0 THEN 'EXHAUSTED'
		        ELSE status
		    END,
		    updated_at = NOW()
		WHERE ref = $2 AND remaining > 0`,
		string(consumptionJSON), ref)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("review stamp batch not found or exhausted")
	}
	return nil
}

func (s *PGStore) GetDrawing(ctx context.Context, id int64) (*Drawing, error) {
	d := &Drawing{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id, drawing_no, version, prev_version_id, status,
		       review_cert_utxo_ref, review_cert_id, sealed_at, sealed_by,
		       published_at, published_by, proof_hash,
		       project_ref, tenant_id, created_at
		FROM drawings WHERE id = $1`, id).Scan(
		&d.ID, &d.DrawingNo, &d.Version, &d.PrevVersionID, &d.Status,
		&d.ReviewCertUTXORef, &d.ReviewCertID, &d.SealedAt, &d.SealedBy,
		&d.PublishedAt, &d.PublishedBy, &d.ProofHash,
		&d.ProjectRef, &d.TenantID, &d.CreatedAt)
	return d, err
}

func (s *PGStore) GetDrawingByNoVersion(ctx context.Context, tenantID int, drawingNo string, version int) (*Drawing, error) {
	d := &Drawing{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id, drawing_no, version, prev_version_id, status,
		       review_cert_utxo_ref, review_cert_id, sealed_at, sealed_by,
		       published_at, published_by, proof_hash,
		       project_ref, tenant_id, created_at
		FROM drawings WHERE tenant_id = $1 AND drawing_no = $2 AND version = $3`,
		tenantID, drawingNo, version).Scan(
		&d.ID, &d.DrawingNo, &d.Version, &d.PrevVersionID, &d.Status,
		&d.ReviewCertUTXORef, &d.ReviewCertID, &d.SealedAt, &d.SealedBy,
		&d.PublishedAt, &d.PublishedBy, &d.ProofHash,
		&d.ProjectRef, &d.TenantID, &d.CreatedAt)
	return d, err
}

func (s *PGStore) CreateDrawing(ctx context.Context, d *Drawing) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO drawings (
			drawing_no, version, prev_version_id, status,
			review_cert_utxo_ref, review_cert_id, sealed_at, sealed_by,
			published_at, published_by, proof_hash,
			project_ref, tenant_id, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		RETURNING id`,
		d.DrawingNo, d.Version, d.PrevVersionID, d.Status,
		d.ReviewCertUTXORef, d.ReviewCertID, d.SealedAt, d.SealedBy,
		d.PublishedAt, d.PublishedBy, d.ProofHash,
		d.ProjectRef, d.TenantID, d.CreatedAt).Scan(&id)
	return id, err
}

func (s *PGStore) UpdateDrawingStatus(ctx context.Context, id int64, status DrawingStatus, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE drawings SET status = $1, published_at = $2, updated_at = NOW()
		WHERE id = $3`,
		status, at, id)
	return err
}

func (s *PGStore) CountActiveReviewStamps(ctx context.Context, projectRef string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM genesis_utxos g
		WHERE g.resource_type = 'RIGHT_REVIEW_STAMP'
		  AND g.status = 'ACTIVE'
		  AND g.consumed_by::text LIKE '%' || $1 || '%'`,
		projectRef).Scan(&count)
	return count, err
}

func (s *PGStore) CheckDrawingNoUnique(ctx context.Context, tenantID int, drawingNo string, excludeID int64) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM drawings
		WHERE tenant_id = $1 AND drawing_no = $2 AND id != $3`,
		tenantID, drawingNo, excludeID).Scan(&count)
	return count == 0, err
}
