package publishing

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

type ReviewCertificate struct {
	ID          int64           `json:"id"`
	CertRef     string          `json:"cert_ref"`
	ProjectRef  string          `json:"project_ref"`
	DrawingNo   string          `json:"drawing_no"`
	ExecutorRef string          `json:"executor_ref"`
	Payload     json.RawMessage `json:"payload"`
	TenantID    int             `json:"tenant_id"`
	CreatedAt   time.Time       `json:"created_at"`
}

type DrawingVersion struct {
	ID            int64           `json:"id"`
	DrawingNo     string          `json:"drawing_no"`
	VersionNo     int             `json:"version_no"`
	ProjectRef    string          `json:"project_ref"`
	ReviewCertRef string          `json:"review_cert_ref"`
	FileHash      string          `json:"file_hash"`
	ProofHash     string          `json:"proof_hash"`
	PublisherRef  string          `json:"publisher_ref"`
	Status        string          `json:"status"`
	Payload       json.RawMessage `json:"payload"`
	TenantID      int             `json:"tenant_id"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

type IssueReviewCertInput struct {
	ProjectRef  string          `json:"project_ref"`
	DrawingNo   string          `json:"drawing_no"`
	ExecutorRef string          `json:"executor_ref"`
	Payload     json.RawMessage `json:"payload"`
}

type PublishDrawingInput struct {
	ProjectRef    string          `json:"project_ref"`
	DrawingNo     string          `json:"drawing_no"`
	ReviewCertRef string          `json:"review_cert_ref"`
	FileHash      string          `json:"file_hash"`
	PublisherRef  string          `json:"publisher_ref"`
	Payload       json.RawMessage `json:"payload"`
}

type ReviewOpinionStats struct {
	TotalOpinions     int `json:"total_opinions"`
	ProcessedOpinions int `json:"processed_opinions"`
	MajorOpinions     int `json:"major_opinions"`
	ProcessingRate    int `json:"processing_rate"`
}

type Validator interface {
	CheckValidForRule002(ctx context.Context, executorRef string) (bool, error)
	GetReviewOpinionStats(ctx context.Context, projectRef, drawingNo string) (*ReviewOpinionStats, error)
}

type Store interface {
	CreateReviewCert(ctx context.Context, item *ReviewCertificate) error
	GetLatestReviewCert(ctx context.Context, tenantID int, projectRef, drawingNo string) (*ReviewCertificate, error)
	GetReviewCertByRef(ctx context.Context, tenantID int, certRef string) (*ReviewCertificate, error)
	PublishDrawing(ctx context.Context, item *DrawingVersion) error
	GetCurrentDrawing(ctx context.Context, tenantID int, drawingNo string) (*DrawingVersion, error)
	GetDrawingChain(ctx context.Context, tenantID int, drawingNo string) ([]*DrawingVersion, error)
	ListProjectDrawings(ctx context.Context, tenantID int, projectRef string) ([]*DrawingVersion, error)
	GetPreviousProofHash(ctx context.Context, tenantID int, drawingNo string) (string, error)
}

type Service struct {
	store     Store
	tenantID  int
	validator Validator
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

func (s *Service) SetValidator(v Validator) {
	s.validator = v
}

func (s *Service) IssueReviewCert(ctx context.Context, in IssueReviewCertInput) (*ReviewCertificate, error) {
	projectRef := strings.TrimSpace(in.ProjectRef)
	drawingNo := strings.TrimSpace(in.DrawingNo)
	executorRef := strings.TrimSpace(in.ExecutorRef)
	if projectRef == "" || drawingNo == "" || executorRef == "" {
		return nil, fmt.Errorf("project_ref, drawing_no and executor_ref are required")
	}

	if s.validator != nil {
		valid, err := s.validator.CheckValidForRule002(ctx, executorRef)
		if err != nil {
			return nil, fmt.Errorf("RULE-002 校验失败: %w", err)
		}
		if !valid {
			return nil, fmt.Errorf("RULE-002 校验不通过: 执行体 %s 不具备 REG_STRUCT 证书或非总院人员", executorRef)
		}

		stats, err := s.validator.GetReviewOpinionStats(ctx, projectRef, drawingNo)
		if err == nil && stats != nil {
			if stats.ProcessingRate < 100 {
				return nil, fmt.Errorf("RULE-002 校验不通过: 意见处理率 %d%%，必须 100%%", stats.ProcessingRate)
			}
			if stats.MajorOpinions > 0 {
				return nil, fmt.Errorf("RULE-002 校验不通过: 存在 %d 条重大意见，必须为零", stats.MajorOpinions)
			}
		}
	}

	payload := in.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	item := &ReviewCertificate{
		CertRef:     fmt.Sprintf("v://%d/publishing/review-cert/%d", s.tenantID, time.Now().UnixNano()),
		ProjectRef:  projectRef,
		DrawingNo:   drawingNo,
		ExecutorRef: executorRef,
		Payload:     payload,
		TenantID:    s.tenantID,
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.store.CreateReviewCert(ctx, item); err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Service) Publish(ctx context.Context, in PublishDrawingInput) (*DrawingVersion, error) {
	projectRef := strings.TrimSpace(in.ProjectRef)
	drawingNo := strings.TrimSpace(in.DrawingNo)
	if projectRef == "" || drawingNo == "" {
		return nil, fmt.Errorf("project_ref and drawing_no are required")
	}

	var reviewCert *ReviewCertificate
	var err error
	reviewRef := strings.TrimSpace(in.ReviewCertRef)
	if reviewRef != "" {
		reviewCert, err = s.store.GetReviewCertByRef(ctx, s.tenantID, reviewRef)
	} else {
		reviewCert, err = s.store.GetLatestReviewCert(ctx, s.tenantID, projectRef, drawingNo)
	}
	if err != nil {
		return nil, err
	}
	if reviewCert == nil {
		return nil, fmt.Errorf("review certificate is required before publishing")
	}

	payload := in.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}

	prevProofHash, _ := s.store.GetPreviousProofHash(ctx, s.tenantID, drawingNo)

	item := &DrawingVersion{
		DrawingNo:     drawingNo,
		ProjectRef:    projectRef,
		ReviewCertRef: reviewCert.CertRef,
		FileHash:      strings.TrimSpace(in.FileHash),
		PublisherRef:  strings.TrimSpace(in.PublisherRef),
		Status:        "CURRENT",
		Payload:       payload,
		TenantID:      s.tenantID,
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}
	if item.PublisherRef == "" {
		item.PublisherRef = reviewCert.ExecutorRef
	}

	item.ProofHash = computeProofHash(drawingNo, item.FileHash, reviewCert.CertRef, prevProofHash, item.CreatedAt)

	if err := s.store.PublishDrawing(ctx, item); err != nil {
		return nil, err
	}
	return item, nil
}

func computeProofHash(drawingNo, fileHash, reviewCertRef, prevProofHash string, createdAt time.Time) string {
	h := sha256.New()
	h.Write([]byte(drawingNo))
	h.Write([]byte(fileHash))
	h.Write([]byte(reviewCertRef))
	h.Write([]byte(prevProofHash))
	h.Write([]byte(createdAt.UTC().Format(time.RFC3339Nano)))
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Service) Current(ctx context.Context, drawingNo string) (*DrawingVersion, error) {
	drawingNo = strings.TrimSpace(drawingNo)
	if drawingNo == "" {
		return nil, fmt.Errorf("drawing_no is required")
	}
	return s.store.GetCurrentDrawing(ctx, s.tenantID, drawingNo)
}

func (s *Service) Chain(ctx context.Context, drawingNo string) ([]*DrawingVersion, error) {
	drawingNo = strings.TrimSpace(drawingNo)
	if drawingNo == "" {
		return nil, fmt.Errorf("drawing_no is required")
	}
	return s.store.GetDrawingChain(ctx, s.tenantID, drawingNo)
}

func (s *Service) ListByProject(ctx context.Context, projectRef string) ([]*DrawingVersion, error) {
	projectRef = strings.TrimSpace(projectRef)
	if projectRef == "" {
		return nil, fmt.Errorf("project_ref is required")
	}
	return s.store.ListProjectDrawings(ctx, s.tenantID, projectRef)
}

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) CreateReviewCert(ctx context.Context, item *ReviewCertificate) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO review_certificates (
			cert_ref, project_ref, drawing_no, executor_ref, payload, tenant_id, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7)
		RETURNING id
	`,
		item.CertRef,
		item.ProjectRef,
		item.DrawingNo,
		item.ExecutorRef,
		item.Payload,
		item.TenantID,
		item.CreatedAt,
	).Scan(&item.ID)
}

func (s *PGStore) GetLatestReviewCert(ctx context.Context, tenantID int, projectRef, drawingNo string) (*ReviewCertificate, error) {
	item := &ReviewCertificate{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id, cert_ref, project_ref, drawing_no, executor_ref, payload, tenant_id, created_at
		FROM review_certificates
		WHERE tenant_id=$1 AND project_ref=$2 AND drawing_no=$3
		ORDER BY id DESC
		LIMIT 1
	`, tenantID, projectRef, drawingNo).Scan(
		&item.ID,
		&item.CertRef,
		&item.ProjectRef,
		&item.DrawingNo,
		&item.ExecutorRef,
		&item.Payload,
		&item.TenantID,
		&item.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *PGStore) GetReviewCertByRef(ctx context.Context, tenantID int, certRef string) (*ReviewCertificate, error) {
	item := &ReviewCertificate{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id, cert_ref, project_ref, drawing_no, executor_ref, payload, tenant_id, created_at
		FROM review_certificates
		WHERE tenant_id=$1 AND cert_ref=$2
		LIMIT 1
	`, tenantID, certRef).Scan(
		&item.ID,
		&item.CertRef,
		&item.ProjectRef,
		&item.DrawingNo,
		&item.ExecutorRef,
		&item.Payload,
		&item.TenantID,
		&item.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *PGStore) PublishDrawing(ctx context.Context, item *DrawingVersion) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.ExecContext(ctx, `
		UPDATE drawing_versions
		SET status='SUPERSEDED', updated_at=$3
		WHERE tenant_id=$1 AND drawing_no=$2 AND status='CURRENT'
	`, item.TenantID, item.DrawingNo, time.Now().UTC()); err != nil {
		return err
	}

	var nextVersion int
	if err = tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(version_no),0)+1
		FROM drawing_versions
		WHERE tenant_id=$1 AND drawing_no=$2
	`, item.TenantID, item.DrawingNo).Scan(&nextVersion); err != nil {
		return err
	}
	item.VersionNo = nextVersion

	if err = tx.QueryRowContext(ctx, `
		INSERT INTO drawing_versions (
			drawing_no, version_no, project_ref, review_cert_ref,
			file_hash, proof_hash, publisher_ref, status, payload, tenant_id, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,'CURRENT',$8,$9,$10,$11)
		RETURNING id, status
	`,
		item.DrawingNo,
		item.VersionNo,
		item.ProjectRef,
		item.ReviewCertRef,
		item.FileHash,
		item.ProofHash,
		item.PublisherRef,
		item.Payload,
		item.TenantID,
		item.CreatedAt,
		item.UpdatedAt,
	).Scan(&item.ID, &item.Status); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (s *PGStore) GetCurrentDrawing(ctx context.Context, tenantID int, drawingNo string) (*DrawingVersion, error) {
	item := &DrawingVersion{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id, drawing_no, version_no, project_ref, review_cert_ref, file_hash,
		       COALESCE(proof_hash, ''), publisher_ref, status, payload, tenant_id, created_at, updated_at
		FROM drawing_versions
		WHERE tenant_id=$1 AND drawing_no=$2 AND status='CURRENT'
		ORDER BY version_no DESC
		LIMIT 1
	`, tenantID, drawingNo).Scan(
		&item.ID,
		&item.DrawingNo,
		&item.VersionNo,
		&item.ProjectRef,
		&item.ReviewCertRef,
		&item.FileHash,
		&item.ProofHash,
		&item.PublisherRef,
		&item.Status,
		&item.Payload,
		&item.TenantID,
		&item.CreatedAt,
		&item.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *PGStore) GetDrawingChain(ctx context.Context, tenantID int, drawingNo string) ([]*DrawingVersion, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, drawing_no, version_no, project_ref, review_cert_ref, file_hash,
		       COALESCE(proof_hash, ''), publisher_ref, status, payload, tenant_id, created_at, updated_at
		FROM drawing_versions
		WHERE tenant_id=$1 AND drawing_no=$2
		ORDER BY version_no DESC
	`, tenantID, drawingNo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]*DrawingVersion, 0)
	for rows.Next() {
		item := &DrawingVersion{}
		if err := rows.Scan(
			&item.ID,
			&item.DrawingNo,
			&item.VersionNo,
			&item.ProjectRef,
			&item.ReviewCertRef,
			&item.FileHash,
			&item.ProofHash,
			&item.PublisherRef,
			&item.Status,
			&item.Payload,
			&item.TenantID,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *PGStore) ListProjectDrawings(ctx context.Context, tenantID int, projectRef string) ([]*DrawingVersion, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, drawing_no, version_no, project_ref, review_cert_ref, file_hash,
		       COALESCE(proof_hash, ''), publisher_ref, status, payload, tenant_id, created_at, updated_at
		FROM drawing_versions
		WHERE tenant_id=$1 AND project_ref=$2
		ORDER BY drawing_no ASC, version_no DESC
	`, tenantID, projectRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]*DrawingVersion, 0)
	for rows.Next() {
		item := &DrawingVersion{}
		if err := rows.Scan(
			&item.ID,
			&item.DrawingNo,
			&item.VersionNo,
			&item.ProjectRef,
			&item.ReviewCertRef,
			&item.FileHash,
			&item.ProofHash,
			&item.PublisherRef,
			&item.Status,
			&item.Payload,
			&item.TenantID,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *PGStore) GetPreviousProofHash(ctx context.Context, tenantID int, drawingNo string) (string, error) {
	var proofHash string
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(proof_hash, '')
		FROM drawing_versions
		WHERE tenant_id=$1 AND drawing_no=$2 AND status='CURRENT'
		ORDER BY version_no DESC
		LIMIT 1
	`, tenantID, drawingNo).Scan(&proofHash)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return proofHash, err
}
