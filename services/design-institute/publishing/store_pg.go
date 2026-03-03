//go:build archive
// +build archive

package publishing

import (
	"context"
	"database/sql"
	"fmt"
)

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

// ── DrawingVersion CRUD ───────────────────────────────────────

func (s *PGStore) CreateDrawingVersion(ctx context.Context, d *DrawingVersion) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO drawing_versions
		  (drawing_no, version, title, major, project_ref, spu_ref,
		   executor_ref, file_hash, file_url, status,
		   review_cert_ref, reviewer_ref, chief_eng_ref, reviewed_at,
		   published_at, published_by, supersedes_id,
		   utxo_ref, proof_hash, tenant_id, created_at, updated_at)
		VALUES
		  ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
		   $15,$16,$17,$18,$19,$20,NOW(),NOW())
		RETURNING id
	`,
		d.DrawingNo, d.Version, d.Title, d.Major, d.ProjectRef, d.SPURef,
		d.ExecutorRef, d.FileHash, d.FileURL, string(d.Status),
		d.ReviewCertRef, d.ReviewerRef, d.ChiefEngRef, d.ReviewedAt,
		d.PublishedAt, d.PublishedBy, d.SupersedesID,
		d.UTXORef, d.ProofHash, d.TenantID,
	).Scan(&id)
	return id, err
}

func (s *PGStore) GetDrawingVersion(ctx context.Context, id int64) (*DrawingVersion, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT "+dvCols+" FROM drawing_versions WHERE id=$1", id)
	return scanDV(row)
}

func (s *PGStore) GetDrawingByNo(ctx context.Context, drawingNo, version string) (*DrawingVersion, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT "+dvCols+" FROM drawing_versions WHERE drawing_no=$1 AND version=$2",
		drawingNo, version)
	return scanDV(row)
}

func (s *PGStore) GetCurrentVersion(ctx context.Context, drawingNo string) (*DrawingVersion, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT "+dvCols+
			" FROM drawing_versions WHERE drawing_no=$1 AND status='PUBLISHED' LIMIT 1",
		drawingNo)
	return scanDV(row)
}

func (s *PGStore) SupersedeVersion(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE drawing_versions SET status='SUPERSEDED', updated_at=NOW() WHERE id=$1`, id)
	return err
}

func (s *PGStore) GetVersionChain(ctx context.Context, drawingNo string) ([]*DrawingVersion, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+dvCols+
			" FROM drawing_versions WHERE drawing_no=$1 ORDER BY created_at ASC",
		drawingNo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDVs(rows)
}

func (s *PGStore) GetProjectDrawings(ctx context.Context, projectRef string) ([]*DrawingVersion, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+dvCols+
			" FROM drawing_versions WHERE project_ref=$1 AND status='PUBLISHED' ORDER BY drawing_no",
		projectRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDVs(rows)
}

func (s *PGStore) SetUTXORef(ctx context.Context, id int64, utxoRef, proofHash string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE drawing_versions SET utxo_ref=$1, proof_hash=$2, updated_at=NOW() WHERE id=$3`,
		utxoRef, proofHash, id)
	return err
}

func (s *PGStore) SetStatus(ctx context.Context, id int64, status DrawingStatus) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE drawing_versions SET status=$1, updated_at=NOW() WHERE id=$2`,
		string(status), id)
	return err
}

// ── ReviewCert CRUD ───────────────────────────────────────────

func (s *PGStore) CreateReviewCert(ctx context.Context, c *ReviewCert) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO review_certs
		  (cert_no, drawing_version_id, drawing_no, project_ref,
		   reviewer_ref, chief_eng_ref, issue_count, major_count,
		   resolved_count, resolution_rate, status, valid_until,
		   proof_hash, tenant_id, issued_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		RETURNING id
	`,
		c.CertNo, c.DrawingVersionID, c.DrawingNo, c.ProjectRef,
		c.ReviewerRef, c.ChiefEngRef, c.IssueCount, c.MajorCount,
		c.ResolvedCount, c.ResolutionRate, string(c.Status), c.ValidUntil,
		c.ProofHash, c.TenantID, c.IssuedAt,
	).Scan(&id)
	return id, err
}

func (s *PGStore) GetReviewCert(ctx context.Context, id int64) (*ReviewCert, error) {
	c := &ReviewCert{}
	var status string
	err := s.db.QueryRowContext(ctx, `
		SELECT id, cert_no, drawing_version_id, drawing_no, project_ref,
		       reviewer_ref, chief_eng_ref, issue_count, major_count,
		       resolved_count, resolution_rate, status, valid_until,
		       COALESCE(utxo_ref,''), proof_hash, tenant_id, issued_at
		FROM review_certs WHERE id=$1
	`, id).Scan(
		&c.ID, &c.CertNo, &c.DrawingVersionID, &c.DrawingNo, &c.ProjectRef,
		&c.ReviewerRef, &c.ChiefEngRef, &c.IssueCount, &c.MajorCount,
		&c.ResolvedCount, &c.ResolutionRate, &status, &c.ValidUntil,
		&c.UTXORef, &c.ProofHash, &c.TenantID, &c.IssuedAt,
	)
	if err != nil {
		return nil, err
	}
	c.Status = ReviewStatus(status)
	return c, nil
}

func (s *PGStore) GetCertByDrawingVersion(ctx context.Context, dvID int64) (*ReviewCert, error) {
	c := &ReviewCert{}
	var status string
	err := s.db.QueryRowContext(ctx, `
		SELECT id, cert_no, drawing_version_id, drawing_no, project_ref,
		       reviewer_ref, chief_eng_ref, issue_count, major_count,
		       resolved_count, resolution_rate, status, valid_until,
		       COALESCE(utxo_ref,''), proof_hash, tenant_id, issued_at
		FROM review_certs WHERE drawing_version_id=$1
		ORDER BY issued_at DESC LIMIT 1
	`, dvID).Scan(
		&c.ID, &c.CertNo, &c.DrawingVersionID, &c.DrawingNo, &c.ProjectRef,
		&c.ReviewerRef, &c.ChiefEngRef, &c.IssueCount, &c.MajorCount,
		&c.ResolvedCount, &c.ResolutionRate, &status, &c.ValidUntil,
		&c.UTXORef, &c.ProofHash, &c.TenantID, &c.IssuedAt,
	)
	if err != nil {
		return nil, err
	}
	c.Status = ReviewStatus(status)
	return c, nil
}

// ── 扫描工具 ──────────────────────────────────────────────────

const dvCols = `
	id, drawing_no, version, title, major, project_ref, spu_ref,
	executor_ref, file_hash, COALESCE(file_url,''), status,
	COALESCE(review_cert_ref,''), COALESCE(reviewer_ref,''),
	COALESCE(chief_eng_ref,''), reviewed_at, published_at,
	COALESCE(published_by,''), supersedes_id,
	COALESCE(utxo_ref,''), proof_hash, tenant_id, created_at, updated_at`

func scanDV(row *sql.Row) (*DrawingVersion, error) {
	d := &DrawingVersion{}
	var status string
	err := row.Scan(
		&d.ID, &d.DrawingNo, &d.Version, &d.Title, &d.Major,
		&d.ProjectRef, &d.SPURef, &d.ExecutorRef, &d.FileHash, &d.FileURL,
		&status, &d.ReviewCertRef, &d.ReviewerRef, &d.ChiefEngRef,
		&d.ReviewedAt, &d.PublishedAt, &d.PublishedBy, &d.SupersedesID,
		&d.UTXORef, &d.ProofHash, &d.TenantID, &d.CreatedAt, &d.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("图纸版本不存在")
		}
		return nil, err
	}
	d.Status = DrawingStatus(status)
	return d, nil
}

func scanDVs(rows *sql.Rows) ([]*DrawingVersion, error) {
	var list []*DrawingVersion
	for rows.Next() {
		d := &DrawingVersion{}
		var status string
		if err := rows.Scan(
			&d.ID, &d.DrawingNo, &d.Version, &d.Title, &d.Major,
			&d.ProjectRef, &d.SPURef, &d.ExecutorRef, &d.FileHash, &d.FileURL,
			&status, &d.ReviewCertRef, &d.ReviewerRef, &d.ChiefEngRef,
			&d.ReviewedAt, &d.PublishedAt, &d.PublishedBy, &d.SupersedesID,
			&d.UTXORef, &d.ProofHash, &d.TenantID, &d.CreatedAt, &d.UpdatedAt,
		); err != nil {
			continue
		}
		d.Status = DrawingStatus(status)
		list = append(list, d)
	}
	return list, rows.Err()
}
