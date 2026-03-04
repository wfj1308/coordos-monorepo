package resolver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"coordos/vuri"
	_ "github.com/lib/pq"
)

// Store defines resolver data access contract.
type Store interface {
	GetCredentials(ctx context.Context, holderRef vuri.VRef, validOn time.Time) ([]*Credential, error)
	GetCredentialsByType(ctx context.Context, tenantID int, certType CertType, validOn time.Time) ([]*Credential, error)
	CreateCredential(ctx context.Context, c *Credential) (int64, error)
	RevokeCredential(ctx context.Context, id int64, reason string) error
	GetActiveProjects(ctx context.Context, executorRef vuri.VRef) ([]OccupiedProject, error)
	GetExecutorName(ctx context.Context, executorRef vuri.VRef) (string, error)
	ListExecutorsByTenant(ctx context.Context, tenantID int) ([]vuri.VRef, error)
}

// PGStore is the PostgreSQL store implementation.
type PGStore struct {
	db *sql.DB
}

func NewPGStore(db *sql.DB) Store {
	return &PGStore{db: db}
}

func (s *PGStore) GetCredentials(ctx context.Context, holderRef vuri.VRef, validOn time.Time) ([]*Credential, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, holder_ref, holder_type, cert_type, cert_number,
		       issued_at, expires_at, scope, status, tenant_id, created_at, updated_at
		FROM credentials
		WHERE holder_ref = $1
		  AND status = 'ACTIVE'
		  AND (expires_at IS NULL OR expires_at > $2)
		ORDER BY cert_type, expires_at DESC NULLS LAST
	`, holderRef, validOn)
	if err != nil {
		return nil, fmt.Errorf("query credentials failed: %w", err)
	}
	defer rows.Close()
	return s.scanCredentials(rows)
}

func (s *PGStore) GetCredentialsByType(ctx context.Context, tenantID int, certType CertType, validOn time.Time) ([]*Credential, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, holder_ref, holder_type, cert_type, cert_number,
		       issued_at, expires_at, scope, status, tenant_id, created_at, updated_at
		FROM credentials
		WHERE tenant_id = $1
		  AND cert_type = $2
		  AND status = 'ACTIVE'
		  AND (expires_at IS NULL OR expires_at > $3)
		ORDER BY holder_ref
	`, tenantID, string(certType), validOn)
	if err != nil {
		return nil, fmt.Errorf("query credentials by type failed: %w", err)
	}
	defer rows.Close()
	return s.scanCredentials(rows)
}

func (s *PGStore) CreateCredential(ctx context.Context, c *Credential) (int64, error) {
	scope := joinVRefs(c.Scope)
	var id int64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO credentials
		  (holder_ref, holder_type, cert_type, cert_number,
		   issued_at, expires_at, scope, status, tenant_id, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW(),NOW())
		RETURNING id
	`,
		c.HolderRef, string(c.HolderType), string(c.CertType), c.CertNumber,
		c.IssuedAt, c.ExpiresAt, scope, c.Status, c.TenantID,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("create credential failed: %w", err)
	}
	return id, nil
}

func (s *PGStore) RevokeCredential(ctx context.Context, id int64, reason string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE credentials
		SET status='REVOKED', updated_at=NOW()
		WHERE id=$1
	`, id)
	return err
}

func (s *PGStore) GetActiveProjects(ctx context.Context, executorRef vuri.VRef) ([]OccupiedProject, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT
		       p.ref,
		       p.name,
		       CASE WHEN p.executor_ref = $1 THEN 'EXECUTOR'
		            ELSE 'REVIEWER' END AS role,
		       p.created_at
		FROM project_nodes p
		WHERE p.status IN ('INITIATED','CONTRACTED','IN_PROGRESS')
		  AND (
		    p.executor_ref = $1
		    OR EXISTS (
		      SELECT 1 FROM achievement_utxos a
		      WHERE a.project_ref = p.ref
		        AND a.executor_ref = $1
		        AND a.status = 'PENDING'
		    )
		  )
		ORDER BY p.created_at DESC
	`, executorRef)
	if err != nil {
		return nil, fmt.Errorf("query active projects failed: %w", err)
	}
	defer rows.Close()

	var result []OccupiedProject
	for rows.Next() {
		var op OccupiedProject
		if err := rows.Scan(&op.ProjectRef, &op.ProjectName, &op.Role, &op.Since); err != nil {
			return nil, err
		}
		result = append(result, op)
	}
	return result, rows.Err()
}

func (s *PGStore) GetExecutorName(ctx context.Context, executorRef vuri.VRef) (string, error) {
	var name string
	err := s.db.QueryRowContext(ctx, `
		SELECT name FROM employees WHERE executor_ref = $1 LIMIT 1
	`, executorRef).Scan(&name)
	if err == sql.ErrNoRows {
		return string(executorRef), nil
	}
	return name, err
}

func (s *PGStore) ListExecutorsByTenant(ctx context.Context, tenantID int) ([]vuri.VRef, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT executor_ref
		FROM employees
		WHERE tenant_id = $1
		  AND executor_ref IS NOT NULL
		  AND end_date IS NULL
		ORDER BY executor_ref
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var refs []vuri.VRef
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			return nil, err
		}
		refs = append(refs, vuri.VRef(ref))
	}
	return refs, rows.Err()
}

func (s *PGStore) scanCredentials(rows *sql.Rows) ([]*Credential, error) {
	var result []*Credential
	for rows.Next() {
		c := &Credential{}
		var scopeStr string
		if err := rows.Scan(
			&c.ID, &c.HolderRef, &c.HolderType, &c.CertType, &c.CertNumber,
			&c.IssuedAt, &c.ExpiresAt, &scopeStr, &c.Status, &c.TenantID,
			&c.CreatedAt, &c.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if scopeStr != "" {
			c.Scope = splitVRefs(scopeStr)
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func joinVRefs(refs []vuri.VRef) string {
	if len(refs) == 0 {
		return ""
	}
	parts := make([]string, 0, len(refs))
	for _, ref := range refs {
		parts = append(parts, string(ref))
	}
	return strings.Join(parts, ",")
}

func splitVRefs(value string) []vuri.VRef {
	parts := strings.Split(value, ",")
	refs := make([]vuri.VRef, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		refs = append(refs, vuri.VRef(part))
	}
	return refs
}
