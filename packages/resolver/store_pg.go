package resolver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// ── Store 接口 ────────────────────────────────────────────────

type Store interface {
	// 证书查询
	GetCredentials(ctx context.Context, holderRef string, validOn time.Time) ([]*Credential, error)
	GetCredentialsByType(ctx context.Context, tenantID int, certType CertType, validOn time.Time) ([]*Credential, error)
	CreateCredential(ctx context.Context, c *Credential) (int64, error)
	RevokeCredential(ctx context.Context, id int64, reason string) error

	// 执行体活跃项目查询（用于 Occupied 计算）
	GetActiveProjects(ctx context.Context, executorRef string) ([]OccupiedProject, error)

	// 执行体基本信息（名字等，从 employee 表读取）
	GetExecutorName(ctx context.Context, executorRef string) (string, error)

	// 项目执行体列表（用于 Resolve）
	ListExecutorsByTenant(ctx context.Context, tenantID int) ([]string, error)
}

// ── PostgreSQL 实现 ───────────────────────────────────────────

type PGStore struct {
	db *sql.DB
}

func NewPGStore(db *sql.DB) Store {
	return &PGStore{db: db}
}

// GetCredentials 查某个执行体在指定时间点的有效证书
func (s *PGStore) GetCredentials(ctx context.Context, holderRef string, validOn time.Time) ([]*Credential, error) {
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
		return nil, fmt.Errorf("查询证书失败: %w", err)
	}
	defer rows.Close()
	return s.scanCredentials(rows)
}

// GetCredentialsByType 按证书类型查全租户的有效持证人
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
		return nil, fmt.Errorf("按类型查询证书失败: %w", err)
	}
	defer rows.Close()
	return s.scanCredentials(rows)
}

// CreateCredential 录入证书
func (s *PGStore) CreateCredential(ctx context.Context, c *Credential) (int64, error) {
	scope := strings.Join(c.Scope, ",")
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
		return 0, fmt.Errorf("创建证书失败: %w", err)
	}
	return id, nil
}

// RevokeCredential 吊销证书
func (s *PGStore) RevokeCredential(ctx context.Context, id int64, reason string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE credentials
		SET status='REVOKED', updated_at=NOW()
		WHERE id=$1
	`, id)
	return err
}

// GetActiveProjects 查执行体当前在建项目（从 project_nodes + achievement_utxos 联查）
func (s *PGStore) GetActiveProjects(ctx context.Context, executorRef string) ([]OccupiedProject, error) {
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
		return nil, fmt.Errorf("查询在建项目失败: %w", err)
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

// GetExecutorName 从 employees 表读取显示名
func (s *PGStore) GetExecutorName(ctx context.Context, executorRef string) (string, error) {
	var name string
	err := s.db.QueryRowContext(ctx, `
		SELECT name FROM employees WHERE executor_ref = $1 LIMIT 1
	`, executorRef).Scan(&name)
	if err == sql.ErrNoRows {
		return executorRef, nil // 找不到就直接返回 ref
	}
	return name, err
}

// ListExecutorsByTenant 列出租户内所有有效 executor_ref（用于 Resolve 遍历）
func (s *PGStore) ListExecutorsByTenant(ctx context.Context, tenantID int) ([]string, error) {
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

	var refs []string
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

// scanCredentials 把 sql.Rows 转成 []*Credential
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
			c.Scope = strings.Split(scopeStr, ",")
		}
		result = append(result, c)
	}
	return result, rows.Err()
}
