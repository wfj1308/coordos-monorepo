package anchor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// UnanchoredItem 存储一个需要被锚定的资源信息
type UnanchoredItem struct {
	Ref       string
	ProofHash string
	TenantID  int
}

// Repository 封装了与数据库中 proof_anchors 表的交互逻辑
type Repository struct {
	db *sql.DB
}

// NewRepository 创建一个新的 Repository 实例
func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

// FindUnanchoredAchievementHashes 从 achievement_utxos 表中查找尚未被锚定的 proof_hash
// 它通过 LEFT JOIN proof_anchors 并检查 pa.id IS NULL 来实现
func (r *Repository) FindUnanchoredAchievementHashes(ctx context.Context, limit int) ([]UnanchoredItem, error) {
	query := `
		SELECT a.ref, a.proof_hash, a.tenant_id
		FROM achievement_utxos a
		LEFT JOIN proof_anchors pa ON a.proof_hash = pa.proof_hash
		WHERE a.proof_hash IS NOT NULL AND a.proof_hash != '' AND pa.id IS NULL
		LIMIT $1;
	`
	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query unanchored achievement hashes: %w", err)
	}
	defer rows.Close()

	var items []UnanchoredItem
	for rows.Next() {
		var item UnanchoredItem
		if err := rows.Scan(&item.Ref, &item.ProofHash, &item.TenantID); err != nil {
			return nil, fmt.Errorf("failed to scan unanchored item: %w", err)
		}
		items = append(items, item)
	}

	return items, rows.Err()
}

// CreatePendingAnchors 批量将待锚定的 proof_hash 插入到 proof_anchors 表中
// 使用 ON CONFLICT DO NOTHING 避免重复插入
func (r *Repository) CreatePendingAnchors(ctx context.Context, items []UnanchoredItem) (int64, error) {
	if len(items) == 0 {
		return 0, nil
	}

	var (
		valueStrings []string
		valueArgs    []interface{}
	)

	for i, item := range items {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3))
		valueArgs = append(valueArgs, item.ProofHash)
		valueArgs = append(valueArgs, item.Ref)
		valueArgs = append(valueArgs, item.TenantID)
	}

	stmt := fmt.Sprintf(`
		INSERT INTO proof_anchors (proof_hash, ref, tenant_id, status)
		VALUES %s
		ON CONFLICT (proof_hash) DO NOTHING
	`, strings.Join(valueStrings, ","))

	res, err := r.db.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return 0, fmt.Errorf("failed to insert pending anchors: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected, nil
}