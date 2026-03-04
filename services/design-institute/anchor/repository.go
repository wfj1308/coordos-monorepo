package anchor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// UnanchoredItem represents one resource hash waiting for L0 anchor.
type UnanchoredItem struct {
	Ref       string
	ProofHash string
	TenantID  int
}

// Repository provides database access for proof anchor queue.
type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) FindUnanchoredAchievementHashes(ctx context.Context, limit int) ([]UnanchoredItem, error) {
	query := `
		SELECT COALESCE(NULLIF(a.ref,''), NULLIF(a.utxo_ref,''), '') AS ref, a.proof_hash, a.tenant_id
		FROM achievement_utxos a
		LEFT JOIN proof_anchors pa ON a.proof_hash = pa.proof_hash
		WHERE a.proof_hash IS NOT NULL
		  AND a.proof_hash <> ''
		  AND pa.id IS NULL
		  AND (COALESCE(NULLIF(a.ref,''), NULLIF(a.utxo_ref,''), '') <> '')
		LIMIT $1
	`
	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("query unanchored achievement hashes failed: %w", err)
	}
	defer rows.Close()

	items := make([]UnanchoredItem, 0, limit)
	for rows.Next() {
		var it UnanchoredItem
		if err := rows.Scan(&it.Ref, &it.ProofHash, &it.TenantID); err != nil {
			return nil, fmt.Errorf("scan unanchored achievement hash failed: %w", err)
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unanchored achievement hashes failed: %w", err)
	}
	return items, nil
}

func (r *Repository) CreatePendingAnchors(ctx context.Context, items []UnanchoredItem) (int64, error) {
	if len(items) == 0 {
		return 0, nil
	}

	valueStrings := make([]string, 0, len(items))
	valueArgs := make([]any, 0, len(items)*3)
	for i, it := range items {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,'PENDING')", i*3+1, i*3+2, i*3+3))
		valueArgs = append(valueArgs, it.ProofHash, it.Ref, it.TenantID)
	}

	stmt := fmt.Sprintf(`
		INSERT INTO proof_anchors (proof_hash, ref, tenant_id, status)
		VALUES %s
		ON CONFLICT (proof_hash) DO NOTHING
	`, strings.Join(valueStrings, ","))
	res, err := r.db.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return 0, fmt.Errorf("insert pending anchors failed: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read pending anchor affected rows failed: %w", err)
	}
	return affected, nil
}
