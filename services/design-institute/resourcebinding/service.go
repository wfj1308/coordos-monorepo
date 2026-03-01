package resourcebinding

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type Status string

const (
	StatusActive   Status = "ACTIVE"
	StatusReleased Status = "RELEASED"
)

type Binding struct {
	ID           int64      `json:"id"`
	ResourceRef  string     `json:"resource_ref"`
	ResourceType string     `json:"resource_type"`
	ProjectRef   string     `json:"project_ref"`
	ExecutorRef  string     `json:"executor_ref"`
	SPURef       string     `json:"spu_ref"`
	Status       Status     `json:"status"`
	Note         string     `json:"note"`
	TenantID     int        `json:"tenant_id"`
	BoundAt      time.Time  `json:"bound_at"`
	ReleasedAt   *time.Time `json:"released_at,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

type CreateInput struct {
	ResourceRef  string `json:"resource_ref"`
	ResourceType string `json:"resource_type"`
	ProjectRef   string `json:"project_ref"`
	ExecutorRef  string `json:"executor_ref"`
	SPURef       string `json:"spu_ref"`
	Note         string `json:"note"`
}

type Filter struct {
	ResourceRef *string
	ProjectRef  *string
	ExecutorRef *string
	Status      *Status
	Limit       int
	Offset      int
}

type Store interface {
	Create(ctx context.Context, item *Binding) error
	List(ctx context.Context, tenantID int, f Filter) ([]*Binding, int, error)
	Release(ctx context.Context, tenantID int, id int64, releasedAt, updatedAt time.Time) error
}

type Service struct {
	store    Store
	tenantID int
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

func (s *Service) Create(ctx context.Context, in CreateInput) (*Binding, error) {
	resourceRef := strings.TrimSpace(in.ResourceRef)
	projectRef := strings.TrimSpace(in.ProjectRef)
	if resourceRef == "" || projectRef == "" {
		return nil, fmt.Errorf("resource_ref and project_ref are required")
	}
	resourceType := strings.ToUpper(strings.TrimSpace(in.ResourceType))
	if resourceType == "" {
		resourceType = "GENERIC"
	}
	now := time.Now().UTC()
	item := &Binding{
		ResourceRef:  resourceRef,
		ResourceType: resourceType,
		ProjectRef:   projectRef,
		ExecutorRef:  strings.TrimSpace(in.ExecutorRef),
		SPURef:       strings.TrimSpace(in.SPURef),
		Status:       StatusActive,
		Note:         strings.TrimSpace(in.Note),
		TenantID:     s.tenantID,
		BoundAt:      now,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := s.store.Create(ctx, item); err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Service) List(ctx context.Context, f Filter) ([]*Binding, int, error) {
	if f.Limit <= 0 {
		f.Limit = 20
	}
	if f.Offset < 0 {
		f.Offset = 0
	}
	return s.store.List(ctx, s.tenantID, f)
}

func (s *Service) Release(ctx context.Context, id int64) error {
	if id <= 0 {
		return fmt.Errorf("invalid id")
	}
	now := time.Now().UTC()
	return s.store.Release(ctx, s.tenantID, id, now, now)
}

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) Create(ctx context.Context, item *Binding) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO resource_bindings (
			resource_ref, resource_type, project_ref, executor_ref, spu_ref,
			status, note, tenant_id, bound_at, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		RETURNING id
	`,
		item.ResourceRef,
		item.ResourceType,
		item.ProjectRef,
		item.ExecutorRef,
		item.SPURef,
		item.Status,
		item.Note,
		item.TenantID,
		item.BoundAt,
		item.CreatedAt,
		item.UpdatedAt,
	).Scan(&item.ID)
}

func (s *PGStore) List(ctx context.Context, tenantID int, f Filter) ([]*Binding, int, error) {
	where := []string{"tenant_id=$1"}
	args := []any{tenantID}
	pos := 2
	if f.ResourceRef != nil && strings.TrimSpace(*f.ResourceRef) != "" {
		where = append(where, fmt.Sprintf("resource_ref=$%d", pos))
		args = append(args, strings.TrimSpace(*f.ResourceRef))
		pos++
	}
	if f.ProjectRef != nil && strings.TrimSpace(*f.ProjectRef) != "" {
		where = append(where, fmt.Sprintf("project_ref=$%d", pos))
		args = append(args, strings.TrimSpace(*f.ProjectRef))
		pos++
	}
	if f.ExecutorRef != nil && strings.TrimSpace(*f.ExecutorRef) != "" {
		where = append(where, fmt.Sprintf("executor_ref=$%d", pos))
		args = append(args, strings.TrimSpace(*f.ExecutorRef))
		pos++
	}
	if f.Status != nil {
		where = append(where, fmt.Sprintf("status=$%d", pos))
		args = append(args, *f.Status)
		pos++
	}
	whereSQL := strings.Join(where, " AND ")

	var total int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM resource_bindings WHERE "+whereSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	listSQL := fmt.Sprintf(`
		SELECT id, resource_ref, resource_type, project_ref, executor_ref, spu_ref, status,
		       note, tenant_id, bound_at, released_at, created_at, updated_at
		FROM resource_bindings
		WHERE %s
		ORDER BY id DESC
		LIMIT $%d OFFSET $%d
	`, whereSQL, pos, pos+1)
	args = append(args, f.Limit, f.Offset)
	rows, err := s.db.QueryContext(ctx, listSQL, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	out := make([]*Binding, 0)
	for rows.Next() {
		item := &Binding{}
		if err := rows.Scan(
			&item.ID,
			&item.ResourceRef,
			&item.ResourceType,
			&item.ProjectRef,
			&item.ExecutorRef,
			&item.SPURef,
			&item.Status,
			&item.Note,
			&item.TenantID,
			&item.BoundAt,
			&item.ReleasedAt,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, 0, err
		}
		out = append(out, item)
	}
	return out, total, rows.Err()
}

func (s *PGStore) Release(ctx context.Context, tenantID int, id int64, releasedAt, updatedAt time.Time) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE resource_bindings
		SET status='RELEASED', released_at=$1, updated_at=$2
		WHERE tenant_id=$3 AND id=$4 AND status='ACTIVE'
	`, releasedAt, updatedAt, tenantID, id)
	if err != nil {
		return err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if aff == 0 {
		return sql.ErrNoRows
	}
	return nil
}
