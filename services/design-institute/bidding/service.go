package bidding

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Status string

const (
	StatusDraft     Status = "DRAFT"
	StatusPublished Status = "PUBLISHED"
	StatusArchived  Status = "ARCHIVED"
)

type BidProfile struct {
	ID             int64           `json:"id"`
	Ref            string          `json:"ref"`
	Name           string          `json:"name"`
	ProjectRef     string          `json:"project_ref"`
	SPURef         string          `json:"spu_ref"`
	ProfileIDs     json.RawMessage `json:"profile_ids"`
	Requirements   json.RawMessage `json:"requirements"`
	PackagePayload json.RawMessage `json:"package_payload"`
	Status         Status          `json:"status"`
	TenantID       int             `json:"tenant_id"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type CreateInput struct {
	Name           string          `json:"name"`
	ProjectRef     string          `json:"project_ref"`
	SPURef         string          `json:"spu_ref"`
	ProfileIDs     json.RawMessage `json:"profile_ids"`
	Requirements   json.RawMessage `json:"requirements"`
	PackagePayload json.RawMessage `json:"package_payload"`
}

type Filter struct {
	ProjectRef *string
	Status     *Status
	Keyword    string
	Limit      int
	Offset     int
}

type Store interface {
	Create(ctx context.Context, p *BidProfile) error
	Get(ctx context.Context, tenantID int, id int64) (*BidProfile, error)
	List(ctx context.Context, tenantID int, f Filter) ([]*BidProfile, int, error)
	UpdateStatus(ctx context.Context, tenantID int, id int64, status Status, updatedAt time.Time) error
}

type Service struct {
	store    Store
	tenantID int
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

func (s *Service) Create(ctx context.Context, in CreateInput) (*BidProfile, error) {
	name := strings.TrimSpace(in.Name)
	projectRef := strings.TrimSpace(in.ProjectRef)
	spuRef := strings.TrimSpace(in.SPURef)
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if projectRef == "" {
		return nil, fmt.Errorf("project_ref is required")
	}
	if spuRef == "" {
		spuRef = "v://zhongbei/spu/bidding/bid_profile@v1"
	}
	profileIDs := in.ProfileIDs
	if len(profileIDs) == 0 {
		profileIDs = json.RawMessage("[]")
	}
	requirements := in.Requirements
	if len(requirements) == 0 {
		requirements = json.RawMessage("{}")
	}
	packagePayload := in.PackagePayload
	if len(packagePayload) == 0 {
		packagePayload = json.RawMessage("{}")
	}

	now := time.Now().UTC()
	item := &BidProfile{
		Ref:            fmt.Sprintf("v://%d/bidding/profile/%d", s.tenantID, now.UnixNano()),
		Name:           name,
		ProjectRef:     projectRef,
		SPURef:         spuRef,
		ProfileIDs:     profileIDs,
		Requirements:   requirements,
		PackagePayload: packagePayload,
		Status:         StatusDraft,
		TenantID:       s.tenantID,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := s.store.Create(ctx, item); err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Service) Get(ctx context.Context, id int64) (*BidProfile, error) {
	if id <= 0 {
		return nil, fmt.Errorf("invalid id")
	}
	return s.store.Get(ctx, s.tenantID, id)
}

func (s *Service) List(ctx context.Context, f Filter) ([]*BidProfile, int, error) {
	if f.Limit <= 0 {
		f.Limit = 20
	}
	if f.Offset < 0 {
		f.Offset = 0
	}
	return s.store.List(ctx, s.tenantID, f)
}

func (s *Service) Publish(ctx context.Context, id int64) error {
	if id <= 0 {
		return fmt.Errorf("invalid id")
	}
	return s.store.UpdateStatus(ctx, s.tenantID, id, StatusPublished, time.Now().UTC())
}

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) Create(ctx context.Context, p *BidProfile) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO bid_profiles (
			ref, name, project_ref, spu_ref, profile_ids, requirements, package_payload,
			status, tenant_id, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		RETURNING id
	`,
		p.Ref,
		p.Name,
		p.ProjectRef,
		p.SPURef,
		p.ProfileIDs,
		p.Requirements,
		p.PackagePayload,
		p.Status,
		p.TenantID,
		p.CreatedAt,
		p.UpdatedAt,
	).Scan(&p.ID)
}

func (s *PGStore) Get(ctx context.Context, tenantID int, id int64) (*BidProfile, error) {
	item := &BidProfile{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id, ref, name, project_ref, spu_ref, profile_ids, requirements, package_payload,
		       status, tenant_id, created_at, updated_at
		FROM bid_profiles
		WHERE tenant_id=$1 AND id=$2
	`, tenantID, id).Scan(
		&item.ID,
		&item.Ref,
		&item.Name,
		&item.ProjectRef,
		&item.SPURef,
		&item.ProfileIDs,
		&item.Requirements,
		&item.PackagePayload,
		&item.Status,
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

func (s *PGStore) List(ctx context.Context, tenantID int, f Filter) ([]*BidProfile, int, error) {
	where := []string{"tenant_id=$1"}
	args := []any{tenantID}
	pos := 2
	if f.ProjectRef != nil && strings.TrimSpace(*f.ProjectRef) != "" {
		where = append(where, fmt.Sprintf("project_ref=$%d", pos))
		args = append(args, strings.TrimSpace(*f.ProjectRef))
		pos++
	}
	if f.Status != nil {
		where = append(where, fmt.Sprintf("status=$%d", pos))
		args = append(args, *f.Status)
		pos++
	}
	if v := strings.TrimSpace(f.Keyword); v != "" {
		where = append(where, fmt.Sprintf("(name ILIKE $%d OR ref ILIKE $%d)", pos, pos))
		args = append(args, "%"+v+"%")
		pos++
	}
	whereSQL := strings.Join(where, " AND ")

	var total int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM bid_profiles WHERE "+whereSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	listSQL := fmt.Sprintf(`
		SELECT id, ref, name, project_ref, spu_ref, profile_ids, requirements, package_payload,
		       status, tenant_id, created_at, updated_at
		FROM bid_profiles
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

	out := make([]*BidProfile, 0)
	for rows.Next() {
		item := &BidProfile{}
		if err := rows.Scan(
			&item.ID,
			&item.Ref,
			&item.Name,
			&item.ProjectRef,
			&item.SPURef,
			&item.ProfileIDs,
			&item.Requirements,
			&item.PackagePayload,
			&item.Status,
			&item.TenantID,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, 0, err
		}
		out = append(out, item)
	}
	return out, total, rows.Err()
}

func (s *PGStore) UpdateStatus(ctx context.Context, tenantID int, id int64, status Status, updatedAt time.Time) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE bid_profiles
		SET status=$1, updated_at=$2
		WHERE tenant_id=$3 AND id=$4
	`, status, updatedAt, tenantID, id)
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
