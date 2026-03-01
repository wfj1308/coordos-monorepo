package rights

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/lib/pq"
)

type RightType string

const (
	RightReviewStamp RightType = "REVIEW_STAMP"
	RightSignStamp   RightType = "SIGN_STAMP"
	RightInvoice     RightType = "INVOICE"
)

type Status string

const (
	StatusActive   Status = "ACTIVE"
	StatusRevoked  Status = "REVOKED"
	StatusExpired  Status = "EXPIRED"
	StatusDisabled Status = "DISABLED"
)

type Right struct {
	ID         int64
	Ref        string
	RightType  RightType
	HolderRef  string
	Scope      string
	Status     Status
	ValidFrom  *time.Time
	ValidUntil *time.Time
	TenantID   int
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type CreateInput struct {
	Ref        string
	RightType  RightType
	HolderRef  string
	Scope      string
	Status     Status
	ValidFrom  *time.Time
	ValidUntil *time.Time
}

type Filter struct {
	HolderRef *string
	RightType *RightType
	Status    *Status
	TenantID  int
	Limit     int
	Offset    int
}

type Store interface {
	Create(ctx context.Context, r *Right) error
	List(ctx context.Context, f Filter) ([]*Right, int, error)
	ListByHolderRefs(ctx context.Context, tenantID int, holderRefs []string) ([]*Right, error)
}

type Service struct {
	store    Store
	tenantID int
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

func (s *Service) Create(ctx context.Context, in CreateInput) (*Right, error) {
	rt := normalizeRightType(in.RightType)
	if !isSupportedRightType(rt) {
		return nil, fmt.Errorf("unsupported right_type: %s", in.RightType)
	}
	holderRef := strings.TrimSpace(in.HolderRef)
	if holderRef == "" {
		return nil, fmt.Errorf("holder_ref is required")
	}
	st := in.Status
	if st == "" {
		st = StatusActive
	}
	ref := strings.TrimSpace(in.Ref)
	if ref == "" {
		ref = BuildRef(s.tenantID, rt, holderRef)
	}

	now := time.Now().UTC()
	item := &Right{
		Ref:        ref,
		RightType:  rt,
		HolderRef:  holderRef,
		Scope:      strings.TrimSpace(in.Scope),
		Status:     st,
		ValidFrom:  in.ValidFrom,
		ValidUntil: in.ValidUntil,
		TenantID:   s.tenantID,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := s.store.Create(ctx, item); err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Service) List(ctx context.Context, f Filter) ([]*Right, int, error) {
	if f.TenantID == 0 {
		f.TenantID = s.tenantID
	}
	if f.Limit == 0 {
		f.Limit = 20
	}
	return s.store.List(ctx, f)
}

func (s *Service) ListByHolderRefs(ctx context.Context, holderRefs []string) ([]*Right, error) {
	return s.store.ListByHolderRefs(ctx, s.tenantID, holderRefs)
}

func BuildRef(tenantID int, rightType RightType, holderRef string) string {
	tenant := "unknown"
	if tenantID > 0 {
		tenant = fmt.Sprintf("%d", tenantID)
	}
	return fmt.Sprintf(
		"v://%s/right/%s/%s@v1",
		tenant,
		strings.ToLower(string(rightType)),
		url.PathEscape(strings.TrimSpace(holderRef)),
	)
}

func normalizeRightType(v RightType) RightType {
	return RightType(strings.ToUpper(strings.TrimSpace(string(v))))
}

func isSupportedRightType(v RightType) bool {
	switch v {
	case RightReviewStamp, RightSignStamp, RightInvoice:
		return true
	default:
		return false
	}
}

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) Create(ctx context.Context, r *Right) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO rights (
			ref, right_type, holder_ref, scope, status,
			valid_from, valid_until, tenant_id, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		RETURNING id
	`,
		r.Ref, r.RightType, r.HolderRef, r.Scope, r.Status,
		r.ValidFrom, r.ValidUntil, r.TenantID, r.CreatedAt, r.UpdatedAt,
	).Scan(&r.ID)
}

func (s *PGStore) List(ctx context.Context, f Filter) ([]*Right, int, error) {
	args := []any{f.TenantID}
	conds := []string{"tenant_id=$1"}
	if f.HolderRef != nil && strings.TrimSpace(*f.HolderRef) != "" {
		args = append(args, strings.TrimSpace(*f.HolderRef))
		conds = append(conds, fmt.Sprintf("holder_ref=$%d", len(args)))
	}
	if f.RightType != nil && strings.TrimSpace(string(*f.RightType)) != "" {
		args = append(args, normalizeRightType(*f.RightType))
		conds = append(conds, fmt.Sprintf("right_type=$%d", len(args)))
	}
	if f.Status != nil && strings.TrimSpace(string(*f.Status)) != "" {
		args = append(args, strings.ToUpper(strings.TrimSpace(string(*f.Status))))
		conds = append(conds, fmt.Sprintf("status=$%d", len(args)))
	}
	where := strings.Join(conds, " AND ")

	var total int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(1) FROM rights WHERE "+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	args = append(args, f.Limit, f.Offset)
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, ref, right_type, holder_ref, scope, status,
		       valid_from, valid_until, tenant_id, created_at, updated_at
		FROM rights
		WHERE `+where+`
		ORDER BY id DESC
		LIMIT $`+fmt.Sprintf("%d", len(args)-1)+` OFFSET $`+fmt.Sprintf("%d", len(args))+``,
		args...,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := make([]*Right, 0, f.Limit)
	for rows.Next() {
		item := &Right{}
		if err := rows.Scan(
			&item.ID, &item.Ref, &item.RightType, &item.HolderRef, &item.Scope, &item.Status,
			&item.ValidFrom, &item.ValidUntil, &item.TenantID, &item.CreatedAt, &item.UpdatedAt,
		); err != nil {
			return nil, 0, err
		}
		out = append(out, item)
	}
	return out, total, rows.Err()
}

func (s *PGStore) ListByHolderRefs(ctx context.Context, tenantID int, holderRefs []string) ([]*Right, error) {
	if len(holderRefs) == 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, ref, right_type, holder_ref, scope, status,
		       valid_from, valid_until, tenant_id, created_at, updated_at
		FROM rights
		WHERE tenant_id=$1
		  AND holder_ref = ANY($2)
		  AND status='ACTIVE'
		ORDER BY right_type, holder_ref, id DESC
	`, tenantID, pq.Array(holderRefs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]*Right, 0, len(holderRefs))
	for rows.Next() {
		item := &Right{}
		if err := rows.Scan(
			&item.ID, &item.Ref, &item.RightType, &item.HolderRef, &item.Scope, &item.Status,
			&item.ValidFrom, &item.ValidUntil, &item.TenantID, &item.CreatedAt, &item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
