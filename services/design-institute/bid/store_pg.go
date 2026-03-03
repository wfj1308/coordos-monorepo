package bid

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type PGStore struct {
	db *sql.DB
}

func NewPGStore(db *sql.DB) Store {
	return &PGStore{db: db}
}

// ══════════════════════════════════════════════════════════════
// 投标文档
// ══════════════════════════════════════════════════════════════

func (s *PGStore) CreateBid(ctx context.Context, bid *BidDocument) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO bid_documents (
			bid_ref, tenant_id, namespace_ref, tender_genesis_ref,
			project_name, project_type, owner_name,
			estimated_amount, bid_deadline, our_bid_amount,
			status, proof_hash, resource_count,
			created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		RETURNING id`,
		bid.BidRef, bid.TenantID, bid.NamespaceRef, bid.TenderGenesisRef,
		bid.ProjectName, bid.ProjectType, bid.OwnerName,
		bid.EstimatedAmount, bid.BidDeadline, bid.OurBidAmount,
		bid.Status, bid.ProofHash, bid.ResourceCount,
		bid.CreatedAt, bid.CreatedAt,
	).Scan(&id)
	return id, err
}

func (s *PGStore) GetBid(ctx context.Context, id int64) (*BidDocument, error) {
	bid := &BidDocument{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id, bid_ref, tenant_id, namespace_ref, tender_genesis_ref,
		       project_name, project_type, owner_name,
		       estimated_amount, bid_deadline, our_bid_amount,
		       bid_package_ref, status, proof_hash, resource_count,
		       project_ref, contract_id,
		       created_at, submitted_at, awarded_at, failed_at
		FROM bid_documents WHERE id=$1`, id,
	).Scan(
		&bid.ID, &bid.BidRef, &bid.TenantID, &bid.NamespaceRef, &bid.TenderGenesisRef,
		&bid.ProjectName, &bid.ProjectType, &bid.OwnerName,
		&bid.EstimatedAmount, &bid.BidDeadline, &bid.OurBidAmount,
		&bid.BidPackageRef, &bid.Status, &bid.ProofHash, &bid.ResourceCount,
		&bid.ProjectRef, &bid.ContractID,
		&bid.CreatedAt, &bid.SubmittedAt, &bid.AwardedAt, &bid.FailedAt,
	)
	return bid, err
}

func (s *PGStore) GetBidByRef(ctx context.Context, ref string) (*BidDocument, error) {
	bid := &BidDocument{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id, bid_ref, tenant_id, namespace_ref, tender_genesis_ref,
		       project_name, project_type, owner_name,
		       estimated_amount, bid_deadline, our_bid_amount,
		       bid_package_ref, status, proof_hash, resource_count,
		       project_ref, contract_id,
		       created_at, submitted_at, awarded_at, failed_at
		FROM bid_documents WHERE bid_ref=$1`, ref,
	).Scan(
		&bid.ID, &bid.BidRef, &bid.TenantID, &bid.NamespaceRef, &bid.TenderGenesisRef,
		&bid.ProjectName, &bid.ProjectType, &bid.OwnerName,
		&bid.EstimatedAmount, &bid.BidDeadline, &bid.OurBidAmount,
		&bid.BidPackageRef, &bid.Status, &bid.ProofHash, &bid.ResourceCount,
		&bid.ProjectRef, &bid.ContractID,
		&bid.CreatedAt, &bid.SubmittedAt, &bid.AwardedAt, &bid.FailedAt,
	)
	return bid, err
}

func (s *PGStore) UpdateBidStatus(ctx context.Context, id int64, status BidStatus, at time.Time) error {
	var query string
	var args []any

	switch status {
	case StatusSubmitted:
		query = `UPDATE bid_documents SET status=$1, submitted_at=$2, updated_at=$2 WHERE id=$3`
		args = []any{status, at, id}
	case StatusAwarded:
		query = `UPDATE bid_documents SET status=$1, awarded_at=$2, updated_at=$2 WHERE id=$3`
		args = []any{status, at, id}
	case StatusFailed:
		query = `UPDATE bid_documents SET status=$1, failed_at=$2, updated_at=$2 WHERE id=$3`
		args = []any{status, at, id}
	default:
		query = `UPDATE bid_documents SET status=$1, updated_at=$2 WHERE id=$3`
		args = []any{status, at, id}
	}

	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *PGStore) ListBids(ctx context.Context, tenantID int, status *BidStatus, limit, offset int) ([]*BidDocument, int, error) {
	where := "tenant_id=$1"
	args := []any{tenantID}

	if status != nil {
		where += " AND status=$2"
		args = append(args, *status)
	}

	var total int
	s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM bid_documents WHERE "+where, args...).Scan(&total)

	args = append(args, limit, offset)
	query := fmt.Sprintf(`
		SELECT id, bid_ref, tenant_id, namespace_ref, tender_genesis_ref,
		       project_name, project_type, owner_name,
		       estimated_amount, bid_deadline, our_bid_amount,
		       status, proof_hash, resource_count,
		       project_ref, contract_id,
		       created_at, submitted_at, awarded_at, failed_at
		FROM bid_documents WHERE %s
		ORDER BY created_at DESC LIMIT $%d OFFSET $%d`,
		where, len(args)-1, len(args))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var list []*BidDocument
	for rows.Next() {
		bid := &BidDocument{}
		if err := rows.Scan(
			&bid.ID, &bid.BidRef, &bid.TenantID, &bid.NamespaceRef, &bid.TenderGenesisRef,
			&bid.ProjectName, &bid.ProjectType, &bid.OwnerName,
			&bid.EstimatedAmount, &bid.BidDeadline, &bid.OurBidAmount,
			&bid.Status, &bid.ProofHash, &bid.ResourceCount,
			&bid.ProjectRef, &bid.ContractID,
			&bid.CreatedAt, &bid.SubmittedAt, &bid.AwardedAt, &bid.FailedAt,
		); err == nil {
			list = append(list, bid)
		}
	}

	return list, total, nil
}

// ══════════════════════════════════════════════════════════════
// 投标资源
// ══════════════════════════════════════════════════════════════

func (s *PGStore) AddResource(ctx context.Context, res *BidResource) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO bid_resources (
			bid_id, tenant_id, resource_type, resource_ref,
			consume_mode, consume_status, resource_name, resource_data,
			valid_from, valid_until, verify_url, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		RETURNING id`,
		res.BidID, res.TenantID, res.ResourceType, res.ResourceRef,
		res.ConsumeMode, res.ConsumeStatus, res.ResourceName, res.ResourceData,
		res.ValidFrom, res.ValidUntil, res.VerifyURL, res.CreatedAt,
	).Scan(&id)
	return id, err
}

func (s *PGStore) ListResources(ctx context.Context, bidID int64) ([]*BidResource, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, bid_id, tenant_id, resource_type, resource_ref,
		       consume_mode, consume_status, resource_name, resource_data,
		       valid_from, valid_until, verify_url, created_at
		FROM bid_resources WHERE bid_id=$1 ORDER BY id`, bidID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []*BidResource
	for rows.Next() {
		res := &BidResource{}
		if err := rows.Scan(
			&res.ID, &res.BidID, &res.TenantID, &res.ResourceType, &res.ResourceRef,
			&res.ConsumeMode, &res.ConsumeStatus, &res.ResourceName, &res.ResourceData,
			&res.ValidFrom, &res.ValidUntil, &res.VerifyURL, &res.CreatedAt,
		); err == nil {
			list = append(list, res)
		}
	}
	return list, nil
}

func (s *PGStore) UpdateResourceStatus(ctx context.Context, bidID int64, resourceType ResourceType, status ConsumeStatus) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE bid_resources 
		SET consume_status=$1, updated_at=NOW()
		WHERE bid_id=$2 AND resource_type=$3`,
		status, bidID, resourceType)
	return err
}

// ══════════════════════════════════════════════════════════════
// 业绩池
// ══════════════════════════════════════════════════════════════

func (s *PGStore) SearchAchievements(ctx context.Context, f AchievementsFilter) ([]*AchievementInPool, int, error) {
	where := "1=1"
	args := []any{}
	i := 1

	if f.NamespaceRef != "" {
		where += fmt.Sprintf(" AND a.namespace_ref = $%d", i)
		args = append(args, normalizeNamespaceRef(f.NamespaceRef))
		i++
	}

	if f.ProjectType != nil && *f.ProjectType != "" {
		where += fmt.Sprintf(" AND a.project_type = $%d", i)
		args = append(args, *f.ProjectType)
		i++
	}

	withinYears := f.WithinYears
	if withinYears == 0 {
		withinYears = 3
	}
	switch withinYears {
	case 3:
		where += " AND a.within_3years = true"
	case 5:
		where += " AND a.within_5years = true"
	default:
		where += fmt.Sprintf(" AND a.completed_year >= (EXTRACT(YEAR FROM NOW())::INT - %d)", withinYears)
	}

	var total int
	countQuery := "SELECT COUNT(*) FROM achievement_pool a WHERE " + where
	s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total)

	limit := f.Limit
	if limit == 0 {
		limit = 20
	}
	offset := f.Offset

	args = append(args, limit, offset)
	selectQuery := fmt.Sprintf(`
		SELECT
			COALESCE(u.id, 0)                           AS id,
			COALESCE(u.utxo_ref, a.ref)                 AS utxo_ref,
			COALESCE(u.spu_ref, '')                     AS spu_ref,
			COALESCE(u.project_ref, '')                 AS project_ref,
			COALESCE(u.executor_ref, '')                AS executor_ref,
			COALESCE(a.proof_hash, '')                  AS proof_hash,
			COALESCE(a.status, 'ACTIVE')                AS status,
			COALESCE(a.source, 'HISTORICAL_IMPORT')     AS source,
			u.settled_at                                AS settled_at,
			COALESCE(u.tenant_id, 10000)                AS tenant_id,
			COALESCE(a.project_name, '')                AS project_name,
			''::text                                    AS project_status,
			NULL::text                                  AS contract_name,
			COALESCE(a.contract_amount::float8, 0)      AS contract_amount,
			COALESCE(a.project_type, 'OTHER')           AS inferred_project_type,
			COALESCE(a.within_3years, false)            AS within_3_years,
			true                                        AS is_usable_for_bid
		FROM achievement_pool a
		LEFT JOIN achievement_utxos u
		  ON COALESCE(u.ref, u.utxo_ref) = a.ref
		WHERE %s
		ORDER BY COALESCE(u.settled_at, u.ingested_at, u.created_at) DESC NULLS LAST
		LIMIT $%d OFFSET $%d`, where, i, i+1)

	rows, err := s.db.QueryContext(ctx, selectQuery, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var list []*AchievementInPool
	for rows.Next() {
		a := &AchievementInPool{}
		if err := rows.Scan(
			&a.ID, &a.UTXORef, &a.SPURef, &a.ProjectRef, &a.ExecutorRef,
			&a.ProofHash, &a.Status, &a.Source, &a.SettledAt, &a.TenantID,
			&a.ProjectName, &a.ProjectStatus, &a.ContractName, &a.ContractAmount,
			&a.InferredProjType, &a.Within3Years, &a.IsUsableForBid,
		); err == nil {
			list = append(list, a)
		}
	}

	return list, total, nil
}

func (s *PGStore) GetAchievementByRef(ctx context.Context, utxoRef string) (*AchievementInPool, error) {
	a := &AchievementInPool{}
	err := s.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(u.id, 0)                           AS id,
			COALESCE(u.utxo_ref, p.ref)                 AS utxo_ref,
			COALESCE(u.spu_ref, '')                     AS spu_ref,
			COALESCE(u.project_ref, '')                 AS project_ref,
			COALESCE(u.executor_ref, '')                AS executor_ref,
			COALESCE(p.proof_hash, '')                  AS proof_hash,
			COALESCE(p.status, 'ACTIVE')                AS status,
			COALESCE(p.source, 'HISTORICAL_IMPORT')     AS source,
			u.settled_at                                AS settled_at,
			COALESCE(u.tenant_id, 10000)                AS tenant_id,
			COALESCE(p.project_name, '')                AS project_name,
			''::text                                    AS project_status,
			NULL::text                                  AS contract_name,
			COALESCE(p.contract_amount::float8, 0)      AS contract_amount,
			COALESCE(p.project_type, 'OTHER')           AS inferred_project_type,
			COALESCE(p.within_3years, false)            AS within_3_years,
			true                                        AS is_usable_for_bid
		FROM achievement_pool p
		LEFT JOIN achievement_utxos u
		  ON COALESCE(u.ref, u.utxo_ref) = p.ref
		WHERE p.ref=$1 OR COALESCE(u.utxo_ref, '')=$1
		ORDER BY COALESCE(u.settled_at, u.ingested_at, u.created_at) DESC NULLS LAST
		LIMIT 1`, utxoRef,
	).Scan(
		&a.ID, &a.UTXORef, &a.SPURef, &a.ProjectRef, &a.ExecutorRef,
		&a.ProofHash, &a.Status, &a.Source, &a.SettledAt, &a.TenantID,
		&a.ProjectName, &a.ProjectStatus, &a.ContractName, &a.ContractAmount,
		&a.InferredProjType, &a.Within3Years, &a.IsUsableForBid,
	)
	return a, err
}

// ══════════════════════════════════════════════════════════════
// 资源验证
// ══════════════════════════════════════════════════════════════

func (s *PGStore) CountCompanyQuals(ctx context.Context, namespace string) (int, error) {
	var count int
	nsPattern := qualificationNamespacePattern(namespace)
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM qualifications 
		WHERE ref LIKE $1 
		  AND holder_type = 'COMPANY'
		  AND status = 'VALID'
		  AND deleted = FALSE`,
		nsPattern).Scan(&count)
	return count, err
}

func (s *PGStore) CountPersonQuals(ctx context.Context, namespace string) (int, error) {
	var count int
	nsPattern := qualificationNamespacePattern(namespace)
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM qualifications 
		WHERE ref LIKE $1 
		  AND holder_type = 'PERSON'
		  AND status = 'VALID'
		  AND deleted = FALSE`,
		nsPattern).Scan(&count)
	return count, err
}

func (s *PGStore) CountAvailableEngineers(ctx context.Context, namespace string) (int, error) {
	// 可用工程师 = 总持有 - 已占用
	query := `
		WITH total AS (
			SELECT COUNT(*) AS cnt FROM qualifications 
			WHERE ref LIKE $1 
			  AND holder_type = 'PERSON'
			  AND status = 'VALID'
			  AND deleted = FALSE
		),
		occupied AS (
			SELECT COUNT(DISTINCT resource_ref) AS cnt 
			FROM bid_resources
			WHERE resource_ref LIKE $1
			  AND resource_type = 'QUAL_PERSON'
			  AND consume_status IN ('OCCUPIED', 'REFERENCED')
		)
		SELECT GREATEST(0, (SELECT cnt FROM total) - (SELECT cnt FROM occupied))`

	var count int
	err := s.db.QueryRowContext(ctx, query, qualificationNamespacePattern(namespace)).Scan(&count)
	return count, err
}

func (s *PGStore) ValidateQualValid(ctx context.Context, qualRef string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM qualifications 
		WHERE ref = $1 
		  AND status = 'VALID'
		  AND deleted = FALSE
		  AND valid_until > NOW()`, qualRef).Scan(&count)
	return count > 0, err
}

func (s *PGStore) FindCandidateExecutors(ctx context.Context, namespace string, spuRef string, limit int) ([]*ExecutorCandidate, error) {
	query := `
		SELECT 
			e.executor_ref,
			emp.name AS employee_name,
			e.capability_level,
			e.skills,
			CASE WHEN (SELECT COUNT(*) FROM bid_resources br 
			           WHERE br.resource_ref = e.executor_ref 
			             AND br.consume_status = 'OCCUPIED') = 0 
			THEN true ELSE false END AS is_available,
			(SELECT COUNT(*) FROM achievement_utxos a 
			 WHERE a.executor_ref = e.executor_ref 
			   AND a.status = 'SETTLED'
			   AND a.settled_at >= NOW() - INTERVAL '3 years') AS recent_achievements
		FROM executors e
		LEFT JOIN employees emp ON emp.id = e.employee_id
		WHERE e.executor_ref LIKE $1
		ORDER BY e.capability_level DESC, recent_achievements DESC
		LIMIT $2`

	rows, err := s.db.QueryContext(ctx, query, namespace+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candidates []*ExecutorCandidate
	for rows.Next() {
		c := &ExecutorCandidate{}
		var skillsJSON []byte
		if err := rows.Scan(&c.ExecutorRef, &c.EmployeeName, &c.CapabilityLevel,
			&skillsJSON, &c.IsAvailable, &c.RecentAchievements); err == nil {
			if len(skillsJSON) > 0 {
				json.Unmarshal(skillsJSON, &c.Skills)
			}
			c.MatchingScore = s.calcMatchingScore(c)
			candidates = append(candidates, c)
		}
	}
	return candidates, nil
}

func (s *PGStore) GetExecutorCapabilities(ctx context.Context, executorRef string) (*ExecutorCandidate, error) {
	query := `
		SELECT 
			e.executor_ref,
			emp.name AS employee_name,
			e.capability_level,
			e.skills,
			CASE WHEN (SELECT COUNT(*) FROM bid_resources br 
			           WHERE br.resource_ref = e.executor_ref 
			             AND br.consume_status = 'OCCUPIED') = 0 
			THEN true ELSE false END AS is_available,
			(SELECT COUNT(*) FROM achievement_utxos a 
			 WHERE a.executor_ref = e.executor_ref 
			   AND a.status = 'SETTLED'
			   AND a.settled_at >= NOW() - INTERVAL '3 years') AS recent_achievements
		FROM executors e
		LEFT JOIN employees emp ON emp.id = e.employee_id
		WHERE e.executor_ref = $1
		LIMIT 1`

	c := &ExecutorCandidate{}
	var skillsJSON []byte
	err := s.db.QueryRowContext(ctx, query, executorRef).Scan(
		&c.ExecutorRef, &c.EmployeeName, &c.CapabilityLevel,
		&skillsJSON, &c.IsAvailable, &c.RecentAchievements)
	if err != nil {
		return nil, err
	}
	if len(skillsJSON) > 0 {
		json.Unmarshal(skillsJSON, &c.Skills)
	}
	c.MatchingScore = s.calcMatchingScore(c)
	return c, nil
}

func (s *PGStore) MatchExecutorAchievements(ctx context.Context, executorRef string, projectType string, withinYears int, limit int) ([]*AchievementMatch, error) {
	query := `
		SELECT 
			a.utxo_ref,
			p.name AS project_name,
			a.spu_ref,
			a.settled_at,
			COALESCE(c.contract_amount, 0) AS contract_amount,
			a.proof_hash
		FROM achievement_utxos a
		LEFT JOIN project_nodes p ON p.ref = a.project_ref
		LEFT JOIN contracts c ON c.id = (SELECT contract_id FROM project_nodes WHERE ref = a.project_ref LIMIT 1)
		WHERE a.executor_ref = $1
		  AND a.status = 'SETTLED'
		  AND a.settled_at >= NOW() - ($2 * INTERVAL '1 year')
		  AND ($3 = '' OR a.spu_ref LIKE '%' || $3 || '%')
		ORDER BY a.settled_at DESC
		LIMIT $4`

	rows, err := s.db.QueryContext(ctx, query, executorRef, withinYears, projectType, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var matches []*AchievementMatch
	for rows.Next() {
		m := &AchievementMatch{}
		var settledAt *time.Time
		if err := rows.Scan(&m.UTXORef, &m.ProjectName, &m.SPURef,
			&settledAt, &m.ContractAmount, &m.ProofHash); err == nil {
			if settledAt != nil {
				m.SettledAt = settledAt.Format("2006-01-02")
			}
			m.MatchScore = 10
			if m.ContractAmount > 0 {
				m.MatchScore += int(m.ContractAmount / 100000)
				if m.MatchScore > 50 {
					m.MatchScore = 50
				}
			}
			matches = append(matches, m)
		}
	}
	return matches, nil
}

func (s *PGStore) calcMatchingScore(c *ExecutorCandidate) int {
	score := 0

	// 能力等级评分
	switch c.CapabilityLevel {
	case "SENIOR_ENGINEER":
		score += 40
	case "ENGINEER":
		score += 30
	case "ASSISTANT_ENGINEER":
		score += 20
	default:
		score += 10
	}

	// 可用性
	if c.IsAvailable {
		score += 20
	}

	// 业绩
	achievementBonus := c.RecentAchievements * 5
	if achievementBonus > 40 {
		achievementBonus = 40
	}
	score += achievementBonus

	return score
}

func qualificationNamespacePattern(namespace string) string {
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		return "%"
	}
	if !strings.HasPrefix(ns, "v://") {
		ns = "v://" + ns
	}
	return ns + "/%"
}
