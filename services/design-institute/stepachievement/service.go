package stepachievement

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"

	vmlcore "coordos/packages/vml-core"
)

type CreateInput struct {
	NamespaceRef  string   `json:"namespace_ref"`
	ProjectRef    string   `json:"project_ref"`
	SPURef        string   `json:"spu_ref,omitempty"`
	TripRef       string   `json:"trip_ref,omitempty"`
	StepSeq       int      `json:"step_seq"`
	ExecutorRef   string   `json:"executor_ref"`
	ContainerRef  string   `json:"container_ref"`
	InputRefs     []string `json:"input_refs,omitempty"`
	OutputType    string   `json:"output_type"`
	OutputName    string   `json:"output_name,omitempty"`
	QuotaConsumed float64  `json:"quota_consumed,omitempty"`
	QuotaUnit     string   `json:"quota_unit,omitempty"`
	Source        string   `json:"source,omitempty"`
	TenantID      int      `json:"tenant_id"`
}

type SignInput struct {
	Ref         string `json:"ref"`
	SignedBy    string `json:"signed_by"`
	IsFinalStep bool   `json:"is_final_step"`
	ProjectRef  string `json:"project_ref"`
}

type StepAchievement struct {
	ID            int64      `json:"id"`
	Ref           string     `json:"ref"`
	NamespaceRef  string     `json:"namespace_ref"`
	ProjectRef    string     `json:"project_ref"`
	StepSeq       int        `json:"step_seq"`
	ExecutorRef   string     `json:"executor_ref"`
	ContainerRef  string     `json:"container_ref"`
	InputRefs     []string   `json:"input_refs"`
	OutputType    string     `json:"output_type"`
	OutputName    string     `json:"output_name"`
	QuotaConsumed float64    `json:"quota_consumed"`
	QuotaUnit     string     `json:"quota_unit"`
	InputsHash    string     `json:"inputs_hash"`
	ProofHash     string     `json:"proof_hash"`
	PrevProofHash string     `json:"prev_proof_hash,omitempty"`
	Status        string     `json:"status"`
	SignedBy      string     `json:"signed_by,omitempty"`
	SignedAt      *time.Time `json:"signed_at,omitempty"`
	Source        string     `json:"source"`
	CreatedAt     time.Time  `json:"created_at"`
}

type ProjectProgress struct {
	ProjectRef    string            `json:"project_ref"`
	TotalSteps    int               `json:"total_steps"`
	SignedSteps   int               `json:"signed_steps"`
	DraftSteps    int               `json:"draft_steps"`
	Steps         []StepAchievement `json:"steps"`
	ReadyToSettle bool              `json:"ready_to_settle"`
}

type Service struct {
	db       *sql.DB
	tenantID int
}

func NewService(db *sql.DB, tenantID int) *Service {
	return &Service{db: db, tenantID: tenantID}
}

func (s *Service) TenantID() int { return s.tenantID }

func (s *Service) Create(ctx context.Context, in CreateInput) (*StepAchievement, error) {
	if in.ExecutorRef == "" {
		return nil, fmt.Errorf("executor_ref 不能为空")
	}
	if in.ContainerRef == "" {
		return nil, fmt.Errorf("container_ref 不能为空（Container Doctrine LAW-3）")
	}
	if in.OutputType == "" {
		in.OutputType = "DESIGN_DOC"
	}
	if in.Source == "" {
		in.Source = "TRIP_DERIVED"
	}
	if in.QuotaUnit == "" {
		in.QuotaUnit = "项"
	}

	projectSlug := extractSlug(in.ProjectRef)
	ref := fmt.Sprintf("%s/utxo/step/%s/%03d", in.NamespaceRef, projectSlug, in.StepSeq)

	stepData := map[string]any{
		"project_ref":    in.ProjectRef,
		"spu_ref":        in.SPURef,
		"trip_ref":       in.TripRef,
		"step_seq":       in.StepSeq,
		"executor_ref":   in.ExecutorRef,
		"container_ref":  in.ContainerRef,
		"input_refs":     in.InputRefs,
		"output_type":    in.OutputType,
		"output_name":    in.OutputName,
		"quota_consumed": in.QuotaConsumed,
		"quota_unit":     in.QuotaUnit,
		"source":         in.Source,
	}

	canonicalStepBytes, err := vmlcore.CanonicalBytes(stepData)
	if err != nil {
		return nil, fmt.Errorf("Create StepAchievement: failed to compute canonical bytes: %w", err)
	}

	hash1 := sha256.Sum256(canonicalStepBytes)
	inputsHash := "sha256:" + hex.EncodeToString(hash1[:])

	var prevProofHash string
	if in.StepSeq > 1 {
		prevProofHash, err = s.getPreviousProofHash(ctx, in.ProjectRef, in.StepSeq-1)
		if err != nil {
			return nil, fmt.Errorf("Create StepAchievement: %w", err)
		}
	}

	var proofHash string
	if in.StepSeq == 1 {
		proofHash = inputsHash
	} else {
		prevHashBytes, decodeErr := hex.DecodeString(strings.TrimPrefix(prevProofHash, "sha256:"))
		if decodeErr != nil {
			return nil, fmt.Errorf("Create StepAchievement: failed to decode previous proof hash: %w", decodeErr)
		}
		combined := append(prevHashBytes, canonicalStepBytes...)
		hash2 := sha256.Sum256(combined)
		proofHash = "sha256:" + hex.EncodeToString(hash2[:])
	}

	inputRefsArr := "{}"
	if len(in.InputRefs) > 0 {
		quoted := make([]string, len(in.InputRefs))
		for i, s := range in.InputRefs {
			quoted[i] = `"` + s + `"`
		}
		inputRefsArr = "{" + strings.Join(quoted, ",") + "}"
	}

	var id int64
	err = s.db.QueryRowContext(ctx, `
		INSERT INTO step_achievement_utxos (
			ref, namespace_ref, project_ref,
			spu_ref, trip_ref, step_seq,
			executor_ref, container_ref,
			input_refs, output_type, output_name,
			quota_consumed, quota_unit, inputs_hash, proof_hash,
			status, source, tenant_id
		) VALUES (
			$1,$2,$3,
			$4,$5,$6,
			$7,$8,
			$9,$10,$11,
			$12,$13,$14,$15,
			'DRAFT',$16,$17
		)
		ON CONFLICT (ref) DO UPDATE SET
			output_name    = EXCLUDED.output_name,
			quota_consumed = EXCLUDED.quota_consumed,
			proof_hash     = EXCLUDED.proof_hash,
			updated_at     = NOW()
		RETURNING id
	`,
		ref, in.NamespaceRef, in.ProjectRef,
		nullStr(in.SPURef), nullStr(in.TripRef), in.StepSeq,
		in.ExecutorRef, in.ContainerRef,
		inputRefsArr, in.OutputType, nullStr(in.OutputName),
		in.QuotaConsumed, in.QuotaUnit, inputsHash, proofHash,
		in.Source, s.tenantID,
	).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("Create StepAchievement: %w", err)
	}

	if in.QuotaConsumed > 0 {
		go func(projectRef string, amount float64) {
			if quotaErr := s.consumeContractQuota(context.Background(), projectRef, amount); quotaErr != nil {
				log.Printf("ERROR: consumeContractQuota for project %s failed: %v", projectRef, quotaErr)
			}
		}(in.ProjectRef, in.QuotaConsumed)
	}

	return s.Get(ctx, ref)
}

func (s *Service) Sign(ctx context.Context, in SignInput) (*StepAchievement, error) {
	if in.SignedBy == "" {
		return nil, fmt.Errorf("signed_by 不能为空")
	}

	now := time.Now()
	_, err := s.db.ExecContext(ctx, `
		UPDATE step_achievement_utxos SET
			status    = 'SIGNED',
			signed_by = $1,
			signed_at = $2,
			updated_at = NOW()
		WHERE ref = $3 AND status = 'DRAFT'
	`, in.SignedBy, now, in.Ref)
	if err != nil {
		return nil, fmt.Errorf("Sign StepAchievement: %w", err)
	}

	sa, err := s.Get(ctx, in.Ref)
	if err != nil {
		return nil, err
	}

	if in.IsFinalStep {
		if triggerErr := s.triggerSettlement(ctx, in.ProjectRef, sa); triggerErr != nil {
			fmt.Printf("triggerSettlement warning: %v\n", triggerErr)
		}
	}

	return sa, nil
}

func (s *Service) triggerSettlement(ctx context.Context, projectRef string, lastStep *StepAchievement) error {
	var unsignedCount int
	s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM step_achievement_utxos WHERE project_ref = $1 AND status NOT IN ('SIGNED', 'SETTLED')`,
		projectRef,
	).Scan(&unsignedCount)

	if unsignedCount > 0 {
		return fmt.Errorf("还有 %d 个步骤未签发，无法触发结算", unsignedCount)
	}

	var reviewCount int
	s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM step_achievement_utxos
		WHERE project_ref = $1 AND status = 'SIGNED'
		  AND (output_type = 'REVIEW_CERT' OR COALESCE(spu_ref, '') ILIKE '%review_certificate%')
	`, projectRef).Scan(&reviewCount)
	if reviewCount <= 0 {
		return fmt.Errorf("缺少已签发审图步骤，无法触发结算")
	}

	settlementRef := lastStep.NamespaceRef + "/utxo/step/settlement/" + extractSlug(projectRef)

	settlementData := map[string]any{
		"ref":         settlementRef,
		"project_ref": projectRef,
		"prev_hash":   lastStep.ProofHash,
		"output_type": "SETTLEMENT",
	}
	settlementHash, err := computeCanonicalHash(settlementData)
	if err != nil {
		return fmt.Errorf("triggerSettlement: failed to compute settlement hash: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO step_achievement_utxos (
			ref, namespace_ref, project_ref,
			step_seq, executor_ref, container_ref,
			input_refs, output_type, output_name,
			inputs_hash, proof_hash,
			status, source, tenant_id
		) VALUES (
			$1,$2,$3, 9999,$4,$5,
			'{}'::text[], 'SETTLEMENT', '项目结算',
			$6,$7,
			'SETTLED','TRIP_DERIVED',$8
		) ON CONFLICT (ref) DO NOTHING
	`,
		settlementRef, lastStep.NamespaceRef, projectRef,
		lastStep.ExecutorRef, lastStep.ContainerRef,
		settlementHash, settlementHash,
		s.tenantID,
	)
	return err
}

func (s *Service) Get(ctx context.Context, ref string) (*StepAchievement, error) {
	var sa StepAchievement
	var inputRefs []byte
	var signedBy sql.NullString
	var signedAt sql.NullTime

	err := s.db.QueryRowContext(ctx, `
		SELECT id, ref, namespace_ref, project_ref,
		       step_seq, executor_ref, container_ref,
		       input_refs, output_type, COALESCE(output_name,''),
		       COALESCE(quota_consumed,0), COALESCE(quota_unit,'项'),
		       COALESCE(inputs_hash,''), COALESCE(proof_hash,''),
		       status, signed_by, signed_at,
		       source, created_at
		FROM step_achievement_utxos WHERE ref=$1
	`, ref).Scan(
		&sa.ID, &sa.Ref, &sa.NamespaceRef, &sa.ProjectRef,
		&sa.StepSeq, &sa.ExecutorRef, &sa.ContainerRef,
		&inputRefs, &sa.OutputType, &sa.OutputName,
		&sa.QuotaConsumed, &sa.QuotaUnit,
		&sa.InputsHash, &sa.ProofHash,
		&sa.Status, &signedBy, &signedAt,
		&sa.Source, &sa.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("StepAchievement not found: %s", ref)
	}
	if signedBy.Valid {
		sa.SignedBy = signedBy.String
	}
	if signedAt.Valid {
		sa.SignedAt = &signedAt.Time
	}
	return &sa, nil
}

func (s *Service) ListByProject(ctx context.Context, projectRef string) ([]StepAchievement, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, ref, namespace_ref, project_ref,
		       step_seq, executor_ref, container_ref, input_refs,
		       output_type, COALESCE(output_name,''),
		       COALESCE(quota_consumed,0), COALESCE(quota_unit,'项'),
		       COALESCE(inputs_hash,''), COALESCE(proof_hash,''),
		       status, COALESCE(signed_by,''), signed_at,
		       source, created_at
		FROM step_achievement_utxos
		WHERE project_ref = $1
		ORDER BY step_seq ASC
	`, projectRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []StepAchievement
	for rows.Next() {
		var sa StepAchievement
		var inputRefs []byte
		var signedAt sql.NullTime
		rows.Scan(
			&sa.ID, &sa.Ref, &sa.NamespaceRef, &sa.ProjectRef,
			&sa.StepSeq, &sa.ExecutorRef, &sa.ContainerRef,
			&inputRefs, &sa.OutputType, &sa.OutputName,
			&sa.QuotaConsumed, &sa.QuotaUnit,
			&sa.InputsHash, &sa.ProofHash,
			&sa.Status, &sa.SignedBy, &signedAt,
			&sa.Source, &sa.CreatedAt,
		)
		if signedAt.Valid {
			sa.SignedAt = &signedAt.Time
		}
		items = append(items, sa)
	}
	return items, nil
}

func (s *Service) GetProgress(ctx context.Context, projectRef string) (*ProjectProgress, error) {
	steps, err := s.ListByProject(ctx, projectRef)
	if err != nil {
		return nil, err
	}

	p := &ProjectProgress{ProjectRef: projectRef, Steps: steps}
	hasSignedReview := false
	for _, step := range steps {
		if step.OutputType == "SETTLEMENT" || step.Status == "SETTLED" {
			continue
		}
		p.TotalSteps++
		switch step.Status {
		case "SIGNED":
			p.SignedSteps++
		case "DRAFT":
			p.DraftSteps++
		}
		if step.Status == "SIGNED" && step.OutputType == "REVIEW_CERT" {
			hasSignedReview = true
		}
	}
	p.ReadyToSettle = p.TotalSteps > 0 && p.SignedSteps == p.TotalSteps && hasSignedReview
	return p, nil
}

func (s *Service) consumeContractQuota(ctx context.Context, projectRef string, amount float64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE genesis_utxos SET available_amount = GREATEST(0, available_amount - $1)
		WHERE ref = (SELECT contract_ref FROM project_nodes WHERE ref = $2)
		  AND resource_type = 'CONTRACT_FUND'
	`, amount, projectRef)
	return err
}

func (s *Service) getPreviousProofHash(ctx context.Context, projectRef string, stepSeq int) (string, error) {
	var prevProofHash string
	err := s.db.QueryRowContext(ctx, `
		SELECT proof_hash FROM step_achievement_utxos
		WHERE project_ref = $1 AND step_seq = $2
	`, projectRef, stepSeq).Scan(&prevProofHash)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("前一步骤 (step_seq: %d) 的 proof_hash 未找到", stepSeq)
	}
	if err != nil {
		return "", fmt.Errorf("查询前一步骤 proof_hash 失败: %w", err)
	}
	return prevProofHash, nil
}

func computeCanonicalHash(data map[string]any) (string, error) {
	bytes, err := vmlcore.CanonicalBytes(data)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(bytes)
	return "sha256:" + hex.EncodeToString(hash[:]), nil
}

func extractSlug(ref string) string {
	parts := strings.Split(ref, "/")
	if len(parts) == 0 {
		return ref
	}
	return parts[len(parts)-1]
}

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
