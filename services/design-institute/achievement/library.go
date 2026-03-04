package achievement

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

type LibraryEngineerParticipant struct {
	EngineerID   string `json:"engineer_id"`
	EngineerName string `json:"engineer_name,omitempty"`
	CertType     string `json:"cert_type"`
	Role         string `json:"role,omitempty"`
	Contribution string `json:"contribution,omitempty"`
}

type LibraryProjectRow struct {
	ProjectName    string                       `json:"project_name"`
	OwnerName      string                       `json:"owner_name"`
	ProjectType    string                       `json:"project_type"`
	ContractAmount float64                      `json:"contract_amount"`
	CompletedYear  int                          `json:"completed_year"`
	CompletedAt    string                       `json:"completed_at,omitempty"`
	Region         string                       `json:"region,omitempty"`
	Scale          string                       `json:"scale,omitempty"`
	QualRef        string                       `json:"qual_ref,omitempty"`
	Engineers      []LibraryEngineerParticipant `json:"engineers,omitempty"`
}

type LibraryBatchImportResult struct {
	NamespaceRef    string             `json:"namespace_ref"`
	Total           int                `json:"total"`
	Success         int                `json:"success"`
	Failed          int                `json:"failed"`
	ReceiptsCreated int                `json:"receipts_created"`
	Results         []LibraryRowResult `json:"results"`
}

type LibraryRowResult struct {
	Row            int    `json:"row"`
	ProjectName    string `json:"project_name"`
	AchievementRef string `json:"achievement_ref,omitempty"`
	ReceiptCount   int    `json:"receipt_count"`
	Status         string `json:"status"`
	Error          string `json:"error,omitempty"`
}

type LibraryAchievementItem struct {
	Ref            string  `json:"ref"`
	NamespaceRef   string  `json:"namespace_ref"`
	ProjectName    string  `json:"project_name"`
	ProjectType    string  `json:"project_type"`
	OwnerName      string  `json:"owner_name"`
	Region         string  `json:"region,omitempty"`
	Scale          string  `json:"scale,omitempty"`
	ContractAmount float64 `json:"contract_amount"`
	CompletedYear  int     `json:"completed_year"`
	Source         string  `json:"source"`
	ProofHash      string  `json:"proof_hash"`
	Status         string  `json:"status"`
	Within3Years   bool    `json:"within_3years"`
	Within5Years   bool    `json:"within_5years"`
	EngineerCount  int     `json:"engineer_count"`
	LeadEngineer   string  `json:"lead_engineer,omitempty"`
}

type LibraryEngineerAchievementItem struct {
	Ref            string  `json:"ref"`
	AchievementRef string  `json:"achievement_ref"`
	ExecutorRef    string  `json:"executor_ref"`
	EngineerID     string  `json:"engineer_id"`
	EngineerName   string  `json:"engineer_name"`
	ContainerRef   string  `json:"container_ref"`
	Role           string  `json:"role"`
	Contribution   string  `json:"contribution,omitempty"`
	Source         string  `json:"source"`
	ProofHash      string  `json:"proof_hash"`
	ProjectName    string  `json:"project_name"`
	ProjectType    string  `json:"project_type"`
	OwnerName      string  `json:"owner_name"`
	ContractAmount float64 `json:"contract_amount"`
	CompletedYear  int     `json:"completed_year"`
	Within3Years   bool    `json:"within_3years"`
}

type LibraryQueryFilter struct {
	NamespaceRef string
	ProjectType  string
	MinAmount    float64
	Within3Years bool
	Within5Years bool
	Source       string
	Limit        int
	Offset       int
}

func (s *Service) BatchImportCSV(ctx context.Context, nsRef string, r io.Reader) (*LibraryBatchImportResult, error) {
	rows, err := ParseAchievementCSV(r)
	if err != nil {
		return nil, err
	}
	return s.batchImportProjects(ctx, nsRef, rows, "HISTORICAL_IMPORT")
}

func (s *Service) BatchImportJSON(ctx context.Context, nsRef string, body []byte) (*LibraryBatchImportResult, error) {
	var rows []LibraryProjectRow
	if err := json.Unmarshal(body, &rows); err != nil {
		var wrapper struct {
			Projects []LibraryProjectRow `json:"projects"`
		}
		if err2 := json.Unmarshal(body, &wrapper); err2 != nil {
			return nil, fmt.Errorf("invalid json payload: %w", err)
		}
		rows = wrapper.Projects
	}
	return s.batchImportProjects(ctx, nsRef, rows, "HISTORICAL_IMPORT")
}

func (s *Service) batchImportProjects(ctx context.Context, nsRef string, rows []LibraryProjectRow, source string) (*LibraryBatchImportResult, error) {
	nsRef = normalizeNamespaceRef(nsRef)
	if nsRef == "" {
		return nil, fmt.Errorf("namespace is required")
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no projects to import")
	}
	out := &LibraryBatchImportResult{
		NamespaceRef: nsRef,
		Total:        len(rows),
		Results:      make([]LibraryRowResult, 0, len(rows)),
	}
	for i, row := range rows {
		item, err := s.registerLibraryProject(ctx, nsRef, row, source)
		res := LibraryRowResult{
			Row:         i + 2,
			ProjectName: row.ProjectName,
		}
		if err != nil {
			res.Status = "failed"
			res.Error = err.Error()
			out.Failed++
		} else {
			res.Status = "success"
			res.AchievementRef = item.AchievementRef
			res.ReceiptCount = item.ReceiptCount
			out.Success++
			out.ReceiptsCreated += item.ReceiptCount
		}
		out.Results = append(out.Results, res)
	}
	return out, nil
}

type registerLibraryResult struct {
	AchievementRef string
	ReceiptCount   int
}

func (s *Service) registerLibraryProject(ctx context.Context, nsRef string, row LibraryProjectRow, source string) (*registerLibraryResult, error) {
	db, err := s.storeDB()
	if err != nil {
		return nil, err
	}

	projectName := strings.TrimSpace(row.ProjectName)
	ownerName := strings.TrimSpace(row.OwnerName)
	if projectName == "" || ownerName == "" {
		return nil, fmt.Errorf("project_name and owner_name are required")
	}
	projectType := normalizeProjectType(row.ProjectType)
	if projectType == "" {
		projectType = "OTHER"
	}
	year := row.CompletedYear
	if year <= 0 {
		year = time.Now().Year()
	}
	seq, err := s.nextAchievementSeq(ctx, db, nsRef, projectType, year)
	if err != nil {
		return nil, err
	}
	achievementRef := fmt.Sprintf("%s/utxo/achievement/%s/%d/%03d", nsRef, strings.ToLower(projectType), year, seq)
	legacySPURef := "v://coordos/spu/achievement/library_import@v1"
	projectRef := fmt.Sprintf("%s/project/library/%d/%03d", nsRef, year, seq)
	executorRef := fmt.Sprintf("%s/executor/org/library@v1", nsRef)

	inputsHash := hashSHA256(
		projectName,
		ownerName,
		fmt.Sprintf("%.2f", row.ContractAmount),
		strconv.Itoa(year),
	)
	proofHash := hashSHA256(achievementRef, inputsHash, source, nsRef)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var completedAt any
	if strings.TrimSpace(row.CompletedAt) != "" {
		completedAt = strings.TrimSpace(row.CompletedAt)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO achievement_utxos (
			utxo_ref, ref, spu_ref, project_ref, executor_ref,
			namespace_ref, project_name, project_type, owner_name,
			region, scale, contract_amount, completed_year, completed_at,
			qual_ref, source, inputs_hash, proof_hash, status, tenant_id,
			created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,'ACTIVE',$19,NOW(),NOW())
		ON CONFLICT (ref) DO UPDATE SET
			utxo_ref=EXCLUDED.utxo_ref,
			spu_ref=EXCLUDED.spu_ref,
			project_ref=EXCLUDED.project_ref,
			executor_ref=EXCLUDED.executor_ref,
			project_name=EXCLUDED.project_name,
			owner_name=EXCLUDED.owner_name,
			project_type=EXCLUDED.project_type,
			region=EXCLUDED.region,
			scale=EXCLUDED.scale,
			contract_amount=EXCLUDED.contract_amount,
			completed_year=EXCLUDED.completed_year,
			completed_at=EXCLUDED.completed_at,
			qual_ref=EXCLUDED.qual_ref,
			source=EXCLUDED.source,
			inputs_hash=EXCLUDED.inputs_hash,
			proof_hash=EXCLUDED.proof_hash,
			status=EXCLUDED.status,
			updated_at=NOW()
	`,
		achievementRef,
		achievementRef,
		legacySPURef,
		projectRef,
		executorRef,
		nsRef,
		projectName,
		projectType,
		ownerName,
		nullText(row.Region),
		nullText(row.Scale),
		row.ContractAmount,
		year,
		completedAt,
		nullText(row.QualRef),
		source,
		inputsHash,
		proofHash,
		s.tenantID,
	); err != nil {
		return nil, fmt.Errorf("insert achievement failed: %w", err)
	}

	receiptsCreated := 0
	for i, eng := range row.Engineers {
		engineerID := strings.TrimSpace(eng.EngineerID)
		certType := strings.TrimSpace(eng.CertType)
		if engineerID == "" || certType == "" {
			continue
		}
		role := normalizeEngineerRole(eng.Role)
		executorRef := fmt.Sprintf("%s/executor/person/%s@v1", nsRef, engineerID)
		containerRef := fmt.Sprintf("%s/container/cert/%s/%s@v1",
			nsRef, strings.ToLower(strings.ReplaceAll(certType, "_", "-")), engineerID)
		receiptRef := fmt.Sprintf("%s/receipt/achievement/%s/%d/%05d", nsRef, engineerID, year, seq*100+i+1)
		engInputsHash := hashSHA256(achievementRef, executorRef, containerRef, role)
		engProofHash := hashSHA256(receiptRef, engInputsHash, source)

		engineerName := strings.TrimSpace(eng.EngineerName)
		if engineerName == "" {
			_ = tx.QueryRowContext(ctx,
				`SELECT COALESCE(name,'') FROM employees WHERE executor_ref=$1 LIMIT 1`,
				executorRef,
			).Scan(&engineerName)
		}
		if engineerName == "" {
			engineerName = engineerID
		}

		if _, err := tx.ExecContext(ctx, `
			INSERT INTO engineer_achievement_receipts (
				ref, achievement_ref, executor_ref, engineer_name, engineer_id,
				container_ref, role, contribution, inputs_hash, proof_hash,
				source, status, tenant_id, created_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'ACTIVE',$12,NOW())
			ON CONFLICT (ref) DO UPDATE SET
				achievement_ref=EXCLUDED.achievement_ref,
				executor_ref=EXCLUDED.executor_ref,
				engineer_name=EXCLUDED.engineer_name,
				engineer_id=EXCLUDED.engineer_id,
				container_ref=EXCLUDED.container_ref,
				role=EXCLUDED.role,
				contribution=EXCLUDED.contribution,
				inputs_hash=EXCLUDED.inputs_hash,
				proof_hash=EXCLUDED.proof_hash,
				source=EXCLUDED.source,
				status=EXCLUDED.status
		`,
			receiptRef,
			achievementRef,
			executorRef,
			engineerName,
			engineerID,
			containerRef,
			role,
			nullText(eng.Contribution),
			engInputsHash,
			engProofHash,
			source,
			s.tenantID,
		); err != nil {
			return nil, fmt.Errorf("insert engineer receipt failed: %w", err)
		}
		receiptsCreated++
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &registerLibraryResult{
		AchievementRef: achievementRef,
		ReceiptCount:   receiptsCreated,
	}, nil
}

func (s *Service) QueryAchievementPool(ctx context.Context, in LibraryQueryFilter) ([]LibraryAchievementItem, int, error) {
	db, err := s.storeDB()
	if err != nil {
		return nil, 0, err
	}
	nsRef := normalizeNamespaceRef(in.NamespaceRef)
	if nsRef == "" {
		return nil, 0, fmt.Errorf("namespace is required")
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	offset := in.Offset
	if offset < 0 {
		offset = 0
	}

	conds := []string{"namespace_ref = $1"}
	args := []any{nsRef}
	argN := 2
	if v := normalizeProjectType(in.ProjectType); v != "" {
		conds = append(conds, fmt.Sprintf("project_type = $%d", argN))
		args = append(args, v)
		argN++
	}
	if in.MinAmount > 0 {
		conds = append(conds, fmt.Sprintf("contract_amount >= $%d", argN))
		args = append(args, in.MinAmount)
		argN++
	}
	if in.Within3Years {
		conds = append(conds, "within_3years = TRUE")
	} else if in.Within5Years {
		conds = append(conds, "within_5years = TRUE")
	}
	if src := strings.TrimSpace(strings.ToUpper(in.Source)); src != "" {
		conds = append(conds, fmt.Sprintf("source = $%d", argN))
		args = append(args, src)
		argN++
	}

	whereClause := strings.Join(conds, " AND ")
	var total int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM achievement_pool WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count achievement_pool failed: %w", err)
	}

	args = append(args, limit, offset)
	q := fmt.Sprintf(`
		SELECT ref, namespace_ref, project_name, project_type, owner_name,
		       COALESCE(region,''), COALESCE(scale,''), COALESCE(contract_amount,0),
		       COALESCE(completed_year,0), COALESCE(source,''), COALESCE(proof_hash,''),
		       COALESCE(status,''), COALESCE(within_3years,FALSE), COALESCE(within_5years,FALSE),
		       COALESCE(engineer_count,0), COALESCE(lead_engineer,'')
		FROM achievement_pool
		WHERE %s
		ORDER BY completed_year DESC, contract_amount DESC
		LIMIT $%d OFFSET $%d
	`, whereClause, argN, argN+1)

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query achievement_pool failed: %w", err)
	}
	defer rows.Close()

	items := make([]LibraryAchievementItem, 0, limit)
	for rows.Next() {
		var item LibraryAchievementItem
		if err := rows.Scan(
			&item.Ref,
			&item.NamespaceRef,
			&item.ProjectName,
			&item.ProjectType,
			&item.OwnerName,
			&item.Region,
			&item.Scale,
			&item.ContractAmount,
			&item.CompletedYear,
			&item.Source,
			&item.ProofHash,
			&item.Status,
			&item.Within3Years,
			&item.Within5Years,
			&item.EngineerCount,
			&item.LeadEngineer,
		); err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

func (s *Service) QueryEngineerAchievementPool(ctx context.Context, nsRef, engineerID, projectType string, within3Years bool, limit, offset int) ([]LibraryEngineerAchievementItem, int, error) {
	db, err := s.storeDB()
	if err != nil {
		return nil, 0, err
	}
	nsRef = normalizeNamespaceRef(nsRef)
	engineerID = strings.TrimSpace(engineerID)
	if nsRef == "" || engineerID == "" {
		return nil, 0, fmt.Errorf("namespace and engineer id are required")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}

	executorRef := fmt.Sprintf("%s/executor/person/%s@v1", nsRef, engineerID)
	conds := []string{"executor_ref = $1", "engineer_id = $2"}
	args := []any{executorRef, engineerID}
	argN := 3
	if pt := normalizeProjectType(projectType); pt != "" {
		conds = append(conds, fmt.Sprintf("project_type = $%d", argN))
		args = append(args, pt)
		argN++
	}
	if within3Years {
		conds = append(conds, "within_3years = TRUE")
	}
	whereClause := strings.Join(conds, " AND ")

	var total int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM engineer_achievement_pool WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count engineer_achievement_pool failed: %w", err)
	}

	args = append(args, limit, offset)
	q := fmt.Sprintf(`
		SELECT ref, achievement_ref, executor_ref, engineer_id, engineer_name,
		       container_ref, role, COALESCE(contribution,''), source, proof_hash,
		       project_name, project_type, owner_name, COALESCE(contract_amount,0),
		       COALESCE(completed_year,0), COALESCE(within_3years,FALSE)
		FROM engineer_achievement_pool
		WHERE %s
		ORDER BY completed_year DESC, contract_amount DESC
		LIMIT $%d OFFSET $%d
	`, whereClause, argN, argN+1)

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query engineer_achievement_pool failed: %w", err)
	}
	defer rows.Close()

	items := make([]LibraryEngineerAchievementItem, 0, limit)
	for rows.Next() {
		var item LibraryEngineerAchievementItem
		if err := rows.Scan(
			&item.Ref,
			&item.AchievementRef,
			&item.ExecutorRef,
			&item.EngineerID,
			&item.EngineerName,
			&item.ContainerRef,
			&item.Role,
			&item.Contribution,
			&item.Source,
			&item.ProofHash,
			&item.ProjectName,
			&item.ProjectType,
			&item.OwnerName,
			&item.ContractAmount,
			&item.CompletedYear,
			&item.Within3Years,
		); err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

func (s *Service) VerifyLibraryRef(ctx context.Context, ref string) (map[string]any, error) {
	db, err := s.storeDB()
	if err != nil {
		return nil, err
	}
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, fmt.Errorf("ref is required")
	}
	canonicalRef := s.resolveRefAlias(ctx, db, ref)

	var project struct {
		Ref          string
		UTXORef      string
		NamespaceRef string
		SPURef       string
		ProjectRef   string
		ExecutorRef  string
		InputsHash   string
		Source       string
		ProofHash    string
		Status       string
		ProjectName  string
		ProjectType  string
		OwnerName    string
		Payload      json.RawMessage
	}
	err = db.QueryRowContext(ctx, `
		SELECT
			COALESCE(ref,''),
			COALESCE(utxo_ref,''),
			COALESCE(namespace_ref,''),
			COALESCE(spu_ref,''),
			COALESCE(project_ref,''),
			COALESCE(executor_ref,''),
			COALESCE(inputs_hash,''),
			COALESCE(source,''),
			COALESCE(proof_hash,''),
			COALESCE(status,''),
			COALESCE(project_name,''),
			COALESCE(project_type,''),
			COALESCE(owner_name,''),
			COALESCE(payload,'{}'::jsonb)
		FROM achievement_utxos
		WHERE ref=$1 OR utxo_ref=$1 OR ref=$2 OR utxo_ref=$2
		LIMIT 1
	`, ref, canonicalRef).Scan(
		&project.Ref,
		&project.UTXORef,
		&project.NamespaceRef,
		&project.SPURef,
		&project.ProjectRef,
		&project.ExecutorRef,
		&project.InputsHash,
		&project.Source,
		&project.ProofHash,
		&project.Status,
		&project.ProjectName,
		&project.ProjectType,
		&project.OwnerName,
		&project.Payload,
	)
	if err == nil {
		resolvedRef := strings.TrimSpace(project.Ref)
		if resolvedRef == "" {
			resolvedRef = strings.TrimSpace(project.UTXORef)
		}

		candidates := make([]string, 0, 6)
		if resolvedRef != "" && project.InputsHash != "" && project.Source != "" && project.NamespaceRef != "" {
			candidates = append(candidates, hashSHA256(resolvedRef, project.InputsHash, project.Source, project.NamespaceRef))
		}
		if project.SPURef != "" && project.ProjectRef != "" && project.ExecutorRef != "" {
			candidates = append(candidates, ComputeProofHash(project.SPURef, project.ProjectRef, project.ExecutorRef, project.Payload))
		}
		if project.SPURef != "" && project.ProjectRef != "" && project.ExecutorRef != "" && resolvedRef != "" {
			rawNS := extractNamespaceFromRef(resolvedRef)
			if rawNS != "" {
				candidates = append(candidates, ComputeProofHashWithNamespace(project.SPURef, project.ProjectRef, project.ExecutorRef, rawNS, project.Payload))
			}
		}
		if project.SPURef != "" && project.ProjectRef != "" && project.ExecutorRef != "" && project.NamespaceRef != "" {
			ns := strings.TrimSpace(project.NamespaceRef)
			candidates = append(candidates, ComputeProofHashWithNamespace(project.SPURef, project.ProjectRef, project.ExecutorRef, ns, project.Payload))
			candidates = append(candidates, ComputeProofHashWithNamespace(project.SPURef, project.ProjectRef, project.ExecutorRef, strings.TrimPrefix(ns, "v://"), project.Payload))
		}

		recomputed := ""
		verified := false
		for _, candidate := range candidates {
			if candidate == "" {
				continue
			}
			if recomputed == "" {
				recomputed = candidate
			}
			if hashEquals(project.ProofHash, candidate) {
				recomputed = candidate
				verified = true
				break
			}
		}

		return map[string]any{
			"ref":             resolvedRef,
			"query_ref":       ref,
			"canonical_ref":   canonicalRef,
			"utxo_ref":        project.UTXORef,
			"type":            "achievement_utxo",
			"project_name":    project.ProjectName,
			"project_type":    project.ProjectType,
			"owner_name":      project.OwnerName,
			"source":          project.Source,
			"status":          project.Status,
			"proof_hash":      project.ProofHash,
			"recomputed_hash": recomputed,
			"verified":        verified,
		}, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	var receipt struct {
		Ref            string
		AchievementRef string
		ExecutorRef    string
		ContainerRef   string
		Role           string
		InputsHash     string
		Source         string
		ProofHash      string
		Status         string
		EngineerID     string
		EngineerName   string
	}
	err = db.QueryRowContext(ctx, `
		SELECT ref, achievement_ref, executor_ref, container_ref, COALESCE(role,''), COALESCE(inputs_hash,''),
		       COALESCE(source,''), COALESCE(proof_hash,''), COALESCE(status,''), COALESCE(engineer_id,''), COALESCE(engineer_name,'')
		FROM engineer_achievement_receipts
		WHERE ref=$1 OR ref=$2
		LIMIT 1
	`, ref, canonicalRef).Scan(
		&receipt.Ref,
		&receipt.AchievementRef,
		&receipt.ExecutorRef,
		&receipt.ContainerRef,
		&receipt.Role,
		&receipt.InputsHash,
		&receipt.Source,
		&receipt.ProofHash,
		&receipt.Status,
		&receipt.EngineerID,
		&receipt.EngineerName,
	)
	if err == nil {
		recomputedInputs := hashSHA256(receipt.AchievementRef, receipt.ExecutorRef, receipt.ContainerRef, receipt.Role)
		recomputedProof := hashSHA256(receipt.Ref, recomputedInputs, receipt.Source)
		return map[string]any{
			"ref":                    receipt.Ref,
			"type":                   "engineer_achievement_receipt",
			"engineer_id":            receipt.EngineerID,
			"engineer_name":          receipt.EngineerName,
			"achievement_ref":        receipt.AchievementRef,
			"executor_ref":           receipt.ExecutorRef,
			"container_ref":          receipt.ContainerRef,
			"role":                   receipt.Role,
			"source":                 receipt.Source,
			"status":                 receipt.Status,
			"proof_hash":             receipt.ProofHash,
			"inputs_hash":            receipt.InputsHash,
			"recomputed_inputs_hash": recomputedInputs,
			"recomputed_hash":        recomputedProof,
			"verified":               receipt.ProofHash == recomputedProof && receipt.InputsHash == recomputedInputs,
		}, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}
	return nil, fmt.Errorf("ref not found: %s", ref)
}

func (s *Service) resolveRefAlias(ctx context.Context, db *sql.DB, ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ref
	}
	var canonical string
	err := db.QueryRowContext(ctx, `
		SELECT canonical_ref
		FROM ref_aliases
		WHERE tenant_id=$1
		  AND alias_ref=$2
		  AND status='ACTIVE'
		ORDER BY id DESC
		LIMIT 1
	`, s.tenantID, ref).Scan(&canonical)
	if err != nil || strings.TrimSpace(canonical) == "" {
		return ref
	}
	return strings.TrimSpace(canonical)
}

func GenerateAchievementCSVTemplate() string {
	return strings.Join([]string{
		"项目名称,业主单位,项目类型,合同金额(万元),完工年份,地区,规模,工程师ID,工程师姓名,证书类型,角色,承担工作,完工日期,资质引用",
		"陕西榆林绥德至延川高速,陕西省交通运输厅,HIGHWAY,45000,2023,陕西榆林,双向四车道,cyp4310,陈一平,REG_STRUCTURE,DESIGN_LEAD,负责桥梁结构设计,2023-12-30,v://cn.zhongbei/container/cert/qual-highway-a@v1",
		",,,,,,,lz0012,李志,REG_CIVIL_GEOTEC,PARTICIPANT,负责路基岩土勘察,,",
		"榆林市道路改造,榆林市城投,MUNICIPAL,3200,2022,陕西榆林,城市主干道改造,dyc4019,杜永春,REG_STRUCTURE,LEAD_ENGINEER,主持全过程设计,2022-08-18,v://cn.zhongbei/container/cert/qual-municipal-a@v1",
	}, "\n")
}

func ParseAchievementCSV(r io.Reader) ([]LibraryProjectRow, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read csv header failed: %w", err)
	}

	index := map[string]int{}
	for i, raw := range header {
		key := normalizeCSVHeader(raw)
		if key != "" {
			index[key] = i
		}
	}

	required := []string{"project_name", "owner_name", "project_type", "contract_amount", "completed_year", "engineer_id", "cert_type"}
	// Fallback: if header aliases were not matched (e.g. encoding differences),
	// use canonical template order by column position.
	if !hasRequiredCSVColumns(index, required) && len(header) >= 11 {
		ordered := []string{
			"project_name", "owner_name", "project_type", "contract_amount", "completed_year",
			"region", "scale", "engineer_id", "engineer_name", "cert_type",
			"role", "contribution", "completed_at", "qual_ref",
		}
		for i, key := range ordered {
			if i >= len(header) {
				break
			}
			if _, ok := index[key]; !ok {
				index[key] = i
			}
		}
	}
	for _, k := range required {
		if _, ok := index[k]; !ok {
			return nil, fmt.Errorf("csv missing required column: %s", k)
		}
	}

	get := func(row []string, key string) string {
		i, ok := index[key]
		if !ok || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}

	projects := make([]LibraryProjectRow, 0, 64)
	var current *LibraryProjectRow
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read csv row failed: %w", err)
		}
		if isEmptyCSVRecord(rec) {
			continue
		}

		projectName := get(rec, "project_name")
		if projectName != "" {
			if current != nil {
				projects = append(projects, *current)
			}
			current = &LibraryProjectRow{
				ProjectName:    projectName,
				OwnerName:      get(rec, "owner_name"),
				ProjectType:    normalizeProjectType(get(rec, "project_type")),
				ContractAmount: parseFloat(get(rec, "contract_amount")),
				CompletedYear:  parseInt(get(rec, "completed_year")),
				CompletedAt:    get(rec, "completed_at"),
				Region:         get(rec, "region"),
				Scale:          get(rec, "scale"),
				QualRef:        get(rec, "qual_ref"),
			}
		}

		if current == nil {
			continue
		}

		engineerID := get(rec, "engineer_id")
		certType := get(rec, "cert_type")
		if engineerID != "" && certType != "" {
			current.Engineers = append(current.Engineers, LibraryEngineerParticipant{
				EngineerID:   engineerID,
				EngineerName: get(rec, "engineer_name"),
				CertType:     certType,
				Role:         normalizeEngineerRole(get(rec, "role")),
				Contribution: get(rec, "contribution"),
			})
		}
	}
	if current != nil {
		projects = append(projects, *current)
	}

	out := make([]LibraryProjectRow, 0, len(projects))
	for _, p := range projects {
		if strings.TrimSpace(p.ProjectName) == "" || strings.TrimSpace(p.OwnerName) == "" {
			continue
		}
		if p.CompletedYear == 0 {
			p.CompletedYear = time.Now().Year()
		}
		if p.ProjectType == "" {
			p.ProjectType = "OTHER"
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no valid project rows in csv")
	}
	return out, nil
}

func (s *Service) storeDB() (*sql.DB, error) {
	pg, ok := s.store.(*PGStore)
	if !ok || pg == nil || pg.db == nil {
		return nil, fmt.Errorf("achievement store does not expose postgres db")
	}
	return pg.db, nil
}

func (s *Service) nextAchievementSeq(ctx context.Context, db *sql.DB, nsRef, projectType string, year int) (int, error) {
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM achievement_utxos
		WHERE namespace_ref=$1 AND project_type=$2 AND completed_year=$3
	`, nsRef, projectType, year).Scan(&count); err != nil {
		return 0, err
	}
	return count + 1, nil
}

func normalizeNamespaceRef(ns string) string {
	ns = strings.TrimSpace(ns)
	if ns == "" {
		return ""
	}
	if strings.HasPrefix(ns, "v://") {
		return ns
	}
	return "v://" + ns
}

func normalizeProjectType(v string) string {
	v = strings.TrimSpace(strings.ToUpper(v))
	if v == "" {
		return ""
	}
	switch v {
	case "HIGHWAY", "BRIDGE", "MUNICIPAL", "ARCH", "WATER", "LANDSCAPE", "TUNNEL", "OTHER":
		return v
	case "公路":
		return "HIGHWAY"
	case "桥梁":
		return "BRIDGE"
	case "市政":
		return "MUNICIPAL"
	case "建筑":
		return "ARCH"
	case "水利":
		return "WATER"
	case "园林":
		return "LANDSCAPE"
	case "隧道":
		return "TUNNEL"
	default:
		return "OTHER"
	}
}

func normalizeEngineerRole(v string) string {
	v = strings.TrimSpace(strings.ToUpper(v))
	switch v {
	case "LEAD_ENGINEER", "DESIGN_LEAD", "PARTICIPANT", "REVIEWER":
		return v
	case "项目负责人":
		return "LEAD_ENGINEER"
	case "专业负责人":
		return "DESIGN_LEAD"
	case "参与":
		return "PARTICIPANT"
	case "审核", "审定":
		return "REVIEWER"
	default:
		return "PARTICIPANT"
	}
}

func normalizeCSVHeader(raw string) string {
	key := strings.TrimSpace(raw)
	switch key {
	case "项目名称", "工程名称", "project_name":
		return "project_name"
	case "业主单位", "建设单位", "owner_name":
		return "owner_name"
	case "项目类型", "工程类型", "project_type":
		return "project_type"
	case "合同金额(万元)", "合同金额", "金额", "contract_amount":
		return "contract_amount"
	case "完工年份", "竣工年份", "completed_year":
		return "completed_year"
	case "完工日期", "竣工日期", "completed_at":
		return "completed_at"
	case "地区", "所在地区", "region":
		return "region"
	case "规模", "工程规模", "scale":
		return "scale"
	case "工程师ID", "人员ID", "engineer_id":
		return "engineer_id"
	case "工程师姓名", "姓名", "engineer_name":
		return "engineer_name"
	case "证书类型", "注册类别", "cert_type":
		return "cert_type"
	case "角色", "参与角色", "role":
		return "role"
	case "承担工作", "工作内容", "contribution":
		return "contribution"
	case "资质引用", "qual_ref":
		return "qual_ref"
	default:
		return ""
	}
}

func hashSHA256(parts ...string) string {
	joined := strings.Join(parts, "")
	sum := sha256.Sum256([]byte(joined))
	return fmt.Sprintf("sha256:%x", sum[:])
}

func nullText(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return strings.TrimSpace(s)
}

func isEmptyCSVRecord(rec []string) bool {
	for _, v := range rec {
		if strings.TrimSpace(v) != "" {
			return false
		}
	}
	return true
}

func parseFloat(s string) float64 {
	s = strings.TrimSpace(strings.ReplaceAll(s, ",", ""))
	if s == "" {
		return 0
	}
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

func parseInt(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	n, _ := strconv.Atoi(s)
	return n
}

func hasRequiredCSVColumns(index map[string]int, required []string) bool {
	for _, k := range required {
		if _, ok := index[k]; !ok {
			return false
		}
	}
	return true
}

func sortProjectRows(rows []LibraryProjectRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].CompletedYear == rows[j].CompletedYear {
			return rows[i].ProjectName < rows[j].ProjectName
		}
		return rows[i].CompletedYear > rows[j].CompletedYear
	})
}
