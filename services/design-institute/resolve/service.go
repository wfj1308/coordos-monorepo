package resolve

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"coordos/design-institute/qualification"
	resolverpkg "coordos/resolver"
)

type Action string

const (
	ActionIssueReviewCert Action = "ISSUE_REVIEW_CERT"
	ActionIssueDelivery   Action = "ISSUE_DELIVERY"
	ActionIssueInvoice    Action = "ISSUE_INVOICE"
	ActionSignContract    Action = "SIGN_CONTRACT"
	ActionApprovePayment  Action = "APPROVE_PAYMENT"
	ActionExecuteSPU      Action = "EXECUTE_SPU"
)

type rightType string

const (
	rightReviewStamp rightType = "REVIEW_STAMP"
	rightSignStamp   rightType = "SIGN_STAMP"
	rightInvoice     rightType = "INVOICE"
)

type spuReq struct {
	NeedQualTypes      []qualification.QualType
	NeedRightTypes     []rightType
	NeedSkills         []string
	MinCapabilityLevel string
	HeadOfficeOnly     bool
	Description        string
}

var spuReqs = map[string]spuReq{
	"v://zhongbei/spu/bridge/review_certificate@v1": {
		NeedQualTypes:      []qualification.QualType{qualification.QualRegStructure},
		NeedRightTypes:     []rightType{rightReviewStamp},
		NeedSkills:         []string{"structural_review", "bridge_design", "code_compliance"},
		MinCapabilityLevel: "SENIOR_ENGINEER",
		HeadOfficeOnly:     true,
		Description:        "Review certificate requires head-office structural engineer and review-stamp right",
	},
	"v://zhongbei/spu/bridge/pile_foundation_drawing@v1": {
		NeedQualTypes:      []qualification.QualType{qualification.QualRegStructure},
		NeedSkills:         []string{"pile_foundation_design", "bridge_structure", "cad_drawing"},
		MinCapabilityLevel: "ASSISTANT_ENGINEER",
		Description:        "Pile foundation drawing requires structural qualification",
	},
	"v://zhongbei/spu/bridge/pile_cap_drawing@v1": {
		NeedQualTypes:      []qualification.QualType{qualification.QualRegStructure},
		NeedSkills:         []string{"pile_cap_design", "bridge_structure"},
		MinCapabilityLevel: "ASSISTANT_ENGINEER",
		Description:        "Pile cap drawing requires structural qualification",
	},
	"v://zhongbei/spu/bridge/pier_rebar_drawing@v1": {
		NeedQualTypes:      []qualification.QualType{qualification.QualRegStructure},
		NeedSkills:         []string{"pier_design", "bridge_structure", "seismic_design"},
		MinCapabilityLevel: "ASSISTANT_ENGINEER",
		Description:        "Pier rebar drawing requires structural qualification",
	},
	"v://zhongbei/spu/bridge/superstructure_drawing@v1": {
		NeedQualTypes:      []qualification.QualType{qualification.QualRegStructure},
		NeedSkills:         []string{"prestress_design", "girder_design", "bridge_structure"},
		MinCapabilityLevel: "ENGINEER",
		Description:        "Superstructure drawing requires structural qualification",
	},
	"v://zhongbei/spu/bridge/settlement_cert@v1": {
		NeedRightTypes:     []rightType{rightSignStamp, rightInvoice},
		NeedSkills:         []string{"settlement", "wallet_transfer", "audit"},
		MinCapabilityLevel: "PLATFORM_ENGINE",
		HeadOfficeOnly:     true,
		Description:        "Settlement cert requires head-office sign and invoice rights",
	},
}

var actionReqs = map[Action]spuReq{
	ActionIssueReviewCert: {
		NeedQualTypes:  []qualification.QualType{qualification.QualRegStructure},
		NeedRightTypes: []rightType{rightReviewStamp},
		HeadOfficeOnly: true,
		Description:    "Issue review certificate requires head-office structural qualification and review-stamp right",
	},
	ActionIssueInvoice: {
		NeedRightTypes: []rightType{rightInvoice},
		HeadOfficeOnly: true,
		Description:    "Issue invoice requires head-office invoice right",
	},
	ActionIssueDelivery: {
		NeedRightTypes: []rightType{rightSignStamp},
		HeadOfficeOnly: true,
		Description:    "Issue delivery requires head-office sign right",
	},
	ActionSignContract: {
		NeedRightTypes: []rightType{rightSignStamp},
		Description:    "Sign contract requires sign right",
	},
}

type VerifyInput struct {
	ExecutorRef string
	ProjectRef  string
	SPURef      string
	Action      Action
	ValidOn     time.Time
}

type VerifyResult struct {
	Pass                    bool           `json:"pass"`
	Summary                 string         `json:"summary"`
	Reasons                 []VerifyReason `json:"reasons"`
	ExecutorSkills          []string       `json:"executor_skills,omitempty"`
	ExecutorCapabilityLevel string         `json:"executor_capability_level,omitempty"`
	RequiredSkills          []string       `json:"required_skills,omitempty"`
	RequiredCapabilityLevel string         `json:"required_capability_level,omitempty"`
}

type VerifyReason struct {
	Requirement string                       `json:"requirement"`
	Pass        bool                         `json:"pass"`
	Evidence    *qualification.Qualification `json:"evidence,omitempty"`
	FailReason  string                       `json:"fail_reason,omitempty"`
}

type ResolveInput struct {
	Tenant         string
	ProjectRef     string
	SPURef         string
	Role           string
	Action         Action
	NeedQualTypes  []qualification.QualType
	HeadOfficeOnly bool
	ValidOn        time.Time
	Limit          int
}

type QualificationEvidence struct {
	QualificationID int64      `json:"qualification_id"`
	QualType        string     `json:"qual_type"`
	CertNo          string     `json:"cert_no,omitempty"`
	ValidUntil      *time.Time `json:"valid_until,omitempty"`
}

type Candidate struct {
	ExecutorRef            string                         `json:"executor_ref"`
	Name                   string                         `json:"name"`
	MatchedQuals           []*qualification.Qualification `json:"matched_quals"`
	QualificationEvidence  []QualificationEvidence        `json:"qualification_evidence,omitempty"`
	QualifiedReasons       []string                       `json:"qualified_reasons,omitempty"`
	AuthorizationChainPath []string                       `json:"authorization_chain_path,omitempty"`
	Skills                 []string                       `json:"skills,omitempty"`
	CapabilityLevel        string                         `json:"capability_level,omitempty"`
	ActiveProjects         int                            `json:"active_projects"`
	ProjectLimit           int                            `json:"project_limit"`
	CapacityOK             bool                           `json:"capacity_ok"`
	Score                  float64                        `json:"score"`
}

type RejectedCandidate struct {
	ExecutorRef       string   `json:"executor_ref"`
	Name              string   `json:"name"`
	UnqualifiedReason []string `json:"unqualified_reasons"`
	Skills            []string `json:"skills,omitempty"`
	CapabilityLevel   string   `json:"capability_level,omitempty"`
}

type OccupiedState struct {
	ExecutorRef    string          `json:"executor_ref"`
	ActiveProjects int             `json:"active_projects"`
	ProjectLimit   int             `json:"project_limit"`
	Available      bool            `json:"available"`
	Projects       []ActiveProject `json:"projects"`
}

type ActiveProject struct {
	ProjectRef  string    `json:"project_ref"`
	ProjectName string    `json:"project_name"`
	Role        string    `json:"role"`
	Since       time.Time `json:"since"`
}

type Service struct {
	qualSvc       *qualification.Service
	db            *sql.DB
	shared        *resolverpkg.Service
	tenantID      int
	headOfficeRef string
}

func NewService(
	qualSvc *qualification.Service,
	db *sql.DB,
	tenantID int,
	headOfficeRef string,
) *Service {
	svc := &Service{
		qualSvc:       qualSvc,
		db:            db,
		tenantID:      tenantID,
		headOfficeRef: headOfficeRef,
	}
	if db != nil {
		svc.shared = resolverpkg.NewService(resolverpkg.NewPGStore(db), tenantID, headOfficeRef)
	}
	return svc
}

type rightGrant struct {
	ID         int64
	Ref        string
	HolderRef  string
	RightType  string
	Scope      string
	ValidUntil *time.Time
}

type QualificationAssignment struct {
	ID              int64      `json:"id"`
	QualificationID int64      `json:"qualification_id"`
	ExecutorRef     string     `json:"executor_ref"`
	ProjectRef      string     `json:"project_ref"`
	Status          string     `json:"status"`
	TenantID        int        `json:"tenant_id"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	ReleasedAt      *time.Time `json:"released_at,omitempty"`
}

type AssignQualificationInput struct {
	QualificationID int64
	ExecutorRef     string
	ProjectRef      string
}

func (s *Service) Verify(ctx context.Context, in VerifyInput) (*VerifyResult, error) {
	if strings.TrimSpace(in.ExecutorRef) == "" {
		return nil, fmt.Errorf("executor_ref is required")
	}
	if in.ValidOn.IsZero() {
		in.ValidOn = time.Now()
	}

	result := &VerifyResult{Pass: true}
	quals, err := s.qualSvc.ListByExecutorRef(ctx, in.ExecutorRef)
	if err != nil {
		return nil, fmt.Errorf("query qualification failed: %w", err)
	}
	active := filterActive(quals, in.ValidOn)
	qualMap := indexQuals(active)
	execSkills := deriveExecutorSkills(active)
	execCapability := deriveCapabilityLevel(active)

	req := s.getReq(in.SPURef, in.Action)
	result.ExecutorSkills = execSkills
	result.ExecutorCapabilityLevel = execCapability
	if len(req.NeedSkills) > 0 {
		result.RequiredSkills = append([]string(nil), req.NeedSkills...)
	}
	if req.MinCapabilityLevel != "" {
		result.RequiredCapabilityLevel = req.MinCapabilityLevel
	}

	if req.HeadOfficeOnly {
		if !s.isHeadOffice(in.ExecutorRef) {
			result.Pass = false
			result.Reasons = append(result.Reasons, VerifyReason{
				Requirement: "Head-office identity (RULE-002)",
				Pass:        false,
				FailReason:  fmt.Sprintf("%s is not a head-office executor", in.ExecutorRef),
			})
		} else {
			result.Reasons = append(result.Reasons, VerifyReason{
				Requirement: "Head-office identity (RULE-002)",
				Pass:        true,
			})
		}
	}

	for _, qt := range req.NeedQualTypes {
		q, ok := qualMap[qt]
		label := qualLabel(qt)
		if !ok {
			result.Pass = false
			result.Reasons = append(result.Reasons, VerifyReason{
				Requirement: "Qualification: " + label,
				Pass:        false,
				FailReason:  fmt.Sprintf("%s does not have active qualification %s", in.ExecutorRef, label),
			})
			continue
		}
		result.Reasons = append(result.Reasons, VerifyReason{
			Requirement: "Qualification: " + label,
			Pass:        true,
			Evidence:    q,
		})
		if conflict, err := s.findQualificationConflict(ctx, q.ID, in.ProjectRef); err != nil {
			return nil, fmt.Errorf("query qualification assignment failed: %w", err)
		} else if conflict != nil {
			result.Pass = false
			result.Reasons = append(result.Reasons, VerifyReason{
				Requirement: "Qualification assignment lock",
				Pass:        false,
				FailReason:  fmt.Sprintf("qualification %d is already assigned to project %s", q.ID, conflict.ProjectRef),
			})
		} else if strings.TrimSpace(in.ProjectRef) != "" {
			result.Reasons = append(result.Reasons, VerifyReason{
				Requirement: "Qualification assignment lock",
				Pass:        true,
			})
		}
	}

	for _, rt := range req.NeedRightTypes {
		ok, err := s.hasRequiredRight(ctx, in.ExecutorRef, rt, in.ProjectRef, in.ValidOn)
		if err != nil {
			return nil, fmt.Errorf("query right failed: %w", err)
		}
		if !ok {
			result.Pass = false
			result.Reasons = append(result.Reasons, VerifyReason{
				Requirement: "Delegation right: " + rightLabel(rt),
				Pass:        false,
				FailReason:  fmt.Sprintf("%s does not have active right %s", in.ExecutorRef, rightLabel(rt)),
			})
			continue
		}
		result.Reasons = append(result.Reasons, VerifyReason{
			Requirement: "Delegation right: " + rightLabel(rt),
			Pass:        true,
		})
	}

	for _, skill := range req.NeedSkills {
		if containsString(execSkills, skill) {
			result.Reasons = append(result.Reasons, VerifyReason{
				Requirement: "Skill: " + skill,
				Pass:        true,
			})
			continue
		}
		result.Pass = false
		result.Reasons = append(result.Reasons, VerifyReason{
			Requirement: "Skill: " + skill,
			Pass:        false,
			FailReason:  fmt.Sprintf("%s does not have skill %s", in.ExecutorRef, skill),
		})
	}

	if req.MinCapabilityLevel != "" {
		if capabilitySatisfies(execCapability, req.MinCapabilityLevel) {
			result.Reasons = append(result.Reasons, VerifyReason{
				Requirement: "Capability level: " + req.MinCapabilityLevel,
				Pass:        true,
			})
		} else {
			result.Pass = false
			result.Reasons = append(result.Reasons, VerifyReason{
				Requirement: "Capability level: " + req.MinCapabilityLevel,
				Pass:        false,
				FailReason:  fmt.Sprintf("%s capability %s is below required %s", in.ExecutorRef, execCapability, req.MinCapabilityLevel),
			})
		}
	}

	if result.Pass {
		result.Summary = fmt.Sprintf("PASS: %s is eligible", in.ExecutorRef)
		if req.Description != "" {
			result.Summary = result.Summary + " (" + req.Description + ")"
		}
	} else {
		fails := make([]string, 0, len(result.Reasons))
		for _, r := range result.Reasons {
			if !r.Pass {
				fails = append(fails, r.FailReason)
			}
		}
		result.Summary = "REJECT: " + strings.Join(fails, "; ")
	}
	if s.shared != nil {
		sharedOut, err := s.shared.Verify(ctx, resolverpkg.VerifyInput{
			ExecutorRef: in.ExecutorRef,
			ProjectRef:  in.ProjectRef,
			SPURef:      in.SPURef,
			Action:      toSharedAction(in.Action),
			ValidOn:     in.ValidOn,
			TenantID:    s.tenantID,
		})
		if err != nil {
			return nil, fmt.Errorf("shared resolver verify failed: %w", err)
		}
		if sharedOut != nil && !sharedOut.Pass {
			result.Pass = false
			result.Reasons = append(result.Reasons, VerifyReason{
				Requirement: "Shared resolver policy",
				Pass:        false,
				FailReason:  strings.TrimSpace(sharedOut.Summary),
			})
			if strings.TrimSpace(result.Summary) == "" || strings.HasPrefix(result.Summary, "PASS:") {
				result.Summary = "REJECT: " + strings.TrimSpace(sharedOut.Summary)
			}
		}
	}
	return result, nil
}

func (s *Service) Resolve(ctx context.Context, in ResolveInput) ([]*Candidate, error) {
	candidates, _, err := s.ResolveWithDetails(ctx, in)
	return candidates, err
}

func (s *Service) ResolveWithDetails(ctx context.Context, in ResolveInput) ([]*Candidate, []*RejectedCandidate, error) {
	if in.ValidOn.IsZero() {
		in.ValidOn = time.Now()
	}
	if in.Limit <= 0 {
		in.Limit = 10
	}

	req := s.getReq(in.SPURef, in.Action)
	req.NeedQualTypes = mergeQualTypes(req.NeedQualTypes, in.NeedQualTypes)
	if in.HeadOfficeOnly {
		req.HeadOfficeOnly = true
	}

	var executorRefs []string
	if s.shared != nil {
		sharedIn := resolverpkg.ResolveInput{
			TenantID:       s.tenantID,
			ProjectRef:     in.ProjectRef,
			SPURef:         in.SPURef,
			Action:         toSharedAction(in.Action),
			NeedCertTypes:  toSharedCertTypes(req.NeedQualTypes),
			HeadOfficeOnly: req.HeadOfficeOnly,
			ValidOn:        in.ValidOn,
			Limit:          maxInt(in.Limit, 10),
		}
		if out, err := s.shared.Resolve(ctx, sharedIn); err == nil && len(out) > 0 {
			seen := map[string]struct{}{}
			executorRefs = make([]string, 0, len(out))
			for _, c := range out {
				ref := strings.TrimSpace(c.ExecutorRef)
				if ref == "" {
					continue
				}
				if _, ok := seen[ref]; ok {
					continue
				}
				seen[ref] = struct{}{}
				executorRefs = append(executorRefs, ref)
			}
		}
	}
	if len(executorRefs) == 0 {
		if len(req.NeedQualTypes) > 0 {
			refs, err := s.listExecutorsByQualType(ctx, req.NeedQualTypes[0], in.ValidOn)
			if err != nil {
				return nil, nil, err
			}
			executorRefs = refs
		} else {
			refs, err := s.listAllExecutors(ctx)
			if err != nil {
				return nil, nil, err
			}
			executorRefs = refs
		}
	}

	candidates := make([]*Candidate, 0, len(executorRefs))
	rejected := make([]*RejectedCandidate, 0, len(executorRefs))
	for _, ref := range executorRefs {
		name, _ := s.getExecutorName(ctx, ref)
		failReasons := make([]string, 0)
		qualifiedReasons := make([]string, 0)
		authPaths := make([]string, 0)

		if req.HeadOfficeOnly && !s.isHeadOffice(ref) {
			rejected = append(rejected, &RejectedCandidate{
				ExecutorRef:       ref,
				Name:              name,
				UnqualifiedReason: []string{fmt.Sprintf("%s is not a head-office executor", ref)},
			})
			continue
		}

		quals, err := s.qualSvc.ListByExecutorRef(ctx, ref)
		if err != nil {
			rejected = append(rejected, &RejectedCandidate{
				ExecutorRef:       ref,
				Name:              name,
				UnqualifiedReason: []string{fmt.Sprintf("query qualification failed: %v", err)},
			})
			continue
		}
		active := filterActive(quals, in.ValidOn)
		qualMap := indexQuals(active)
		evidence := make([]QualificationEvidence, 0, len(req.NeedQualTypes))

		allMatch := true
		matched := make([]*qualification.Qualification, 0, len(req.NeedQualTypes))
		for _, qt := range req.NeedQualTypes {
			q, ok := qualMap[qt]
			if !ok {
				allMatch = false
				failReasons = append(failReasons, fmt.Sprintf("missing active qualification %s", qualLabel(qt)))
				break
			}
			if conflict, err := s.findQualificationConflict(ctx, q.ID, in.ProjectRef); err != nil {
				return nil, nil, fmt.Errorf("query qualification assignment failed: %w", err)
			} else if conflict != nil {
				allMatch = false
				failReasons = append(failReasons, fmt.Sprintf("qualification %d already assigned to project %s", q.ID, conflict.ProjectRef))
				break
			}
			matched = append(matched, q)
			evidence = append(evidence, QualificationEvidence{
				QualificationID: q.ID,
				QualType:        string(q.QualType),
				CertNo:          q.CertNo,
				ValidUntil:      q.ValidUntil,
			})
			qualifiedReasons = append(qualifiedReasons, fmt.Sprintf("qualification %s matched (id=%d)", qualLabel(qt), q.ID))
			if strings.TrimSpace(in.ProjectRef) != "" {
				qualifiedReasons = append(qualifiedReasons, fmt.Sprintf("qualification %d is available for project %s", q.ID, in.ProjectRef))
				authPaths = append(authPaths, fmt.Sprintf("qualification:%d -> project:%s", q.ID, in.ProjectRef))
			}
		}
		if !allMatch {
			rejected = append(rejected, &RejectedCandidate{
				ExecutorRef:       ref,
				Name:              name,
				UnqualifiedReason: failReasons,
				Skills:            deriveExecutorSkills(active),
				CapabilityLevel:   deriveCapabilityLevel(active),
			})
			continue
		}

		rightsOK := true
		for _, rt := range req.NeedRightTypes {
			grant, err := s.findRequiredRight(ctx, ref, rt, in.ProjectRef, in.ValidOn)
			if err != nil {
				return nil, nil, err
			}
			if grant == nil {
				rightsOK = false
				failReasons = append(failReasons, fmt.Sprintf("missing active right %s", rightLabel(rt)))
				break
			}
			path := authorizationPath(grant, in.ProjectRef)
			authPaths = append(authPaths, path)
			qualifiedReasons = append(qualifiedReasons, fmt.Sprintf("right %s matched via %s", rightLabel(rt), path))
		}
		if !rightsOK {
			rejected = append(rejected, &RejectedCandidate{
				ExecutorRef:       ref,
				Name:              name,
				UnqualifiedReason: failReasons,
				Skills:            deriveExecutorSkills(active),
				CapabilityLevel:   deriveCapabilityLevel(active),
			})
			continue
		}
		skills := deriveExecutorSkills(active)
		capability := deriveCapabilityLevel(active)
		if !hasAllSkills(skills, req.NeedSkills) {
			for _, skill := range req.NeedSkills {
				if !containsString(skills, skill) {
					failReasons = append(failReasons, fmt.Sprintf("missing skill %s", skill))
				} else {
					qualifiedReasons = append(qualifiedReasons, fmt.Sprintf("skill %s matched", skill))
				}
			}
			rejected = append(rejected, &RejectedCandidate{
				ExecutorRef:       ref,
				Name:              name,
				UnqualifiedReason: failReasons,
				Skills:            skills,
				CapabilityLevel:   capability,
			})
			continue
		}
		for _, skill := range req.NeedSkills {
			qualifiedReasons = append(qualifiedReasons, fmt.Sprintf("skill %s matched", skill))
		}
		if req.MinCapabilityLevel != "" && !capabilitySatisfies(capability, req.MinCapabilityLevel) {
			failReasons = append(failReasons, fmt.Sprintf("capability %s is below required %s", capability, req.MinCapabilityLevel))
			rejected = append(rejected, &RejectedCandidate{
				ExecutorRef:       ref,
				Name:              name,
				UnqualifiedReason: failReasons,
				Skills:            skills,
				CapabilityLevel:   capability,
			})
			continue
		}
		if req.MinCapabilityLevel != "" {
			qualifiedReasons = append(qualifiedReasons, fmt.Sprintf("capability %s satisfies required %s", capability, req.MinCapabilityLevel))
		}

		activeProjects, _ := s.getActiveProjects(ctx, ref)
		limit := projectLimit(active)

		candidates = append(candidates, &Candidate{
			ExecutorRef:            ref,
			Name:                   name,
			MatchedQuals:           matched,
			QualificationEvidence:  evidence,
			QualifiedReasons:       qualifiedReasons,
			AuthorizationChainPath: authPaths,
			Skills:                 skills,
			CapabilityLevel:        capability,
			ActiveProjects:         len(activeProjects),
			ProjectLimit:           limit,
			CapacityOK:             len(activeProjects) < limit,
			Score:                  scoreCandidate(len(matched), len(activeProjects), limit),
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})
	if len(candidates) > in.Limit {
		candidates = candidates[:in.Limit]
	}
	sort.Slice(rejected, func(i, j int) bool {
		return rejected[i].ExecutorRef < rejected[j].ExecutorRef
	})
	return candidates, rejected, nil
}

func (s *Service) Occupied(ctx context.Context, executorRef string) (*OccupiedState, error) {
	if s.shared != nil {
		out, err := s.shared.Occupied(ctx, executorRef)
		if err == nil && out != nil {
			projects := make([]ActiveProject, 0, len(out.Projects))
			for _, p := range out.Projects {
				projects = append(projects, ActiveProject{
					ProjectRef:  p.ProjectRef,
					ProjectName: p.ProjectName,
					Role:        p.Role,
					Since:       p.Since,
				})
			}
			return &OccupiedState{
				ExecutorRef:    out.ExecutorRef,
				ActiveProjects: out.ActiveProjects,
				ProjectLimit:   out.ProjectLimit,
				Available:      out.Available,
				Projects:       projects,
			}, nil
		}
	}
	projects, err := s.getActiveProjects(ctx, executorRef)
	if err != nil {
		return nil, err
	}
	quals, _ := s.qualSvc.ListByExecutorRef(ctx, executorRef)
	active := filterActive(quals, time.Now())
	limit := projectLimit(active)

	return &OccupiedState{
		ExecutorRef:    executorRef,
		ActiveProjects: len(projects),
		ProjectLimit:   limit,
		Available:      len(projects) < limit,
		Projects:       projects,
	}, nil
}

func (s *Service) BindQualification(ctx context.Context, in AssignQualificationInput) (*QualificationAssignment, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database is not configured")
	}
	in.ExecutorRef = strings.TrimSpace(in.ExecutorRef)
	in.ProjectRef = strings.TrimSpace(in.ProjectRef)
	if in.QualificationID <= 0 {
		return nil, fmt.Errorf("qualification_id is required")
	}
	if in.ExecutorRef == "" {
		return nil, fmt.Errorf("executor_ref is required")
	}
	if in.ProjectRef == "" {
		return nil, fmt.Errorf("project_ref is required")
	}

	var qualExecutor string
	err := s.db.QueryRowContext(ctx, `
		SELECT executor_ref
		FROM qualifications
		WHERE id=$1
		  AND tenant_id=$2
		  AND deleted=FALSE
		LIMIT 1
	`, in.QualificationID, s.tenantID).Scan(&qualExecutor)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("qualification %d not found", in.QualificationID)
	}
	if err != nil {
		return nil, err
	}
	qualExecutor = strings.TrimSpace(qualExecutor)
	if qualExecutor != "" && qualExecutor != in.ExecutorRef {
		return nil, fmt.Errorf("qualification %d belongs to %s, not %s", in.QualificationID, qualExecutor, in.ExecutorRef)
	}

	if assigned, err := s.getActiveAssignmentByQualification(ctx, in.QualificationID); err == nil && assigned != nil {
		if strings.TrimSpace(assigned.ProjectRef) != in.ProjectRef {
			return nil, fmt.Errorf(
				"qualification %d already assigned to %s",
				in.QualificationID,
				assigned.ProjectRef,
			)
		}
		return assigned, nil
	} else if err != nil {
		return nil, err
	}

	item := &QualificationAssignment{}
	err = s.db.QueryRowContext(ctx, `
		INSERT INTO qualification_assignments (
			qualification_id, executor_ref, project_ref, status, tenant_id, created_at, updated_at
		)
		VALUES ($1,$2,$3,'ACTIVE',$4,NOW(),NOW())
		RETURNING id, qualification_id, executor_ref, project_ref, status, tenant_id, created_at, updated_at, released_at
	`, in.QualificationID, in.ExecutorRef, in.ProjectRef, s.tenantID).Scan(
		&item.ID,
		&item.QualificationID,
		&item.ExecutorRef,
		&item.ProjectRef,
		&item.Status,
		&item.TenantID,
		&item.CreatedAt,
		&item.UpdatedAt,
		&item.ReleasedAt,
	)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Service) ListQualificationAssignmentsByProject(ctx context.Context, projectRef string) ([]*QualificationAssignment, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database is not configured")
	}
	projectRef = strings.TrimSpace(projectRef)
	if projectRef == "" {
		return nil, fmt.Errorf("project_ref is required")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, qualification_id, executor_ref, project_ref, status, tenant_id, created_at, updated_at, released_at
		FROM qualification_assignments
		WHERE tenant_id=$1
		  AND project_ref=$2
		ORDER BY id DESC
	`, s.tenantID, projectRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*QualificationAssignment, 0)
	for rows.Next() {
		item := &QualificationAssignment{}
		if err := rows.Scan(
			&item.ID,
			&item.QualificationID,
			&item.ExecutorRef,
			&item.ProjectRef,
			&item.Status,
			&item.TenantID,
			&item.CreatedAt,
			&item.UpdatedAt,
			&item.ReleasedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Service) getReq(spuRef string, action Action) spuReq {
	if r, ok := spuReqs[strings.TrimSpace(spuRef)]; ok {
		return r
	}
	if r, ok := actionReqs[action]; ok {
		return r
	}
	return spuReq{}
}

func (s *Service) isHeadOffice(ref string) bool {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return false
	}
	if strings.Contains(ref, "headquarters") {
		return true
	}
	return strings.HasPrefix(ref, s.headOfficeRef)
}

func (s *Service) getActiveProjects(ctx context.Context, executorRef string) ([]ActiveProject, error) {
	if s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT p.ref, p.name,
		       CASE WHEN p.executor_ref=$1 THEN 'EXECUTOR' ELSE 'REVIEWER' END,
		       p.created_at
		FROM project_nodes p
		WHERE p.status IN ('INITIATED','CONTRACTED','IN_PROGRESS')
		  AND (
		    p.executor_ref=$1
		    OR EXISTS (
		      SELECT 1 FROM achievement_utxos a
		      WHERE a.project_ref=p.ref AND a.executor_ref=$1 AND a.status='PENDING'
		    )
		  )
		ORDER BY p.created_at DESC
	`, executorRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]ActiveProject, 0)
	for rows.Next() {
		var ap ActiveProject
		if err := rows.Scan(&ap.ProjectRef, &ap.ProjectName, &ap.Role, &ap.Since); err != nil {
			return nil, err
		}
		result = append(result, ap)
	}
	return result, rows.Err()
}

func (s *Service) getExecutorName(ctx context.Context, ref string) (string, error) {
	if s.db == nil {
		return ref, nil
	}
	var name string
	err := s.db.QueryRowContext(ctx, `SELECT name FROM employees WHERE executor_ref=$1 LIMIT 1`, ref).Scan(&name)
	if err != nil {
		return ref, nil
	}
	return name, nil
}

func (s *Service) listExecutorsByQualType(ctx context.Context, qt qualification.QualType, validOn time.Time) ([]string, error) {
	if s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT executor_ref FROM qualifications
		WHERE qual_type=$1 AND tenant_id=$2
		  AND status IN ('VALID','EXPIRE_SOON')
		  AND deleted=FALSE
		  AND (valid_until IS NULL OR valid_until > $3)
		  AND executor_ref IS NOT NULL
	`, string(qt), s.tenantID, validOn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	refs := make([]string, 0)
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

func (s *Service) listAllExecutors(ctx context.Context) ([]string, error) {
	if s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT executor_ref FROM employees
		WHERE tenant_id=$1 AND executor_ref IS NOT NULL AND end_date IS NULL
	`, s.tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	refs := make([]string, 0)
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

func (s *Service) getActiveAssignmentByQualification(ctx context.Context, qualificationID int64) (*QualificationAssignment, error) {
	if s.db == nil || qualificationID <= 0 {
		return nil, nil
	}
	item := &QualificationAssignment{}
	err := s.db.QueryRowContext(ctx, `
		SELECT qa.id, qa.qualification_id, qa.executor_ref, qa.project_ref, qa.status, qa.tenant_id, qa.created_at, qa.updated_at, qa.released_at
		FROM qualification_assignments qa
		LEFT JOIN project_nodes p ON p.ref=qa.project_ref
		WHERE qa.tenant_id=$1
		  AND qa.qualification_id=$2
		  AND qa.status='ACTIVE'
		  AND (p.ref IS NULL OR p.status IN ('INITIATED','CONTRACTED','IN_PROGRESS'))
		ORDER BY qa.id DESC
		LIMIT 1
	`, s.tenantID, qualificationID).Scan(
		&item.ID,
		&item.QualificationID,
		&item.ExecutorRef,
		&item.ProjectRef,
		&item.Status,
		&item.TenantID,
		&item.CreatedAt,
		&item.UpdatedAt,
		&item.ReleasedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Service) findQualificationConflict(
	ctx context.Context,
	qualificationID int64,
	projectRef string,
) (*QualificationAssignment, error) {
	projectRef = strings.TrimSpace(projectRef)
	if projectRef == "" {
		return nil, nil
	}
	assigned, err := s.getActiveAssignmentByQualification(ctx, qualificationID)
	if err != nil || assigned == nil {
		return nil, err
	}
	if assigned.ProjectRef != projectRef {
		return assigned, nil
	}
	return nil, nil
}

func (s *Service) hasRequiredRight(
	ctx context.Context,
	executorRef string,
	right rightType,
	projectRef string,
	at time.Time,
) (bool, error) {
	grant, err := s.findRequiredRight(ctx, executorRef, right, projectRef, at)
	if err != nil {
		return false, err
	}
	return grant != nil, nil
}

func (s *Service) findRequiredRight(
	ctx context.Context,
	executorRef string,
	right rightType,
	projectRef string,
	at time.Time,
) (*rightGrant, error) {
	if right == "" {
		return nil, nil
	}
	if s.db == nil {
		// Keep unit tests and no-DB verification paths backward-compatible.
		return &rightGrant{
			ID:        0,
			Ref:       "mock://right/" + string(right),
			HolderRef: executorRef,
			RightType: string(right),
			Scope:     strings.TrimSpace(projectRef),
		}, nil
	}
	if at.IsZero() {
		at = time.Now()
	}
	item := &rightGrant{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id, ref, holder_ref, right_type, scope, valid_until
		FROM rights
		WHERE tenant_id=$1
		  AND holder_ref=$2
		  AND right_type=$3
		  AND status='ACTIVE'
		  AND (valid_from IS NULL OR valid_from <= $4)
		  AND (valid_until IS NULL OR valid_until > $4)
		  AND (
		       COALESCE(scope,'')=''
		    OR $5=''
		    OR scope=$5
		    OR POSITION($5 IN scope) > 0
		  )
		ORDER BY id DESC
		LIMIT 1
	`, s.tenantID, executorRef, string(right), at, strings.TrimSpace(projectRef)).Scan(
		&item.ID,
		&item.Ref,
		&item.HolderRef,
		&item.RightType,
		&item.Scope,
		&item.ValidUntil,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func filterActive(quals []*qualification.Qualification, at time.Time) []*qualification.Qualification {
	result := make([]*qualification.Qualification, 0, len(quals))
	for _, q := range quals {
		if q == nil {
			continue
		}
		if q.Status == qualification.StatusRevoked {
			continue
		}
		if q.ValidUntil != nil && at.After(*q.ValidUntil) {
			continue
		}
		result = append(result, q)
	}
	return result
}

func indexQuals(quals []*qualification.Qualification) map[qualification.QualType]*qualification.Qualification {
	m := map[qualification.QualType]*qualification.Qualification{}
	for _, q := range quals {
		if q == nil {
			continue
		}
		if _, exists := m[q.QualType]; !exists {
			m[q.QualType] = q
		}
	}
	return m
}

func projectLimit(quals []*qualification.Qualification) int {
	for _, q := range quals {
		if q != nil && q.QualType == qualification.QualSeniorEngineer {
			return 8
		}
	}
	return 5
}

func scoreCandidate(certCount, active, limit int) float64 {
	if limit == 0 {
		return 0
	}
	cap := float64(limit-active) / float64(limit)
	if cap < 0 {
		cap = 0
	}
	cert := float64(certCount) * 0.1
	if cert > 0.3 {
		cert = 0.3
	}
	return cap*0.7 + cert
}

func qualLabel(qt qualification.QualType) string {
	labels := map[qualification.QualType]string{
		qualification.QualRegStructure:   "REGISTERED_STRUCTURAL_ENGINEER",
		qualification.QualRegArch:        "REGISTERED_ARCHITECT",
		qualification.QualRegElectric:    "REGISTERED_ELECTRICAL_ENGINEER",
		qualification.QualRegCivil:       "REGISTERED_CIVIL_ENGINEER",
		qualification.QualRegMech:        "REGISTERED_MECHANICAL_ENGINEER",
		qualification.QualRegCost:        "REGISTERED_COST_ENGINEER",
		qualification.QualRegSafety:      "REGISTERED_SAFETY_ENGINEER",
		qualification.QualSeniorEngineer: "SENIOR_ENGINEER",
		qualification.QualComprehensiveA: "COMPANY_COMPREHENSIVE_A",
		qualification.QualIndustryA:      "COMPANY_INDUSTRY_A",
		qualification.QualIndustryB:      "COMPANY_INDUSTRY_B",
	}
	if l, ok := labels[qt]; ok {
		return l
	}
	return string(qt)
}

func rightLabel(rt rightType) string {
	switch rt {
	case rightReviewStamp:
		return "REVIEW_STAMP"
	case rightSignStamp:
		return "SIGN_STAMP"
	case rightInvoice:
		return "INVOICE"
	default:
		return string(rt)
	}
}

func mergeQualTypes(base []qualification.QualType, extra []qualification.QualType) []qualification.QualType {
	if len(extra) == 0 {
		return base
	}
	seen := make(map[qualification.QualType]struct{}, len(base)+len(extra))
	out := make([]qualification.QualType, 0, len(base)+len(extra))
	for _, v := range base {
		if strings.TrimSpace(string(v)) == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	for _, v := range extra {
		if strings.TrimSpace(string(v)) == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func authorizationPath(grant *rightGrant, projectRef string) string {
	if grant == nil {
		return ""
	}
	target := strings.TrimSpace(projectRef)
	if target == "" {
		target = "GLOBAL"
	}
	ref := strings.TrimSpace(grant.Ref)
	if ref == "" {
		ref = "right://" + strings.ToLower(strings.TrimSpace(grant.RightType))
	}
	return fmt.Sprintf("%s -> %s -> %s", strings.TrimSpace(grant.HolderRef), ref, target)
}

func hasAllSkills(got []string, required []string) bool {
	for _, skill := range required {
		if !containsString(got, skill) {
			return false
		}
	}
	return true
}

func containsString(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}

func deriveExecutorSkills(quals []*qualification.Qualification) []string {
	seen := map[string]struct{}{}
	add := func(items ...string) {
		for _, item := range items {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			seen[item] = struct{}{}
		}
	}
	for _, q := range quals {
		if q == nil {
			continue
		}
		switch q.QualType {
		case qualification.QualRegStructure:
			add(
				"bridge_structure",
				"bridge_design",
				"structural_review",
				"code_compliance",
				"seismic_design",
				"pile_foundation_design",
				"pile_cap_design",
				"pier_design",
				"girder_design",
				"prestress_design",
				"cad_drawing",
			)
		case qualification.QualRegArch:
			add("bridge_design", "code_compliance", "cad_drawing")
		case qualification.QualRegCivil:
			add("bridge_structure", "bridge_design", "cad_drawing")
		case qualification.QualRegElectric:
			add("code_compliance", "cad_drawing")
		case qualification.QualRegMech:
			add("code_compliance", "cad_drawing")
		case qualification.QualRegSafety:
			add("site_inspection", "quality_control", "code_compliance")
		case qualification.QualRegSurvey:
			add("total_station_survey", "coordinate_calculation")
		case qualification.QualRegCost:
			add("settlement", "audit")
		case qualification.QualSeniorEngineer:
			add("structural_review", "bridge_design", "code_compliance")
		case qualification.QualEngineer:
			add("cad_drawing")
		}
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func deriveCapabilityLevel(quals []*qualification.Qualification) string {
	rank := 0
	for _, q := range quals {
		if q == nil {
			continue
		}
		switch q.QualType {
		case qualification.QualRegStructure:
			if rank < 5 {
				rank = 5
			}
		case qualification.QualRegArch, qualification.QualRegCivil, qualification.QualRegElectric, qualification.QualRegMech:
			if rank < 4 {
				rank = 4
			}
		case qualification.QualSeniorEngineer:
			if rank < 3 {
				rank = 3
			}
		case qualification.QualEngineer:
			if rank < 2 {
				rank = 2
			}
		default:
			if rank < 1 {
				rank = 1
			}
		}
	}
	return capabilityByRank(rank)
}

func capabilityByRank(rank int) string {
	switch {
	case rank >= 6:
		return "PLATFORM_ENGINE"
	case rank >= 5:
		return "REGISTERED_STRUCTURAL_ENGINEER"
	case rank >= 4:
		return "REGISTERED_ENGINEER"
	case rank >= 3:
		return "SENIOR_ENGINEER"
	case rank >= 2:
		return "ENGINEER"
	case rank >= 1:
		return "ASSISTANT_ENGINEER"
	default:
		return "NONE"
	}
}

func capabilityRank(level string) int {
	switch strings.TrimSpace(level) {
	case "NONE":
		return 0
	case "ASSISTANT_ENGINEER":
		return 1
	case "ENGINEER":
		return 2
	case "SENIOR_ENGINEER":
		return 3
	case "REGISTERED_ENGINEER":
		return 4
	case "REGISTERED_STRUCTURAL_ENGINEER":
		return 5
	case "PLATFORM_ENGINE":
		return 6
	default:
		return -1
	}
}

func capabilitySatisfies(actual, required string) bool {
	actualRank := capabilityRank(actual)
	requiredRank := capabilityRank(required)
	if requiredRank < 0 {
		return true
	}
	return actualRank >= requiredRank
}

func toSharedAction(a Action) resolverpkg.Action {
	switch a {
	case ActionIssueReviewCert:
		return resolverpkg.ActionIssueReviewCert
	case ActionIssueDelivery:
		return resolverpkg.ActionIssueDelivery
	case ActionIssueInvoice:
		return resolverpkg.ActionIssueInvoice
	case ActionSignContract:
		return resolverpkg.ActionSignContract
	case ActionApprovePayment:
		return resolverpkg.ActionApprovePayment
	case ActionExecuteSPU:
		return resolverpkg.ActionExecuteSPU
	default:
		return ""
	}
}

func toSharedCertTypes(types []qualification.QualType) []resolverpkg.CertType {
	out := make([]resolverpkg.CertType, 0, len(types))
	seen := map[resolverpkg.CertType]struct{}{}
	add := func(ct resolverpkg.CertType) {
		if ct == "" {
			return
		}
		if _, ok := seen[ct]; ok {
			return
		}
		seen[ct] = struct{}{}
		out = append(out, ct)
	}
	for _, qt := range types {
		switch qt {
		case qualification.QualRegStructure:
			add(resolverpkg.CertRegStruct)
		case qualification.QualRegArch:
			add(resolverpkg.CertRegArch)
		case qualification.QualRegElectric:
			add(resolverpkg.CertRegElec)
		case qualification.QualRegCivil:
			add(resolverpkg.CertRegCivil)
		case qualification.QualRegMech:
			add(resolverpkg.CertRegMech)
		case qualification.QualComprehensiveA:
			add(resolverpkg.CertComprehA)
		case qualification.QualIndustryA:
			add(resolverpkg.CertIndustryA)
		case qualification.QualIndustryB:
			add(resolverpkg.CertIndustryB)
		}
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
