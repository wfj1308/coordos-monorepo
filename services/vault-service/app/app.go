// Package app implements all use cases for CoordOS vault service.
//
// Every operation flows through here:
//
// Request -> Permission Guard -> State Machine -> Rule Engine -> Store -> Audit
//
// Nothing bypasses this layer.
package app

import (
	"fmt"
	"strings"
	"time"

	pc "coordos/project-core"
	"coordos/vault-service/infra/store"
)

// Dependencies injected at startup.

type Deps struct {
	Projects     store.ProjectTreeStore
	Genesis      store.GenesisStore
	Contracts    store.ContractStore
	Parcels      store.ParcelStore
	UTXOs        store.UTXOStore
	Settlements  store.SettlementStore
	Wallets      store.WalletStore
	Audit        store.AuditStore
	Rules        pc.ProjectRules
	Fission      *pc.FissionEngine
	StateMachine *pc.StateMachine
}

// Actor is the authenticated user context.

type Actor struct {
	Ref      pc.VRef
	TenantID string
	Roles    []string // PLATFORM/HEAD_OFFICE/BRANCH/PERSON
}

// ProjectApp handles project node use-cases.

type ProjectApp struct{ d Deps }

func NewProjectApp(d Deps) *ProjectApp { return &ProjectApp{d} }

// CreateRootProject creates the root project.
func (a *ProjectApp) CreateRootProject(actor Actor, req CreateProjectReq) (*pc.ProjectNode, error) {
	// Only HEAD_OFFICE/PLATFORM can create root project.
	if err := requireRole(actor, "HEAD_OFFICE", "PLATFORM"); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	platformRef := req.PlatformRef
	if strings.TrimSpace(string(platformRef)) == "" {
		platformRef = actor.Ref
	}
	node := &pc.ProjectNode{
		Ref:         pc.VRef(fmt.Sprintf("v://%s/project/%s", actor.TenantID, req.ID)),
		ParentRef:   "",
		TenantID:    actor.TenantID,
		OwnerRef:    req.OwnerRef,
		PlatformRef: platformRef,
		Status:      pc.StatusInitiated,
		Depth:       0,
		Path:        req.ID,
		Children:    []pc.VRef{},
		Milestones:  []pc.MilestoneEvent{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if err := a.d.Projects.CreateNode(actor.TenantID, node); err != nil {
		return nil, fmt.Errorf("create root project failed: %w", err)
	}

	a.d.Audit.RecordEvent(actor.TenantID, store.AuditEvent{
		TenantID:   actor.TenantID,
		ProjectRef: node.Ref,
		ActorRef:   actor.Ref,
		Verb:       "CREATE_ROOT_PROJECT",
		Payload:    map[string]interface{}{"name": req.Name},
		Timestamp:  now,
	})

	return node, nil
}

// CreateChildProject creates child project and enforces RULE-004.
func (a *ProjectApp) CreateChildProject(actor Actor, req CreateChildProjectReq) (*pc.ProjectNode, error) {
	// 1) Load parent node.
	parent, err := a.d.Projects.GetNode(actor.TenantID, req.ParentRef)
	if err != nil {
		return nil, fmt.Errorf("parent node not found: %w", err)
	}

	// 2) Permission check.
	if actor.Ref != parent.ContractorRef && actor.Ref != parent.PlatformRef {
		if err := requireRole(actor, "PLATFORM"); err != nil {
			return nil, &PermissionError{
				Actor:  actor.Ref,
				Action: "CREATE_CHILD_PROJECT",
				Reason: "only parent contractor/platform can create child project",
			}
		}
	}

	// 3) Parent status must allow CONFIGURE.
	ctx := a.buildContext(actor.TenantID)
	sm := pc.NewStateMachine(ctx.ProjectTree, ctx.AuditStore)
	if err := sm.ValidateVerb(parent, pc.VerbConfigure); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	childPath := parent.Path + "/" + req.ID
	child := &pc.ProjectNode{
		Ref:            pc.ChildRef(parent.Ref, req.ID),
		ParentRef:      parent.Ref,
		TenantID:       actor.TenantID,
		OwnerRef:       req.OwnerRef,
		ContractorRef:  req.ContractorRef,
		ExecutorRef:    req.ExecutorRef,
		PlatformRef:    parent.PlatformRef,
		ContractRef:    req.ContractRef,
		ProcurementRef: req.ProcurementRef,
		Status:         pc.StatusInitiated,
		Depth:          parent.Depth + 1,
		Path:           childPath,
		Children:       []pc.VRef{},
		Milestones:     []pc.MilestoneEvent{},
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	// 4) Enforce RULE-004.
	if err := a.d.Projects.ValidateChildConstraint(actor.TenantID, child); err != nil {
		return nil, err
	}

	if err := a.d.Projects.CreateNode(actor.TenantID, child); err != nil {
		return nil, fmt.Errorf("create child project failed: %w", err)
	}

	a.d.Audit.RecordEvent(actor.TenantID, store.AuditEvent{
		TenantID:   actor.TenantID,
		ProjectRef: child.Ref,
		ActorRef:   actor.Ref,
		Verb:       "CREATE_CHILD_PROJECT",
		Payload:    map[string]interface{}{"parent": parent.Ref, "depth": child.Depth},
		Timestamp:  now,
	})

	return child, nil
}

// TransitionStatus transitions project lifecycle state.
func (a *ProjectApp) TransitionStatus(actor Actor, projectRef pc.VRef, target pc.LifecycleStatus) error {
	node, err := a.d.Projects.GetNode(actor.TenantID, projectRef)
	if err != nil {
		return err
	}
	ctx := a.buildContext(actor.TenantID)
	sm := pc.NewStateMachine(ctx.ProjectTree, ctx.AuditStore)
	return sm.Transition(node, target, actor.Ref, ctx)
}

// FissionApp handles GenesisUTXO fission.

type FissionApp struct{ d Deps }

func NewFissionApp(d Deps) *FissionApp { return &FissionApp{d} }

// ExecuteFission is the only entry for delegated-chain fission.
func (a *FissionApp) ExecuteFission(actor Actor, req pc.FissionRequest) (*pc.FissionResult, error) {
	// 1) Permission check.
	parent, err := a.d.Genesis.GetFull(actor.TenantID, req.ParentGenesisRef)
	if err != nil {
		return nil, fmt.Errorf("parent genesis utxo not found: %w", err)
	}
	node, err := a.d.Projects.GetNode(actor.TenantID, parent.ProjectRef)
	if err != nil {
		return nil, err
	}
	if actor.Ref != node.ContractorRef && actor.Ref != node.PlatformRef {
		return nil, &PermissionError{
			Actor:  actor.Ref,
			Action: "FISSION",
			Reason: "only project contractor/platform can execute fission",
		}
	}

	// 2) Execute fission with RULE-003/004 validation.
	result, err := a.d.Fission.Fission(req)
	if err != nil {
		a.d.Audit.RecordViolation(actor.TenantID, "FISSION", store.AuditEvent{
			TenantID:   actor.TenantID,
			ProjectRef: req.ParentProjectRef,
			ActorRef:   actor.Ref,
			Verb:       "FISSION",
			Timestamp:  time.Now().UTC(),
		}, err.Error())
		return nil, err
	}

	a.d.Audit.RecordEvent(actor.TenantID, store.AuditEvent{
		TenantID:   actor.TenantID,
		ProjectRef: req.ChildProjectRef,
		ActorRef:   actor.Ref,
		Verb:       "FISSION",
		Payload: map[string]interface{}{
			"parent_genesis": req.ParentGenesisRef,
			"child_genesis":  result.ChildGenesis.Ref,
			"quota":          req.RequestedQuota,
		},
		Timestamp: time.Now().UTC(),
	})

	return result, nil
}

// EventApp handles unified event ingestion and rule validation.

type EventApp struct{ d Deps }

func NewEventApp(d Deps) *EventApp { return &EventApp{d} }

// Submit validates and commits a project event.
func (a *EventApp) Submit(actor Actor, evt pc.ProjectEvent) error {
	if evt.TenantID == "" {
		evt.TenantID = actor.TenantID
	}
	if evt.ActorRef == "" {
		evt.ActorRef = actor.Ref
	}
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now().UTC()
	}

	// Validate current state machine permission.
	node, err := a.d.Projects.GetNode(actor.TenantID, evt.ProjectRef)
	if err != nil {
		return fmt.Errorf("project node not found: %w", err)
	}
	ctx := a.buildContext(actor.TenantID)
	sm := pc.NewStateMachine(ctx.ProjectTree, ctx.AuditStore)
	if err := sm.ValidateVerb(node, evt.Verb); err != nil {
		return err
	}

	// Execute rule engine checks.
	engine := pc.NewRuleEngine(a.d.Rules, ctx.AuditStore)
	return engine.Execute(evt, ctx)
}

// SettleApp handles settlement use-cases.

type SettleApp struct{ d Deps }

func NewSettleApp(d Deps) *SettleApp { return &SettleApp{d} }

// TriggerSettle triggers settlement from leaf to root.
func (a *SettleApp) TriggerSettle(actor Actor, projectRef pc.VRef) error {
	evt := pc.ProjectEvent{
		EventID:    fmt.Sprintf("settle-%d", time.Now().UnixNano()),
		ProjectRef: projectRef,
		TenantID:   actor.TenantID,
		ActorRef:   actor.Ref,
		Verb:       pc.VerbSettle,
		Timestamp:  time.Now().UTC(),
	}

	ctx := a.buildContext(actor.TenantID)
	node, err := a.d.Projects.GetNode(actor.TenantID, projectRef)
	if err != nil {
		return fmt.Errorf("project node not found: %w", err)
	}
	if strings.TrimSpace(string(node.PlatformRef)) == "" {
		if strings.TrimSpace(string(actor.Ref)) == "" {
			return fmt.Errorf("project platform_ref is empty and actor ref is empty")
		}
		node.PlatformRef = actor.Ref
		node.UpdatedAt = time.Now().UTC()
		if err := a.d.Projects.UpdateNode(actor.TenantID, node); err != nil {
			return fmt.Errorf("backfill platform_ref failed: %w", err)
		}
	}

	engine := pc.NewRuleEngine(a.d.Rules, ctx.AuditStore)
	if err := engine.Execute(evt, ctx); err != nil {
		return err
	}

	// Transition to SETTLED.
	sm := pc.NewStateMachine(ctx.ProjectTree, ctx.AuditStore)
	return sm.Transition(node, pc.StatusSettled, actor.Ref, ctx)
}

// helpers

func (a *ProjectApp) buildContext(tenantID string) pc.ProjectContext {
	return pc.ProjectContext{
		TenantID:      tenantID,
		ProjectTree:   pcProjectTreeAdapter{tenantID: tenantID, s: a.d.Projects},
		GenesisStore:  pcGenesisAdapter{tenantID: tenantID, s: a.d.Genesis},
		ContractStore: pcContractAdapter{tenantID: tenantID, s: a.d.Contracts},
		AuditStore:    pcAuditAdapter{tenantID: tenantID, s: a.d.Audit},
	}
}

func (a *EventApp) buildContext(tenantID string) pc.ProjectContext {
	return pc.ProjectContext{
		TenantID:      tenantID,
		ProjectTree:   pcProjectTreeAdapter{tenantID: tenantID, s: a.d.Projects},
		GenesisStore:  pcGenesisAdapter{tenantID: tenantID, s: a.d.Genesis},
		ContractStore: pcContractAdapter{tenantID: tenantID, s: a.d.Contracts},
		AuditStore:    pcAuditAdapter{tenantID: tenantID, s: a.d.Audit},
	}
}

func (a *SettleApp) buildContext(tenantID string) pc.ProjectContext {
	return pc.ProjectContext{
		TenantID:      tenantID,
		ProjectTree:   pcProjectTreeAdapter{tenantID: tenantID, s: a.d.Projects},
		GenesisStore:  pcGenesisAdapter{tenantID: tenantID, s: a.d.Genesis},
		ContractStore: pcContractAdapter{tenantID: tenantID, s: a.d.Contracts},
		AuditStore:    pcAuditAdapter{tenantID: tenantID, s: a.d.Audit},
	}
}

func requireRole(actor Actor, roles ...string) error {
	for _, r := range actor.Roles {
		for _, required := range roles {
			if r == required {
				return nil
			}
		}
	}
	return &PermissionError{Actor: actor.Ref, Action: "unknown", Reason: fmt.Sprintf("required role: %v", roles)}
}

// Request types

type CreateProjectReq struct {
	ID          string
	Name        string
	OwnerRef    pc.VRef
	PlatformRef pc.VRef
}

type CreateChildProjectReq struct {
	ID             string
	Name           string
	ParentRef      pc.VRef
	OwnerRef       pc.VRef
	ContractorRef  pc.VRef
	ExecutorRef    pc.VRef
	ContractRef    pc.VRef
	ProcurementRef pc.VRef
}

// Error types

type PermissionError struct {
	Actor  pc.VRef
	Action string
	Reason string
}

func (e *PermissionError) Error() string {
	return fmt.Sprintf("[PERMISSION] actor=%s action=%s: %s", e.Actor, e.Action, e.Reason)
}
