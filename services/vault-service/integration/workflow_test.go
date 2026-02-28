package integration

import (
	"path/filepath"
	"testing"
	"time"

	pc "coordos/project-core"
	"coordos/vault-service/app"
	"coordos/vault-service/infra/store"
	"coordos/vault-service/infra/store/rocksdb"
)

// TestDelegationChainWorkflow validates a full delegated-chain workflow:
// Project creation -> child delegation -> genesis fission -> event submission.
//
// This is intentionally an integration test across multiple layers:
// app use-cases + project-core engine + native rocksdb persistence.
func TestDelegationChainWorkflow(t *testing.T) {
	backend, deps := newTestServerDeps(t)
	defer backend.Close()

	projectApp := app.NewProjectApp(deps)
	fissionApp := app.NewFissionApp(deps)
	eventApp := app.NewEventApp(deps)

	actor := app.Actor{
		Ref:      pc.VRef("v://10000/person/hq"),
		TenantID: "10000",
		Roles:    []string{"HEAD_OFFICE", "PLATFORM"},
	}

	// 1) Create root project.
	root, err := projectApp.CreateRootProject(actor, app.CreateProjectReq{
		ID:       "bridge-alpha",
		Name:     "Bridge Alpha",
		OwnerRef: pc.VRef("v://10000/org/owner"),
	})
	if err != nil {
		t.Fatalf("create root project: %v", err)
	}

	// Root creation currently doesn't bind contractor/platform refs, while child
	// creation and fission permission checks rely on them. We normalize fixture
	// data here to represent a realistic configured root project.
	root.ContractorRef = actor.Ref
	root.PlatformRef = actor.Ref
	root.Constraint.Energy.CapitalReserve = 1_000_000
	if err := deps.Projects.UpdateNode(actor.TenantID, root); err != nil {
		t.Fatalf("update root for permission anchors: %v", err)
	}

	// 2) Create delegated child project.
	child, err := projectApp.CreateChildProject(actor, app.CreateChildProjectReq{
		ID:             "bridge-alpha-design",
		Name:           "Bridge Alpha Design",
		ParentRef:      root.Ref,
		OwnerRef:       root.OwnerRef,
		ContractorRef:  pc.VRef("v://10000/org/sub-contractor"),
		ExecutorRef:    pc.VRef("v://10000/person/executor-1"),
		ContractRef:    pc.VRef("v://10000/contract/main-001"),
		ProcurementRef: pc.VRef("v://10000/procurement/p-001"),
	})
	if err != nil {
		t.Fatalf("create child project: %v", err)
	}

	// 3) Seed contract and parent genesis required by fission rules.
	if err := deps.Contracts.Create(actor.TenantID, &store.Contract{
		Ref:            pc.VRef("v://10000/contract/main-001"),
		ProjectRef:     root.Ref,
		ContractKind:   "EXTERNAL_MAIN",
		ContractNo:     "MAIN-001",
		ContractName:   "Main Contract",
		AmountWithTax:  2_000_000,
		Status:         "ACTIVE",
		TenantID:       actor.TenantID,
		CreatedAt:      time.Now().UTC(),
		ProcurementRef: pc.VRef("v://10000/procurement/p-001"),
	}); err != nil {
		t.Fatalf("seed contract for fission: %v", err)
	}

	parentGenesis := &pc.GenesisUTXOFull{
		Ref:              pc.VRef("v://10000/genesis/root-001"),
		ProjectRef:       root.Ref,
		TenantID:         actor.TenantID,
		TotalQuota:       1_000_000,
		QuotaUnit:        "CNY",
		UnitPrice:        1000,
		PriceTolerance:   0.10,
		AllowedExecutors: []pc.VRef{child.ExecutorRef, actor.Ref},
		AllowedSkills:    []string{"bridge_design", "review"},
		Constraint: pc.ExecutorConstraint{
			Energy: pc.Energy{CapitalReserve: 1_000_000},
		},
		QualityStandard:  "GB",
		QualityThreshold: 80,
		Status:           pc.GenesisActive,
		CreatedAt:        time.Now().UTC(),
	}
	parentGenesis.ProofHash = parentGenesis.ComputeProofHash()
	if err := deps.Genesis.CreateFull(actor.TenantID, parentGenesis); err != nil {
		t.Fatalf("seed parent genesis: %v", err)
	}

	// 4) Execute fission for child node.
	fissionReq := pc.FissionRequest{
		ParentGenesisRef: parentGenesis.Ref,
		ParentProjectRef: root.Ref,
		ChildProjectRef:  child.Ref,
		ChildExecutorRef: child.ExecutorRef,
		RequestedQuota:   300_000,
		RequestedUnit:    "CNY",
		NegotiatedPrice:  980,
		PriceTolerance:   0.05,
		AllowedSkills:    []string{"bridge_design"},
		ContractRef:      pc.VRef("v://10000/contract/main-001"),
		ProcurementRef:   pc.VRef("v://10000/procurement/p-001"),
		QualityStandard:  "GB",
		QualityThreshold: 82,
		RequestedBy:      actor.Ref,
		RequestedAt:      time.Now().UTC(),
	}
	result, err := fissionApp.ExecuteFission(actor, fissionReq)
	if err != nil {
		t.Fatalf("execute fission: %v", err)
	}
	if result.ChildGenesis == nil || result.ChildGenesis.Ref == "" {
		t.Fatal("fission should create child genesis")
	}

	// 5) Submit CONFIGURE event on child to verify event/rule chain.
	if err := eventApp.Submit(actor, pc.ProjectEvent{
		EventID:    "evt-config-child-1",
		ProjectRef: child.Ref,
		TenantID:   actor.TenantID,
		ActorRef:   actor.Ref,
		Verb:       pc.VerbConfigure,
		Timestamp:  time.Now().UTC(),
		Payload:    map[string]interface{}{"source": "integration-test"},
	}); err != nil {
		t.Fatalf("submit configure event: %v", err)
	}

	// 6) Assertions on persistence and audit outputs.
	gotGenesis, err := deps.Genesis.GetFull(actor.TenantID, result.ChildGenesis.Ref)
	if err != nil {
		t.Fatalf("child genesis should persist: %v", err)
	}
	if gotGenesis.ProjectRef != child.Ref {
		t.Fatalf("child genesis project mismatch: got=%s want=%s", gotGenesis.ProjectRef, child.Ref)
	}

	verb := "FISSION"
	events, err := deps.Audit.QueryEvents(actor.TenantID, store.AuditFilter{
		ProjectRef: &child.Ref,
		Verb:       &verb,
		Limit:      20,
	})
	if err != nil {
		t.Fatalf("query fission audit events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected at least one FISSION audit event")
	}
}

func newTestServerDeps(t *testing.T) (*rocksdb.DB, app.Deps) {
	t.Helper()

	backend, err := rocksdb.Open(filepath.Join(t.TempDir(), "native-rocksdb"))
	if err != nil {
		t.Fatalf("open native rocksdb backend: %v", err)
	}

	deps := app.BuildDeps("10000", app.StoreSet{
		Projects:    backend.ProjectTree(),
		Genesis:     backend.Genesis(),
		Contracts:   backend.Contracts(),
		Parcels:     backend.Parcels(),
		UTXOs:       backend.UTXOs(),
		Settlements: backend.Settlements(),
		Wallets:     backend.Wallets(),
		Audit:       backend.Audit(),
	})
	return backend, deps
}
