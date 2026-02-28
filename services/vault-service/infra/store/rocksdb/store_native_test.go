package rocksdb

import (
	"path/filepath"
	"testing"
	"time"

	pc "coordos/project-core"
	"coordos/vault-service/infra/store"
)

// TestStoreGoldenPathNative verifies that the native backend covers the same
// critical flows as sqlite: project tree, genesis, contract checks, parcel/utxo,
// settlement, wallet accounting, and audit querying.
func TestStoreGoldenPathNative(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rocksdb-data")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open rocksdb db: %v", err)
	}
	defer db.Close()

	tenantID := "10000"
	now := time.Now().UTC()

	projects := db.ProjectTree()
	genesis := db.Genesis()
	contracts := db.Contracts()
	parcels := db.Parcels()
	utxos := db.UTXOs()
	settlements := db.Settlements()
	wallets := db.Wallets()
	audit := db.Audit()

	root := &pc.ProjectNode{
		Ref:       pc.VRef("v://10000/project/root"),
		TenantID:  tenantID,
		OwnerRef:  pc.VRef("v://10000/org/owner"),
		Status:    pc.StatusInitiated,
		Depth:     0,
		Path:      "root",
		Children:  []pc.VRef{},
		CreatedAt: now,
		UpdatedAt: now,
		Constraint: pc.ExecutorConstraint{
			Energy: pc.Energy{CapitalReserve: 500000},
		},
	}
	if err := projects.CreateNode(tenantID, root); err != nil {
		t.Fatalf("create root project: %v", err)
	}

	child := &pc.ProjectNode{
		Ref:       pc.ChildRef(root.Ref, "design"),
		ParentRef: root.Ref,
		TenantID:  tenantID,
		OwnerRef:  root.OwnerRef,
		Status:    pc.StatusInitiated,
		Depth:     1,
		Path:      "root/design",
		Children:  []pc.VRef{},
		CreatedAt: now,
		UpdatedAt: now,
		Constraint: pc.ExecutorConstraint{
			Energy: pc.Energy{CapitalReserve: 300000},
		},
	}
	if err := projects.ValidateChildConstraint(tenantID, child); err != nil {
		t.Fatalf("validate child project constraint: %v", err)
	}
	if err := projects.CreateNode(tenantID, child); err != nil {
		t.Fatalf("create child project: %v", err)
	}
	children, err := projects.GetChildren(tenantID, root.Ref)
	if err != nil {
		t.Fatalf("get project children: %v", err)
	}
	if len(children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(children))
	}

	contract := &store.Contract{
		Ref:              pc.VRef("v://10000/contract/c-001"),
		ProjectRef:       root.Ref,
		ProcurementRef:   pc.VRef("v://10000/procurement/p-001"),
		ContractKind:     "EXTERNAL_MAIN",
		ContractNo:       "C-001",
		ContractName:     "Main Contract",
		PartyA:           pc.VRef("v://10000/org/party-a"),
		PartyB:           pc.VRef("v://10000/org/party-b"),
		AmountWithTax:    1_000_000,
		AmountWithoutTax: 900_000,
		TaxRate:          0.11,
		Status:           "ACTIVE",
		TenantID:         tenantID,
		CreatedAt:        now,
	}
	if err := contracts.Create(tenantID, contract); err != nil {
		t.Fatalf("create contract: %v", err)
	}
	if err := contracts.ValidatePayment(tenantID, contract.Ref, 200_000); err != nil {
		t.Fatalf("validate payment should pass: %v", err)
	}
	if err := contracts.ValidatePayment(tenantID, contract.Ref, 2_000_000); err == nil {
		t.Fatal("expected validate payment to fail when amount exceeds remaining")
	}

	g := &pc.GenesisUTXOFull{
		Ref:              pc.VRef("v://10000/genesis/g-001"),
		ProjectRef:       root.Ref,
		ParentRef:        "",
		TotalQuota:       100_000,
		QuotaUnit:        "CNY",
		UnitPrice:        1000,
		PriceTolerance:   0.05,
		ConsumedQuota:    0,
		AllocatedQuota:   0,
		FrozenQuota:      0,
		Depth:            0,
		AncestorRefs:     []pc.VRef{},
		ChildRefs:        []pc.VRef{},
		AllowedExecutors: []pc.VRef{pc.VRef("v://10000/person/executor")},
		AllowedSkills:    []string{"bridge_design"},
		Constraint:       pc.ExecutorConstraint{},
		QualityStandard:  "GB",
		QualityThreshold: 80,
		PaymentNodes:     []pc.PaymentNode{},
		Status:           pc.GenesisActive,
		CreatedAt:        now,
		TenantID:         tenantID,
	}
	g.ProofHash = g.ComputeProofHash()
	if err := genesis.CreateFull(tenantID, g); err != nil {
		t.Fatalf("create genesis: %v", err)
	}

	parcel := &store.Parcel{
		Ref:         pc.VRef("v://10000/parcel/p-001"),
		ProjectRef:  root.Ref,
		ContractRef: contract.Ref,
		Class:       "WORK_PACKAGE",
		Name:        "Package A",
		Status:      "OPEN",
		TenantID:    tenantID,
		CreatedAt:   now,
		ProofHash:   "parcel-hash",
		Payload:     map[string]interface{}{"segment": "A"},
	}
	if err := parcels.Create(tenantID, parcel); err != nil {
		t.Fatalf("create parcel: %v", err)
	}

	u := &store.UTXO{
		Ref:        pc.VRef("v://10000/utxo/u-001"),
		ProjectRef: root.Ref,
		ParcelRef:  parcel.Ref,
		GenesisRef: g.Ref,
		Kind:       "REVIEW_CERT",
		Status:     "READY",
		TenantID:   tenantID,
		CreatedAt:  now,
		ProofHash:  "utxo-hash",
	}
	if err := utxos.Create(tenantID, u); err != nil {
		t.Fatalf("create utxo: %v", err)
	}

	settlement := &store.Settlement{
		Ref:        pc.VRef("v://10000/settlement/s-001"),
		ProjectRef: root.Ref,
		GenesisRef: g.Ref,
		Amount:     50_000,
		Status:     "PENDING",
		TenantID:   tenantID,
		CreatedAt:  now,
		ProofHash:  "settlement-hash",
	}
	if err := settlements.Create(tenantID, settlement); err != nil {
		t.Fatalf("create settlement: %v", err)
	}

	ownerRef := pc.VRef("v://10000/person/manager")
	if _, err := wallets.GetOrCreate(tenantID, ownerRef); err != nil {
		t.Fatalf("get or create wallet: %v", err)
	}
	if err := wallets.Credit(tenantID, ownerRef, 2000, "initial"); err != nil {
		t.Fatalf("wallet credit: %v", err)
	}
	if err := wallets.Debit(tenantID, ownerRef, 500, "fee"); err != nil {
		t.Fatalf("wallet debit: %v", err)
	}
	balance, err := wallets.GetBalance(tenantID, ownerRef)
	if err != nil {
		t.Fatalf("get wallet balance: %v", err)
	}
	if balance != 1500 {
		t.Fatalf("unexpected wallet balance: %d", balance)
	}

	evtID, err := audit.RecordEvent(tenantID, store.AuditEvent{
		TenantID:   tenantID,
		ProjectRef: root.Ref,
		ActorRef:   ownerRef,
		Verb:       "CREATE_ROOT_PROJECT",
		Payload:    map[string]interface{}{"project": root.Ref},
		Timestamp:  now,
		ProofHash:  "evt-hash",
	})
	if err != nil {
		t.Fatalf("record audit event: %v", err)
	}
	if evtID == "" {
		t.Fatal("audit event id should not be empty")
	}
}

// TestPersistenceAcrossReopen ensures the backend is actually file-persistent
// (this is the key difference from in-memory compatibility behavior).
func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "native")
	tenantID := "10000"
	projectRef := pc.VRef("v://10000/project/persist")

	{
		db, err := Open(dbPath)
		if err != nil {
			t.Fatalf("open backend (first): %v", err)
		}
		projects := db.ProjectTree()
		wallets := db.Wallets()

		if err := projects.CreateNode(tenantID, &pc.ProjectNode{
			Ref:       projectRef,
			TenantID:  tenantID,
			OwnerRef:  pc.VRef("v://10000/org/owner"),
			Status:    pc.StatusInitiated,
			Depth:     0,
			Path:      "persist",
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("create project before reopen: %v", err)
		}
		if err := wallets.Credit(tenantID, pc.VRef("v://10000/person/a"), 100, "seed"); err != nil {
			t.Fatalf("credit before reopen: %v", err)
		}
		_ = db.Close()
	}

	{
		db, err := Open(dbPath)
		if err != nil {
			t.Fatalf("open backend (second): %v", err)
		}
		defer db.Close()

		projects := db.ProjectTree()
		wallets := db.Wallets()

		got, err := projects.GetNode(tenantID, projectRef)
		if err != nil {
			t.Fatalf("get project after reopen: %v", err)
		}
		if got.Ref != projectRef {
			t.Fatalf("unexpected project ref after reopen: %s", got.Ref)
		}

		balance, err := wallets.GetBalance(tenantID, pc.VRef("v://10000/person/a"))
		if err != nil {
			t.Fatalf("get balance after reopen: %v", err)
		}
		if balance != 100 {
			t.Fatalf("unexpected balance after reopen: %d", balance)
		}
	}
}
