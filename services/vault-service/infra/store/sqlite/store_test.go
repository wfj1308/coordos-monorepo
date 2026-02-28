package sqlite

import (
	"path/filepath"
	"testing"
	"time"

	pc "coordos/project-core"
	"coordos/vault-service/infra/store"
)

func TestStoreGoldenPath(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "coordos_vault.db"))
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
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
	ancestors, err := projects.GetAncestors(tenantID, child.Ref)
	if err != nil {
		t.Fatalf("get project ancestors: %v", err)
	}
	if len(ancestors) != 1 || ancestors[0].Ref != root.Ref {
		t.Fatalf("unexpected ancestors: %+v", ancestors)
	}

	invalidChild := *child
	invalidChild.Depth = 3
	if err := projects.ValidateChildConstraint(tenantID, &invalidChild); err == nil {
		t.Fatal("expected child depth validation error")
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
	remaining, err := genesis.GetRemainingQuota(tenantID, g.Ref)
	if err != nil {
		t.Fatalf("get genesis remaining quota: %v", err)
	}
	if remaining != 100_000 {
		t.Fatalf("unexpected remaining quota: %d", remaining)
	}
	g.ConsumedQuota = 30_000
	g.ProofHash = g.ComputeProofHash()
	if err := genesis.UpdateFull(tenantID, g); err != nil {
		t.Fatalf("update genesis: %v", err)
	}
	remaining, err = genesis.GetRemainingQuota(tenantID, g.Ref)
	if err != nil {
		t.Fatalf("get updated genesis remaining quota: %v", err)
	}
	if remaining != 70_000 {
		t.Fatalf("unexpected remaining quota after consume: %d", remaining)
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
		PrevHash:   "",
		Payload:    map[string]interface{}{"score": 95},
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
	wallet, err := wallets.GetOrCreate(tenantID, ownerRef)
	if err != nil {
		t.Fatalf("get or create wallet: %v", err)
	}
	if wallet.Balance != 0 {
		t.Fatalf("new wallet balance should be 0, got %d", wallet.Balance)
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
	ledger, err := wallets.ListLedger(tenantID, ownerRef, 10)
	if err != nil {
		t.Fatalf("list wallet ledger: %v", err)
	}
	if len(ledger) != 2 {
		t.Fatalf("expected 2 ledger entries, got %d", len(ledger))
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
	verb := "CREATE_ROOT_PROJECT"
	events, err := audit.QueryEvents(tenantID, store.AuditFilter{
		ProjectRef: &root.Ref,
		Verb:       &verb,
		Limit:      20,
	})
	if err != nil {
		t.Fatalf("query audit events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected at least one audit event")
	}
	if _, err := audit.RecordViolation(tenantID, "RULE-TEST", store.AuditEvent{
		TenantID:   tenantID,
		ProjectRef: root.Ref,
		ActorRef:   ownerRef,
		Verb:       "TEST",
		Payload:    map[string]interface{}{"reason": "for testing"},
		Timestamp:  now,
	}, "rule test detail"); err != nil {
		t.Fatalf("record audit violation: %v", err)
	}
}
