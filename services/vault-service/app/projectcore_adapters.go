package app

import (
	"fmt"
	"time"

	pc "coordos/project-core"
	"coordos/vault-service/infra/store"
)

// pcProjectTreeAdapter binds tenant-aware vault-store interfaces
// to project-core interfaces (which are tenant-less).
type pcProjectTreeAdapter struct {
	tenantID string
	s        store.ProjectTreeStore
}

func (a pcProjectTreeAdapter) CreateNode(node *pc.ProjectNode) error {
	return a.s.CreateNode(a.tenantID, node)
}

func (a pcProjectTreeAdapter) GetNode(ref pc.VRef) (*pc.ProjectNode, error) {
	return a.s.GetNode(a.tenantID, ref)
}

func (a pcProjectTreeAdapter) GetChildren(ref pc.VRef) ([]*pc.ProjectNode, error) {
	return a.s.GetChildren(a.tenantID, ref)
}

func (a pcProjectTreeAdapter) GetAncestors(ref pc.VRef) ([]*pc.ProjectNode, error) {
	return a.s.GetAncestors(a.tenantID, ref)
}

func (a pcProjectTreeAdapter) UpdateStatus(ref pc.VRef, status pc.LifecycleStatus) error {
	return a.s.UpdateStatus(a.tenantID, ref, status)
}

func (a pcProjectTreeAdapter) ValidateChildConstraint(child *pc.ProjectNode) error {
	return a.s.ValidateChildConstraint(a.tenantID, child)
}

type pcGenesisAdapter struct {
	tenantID string
	s        store.GenesisStore
}

func (a pcGenesisAdapter) GetFull(ref pc.VRef) (*pc.GenesisUTXOFull, error) {
	return a.s.GetFull(a.tenantID, ref)
}

func (a pcGenesisAdapter) Get(ref pc.VRef) (*pc.GenesisUTXO, error) {
	g, err := a.s.GetFull(a.tenantID, ref)
	if err != nil {
		return nil, err
	}
	return &pc.GenesisUTXO{
		Ref:             g.Ref,
		ProjectRef:      g.ProjectRef,
		ParentRef:       g.ParentRef,
		TenantID:        g.TenantID,
		TotalQuota:      g.TotalQuota,
		ConsumedQuota:   g.ConsumedQuota,
		UnitPrice:       g.UnitPrice,
		PriceTolerance:  g.PriceTolerance,
		QualityStandard: g.QualityStandard,
		PaymentNodes:    g.PaymentNodes,
		Status:          string(g.Status),
		CreatedAt:       g.CreatedAt,
		ProofHash:       g.ProofHash,
	}, nil
}

func (a pcGenesisAdapter) CreateFull(g *pc.GenesisUTXOFull) error {
	return a.s.CreateFull(a.tenantID, g)
}

func (a pcGenesisAdapter) Create(utxo *pc.GenesisUTXO) error {
	now := time.Now().UTC()
	g := &pc.GenesisUTXOFull{
		Ref:              utxo.Ref,
		ProjectRef:       utxo.ProjectRef,
		ParentRef:        utxo.ParentRef,
		TotalQuota:       utxo.TotalQuota,
		QuotaUnit:        "CNY",
		UnitPrice:        utxo.UnitPrice,
		PriceTolerance:   utxo.PriceTolerance,
		ConsumedQuota:    utxo.ConsumedQuota,
		AllocatedQuota:   0,
		FrozenQuota:      0,
		Depth:            0,
		AncestorRefs:     []pc.VRef{},
		ChildRefs:        []pc.VRef{},
		AllowedExecutors: []pc.VRef{},
		AllowedSkills:    []string{},
		Constraint:       pc.ExecutorConstraint{},
		QualityStandard:  utxo.QualityStandard,
		QualityThreshold: 0,
		PaymentNodes:     utxo.PaymentNodes,
		Status:           pc.GenesisUTXOStatus(utxo.Status),
		CreatedAt:        now,
		LockedAt:         &now,
		ClosedAt:         nil,
		ProofHash:        utxo.ProofHash,
		PrevHash:         "",
		TenantID:         chooseTenant(utxo.TenantID, a.tenantID),
	}
	if g.Status == "" {
		g.Status = pc.GenesisActive
	}
	if g.ProofHash == "" {
		g.ProofHash = g.ComputeProofHash()
	}
	return a.s.CreateFull(a.tenantID, g)
}

func (a pcGenesisAdapter) UpdateFull(g *pc.GenesisUTXOFull) error {
	return a.s.UpdateFull(a.tenantID, g)
}

func (a pcGenesisAdapter) ConsumeQuota(ref pc.VRef, amount int64) (*pc.GenesisUTXO, error) {
	if amount <= 0 {
		return nil, fmt.Errorf("amount must be positive")
	}
	g, err := a.s.GetFull(a.tenantID, ref)
	if err != nil {
		return nil, err
	}
	if g.RemainingQuota() < amount {
		return nil, fmt.Errorf("insufficient remaining quota")
	}
	g.ConsumedQuota += amount
	if g.RemainingQuota() == 0 {
		g.Status = pc.GenesisExhausted
	}
	g.ProofHash = g.ComputeProofHash()
	if err := a.s.UpdateFull(a.tenantID, g); err != nil {
		return nil, err
	}
	return a.Get(ref)
}

func (a pcGenesisAdapter) GetRemainingQuota(ref pc.VRef) (int64, error) {
	return a.s.GetRemainingQuota(a.tenantID, ref)
}

type pcContractAdapter struct {
	tenantID string
	s        store.ContractStore
}

func (a pcContractAdapter) Get(ref pc.VRef) (*pc.Contract, error) {
	c, err := a.s.Get(a.tenantID, ref)
	if err != nil {
		return nil, err
	}
	return &pc.Contract{
		Ref:              c.Ref,
		ProjectRef:       c.ProjectRef,
		TenantID:         c.TenantID,
		ContractNo:       c.ContractNo,
		ContractName:     c.ContractName,
		PartyA:           c.PartyA,
		PartyB:           c.PartyB,
		BranchRef:        c.BranchRef,
		ManagerRef:       c.ManagerRef,
		AmountWithTax:    c.AmountWithTax,
		AmountWithoutTax: c.AmountWithoutTax,
		TaxRate:          c.TaxRate,
		SignDate:         c.SignDate,
		EffectiveDate:    c.EffectiveDate,
		ExpiryDate:       c.ExpiryDate,
		PaymentNodes:     c.PaymentNodes,
		SealStatus:       c.SealStatus,
		AttachmentRefs:   c.AttachmentRefs,
		ProcurementRef:   c.ProcurementRef,
		ContractKind:     c.ContractKind,
		Status:           c.Status,
		CreatedAt:        c.CreatedAt,
		ProofHash:        c.ProofHash,
	}, nil
}

func (a pcContractAdapter) GetByProject(projectRef pc.VRef) ([]*pc.Contract, error) {
	f := store.ContractFilter{ProjectRef: &projectRef, Limit: 2000, Offset: 0}
	items, _, err := a.s.List(a.tenantID, f)
	if err != nil {
		return nil, err
	}
	out := make([]*pc.Contract, 0, len(items))
	for _, c := range items {
		out = append(out, &pc.Contract{
			Ref:              c.Ref,
			ProjectRef:       c.ProjectRef,
			TenantID:         c.TenantID,
			ContractNo:       c.ContractNo,
			ContractName:     c.ContractName,
			PartyA:           c.PartyA,
			PartyB:           c.PartyB,
			BranchRef:        c.BranchRef,
			ManagerRef:       c.ManagerRef,
			AmountWithTax:    c.AmountWithTax,
			AmountWithoutTax: c.AmountWithoutTax,
			TaxRate:          c.TaxRate,
			SignDate:         c.SignDate,
			EffectiveDate:    c.EffectiveDate,
			ExpiryDate:       c.ExpiryDate,
			PaymentNodes:     c.PaymentNodes,
			SealStatus:       c.SealStatus,
			AttachmentRefs:   c.AttachmentRefs,
			ProcurementRef:   c.ProcurementRef,
			ContractKind:     c.ContractKind,
			Status:           c.Status,
			CreatedAt:        c.CreatedAt,
			ProofHash:        c.ProofHash,
		})
	}
	return out, nil
}

func (a pcContractAdapter) GetRemainingAmount(ref pc.VRef) (int64, error) {
	return a.s.GetRemainingAmount(a.tenantID, ref)
}

func (a pcContractAdapter) ValidatePayment(contractRef pc.VRef, amount int64) error {
	return a.s.ValidatePayment(a.tenantID, contractRef, amount)
}

type pcUTXOAdapter struct {
	tenantID string
	s        store.UTXOStore
}

func (a pcUTXOAdapter) Get(ref pc.VRef) (*pc.UTXORecord, error) {
	u, err := a.s.Get(a.tenantID, ref)
	if err != nil {
		return nil, err
	}
	if u == nil {
		return nil, fmt.Errorf("utxo not found: %s", ref)
	}
	return mapStoreUTXOToCore(u), nil
}

func (a pcUTXOAdapter) ListByProject(projectRef pc.VRef) ([]*pc.UTXORecord, error) {
	items, err := a.s.ListByProject(a.tenantID, projectRef)
	if err != nil {
		return nil, err
	}
	out := make([]*pc.UTXORecord, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		out = append(out, mapStoreUTXOToCore(item))
	}
	return out, nil
}

func mapStoreUTXOToCore(u *store.UTXO) *pc.UTXORecord {
	inputRefs := make([]pc.VRef, 0, len(u.InputRefs))
	inputRefs = append(inputRefs, u.InputRefs...)
	return &pc.UTXORecord{
		Ref:        u.Ref,
		ProjectRef: u.ProjectRef,
		GenesisRef: u.GenesisRef,
		InputRefs:  inputRefs,
		Kind:       u.Kind,
		PrevHash:   u.PrevHash,
		ProofHash:  u.ProofHash,
		Status:     u.Status,
	}
}

type pcUTXORelationAdapter struct {
	tenantID string
	s        store.UTXORelationStore
}

func (a pcUTXORelationAdapter) ListByFrom(ref pc.VRef) ([]*pc.UTXORelationRecord, error) {
	items, err := a.s.ListByFrom(a.tenantID, ref)
	if err != nil {
		return nil, err
	}
	out := make([]*pc.UTXORelationRecord, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		out = append(out, mapStoreUTXORelationToCore(item))
	}
	return out, nil
}

func (a pcUTXORelationAdapter) ListByTo(ref pc.VRef) ([]*pc.UTXORelationRecord, error) {
	items, err := a.s.ListByTo(a.tenantID, ref)
	if err != nil {
		return nil, err
	}
	out := make([]*pc.UTXORelationRecord, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		out = append(out, mapStoreUTXORelationToCore(item))
	}
	return out, nil
}

func mapStoreUTXORelationToCore(r *store.UTXORelation) *pc.UTXORelationRecord {
	metadata := map[string]interface{}{}
	for k, v := range r.Payload {
		metadata[k] = v
	}
	return &pc.UTXORelationRecord{
		Ref:           r.Ref,
		FromRef:       r.FromRef,
		ToRef:         r.ToRef,
		ChangeUTXORef: r.ChangeUTXORef,
		Type:          pc.UTXORelationType(r.Type),
		Reason:        r.Reason,
		Metadata:      metadata,
	}
}

type pcAuditAdapter struct {
	tenantID string
	s        store.AuditStore
}

func (a pcAuditAdapter) RecordEvent(evt pc.ProjectEvent, tenantID string) (string, error) {
	tid := chooseTenant(tenantID, chooseTenant(evt.TenantID, a.tenantID))
	return a.s.RecordEvent(tid, store.AuditEvent{
		EventID:    evt.EventID,
		TenantID:   tid,
		ProjectRef: evt.ProjectRef,
		ActorRef:   evt.ActorRef,
		Verb:       string(evt.Verb),
		Payload:    evt.Payload,
		ProofHash:  evt.Signature,
		Timestamp:  evt.Timestamp,
	})
}

func (a pcAuditAdapter) RecordViolation(rule string, evt pc.ProjectEvent, detail string) (string, error) {
	tid := chooseTenant(evt.TenantID, a.tenantID)
	return a.s.RecordViolation(tid, rule, store.AuditEvent{
		EventID:    evt.EventID,
		TenantID:   tid,
		ProjectRef: evt.ProjectRef,
		ActorRef:   evt.ActorRef,
		Verb:       string(evt.Verb),
		Payload:    evt.Payload,
		ProofHash:  evt.Signature,
		Timestamp:  evt.Timestamp,
	}, detail)
}

func chooseTenant(primary, fallback string) string {
	if primary != "" {
		return primary
	}
	return fallback
}
