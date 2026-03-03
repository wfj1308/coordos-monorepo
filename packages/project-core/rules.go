package projectcore

import (
	"fmt"
	"strings"
)

type projectRules struct{}

func NewProjectRules() ProjectRules {
	return &projectRules{}
}

// RULE-001: TRANSFORM must stay inside executor/resource constraints.
func (r *projectRules) EnforceRule001(evt ProjectEvent, ctx ProjectContext) error {
	if evt.Verb != VerbTransform {
		return nil
	}

	node, err := ctx.ProjectTree.GetNode(evt.ProjectRef)
	if err != nil {
		return &RuleViolationError{Rule: "RULE-001", Detail: "project node not found: " + string(evt.ProjectRef)}
	}

	if node.ExecutorRef != evt.ActorRef {
		return &RuleViolationError{
			Rule:   "RULE-001",
			Detail: fmt.Sprintf("executor mismatch: expected %s got %s", node.ExecutorRef, evt.ActorRef),
		}
	}

	quantity, ok := evt.Payload["quantity"].(float64)
	if !ok {
		return &RuleViolationError{Rule: "RULE-001", Detail: "payload missing quantity"}
	}
	remaining, err := ctx.GenesisStore.GetRemainingQuota(node.GenesisUTXORef)
	if err != nil {
		return &RuleViolationError{Rule: "RULE-001", Detail: "cannot read genesis quota"}
	}
	if int64(quantity) > remaining {
		return &RuleViolationError{
			Rule:   "RULE-001",
			Detail: fmt.Sprintf("exceed genesis remaining quota: requested %d, remaining %d", int64(quantity), remaining),
		}
	}

	skillName, _ := evt.Payload["skill"].(string)
	found := false
	for _, s := range node.Constraint.Skills {
		if s.Name == skillName {
			found = true
			unitPrice, _ := evt.Payload["unit_price"].(float64)
			diff := (unitPrice - float64(s.UnitPrice)) / float64(s.UnitPrice)
			if diff < 0 {
				diff = -diff
			}
			if diff > s.Tolerance {
				return &RuleViolationError{
					Rule:   "RULE-001",
					Detail: fmt.Sprintf("unit price out of tolerance: base %d, requested %.0f, tol %.1f%%", s.UnitPrice, unitPrice, s.Tolerance*100),
				}
			}
			break
		}
	}
	if !found {
		return &RuleViolationError{
			Rule:   "RULE-001",
			Detail: "skill not authorized: " + skillName,
		}
	}

	return nil
}

// RULE-002: head office must have substantive participation.
func (r *projectRules) EnforceRule002(projectRef VRef, ctx ProjectContext) error {
	node, err := ctx.ProjectTree.GetNode(projectRef)
	if err != nil {
		return &RuleViolationError{Rule: "RULE-002", Detail: "project node not found"}
	}

	headOfficePlatform := node.PlatformRef
	hasReview := false
	hasDelivery := false
	for _, m := range node.Milestones {
		if m.SignedBy != headOfficePlatform {
			continue
		}
		switch m.Name {
		case "审图完成", "REVIEW":
			hasReview = true
		case "成果交付", "DELIVER":
			hasDelivery = true
		}
	}

	if !hasReview {
		return &RuleViolationError{
			Rule:   "RULE-002",
			Detail: "missing head-office review milestone signature",
		}
	}
	if !hasDelivery {
		return &RuleViolationError{
			Rule:   "RULE-002",
			Detail: "missing head-office delivery milestone signature",
		}
	}
	return nil
}

// RULE-003: external payment must reference a valid contract.
func (r *projectRules) EnforceRule003(evt ProjectEvent, ctx ProjectContext) error {
	if evt.Verb != VerbPay {
		return nil
	}

	if evt.ContractRef == "" {
		return &RuleViolationError{
			Rule:   "RULE-003",
			Detail: "external payment requires contract_ref",
		}
	}

	contract, err := ctx.ContractStore.Get(evt.ContractRef)
	if err != nil {
		return &RuleViolationError{Rule: "RULE-003", Detail: "contract not found: " + string(evt.ContractRef)}
	}
	if contract.Status != "ACTIVE" {
		return &RuleViolationError{
			Rule:   "RULE-003",
			Detail: fmt.Sprintf("contract not active: %s (%s)", contract.ContractNo, contract.Status),
		}
	}
	if contract.ProcurementRef == "" {
		return &RuleViolationError{
			Rule:   "RULE-003",
			Detail: "contract missing procurement_ref",
		}
	}

	amount, ok := evt.Payload["amount"].(float64)
	if !ok {
		return &RuleViolationError{Rule: "RULE-003", Detail: "payload missing amount"}
	}
	if err := ctx.ContractStore.ValidatePayment(evt.ContractRef, int64(amount)); err != nil {
		return &RuleViolationError{
			Rule:   "RULE-003",
			Detail: "payment exceeds remaining contract balance: " + err.Error(),
		}
	}
	return nil
}

// RULE-004: child constraints cannot exceed parent remaining budget.
// Full enforcement is delegated to ProjectTreeStore.ValidateChildConstraint.
func (r *projectRules) EnforceRule004(child *ProjectNode, parent *ProjectNode) error {
	if child.ParentRef == "" {
		return nil
	}
	childQuota, ok := child.Constraint.Energy.CapitalReserve, true
	if !ok {
		return &RuleViolationError{Rule: "RULE-004", Detail: "child missing constraint definition"}
	}
	_ = childQuota
	_ = parent
	return nil
}

// RULE-005: settlement must satisfy delegated-chain completion and
// full UTXO lineage legality.
func (r *projectRules) EnforceRule005(projectRef VRef, ctx ProjectContext) error {
	children, err := ctx.ProjectTree.GetChildren(projectRef)
	if err != nil {
		return &RuleViolationError{Rule: "RULE-005", Detail: "cannot load children"}
	}

	for _, child := range children {
		if child.Status != StatusSettled && child.Status != StatusArchived {
			return &RuleViolationError{
				Rule:   "RULE-005",
				Detail: fmt.Sprintf("child project not settled: %s", child.Ref),
			}
		}
	}

	if len(children) == 0 {
		node, err := ctx.ProjectTree.GetNode(projectRef)
		if err != nil {
			return &RuleViolationError{Rule: "RULE-005", Detail: "project node not found"}
		}
		hasUTXO := false
		for _, m := range node.Milestones {
			if m.UTXORef != "" && strings.EqualFold(strings.TrimSpace(m.Status), "REACHED") {
				hasUTXO = true
				break
			}
		}
		if !hasUTXO {
			return &RuleViolationError{
				Rule:   "RULE-005",
				Detail: "leaf project has no milestone utxo output",
			}
		}
	}

	if err := validateUTXOLineageForSettlement(projectRef, ctx); err != nil {
		return &RuleViolationError{Rule: "RULE-005", Detail: err.Error()}
	}
	return nil
}

func validateUTXOLineageForSettlement(projectRef VRef, ctx ProjectContext) error {
	if ctx.UTXOStore == nil {
		return fmt.Errorf("UTXOStore not configured, cannot validate lineage")
	}
	utxos, err := ctx.UTXOStore.ListByProject(projectRef)
	if err != nil {
		return fmt.Errorf("list project utxos failed: %w", err)
	}
	if len(utxos) == 0 {
		return fmt.Errorf("project has no utxo outputs")
	}

	node, err := ctx.ProjectTree.GetNode(projectRef)
	if err != nil {
		return fmt.Errorf("project node not found")
	}

	utxoMap := make(map[VRef]*UTXORecord, len(utxos))
	for _, u := range utxos {
		if u == nil || u.Ref == "" {
			return fmt.Errorf("found invalid utxo record")
		}
		utxoMap[u.Ref] = u
	}

	visited := make(map[VRef]bool, len(utxoMap))
	onPath := make(map[VRef]bool, len(utxoMap))

	// Validate milestone-linked outputs first.
	for _, m := range node.Milestones {
		if m.UTXORef == "" || !strings.EqualFold(strings.TrimSpace(m.Status), "REACHED") {
			continue
		}
		if err := validateUTXOChainDFS(m.UTXORef, ctx, utxoMap, visited, onPath); err != nil {
			return fmt.Errorf("milestone utxo %s invalid: %w", m.UTXORef, err)
		}
	}
	// Validate all outputs under this project.
	for _, u := range utxos {
		if err := validateUTXOChainDFS(u.Ref, ctx, utxoMap, visited, onPath); err != nil {
			return fmt.Errorf("utxo %s invalid: %w", u.Ref, err)
		}
	}
	supersededRoots, err := validateUTXORelationsForSettlement(ctx, utxoMap)
	if err != nil {
		return err
	}
	if err := validateSettlementMilestonesOnActiveChains(node, ctx, utxoMap, supersededRoots); err != nil {
		return err
	}
	return nil
}

func validateUTXOChainDFS(
	ref VRef,
	ctx ProjectContext,
	utxoMap map[VRef]*UTXORecord,
	visited map[VRef]bool,
	onPath map[VRef]bool,
) error {
	if visited[ref] {
		return nil
	}
	if onPath[ref] {
		return fmt.Errorf("cycle detected in lineage")
	}

	u, ok := utxoMap[ref]
	if !ok {
		var err error
		u, err = ctx.UTXOStore.Get(ref)
		if err != nil {
			return fmt.Errorf("utxo not found")
		}
	}
	if u == nil {
		return fmt.Errorf("utxo is nil")
	}
	if !strings.EqualFold(strings.TrimSpace(u.Status), "SEALED") {
		return fmt.Errorf("utxo status is not SEALED: %s", u.Status)
	}
	if strings.TrimSpace(u.ProofHash) == "" {
		return fmt.Errorf("utxo proof_hash is empty")
	}

	hasGenesis := strings.TrimSpace(string(u.GenesisRef)) != ""
	hasInput := len(u.InputRefs) > 0
	if hasGenesis == hasInput {
		return fmt.Errorf("utxo must have exactly one source: genesis_ref xor input_refs")
	}

	onPath[ref] = true
	if hasGenesis {
		if _, err := ctx.GenesisStore.Get(u.GenesisRef); err != nil {
			onPath[ref] = false
			return fmt.Errorf("genesis source not found")
		}
	} else {
		if len(u.InputRefs) != 1 {
			onPath[ref] = false
			return fmt.Errorf("input_refs must contain exactly 1 predecessor")
		}
		parentRef := u.InputRefs[0]
		parent, ok := utxoMap[parentRef]
		if !ok {
			var err error
			parent, err = ctx.UTXOStore.Get(parentRef)
			if err != nil {
				onPath[ref] = false
				return fmt.Errorf("predecessor not found")
			}
		}
		if parent == nil {
			onPath[ref] = false
			return fmt.Errorf("predecessor is nil")
		}
		if strings.TrimSpace(u.PrevHash) == "" {
			onPath[ref] = false
			return fmt.Errorf("prev_hash is empty")
		}
		if strings.TrimSpace(parent.ProofHash) == "" {
			onPath[ref] = false
			return fmt.Errorf("predecessor proof_hash is empty")
		}
		if strings.TrimSpace(u.PrevHash) != strings.TrimSpace(parent.ProofHash) {
			onPath[ref] = false
			return fmt.Errorf("prev_hash does not match predecessor proof_hash")
		}
		if err := validateUTXOChainDFS(parentRef, ctx, utxoMap, visited, onPath); err != nil {
			onPath[ref] = false
			return err
		}
	}
	onPath[ref] = false
	visited[ref] = true
	return nil
}

func validateUTXORelationsForSettlement(ctx ProjectContext, utxoMap map[VRef]*UTXORecord) (map[VRef]bool, error) {
	supersededRoots := map[VRef]bool{}
	if ctx.UTXORelations == nil {
		return supersededRoots, nil
	}

	relationByKey := map[string]*UTXORelationRecord{}
	for ref := range utxoMap {
		outgoing, err := ctx.UTXORelations.ListByFrom(ref)
		if err != nil {
			return nil, fmt.Errorf("list utxo relations by from failed: %w", err)
		}
		for _, rel := range outgoing {
			relationByKey[relationKey(rel)] = rel
		}

		incoming, err := ctx.UTXORelations.ListByTo(ref)
		if err != nil {
			return nil, fmt.Errorf("list utxo relations by to failed: %w", err)
		}
		for _, rel := range incoming {
			relationByKey[relationKey(rel)] = rel
		}
	}

	edgesByType := map[UTXORelationType]map[VRef][]VRef{}
	for _, rel := range relationByKey {
		normalizedType, err := normalizeUTXORelationType(rel.Type)
		if err != nil {
			return nil, err
		}
		fromUTXO, err := resolveUTXOForValidation(ctx, utxoMap, rel.FromRef)
		if err != nil {
			return nil, fmt.Errorf("relation %s invalid from_ref: %w", relationRefOrPlaceholder(rel), err)
		}
		toUTXO, err := resolveUTXOForValidation(ctx, utxoMap, rel.ToRef)
		if err != nil {
			return nil, fmt.Errorf("relation %s invalid to_ref: %w", relationRefOrPlaceholder(rel), err)
		}
		if err := validateUTXORelationByType(normalizedType, fromUTXO, toUTXO); err != nil {
			return nil, fmt.Errorf("relation %s (%s) invalid: %w", relationRefOrPlaceholder(rel), normalizedType, err)
		}
		if strings.TrimSpace(string(rel.ChangeUTXORef)) == "" {
			return nil, fmt.Errorf("relation %s missing change_utxo_ref", relationRefOrPlaceholder(rel))
		}
		changeUTXO, err := resolveUTXOForValidation(ctx, utxoMap, rel.ChangeUTXORef)
		if err != nil {
			return nil, fmt.Errorf("relation %s invalid change_utxo_ref: %w", relationRefOrPlaceholder(rel), err)
		}
		if !strings.EqualFold(strings.TrimSpace(changeUTXO.Status), "SEALED") {
			return nil, fmt.Errorf("relation %s change_utxo_ref must be SEALED", relationRefOrPlaceholder(rel))
		}
		if changeUTXO.ProjectRef != fromUTXO.ProjectRef {
			return nil, fmt.Errorf("relation %s change_utxo_ref must belong to same project", relationRefOrPlaceholder(rel))
		}
		if changeUTXO.Ref == fromUTXO.Ref || changeUTXO.Ref == toUTXO.Ref {
			return nil, fmt.Errorf("relation %s change_utxo_ref must differ from from/to refs", relationRefOrPlaceholder(rel))
		}
		if !IsAllowedChangeUTXOKindForRelation(normalizedType, changeUTXO.Kind) {
			return nil, fmt.Errorf(
				"relation %s change_utxo_ref kind %q is invalid for %s",
				relationRefOrPlaceholder(rel),
				changeUTXO.Kind,
				normalizedType,
			)
		}
		if strings.TrimSpace(rel.Reason) == "" {
			return nil, fmt.Errorf("relation %s missing reason", relationRefOrPlaceholder(rel))
		}
		if err := validateRelationAuthorizationChain(rel); err != nil {
			return nil, fmt.Errorf("relation %s %w", relationRefOrPlaceholder(rel), err)
		}
		edges := edgesByType[normalizedType]
		if edges == nil {
			edges = map[VRef][]VRef{}
			edgesByType[normalizedType] = edges
		}
		edges[fromUTXO.Ref] = append(edges[fromUTXO.Ref], toUTXO.Ref)
		if normalizedType == UTXORelationSupersedes {
			// Direction rule: from supersedes to.
			supersededRoots[toUTXO.Ref] = true
		}
	}

	for relationType, edges := range edgesByType {
		if err := validateUTXORelationDAG(relationType, edges); err != nil {
			return nil, err
		}
	}
	return supersededRoots, nil
}

func validateSettlementMilestonesOnActiveChains(
	node *ProjectNode,
	ctx ProjectContext,
	utxoMap map[VRef]*UTXORecord,
	supersededRoots map[VRef]bool,
) error {
	if node == nil || len(supersededRoots) == 0 {
		return nil
	}
	for _, m := range node.Milestones {
		if m.UTXORef == "" || !strings.EqualFold(strings.TrimSpace(m.Status), "REACHED") {
			continue
		}
		rootRef, err := resolveUTXOChainRoot(m.UTXORef, ctx, utxoMap)
		if err != nil {
			return fmt.Errorf("milestone utxo %s root resolve failed: %w", m.UTXORef, err)
		}
		if supersededRoots[rootRef] {
			return fmt.Errorf("milestone utxo %s belongs to superseded chain root %s", m.UTXORef, rootRef)
		}
	}
	return nil
}

func resolveUTXOChainRoot(ref VRef, ctx ProjectContext, utxoMap map[VRef]*UTXORecord) (VRef, error) {
	seen := map[VRef]bool{}
	current := ref
	for {
		if seen[current] {
			return "", fmt.Errorf("cycle detected while resolving chain root")
		}
		seen[current] = true

		u, err := resolveUTXOForValidation(ctx, utxoMap, current)
		if err != nil {
			return "", err
		}
		hasGenesis := strings.TrimSpace(string(u.GenesisRef)) != ""
		hasInput := len(u.InputRefs) > 0
		if hasGenesis && !hasInput {
			return u.Ref, nil
		}
		if !hasGenesis && hasInput {
			if len(u.InputRefs) != 1 {
				return "", fmt.Errorf("input_refs must contain exactly 1 predecessor")
			}
			current = u.InputRefs[0]
			continue
		}
		return "", fmt.Errorf("utxo must have exactly one source: genesis_ref xor input_refs")
	}
}

func relationKey(rel *UTXORelationRecord) string {
	if rel == nil {
		return "nil"
	}
	if strings.TrimSpace(string(rel.Ref)) != "" {
		return string(rel.Ref)
	}
	return string(rel.Type) + "|" + string(rel.FromRef) + "|" + string(rel.ToRef)
}

func relationRefOrPlaceholder(rel *UTXORelationRecord) string {
	if rel == nil {
		return "<nil>"
	}
	if strings.TrimSpace(string(rel.Ref)) != "" {
		return string(rel.Ref)
	}
	return fmt.Sprintf("%s:%s->%s", rel.Type, rel.FromRef, rel.ToRef)
}

func validateRelationAuthorizationChain(rel *UTXORelationRecord) error {
	if rel == nil {
		return fmt.Errorf("is nil")
	}
	if rel.Metadata == nil {
		return fmt.Errorf("missing payload.authorization_chain")
	}
	raw, ok := rel.Metadata["authorization_chain"]
	if !ok {
		return fmt.Errorf("missing payload.authorization_chain")
	}
	chain, err := normalizeAuthorizationChainForValidation(raw)
	if err != nil {
		return fmt.Errorf("invalid payload.authorization_chain: %w", err)
	}
	if len(chain) == 0 {
		return fmt.Errorf("invalid payload.authorization_chain: empty")
	}
	return nil
}

func normalizeAuthorizationChainForValidation(raw interface{}) ([]string, error) {
	switch v := raw.(type) {
	case []string:
		out := make([]string, 0, len(v))
		for _, item := range v {
			trimmed := strings.TrimSpace(item)
			if trimmed == "" {
				continue
			}
			out = append(out, trimmed)
		}
		return out, nil
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("must be a string array")
			}
			trimmed := strings.TrimSpace(text)
			if trimmed == "" {
				continue
			}
			out = append(out, trimmed)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("must be a string array")
	}
}

func normalizeUTXORelationType(raw UTXORelationType) (UTXORelationType, error) {
	v := UTXORelationType(strings.ToUpper(strings.TrimSpace(string(raw))))
	switch v {
	case UTXORelationSupersedes, UTXORelationReassigns, UTXORelationSpecUpgrades:
		return v, nil
	default:
		return "", fmt.Errorf("invalid utxo relation type: %s", raw)
	}
}

func resolveUTXOForValidation(
	ctx ProjectContext,
	utxoMap map[VRef]*UTXORecord,
	ref VRef,
) (*UTXORecord, error) {
	if strings.TrimSpace(string(ref)) == "" {
		return nil, fmt.Errorf("utxo ref is empty")
	}
	if u, ok := utxoMap[ref]; ok && u != nil {
		return u, nil
	}
	u, err := ctx.UTXOStore.Get(ref)
	if err != nil {
		return nil, fmt.Errorf("utxo not found: %s", ref)
	}
	if u == nil {
		return nil, fmt.Errorf("utxo is nil: %s", ref)
	}
	return u, nil
}

func validateUTXORelationByType(
	relationType UTXORelationType,
	fromUTXO, toUTXO *UTXORecord,
) error {
	if fromUTXO == nil || toUTXO == nil {
		return fmt.Errorf("utxo relation endpoints are required")
	}
	if fromUTXO.Ref == toUTXO.Ref {
		return fmt.Errorf("from_ref and to_ref must be different")
	}
	if !strings.EqualFold(strings.TrimSpace(fromUTXO.Status), "SEALED") ||
		!strings.EqualFold(strings.TrimSpace(toUTXO.Status), "SEALED") {
		return fmt.Errorf("both from and to utxo must be SEALED")
	}
	if fromUTXO.ProjectRef != toUTXO.ProjectRef {
		return fmt.Errorf("from/to utxo must belong to same project")
	}

	switch relationType {
	case UTXORelationSupersedes:
		if strings.TrimSpace(string(fromUTXO.GenesisRef)) == "" || strings.TrimSpace(string(toUTXO.GenesisRef)) == "" {
			return fmt.Errorf("supersede requires both utxos to have genesis_ref")
		}
		if len(fromUTXO.InputRefs) > 0 || len(toUTXO.InputRefs) > 0 {
			return fmt.Errorf("supersede requires both utxos to be chain roots")
		}
		if fromUTXO.GenesisRef == toUTXO.GenesisRef {
			return fmt.Errorf("supersede requires different genesis_ref values")
		}
		return nil
	case UTXORelationReassigns, UTXORelationSpecUpgrades:
		if len(toUTXO.InputRefs) != 1 || toUTXO.InputRefs[0] != fromUTXO.Ref {
			return fmt.Errorf("relation requires to.input_refs=[from_ref]")
		}
		if strings.TrimSpace(fromUTXO.ProofHash) == "" || strings.TrimSpace(toUTXO.PrevHash) == "" {
			return fmt.Errorf("relation requires predecessor proof linkage")
		}
		if strings.TrimSpace(toUTXO.PrevHash) != strings.TrimSpace(fromUTXO.ProofHash) {
			return fmt.Errorf("relation prev_hash must match predecessor proof_hash")
		}
		return nil
	default:
		return fmt.Errorf("invalid relation type: %s", relationType)
	}
}

func validateUTXORelationDAG(relationType UTXORelationType, edges map[VRef][]VRef) error {
	visited := map[VRef]bool{}
	onPath := map[VRef]bool{}
	var dfs func(ref VRef) error
	dfs = func(ref VRef) error {
		if visited[ref] {
			return nil
		}
		if onPath[ref] {
			return fmt.Errorf("cycle detected in %s relations", relationType)
		}
		onPath[ref] = true
		for _, next := range edges[ref] {
			if err := dfs(next); err != nil {
				return err
			}
		}
		onPath[ref] = false
		visited[ref] = true
		return nil
	}
	for ref := range edges {
		if err := dfs(ref); err != nil {
			return err
		}
	}
	return nil
}

type RuleEngine struct {
	rules ProjectRules
	audit AuditStore
}

func NewRuleEngine(rules ProjectRules, audit AuditStore) *RuleEngine {
	return &RuleEngine{rules: rules, audit: audit}
}

// Execute runs pre-check rules for one project event.
func (e *RuleEngine) Execute(evt ProjectEvent, ctx ProjectContext) error {
	eventID, _ := ctx.AuditStore.RecordEvent(evt, ctx.TenantID)

	if evt.Verb == VerbPay {
		if err := e.rules.EnforceRule003(evt, ctx); err != nil {
			ctx.AuditStore.RecordViolation("RULE-003", evt, err.Error())
			return err
		}
	}

	if evt.Verb == VerbTransform {
		if err := e.rules.EnforceRule001(evt, ctx); err != nil {
			ctx.AuditStore.RecordViolation("RULE-001", evt, err.Error())
			return err
		}
	}

	if evt.Verb == VerbSettle {
		if err := e.rules.EnforceRule002(evt.ProjectRef, ctx); err != nil {
			ctx.AuditStore.RecordViolation("RULE-002", evt, err.Error())
			return err
		}
		if err := e.rules.EnforceRule005(evt.ProjectRef, ctx); err != nil {
			ctx.AuditStore.RecordViolation("RULE-005", evt, err.Error())
			return err
		}
	}

	_ = eventID
	return nil
}
