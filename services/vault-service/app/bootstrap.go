package app

import (
	pc "coordos/project-core"
	"coordos/vault-service/infra/store"
)

// StoreSet groups all store dependencies required by vault-service.
type StoreSet struct {
	Projects      store.ProjectTreeStore
	Genesis       store.GenesisStore
	Contracts     store.ContractStore
	Parcels       store.ParcelStore
	UTXOs         store.UTXOStore
	UTXORelations store.UTXORelationStore
	Settlements   store.SettlementStore
	Wallets       store.WalletStore
	Audit         store.AuditStore
}

// BuildDeps assembles app dependencies and project-core engines with
// tenant-aware adapters.
func BuildDeps(tenantID string, s StoreSet) Deps {
	rules := pc.NewProjectRules()
	coreProject := pcProjectTreeAdapter{tenantID: tenantID, s: s.Projects}
	coreGenesis := pcGenesisAdapter{tenantID: tenantID, s: s.Genesis}
	coreAudit := pcAuditAdapter{tenantID: tenantID, s: s.Audit}

	return Deps{
		Projects:      s.Projects,
		Genesis:       s.Genesis,
		Contracts:     s.Contracts,
		Parcels:       s.Parcels,
		UTXOs:         s.UTXOs,
		UTXORelations: s.UTXORelations,
		Settlements:   s.Settlements,
		Wallets:       s.Wallets,
		Audit:         s.Audit,
		Rules:         rules,
		Fission:       pc.NewFissionEngine(coreGenesis, coreAudit),
		StateMachine:  pc.NewStateMachine(coreProject, coreAudit),
	}
}
