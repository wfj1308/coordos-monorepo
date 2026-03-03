package main

import (
	"fmt"
	"log"
	"strings"

	"coordos/vault-service/api"
	"coordos/vault-service/app"
	svcconfig "coordos/vault-service/config"
	"coordos/vault-service/infra/store"
	"coordos/vault-service/infra/store/rocksdb"
)

type backend interface {
	Close() error
	ProjectTree() store.ProjectTreeStore
	Genesis() store.GenesisStore
	Contracts() store.ContractStore
	Parcels() store.ParcelStore
	UTXOs() store.UTXOStore
	UTXORelations() store.UTXORelationStore
	Settlements() store.SettlementStore
	Wallets() store.WalletStore
	Audit() store.AuditStore
}

func main() {
	cfg, err := svcconfig.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	db, err := openBackend(cfg)
	if err != nil {
		log.Fatalf("open storage backend: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("close storage backend: %v", err)
		}
	}()

	backfilled, err := db.UTXORelations().BackfillAuthorizationChain(cfg.Tenant.ID, "")
	if err != nil {
		log.Fatalf("backfill utxo relation evidence failed: %v", err)
	}
	if backfilled > 0 {
		log.Printf("utxo relation evidence backfilled tenant=%s count=%d", cfg.Tenant.ID, backfilled)
	}

	deps := app.BuildDeps(cfg.Tenant.ID, app.StoreSet{
		Projects:      db.ProjectTree(),
		Genesis:       db.Genesis(),
		Contracts:     db.Contracts(),
		Parcels:       db.Parcels(),
		UTXOs:         db.UTXOs(),
		UTXORelations: db.UTXORelations(),
		Settlements:   db.Settlements(),
		Wallets:       db.Wallets(),
		Audit:         db.Audit(),
	})

	log.Printf(
		"vault-service starting addr=%s tenant=%s backend=%s",
		cfg.Server.Addr,
		cfg.Tenant.ID,
		strings.ToLower(strings.TrimSpace(cfg.Storage.Backend)),
	)
	if err := api.Run(deps, cfg.Server.Addr, cfg.Server.JWTSecret); err != nil {
		log.Fatalf("vault-service stopped with error: %v", err)
	}
}

func openBackend(cfg svcconfig.Config) (backend, error) {
	b := strings.ToLower(strings.TrimSpace(cfg.Storage.Backend))
	if b != "rocksdb" {
		return nil, fmt.Errorf("unsupported storage backend %q: use rocksdb", cfg.Storage.Backend)
	}
	return rocksdb.Open(cfg.Storage.RocksPath)
}
