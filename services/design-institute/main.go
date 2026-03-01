package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"time"

	"coordos/design-institute/achievement"
	"coordos/design-institute/achievementprofile"
	"coordos/design-institute/api"
	"coordos/design-institute/approve"
	"coordos/design-institute/bidding"
	"coordos/design-institute/company"
	"coordos/design-institute/compliance"
	"coordos/design-institute/contract"
	"coordos/design-institute/costticket"
	"coordos/design-institute/employee"
	"coordos/design-institute/gathering"
	"coordos/design-institute/invoice"
	"coordos/design-institute/namespace"
	"coordos/design-institute/payment"
	"coordos/design-institute/project"
	"coordos/design-institute/publicapi"
	"coordos/design-institute/publishing"
	"coordos/design-institute/qualification"
	"coordos/design-institute/report"
	"coordos/design-institute/resolve"
	"coordos/design-institute/resourcebinding"
	"coordos/design-institute/rights"
	"coordos/design-institute/settlement"

	_ "github.com/lib/pq"
)

type config struct {
	Addr              string
	PGDSN             string
	TenantID          int
	HeadOfficeRefBase string
	SPUCatalogPath    string
}

func main() {
	cfg := loadConfig()

	db, err := sql.Open("postgres", cfg.PGDSN)
	if err != nil {
		log.Fatalf("open postgres failed: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("ping postgres failed: %v", err)
	}
	compatCtx, compatCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer compatCancel()
	if err := ensureSchemaCompat(compatCtx, db); err != nil {
		log.Fatalf("schema compatibility check failed: %v", err)
	}

	projectSvc := project.NewService(project.NewPGStore(db), cfg.TenantID)
	contractSvc := contract.NewService(contract.NewPGStore(db), cfg.TenantID)
	gatheringSvc := gathering.NewService(gathering.NewPGStore(db), cfg.TenantID)
	settlementSvc := settlement.NewService(settlement.NewPGStore(db), cfg.TenantID)
	invoiceSvc := invoice.NewService(invoice.NewPGStore(db), cfg.TenantID)
	costTicketSvc := costticket.NewService(costticket.NewPGStore(db), cfg.TenantID)
	companySvc := company.NewService(company.NewPGStore(db), cfg.TenantID)
	employeeSvc := employee.NewService(employee.NewPGStore(db), cfg.TenantID)
	achievementSvc := achievement.NewService(achievement.NewPGStore(db), cfg.TenantID)
	approveSvc := approve.NewService(approve.NewPGStore(db), cfg.TenantID)
	paymentSvc := payment.NewService(payment.NewPGStore(db), contractSvc, cfg.TenantID)
	reportSvc := report.NewService(report.NewPGStore(db), cfg.TenantID)
	qualificationSvc := qualification.NewService(qualification.NewPGStore(db), cfg.TenantID)
	achievementProfileSvc := achievementprofile.NewService(achievementprofile.NewPGStore(db), cfg.TenantID)
	resolveSvc := resolve.NewService(qualificationSvc, db, cfg.TenantID, cfg.HeadOfficeRefBase)
	rightSvc := rights.NewService(rights.NewPGStore(db), cfg.TenantID)
	namespaceSvc := namespace.NewService(namespace.NewPGStore(db), cfg.TenantID)
	publishingSvc := publishing.NewService(publishing.NewPGStore(db), cfg.TenantID)
	publicAPISvc := publicapi.NewService(publicapi.NewPGStore(db), cfg.TenantID, cfg.SPUCatalogPath)
	biddingSvc := bidding.NewService(bidding.NewPGStore(db), cfg.TenantID)
	complianceSvc := compliance.NewService(compliance.NewPGStore(db), cfg.TenantID)
	resourceBindingSvc := resourcebinding.NewService(resourcebinding.NewPGStore(db), cfg.TenantID)
	achievementSvc.SetRule002Checker(qualificationSvc)

	approveSvc.SetCallbacks(
		func(context.Context, approve.BizType, int64) error { return nil },
		func(context.Context, approve.BizType, int64, string) error { return nil },
	)

	handler := api.NewHandler(api.Services{
		Project:            projectSvc,
		Contract:           contractSvc,
		Gathering:          gatheringSvc,
		Settlement:         settlementSvc,
		Invoice:            invoiceSvc,
		CostTicket:         costTicketSvc,
		Payment:            paymentSvc,
		Company:            companySvc,
		Employee:           employeeSvc,
		Achievement:        achievementSvc,
		Approve:            approveSvc,
		Report:             reportSvc,
		Qualification:      qualificationSvc,
		AchievementProfile: achievementProfileSvc,
		Resolve:            resolveSvc,
		Right:              rightSvc,
		Namespace:          namespaceSvc,
		Publishing:         publishingSvc,
		PublicAPI:          publicAPISvc,
		Bidding:            biddingSvc,
		Compliance:         complianceSvc,
		ResourceBinding:    resourceBindingSvc,
	})

	server := &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("design-institute listening on %s tenant=%d", cfg.Addr, cfg.TenantID)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server failed: %v", err)
	}
}

func ensureSchemaCompat(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`ALTER TABLE balances ADD COLUMN IF NOT EXISTS contract_id BIGINT REFERENCES contracts(id)`,
		`CREATE INDEX IF NOT EXISTS idx_balances_contract ON balances(contract_id)`,
		`CREATE TABLE IF NOT EXISTS qualification_assignments (
			id BIGSERIAL PRIMARY KEY,
			qualification_id BIGINT NOT NULL REFERENCES qualifications(id),
			executor_ref VARCHAR(500) NOT NULL,
			project_ref VARCHAR(500) NOT NULL,
			status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'
				CHECK (status IN ('ACTIVE','RELEASED')),
			tenant_id INT NOT NULL DEFAULT 10000,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			released_at TIMESTAMPTZ
		)`,
		`CREATE INDEX IF NOT EXISTS idx_qa_project_active ON qualification_assignments(tenant_id, project_ref, status)`,
		`CREATE INDEX IF NOT EXISTS idx_qa_qualification_active ON qualification_assignments(tenant_id, qualification_id, status)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_qa_uniq_active_qual
		   ON qualification_assignments(qualification_id) WHERE status='ACTIVE'`,
		`CREATE TABLE IF NOT EXISTS namespaces (
			id BIGSERIAL PRIMARY KEY,
			ref VARCHAR(500) NOT NULL,
			parent_ref VARCHAR(500),
			name VARCHAR(255) NOT NULL,
			inherited_rules TEXT[] NOT NULL DEFAULT '{}',
			owned_genesis TEXT[] NOT NULL DEFAULT '{}',
			tenant_id INT NOT NULL DEFAULT 10000,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (tenant_id, ref)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_namespaces_parent
		   ON namespaces(tenant_id, parent_ref)`,
		`CREATE TABLE IF NOT EXISTS namespace_delegations (
			id BIGSERIAL PRIMARY KEY,
			from_ref VARCHAR(500) NOT NULL,
			to_ref VARCHAR(500) NOT NULL,
			project_ref VARCHAR(500) NOT NULL DEFAULT '',
			action VARCHAR(100) NOT NULL DEFAULT '',
			status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'
				CHECK (status IN ('ACTIVE','DISABLED')),
			tenant_id INT NOT NULL DEFAULT 10000,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_namespace_delegations_match
		   ON namespace_delegations(tenant_id, from_ref, to_ref, status, project_ref, action)`,
		`CREATE TABLE IF NOT EXISTS review_certificates (
			id BIGSERIAL PRIMARY KEY,
			cert_ref VARCHAR(500) NOT NULL,
			project_ref VARCHAR(500) NOT NULL,
			drawing_no VARCHAR(255) NOT NULL,
			executor_ref VARCHAR(500) NOT NULL,
			payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			tenant_id INT NOT NULL DEFAULT 10000,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (tenant_id, cert_ref)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_review_certificates_project
		   ON review_certificates(tenant_id, project_ref, drawing_no, created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS drawing_versions (
			id BIGSERIAL PRIMARY KEY,
			drawing_no VARCHAR(255) NOT NULL,
			version_no INT NOT NULL,
			project_ref VARCHAR(500) NOT NULL,
			review_cert_ref VARCHAR(500) NOT NULL,
			file_hash VARCHAR(255),
			publisher_ref VARCHAR(500),
			status VARCHAR(20) NOT NULL DEFAULT 'CURRENT'
				CHECK (status IN ('CURRENT','SUPERSEDED')),
			payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			tenant_id INT NOT NULL DEFAULT 10000,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (tenant_id, drawing_no, version_no)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_drawing_versions_current
		   ON drawing_versions(tenant_id, drawing_no, status, version_no DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_drawing_versions_project
		   ON drawing_versions(tenant_id, project_ref, drawing_no, version_no DESC)`,
		`CREATE TABLE IF NOT EXISTS bid_profiles (
			id BIGSERIAL PRIMARY KEY,
			ref VARCHAR(500) NOT NULL UNIQUE,
			name VARCHAR(255) NOT NULL,
			project_ref VARCHAR(500) NOT NULL,
			spu_ref VARCHAR(500) NOT NULL,
			profile_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
			requirements JSONB NOT NULL DEFAULT '{}'::jsonb,
			package_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			status VARCHAR(20) NOT NULL DEFAULT 'DRAFT'
				CHECK (status IN ('DRAFT','PUBLISHED','ARCHIVED')),
			tenant_id INT NOT NULL DEFAULT 10000,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_bid_profiles_project
		   ON bid_profiles(tenant_id, project_ref, status)`,
		`CREATE TABLE IF NOT EXISTS violation_records (
			id BIGSERIAL PRIMARY KEY,
			executor_ref VARCHAR(500) NOT NULL,
			project_ref VARCHAR(500) NOT NULL,
			rule_code VARCHAR(100) NOT NULL,
			severity VARCHAR(20) NOT NULL
				CHECK (severity IN ('LOW','MEDIUM','HIGH','CRITICAL')),
			message TEXT NOT NULL DEFAULT '',
			occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			tenant_id INT NOT NULL DEFAULT 10000,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_violation_executor
		   ON violation_records(tenant_id, executor_ref, occurred_at DESC)`,
		`CREATE TABLE IF NOT EXISTS executor_stats (
			id BIGSERIAL PRIMARY KEY,
			executor_ref VARCHAR(500) NOT NULL,
			total_projects INT NOT NULL DEFAULT 0,
			total_utxos INT NOT NULL DEFAULT 0,
			total_violations INT NOT NULL DEFAULT 0,
			last_violation_at TIMESTAMPTZ,
			score INT NOT NULL DEFAULT 0,
			capability_level VARCHAR(20) NOT NULL DEFAULT 'RISK',
			tenant_id INT NOT NULL DEFAULT 10000,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (tenant_id, executor_ref)
		)`,
		`CREATE TABLE IF NOT EXISTS resource_bindings (
			id BIGSERIAL PRIMARY KEY,
			resource_ref VARCHAR(500) NOT NULL,
			resource_type VARCHAR(100) NOT NULL,
			project_ref VARCHAR(500) NOT NULL,
			executor_ref VARCHAR(500) NOT NULL DEFAULT '',
			spu_ref VARCHAR(500) NOT NULL DEFAULT '',
			status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'
				CHECK (status IN ('ACTIVE','RELEASED')),
			note TEXT NOT NULL DEFAULT '',
			tenant_id INT NOT NULL DEFAULT 10000,
			bound_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			released_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_resource_bindings_project
		   ON resource_bindings(tenant_id, project_ref, status, bound_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_resource_bindings_executor
		   ON resource_bindings(tenant_id, executor_ref, status, bound_at DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_resource_bindings_active_unique
		   ON resource_bindings(tenant_id, resource_ref) WHERE status='ACTIVE'`,
	}
	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply statement failed: %w", err)
		}
	}
	return nil
}
