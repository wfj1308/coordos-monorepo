package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"coordos/design-institute/achievement"
	"coordos/design-institute/achievementprofile"
	"coordos/design-institute/anchor"
	"coordos/design-institute/api"
	"coordos/design-institute/approve"
	"coordos/design-institute/bid"
	"coordos/design-institute/bidding"
	"coordos/design-institute/capability"
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
	"coordos/design-institute/register"
	"coordos/design-institute/report"
	"coordos/design-institute/resolve"
	"coordos/design-institute/resourcebinding"
	"coordos/design-institute/review_publish"
	"coordos/design-institute/rights"
	"coordos/design-institute/settlement"
	"coordos/design-institute/stepachievement"

	_ "github.com/lib/pq"
)

type config struct {
	Addr                    string
	PGDSN                   string
	TenantID                int
	HeadOfficeRefBase       string
	SPUCatalogPath          string
	ProofAnchorEnabled      bool
	ProofAnchorScanInterval time.Duration
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
	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	defer runtimeCancel()
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
	bidSvc := bid.NewService(bid.NewPGStore(db), cfg.TenantID)
	capabilitySvc := capability.NewService(db, cfg.TenantID)
	complianceSvc := compliance.NewService(compliance.NewPGStore(db), cfg.TenantID)
	resourceBindingSvc := resourcebinding.NewService(resourcebinding.NewPGStore(db), cfg.TenantID)
	registerSvc := register.NewService(db, cfg.TenantID)
	reviewPublishSvc := review_publish.NewService(review_publish.NewPGStore(db), cfg.TenantID)
	stepAchievementSvc := stepachievement.NewService(db, cfg.TenantID)
	anchorSvc := anchor.NewService(anchor.NewRepository(db), log.Default())
	achievementSvc.SetRule002Checker(qualificationSvc)
	if cfg.ProofAnchorEnabled {
		go anchorSvc.Run(runtimeCtx, cfg.ProofAnchorScanInterval)
	}

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
		Bid:                bidSvc,
		Capability:         capabilitySvc,
		Compliance:         complianceSvc,
		ResourceBinding:    resourceBindingSvc,
		Register:           registerSvc,
		ReviewPublish:      reviewPublishSvc,
		StepAchievement:    stepAchievementSvc,
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
		`ALTER TABLE companies ADD COLUMN IF NOT EXISTS short_name VARCHAR(255)`,
		`ALTER TABLE companies ADD COLUMN IF NOT EXISTS credit_code VARCHAR(64)`,
		`ALTER TABLE companies ADD COLUMN IF NOT EXISTS reg_capital BIGINT`,
		`ALTER TABLE companies ADD COLUMN IF NOT EXISTS legal_rep VARCHAR(255)`,
		`ALTER TABLE companies ADD COLUMN IF NOT EXISTS tech_director VARCHAR(255)`,
		`ALTER TABLE companies ADD COLUMN IF NOT EXISTS established_at DATE`,
		`ALTER TABLE companies ADD COLUMN IF NOT EXISTS cert_no VARCHAR(255)`,
		`ALTER TABLE companies ADD COLUMN IF NOT EXISTS cert_valid_until DATE`,
		`DROP INDEX IF EXISTS idx_companies_credit_code_uq`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_credit_code_uq
		   ON companies(credit_code)`,
		`ALTER TABLE employees ADD COLUMN IF NOT EXISTS id_card VARCHAR(32)`,
		`ALTER TABLE employees ADD COLUMN IF NOT EXISTS company_ref VARCHAR(500)`,
		`DROP INDEX IF EXISTS idx_employees_id_card_uq`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_employees_id_card_uq
		   ON employees(id_card)`,
		`ALTER TABLE qualifications ADD COLUMN IF NOT EXISTS max_concurrent_projects INT`,
		`ALTER TABLE qualifications ALTER COLUMN holder_id SET DEFAULT 0`,
		`ALTER TABLE qualifications ALTER COLUMN holder_name SET DEFAULT ''`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_qualifications_cert_no_uq
		   ON qualifications(cert_no)`,
		`CREATE TABLE IF NOT EXISTS genesis_utxos (
			id BIGSERIAL PRIMARY KEY,
			ref VARCHAR(500) NOT NULL,
			resource_type VARCHAR(100) NOT NULL,
			name VARCHAR(500) NOT NULL,
			total_amount BIGINT NOT NULL DEFAULT 0,
			available_amount BIGINT NOT NULL DEFAULT 0,
			unit VARCHAR(50) NOT NULL DEFAULT '',
			batch_source VARCHAR(50) NOT NULL DEFAULT 'INTERNAL',
			holders JSONB NOT NULL DEFAULT '[]'::jsonb,
			quantity INT NOT NULL DEFAULT 1,
			constraints JSONB NOT NULL DEFAULT '{"time":{},"quantity":{},"scope":{}}'::jsonb,
			consumed_by JSONB NOT NULL DEFAULT '[]'::jsonb,
			remaining INT NOT NULL DEFAULT 1,
			status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
			tenant_id INT NOT NULL DEFAULT 10000,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (tenant_id, ref)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_genesis_utxos_tenant_type
		   ON genesis_utxos(tenant_id, resource_type, status)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_genesis_utxos_ref_uq
		   ON genesis_utxos(ref)`,
		`ALTER TABLE genesis_utxos ADD COLUMN IF NOT EXISTS batch_source VARCHAR(50) DEFAULT 'INTERNAL'`,
		`ALTER TABLE genesis_utxos ADD COLUMN IF NOT EXISTS holders JSONB DEFAULT '[]'::jsonb`,
		`ALTER TABLE genesis_utxos ADD COLUMN IF NOT EXISTS quantity INT DEFAULT 1`,
		`ALTER TABLE genesis_utxos ADD COLUMN IF NOT EXISTS consumed_by JSONB DEFAULT '[]'::jsonb`,
		`ALTER TABLE genesis_utxos ADD COLUMN IF NOT EXISTS remaining INT DEFAULT 1`,
		`DO $$
		BEGIN
			IF EXISTS (
				SELECT 1
				FROM information_schema.columns
				WHERE table_schema='public'
				  AND table_name='genesis_utxos'
				  AND column_name='constraint_json'
			) THEN
				ALTER TABLE genesis_utxos RENAME COLUMN constraint_json TO constraints;
			END IF;
		END $$`,
		`ALTER TABLE namespaces ADD COLUMN IF NOT EXISTS short_code VARCHAR(100)`,
		`ALTER TABLE namespaces ADD COLUMN IF NOT EXISTS org_type VARCHAR(50)`,
		`ALTER TABLE namespaces ADD COLUMN IF NOT EXISTS depth INT NOT NULL DEFAULT 0`,
		`ALTER TABLE namespaces ADD COLUMN IF NOT EXISTS path TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE namespaces ADD COLUMN IF NOT EXISTS accessible_genesis TEXT[] NOT NULL DEFAULT '{}'`,
		`ALTER TABLE namespaces ADD COLUMN IF NOT EXISTS manage_fee_rate DOUBLE PRECISION NOT NULL DEFAULT 0`,
		`ALTER TABLE namespaces ADD COLUMN IF NOT EXISTS route_policy JSONB NOT NULL DEFAULT '{}'::jsonb`,
		`ALTER TABLE namespaces ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_namespaces_ref_uq
		   ON namespaces(ref)`,
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
		`ALTER TABLE drawing_versions ADD COLUMN IF NOT EXISTS proof_hash VARCHAR(128)`,
		`CREATE INDEX IF NOT EXISTS idx_drawing_versions_proof
		   ON drawing_versions(tenant_id, proof_hash)`,
		`CREATE TABLE IF NOT EXISTS proof_anchors (
			id BIGSERIAL PRIMARY KEY,
			proof_hash VARCHAR(255) NOT NULL UNIQUE,
			ref VARCHAR(500) NOT NULL,
			tenant_id INT NOT NULL DEFAULT 10000,
			status VARCHAR(20) NOT NULL DEFAULT 'PENDING'
				CHECK (status IN ('PENDING','ANCHORED','FAILED')),
			anchor_chain VARCHAR(50),
			anchor_tx_hash VARCHAR(255),
			anchor_block BIGINT,
			anchored_at TIMESTAMPTZ,
			error TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_proof_anchors_ref
		   ON proof_anchors(ref)`,
		`CREATE INDEX IF NOT EXISTS idx_proof_anchors_status
		   ON proof_anchors(status, tenant_id)`,
		`CREATE INDEX IF NOT EXISTS idx_proof_anchors_tx
		   ON proof_anchors(anchor_chain, anchor_tx_hash)`,
		`CREATE TABLE IF NOT EXISTS review_opinions (
			id                BIGSERIAL PRIMARY KEY,
			project_ref       VARCHAR(500) NOT NULL,
			drawing_no        VARCHAR(255) NOT NULL,
			total_opinions    INT NOT NULL DEFAULT 0,
			processed_opinions INT NOT NULL DEFAULT 0,
			major_opinions    INT NOT NULL DEFAULT 0,
			tenant_id         INT NOT NULL DEFAULT 10000,
			created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (tenant_id, project_ref, drawing_no)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_review_opinions_project
		   ON review_opinions(tenant_id, project_ref, drawing_no)`,
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
		`ALTER TABLE violation_records ADD COLUMN IF NOT EXISTS violation_type VARCHAR(100)`,
		`ALTER TABLE violation_records ADD COLUMN IF NOT EXISTS utxo_ref VARCHAR(500)`,
		`ALTER TABLE violation_records ADD COLUMN IF NOT EXISTS description TEXT`,
		`ALTER TABLE violation_records ADD COLUMN IF NOT EXISTS penalty NUMERIC NOT NULL DEFAULT 0`,
		`ALTER TABLE violation_records ADD COLUMN IF NOT EXISTS recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		`UPDATE violation_records
		   SET violation_type = COALESCE(NULLIF(violation_type,''), rule_code),
		       description = COALESCE(NULLIF(description,''), message),
		       recorded_at = COALESCE(recorded_at, occurred_at, created_at)
		 WHERE violation_type IS NULL
		    OR description IS NULL
		    OR recorded_at IS NULL`,
		`ALTER TABLE violation_records DROP CONSTRAINT IF EXISTS violation_records_severity_check`,
		`ALTER TABLE violation_records
		   ADD CONSTRAINT violation_records_severity_check
		   CHECK (severity IN ('LOW','MEDIUM','HIGH','CRITICAL','MINOR','MAJOR'))`,
		`CREATE INDEX IF NOT EXISTS idx_violation_executor_recorded
		   ON violation_records(tenant_id, executor_ref, recorded_at DESC)`,
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
		`ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS spu_pass_rate NUMERIC NOT NULL DEFAULT 0`,
		`ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS violation_count INT NOT NULL DEFAULT 0`,
		`ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS capability_level_num NUMERIC NOT NULL DEFAULT 0`,
		`ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS specialty_spus TEXT[] NOT NULL DEFAULT '{}'`,
		`ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS last_computed_at TIMESTAMPTZ`,
		`CREATE OR REPLACE FUNCTION capability_grade(level NUMERIC)
		 RETURNS TEXT
		 LANGUAGE plpgsql
		 AS $$
		 BEGIN
		   IF level >= 4.5 THEN
		     RETURN 'CHIEF_ENGINEER';
		   ELSIF level >= 4 THEN
		     RETURN 'SENIOR_ENGINEER';
		   ELSIF level >= 3 THEN
		     RETURN 'REGISTERED_ENGINEER';
		   ELSIF level >= 2 THEN
		     RETURN 'LEAD_ENGINEER';
		   ELSE
		     RETURN 'ASSISTANT';
		   END IF;
		 END;
		 $$`,
		`CREATE OR REPLACE FUNCTION compute_capability_level(
		   base_level NUMERIC,
		   pass_rate NUMERIC,
		   utxo_count INT,
		   violation_count INT,
		   penalty NUMERIC
		 )
		 RETURNS NUMERIC
		 LANGUAGE plpgsql
		 AS $$
		 DECLARE
		   level NUMERIC := COALESCE(base_level, 2);
		 BEGIN
		   IF COALESCE(utxo_count, 0) >= 20 AND COALESCE(pass_rate, 0) >= 0.95 AND COALESCE(violation_count, 0) = 0 THEN
		     level := level + 0.5;
		   END IF;
		   IF COALESCE(utxo_count, 0) >= 50 THEN
		     level := level + 0.2;
		   END IF;
		   level := level + ((COALESCE(pass_rate, 0) - 0.8) * 0.8) + COALESCE(penalty, 0);
		   IF level < 0 THEN
		     level := 0;
		   END IF;
		   IF level > 5 THEN
		     level := 5;
		   END IF;
		   RETURN level;
		 END;
		 $$`,
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
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			achievement_utxo_id BIGINT,
			credential_id BIGINT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_resource_bindings_project
		   ON resource_bindings(tenant_id, project_ref, status, bound_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_resource_bindings_executor
		   ON resource_bindings(tenant_id, executor_ref, status, bound_at DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_resource_bindings_active_unique
		   ON resource_bindings(tenant_id, resource_ref) WHERE status='ACTIVE'`,
		`ALTER TABLE resource_bindings ADD COLUMN IF NOT EXISTS achievement_utxo_id BIGINT`,
		`ALTER TABLE resource_bindings ADD COLUMN IF NOT EXISTS credential_id BIGINT`,

		// 用证留痕视图
		`CREATE OR REPLACE VIEW credential_trace AS
		 SELECT a.id AS achievement_id, a.utxo_ref, a.spu_ref, a.project_ref, a.executor_ref, a.proof_hash,
		        q.id AS credential_id, q.qual_type, q.cert_no, q.holder_name AS credential_holder,
		        a.executor_ref AS actual_executor, emp.name AS executor_name,
		        rb.bound_at, rb.status AS binding_status
		 FROM achievement_utxos a
		 LEFT JOIN resource_bindings rb ON rb.achievement_utxo_id = a.id
		 LEFT JOIN qualifications q ON q.id = rb.credential_id
		 LEFT JOIN employees emp ON emp.executor_ref = a.executor_ref`,
		`CREATE OR REPLACE VIEW credential_vault AS
		 SELECT q.tenant_id,
		        COALESCE(q.executor_ref, '') AS executor_ref,
		        q.id AS qualification_id,
		        q.qual_type,
		        COALESCE(q.holder_name, '') AS holder_name,
		        COALESCE(q.cert_no, '') AS cert_no,
		        q.status AS qualification_status,
		        q.valid_until,
		        q.updated_at,
		        COALESCE(qa.assignment_count, 0) AS assignment_count,
		        qa.last_assignment_at
		 FROM qualifications q
		 LEFT JOIN LATERAL (
		   SELECT COUNT(*)::INT AS assignment_count,
		          MAX(created_at) AS last_assignment_at
		   FROM qualification_assignments a
		   WHERE a.tenant_id = q.tenant_id
		     AND a.qualification_id = q.id
		 ) qa ON TRUE
		 WHERE COALESCE(q.deleted, FALSE) = FALSE`,
	}
	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			preview := strings.Join(strings.Fields(stmt), " ")
			if len(preview) > 140 {
				preview = preview[:140] + "..."
			}
			return fmt.Errorf("apply statement failed: %s: %w", preview, err)
		}
	}
	return nil
}
