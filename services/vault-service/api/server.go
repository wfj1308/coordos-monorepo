// Package api implements the HTTP server for CoordOS vault service.
package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	pc "coordos/project-core"
	resolverpkg "coordos/resolver"
	"coordos/vault-service/app"
	"coordos/vault-service/infra/store"
)

// Server is the HTTP entrypoint of vault-service.
type Server struct {
	projectApp      *app.ProjectApp
	fissionApp      *app.FissionApp
	eventApp        *app.EventApp
	settleApp       *app.SettleApp
	projects        store.ProjectTreeStore
	genesis         store.GenesisStore
	utxos           store.UTXOStore
	settlements     store.SettlementStore
	diBaseURL       string
	httpClient      *http.Client
	resolverSvc     *resolverpkg.Service
	resolverDB      *sql.DB
	resolverInitErr string
	verifyOnIngest  bool
	verifyFailOpen  bool
	jwtSecret       []byte
	mux             *http.ServeMux
}

func NewServer(d app.Deps, jwtSecret string) *Server {
	diBaseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("VAULT_SERVICE_DI_BASE_URL")), "/")
	verifyDirect := parseBoolEnv("VAULT_SERVICE_VERIFY_EXECUTOR_DIRECT", false)
	s := &Server{
		projectApp:     app.NewProjectApp(d),
		fissionApp:     app.NewFissionApp(d),
		eventApp:       app.NewEventApp(d),
		settleApp:      app.NewSettleApp(d),
		projects:       d.Projects,
		genesis:        d.Genesis,
		utxos:          d.UTXOs,
		settlements:    d.Settlements,
		diBaseURL:      diBaseURL,
		httpClient:     &http.Client{Timeout: 5 * time.Second},
		verifyOnIngest: parseBoolEnv("VAULT_SERVICE_VERIFY_EXECUTOR_ON_INGEST", diBaseURL != "" || verifyDirect),
		verifyFailOpen: parseBoolEnv("VAULT_SERVICE_VERIFY_EXECUTOR_FAIL_OPEN", false),
		jwtSecret:      []byte(jwtSecret),
		mux:            http.NewServeMux(),
	}
	if verifyDirect {
		if err := s.initDirectResolver(); err != nil {
			s.resolverInitErr = err.Error()
		}
	}
	s.registerRoutes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// UI console is loaded from file:// or a different local origin.
	// Handle CORS preflight here so browser fetch can reach business routes.
	applyCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.mux.ServeHTTP(w, r)
}

// registerRoutes configures all HTTP endpoints.
func (s *Server) registerRoutes() {
	// Health check.
	s.mux.HandleFunc("GET /health", s.handleHealth)

	// ProjectTree.
	s.mux.HandleFunc("POST /api/v1/projects", s.auth(s.handleCreateRootProject))
	s.mux.HandleFunc("POST /api/v1/projects/{ref}/children", s.auth(s.handleCreateChildProject))
	s.mux.HandleFunc("GET /api/v1/projects/{ref}", s.auth(s.handleGetProject))
	s.mux.HandleFunc("GET /api/v1/projects/{ref}/tree", s.auth(s.handleGetProjectTree))
	s.mux.HandleFunc("PUT /api/v1/projects/{ref}/status", s.auth(s.handleTransitionStatus))

	// GenesisUTXO fission.
	s.mux.HandleFunc("POST /api/v1/genesis", s.auth(s.handleCreateGenesis))
	s.mux.HandleFunc("POST /api/v1/genesis/{ref}/fission", s.auth(s.handleFission))
	s.mux.HandleFunc("GET /api/v1/genesis/{ref}", s.auth(s.handleGetGenesis))

	// Unified event ingestion.
	s.mux.HandleFunc("POST /api/v1/events", s.auth(s.handleSubmitEvent))
	// SPU -> vault UTXO ingest (compat path from historical docs).
	s.mux.HandleFunc("POST /api/utxo/ingest", s.auth(s.handleUTXOIngest))
	// UTXO query for verification and debugging.
	s.mux.HandleFunc("GET /api/v1/utxos", s.auth(s.handleQueryUTXOs))

	// Settlement.
	s.mux.HandleFunc("POST /api/v1/projects/{ref}/settle", s.auth(s.handleSettle))
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{
		"status":    "ok",
		"service":   "vault-service",
		"timestamp": time.Now().UTC(),
	})
}

func (s *Server) handleCreateRootProject(w http.ResponseWriter, r *http.Request) {
	actor := actorFromCtx(r)
	var req app.CreateProjectReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "invalid request body: "+err.Error())
		return
	}
	node, err := s.projectApp.CreateRootProject(actor, req)
	if err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}
	writeJSON(w, 201, node)
}

func (s *Server) handleCreateChildProject(w http.ResponseWriter, r *http.Request) {
	actor := actorFromCtx(r)
	parentRef := pc.VRef(r.PathValue("ref"))
	var req app.CreateChildProjectReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	req.ParentRef = parentRef
	node, err := s.projectApp.CreateChildProject(actor, req)
	if err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}
	writeJSON(w, 201, node)
}

func (s *Server) handleGetProject(w http.ResponseWriter, r *http.Request) {
	actor := actorFromCtx(r)
	ref := pc.VRef(r.PathValue("ref"))

	node, err := s.projects.GetNode(actor.TenantID, ref)
	if err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}
	ancestors, err := s.projects.GetAncestors(actor.TenantID, ref)
	if err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}
	children, err := s.projects.GetChildren(actor.TenantID, ref)
	if err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}

	writeJSON(w, 200, map[string]interface{}{
		"node":      node,
		"ancestors": ancestors,
		"children":  children,
	})
}

func (s *Server) handleGetProjectTree(w http.ResponseWriter, r *http.Request) {
	actor := actorFromCtx(r)
	ref := pc.VRef(r.PathValue("ref"))

	maxDepth := 10
	if raw := strings.TrimSpace(r.URL.Query().Get("max_depth")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 || n > 32 {
			writeError(w, 400, "invalid max_depth, expected integer in [0,32]")
			return
		}
		maxDepth = n
	}

	root, err := s.projects.GetNode(actor.TenantID, ref)
	if err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}

	tree, err := s.buildProjectTree(actor.TenantID, root, maxDepth, map[pc.VRef]struct{}{})
	if err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}
	writeJSON(w, 200, tree)
}

func (s *Server) handleTransitionStatus(w http.ResponseWriter, r *http.Request) {
	actor := actorFromCtx(r)
	projectRef := pc.VRef(r.PathValue("ref"))
	var body struct {
		Target pc.LifecycleStatus `json:"target"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if err := s.projectApp.TransitionStatus(actor, projectRef, body.Target); err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"status": string(body.Target)})
}

func (s *Server) handleFission(w http.ResponseWriter, r *http.Request) {
	actor := actorFromCtx(r)
	parentGenesisRef := pc.VRef(r.PathValue("ref"))
	var req pc.FissionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	req.ParentGenesisRef = parentGenesisRef
	result, err := s.fissionApp.ExecuteFission(actor, req)
	if err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}
	writeJSON(w, 201, result)
}

func (s *Server) handleGetGenesis(w http.ResponseWriter, r *http.Request) {
	actor := actorFromCtx(r)
	ref := pc.VRef(r.PathValue("ref"))
	genesis, err := s.genesis.GetFull(actor.TenantID, ref)
	if err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}
	writeJSON(w, 200, genesis)
}

func (s *Server) handleCreateGenesis(w http.ResponseWriter, r *http.Request) {
	actor := actorFromCtx(r)
	var body struct {
		Ref              string   `json:"ref"`
		ProjectRef       string   `json:"project_ref"`
		TotalQuota       int64    `json:"total_quota"`
		QuotaUnit        string   `json:"quota_unit"`
		UnitPrice        int64    `json:"unit_price"`
		PriceTolerance   float64  `json:"price_tolerance"`
		AllowedExecutors []string `json:"allowed_executors"`
		AllowedSkills    []string `json:"allowed_skills"`
		QualityStandard  string   `json:"quality_standard"`
		QualityThreshold int      `json:"quality_threshold"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, "invalid request body: "+err.Error())
		return
	}
	body.ProjectRef = strings.TrimSpace(body.ProjectRef)
	if body.ProjectRef == "" {
		writeError(w, 400, "project_ref is required")
		return
	}
	if body.TotalQuota <= 0 {
		writeError(w, 400, "total_quota must be > 0")
		return
	}
	projectRef := pc.VRef(body.ProjectRef)
	if _, err := s.projects.GetNode(actor.TenantID, projectRef); err != nil {
		writeError(w, statusFromErr(err), "project not found for genesis")
		return
	}
	if strings.TrimSpace(body.QuotaUnit) == "" {
		body.QuotaUnit = "CNY"
	}
	if body.UnitPrice <= 0 {
		body.UnitPrice = 1000
	}
	if body.PriceTolerance <= 0 {
		body.PriceTolerance = 0.10
	}
	if len(body.AllowedSkills) == 0 {
		body.AllowedSkills = []string{"bridge_design", "review"}
	}
	if strings.TrimSpace(body.QualityStandard) == "" {
		body.QualityStandard = "GB"
	}
	if body.QualityThreshold <= 0 {
		body.QualityThreshold = 80
	}

	ref := strings.TrimSpace(body.Ref)
	if ref == "" {
		ref = fmt.Sprintf("v://%s/genesis/%d", actor.TenantID, time.Now().UnixNano())
	}

	allowedExecutors := make([]pc.VRef, 0, len(body.AllowedExecutors))
	for _, item := range body.AllowedExecutors {
		item = strings.TrimSpace(item)
		if item != "" {
			allowedExecutors = append(allowedExecutors, pc.VRef(item))
		}
	}
	if len(allowedExecutors) == 0 {
		allowedExecutors = []pc.VRef{actor.Ref}
	}

	now := time.Now().UTC()
	g := &pc.GenesisUTXOFull{
		Ref:              pc.VRef(ref),
		ProjectRef:       projectRef,
		TenantID:         actor.TenantID,
		TotalQuota:       body.TotalQuota,
		QuotaUnit:        body.QuotaUnit,
		UnitPrice:        body.UnitPrice,
		PriceTolerance:   body.PriceTolerance,
		AllowedExecutors: allowedExecutors,
		AllowedSkills:    body.AllowedSkills,
		QualityStandard:  body.QualityStandard,
		QualityThreshold: body.QualityThreshold,
		Status:           pc.GenesisActive,
		CreatedAt:        now,
	}
	g.ProofHash = g.ComputeProofHash()
	if err := s.genesis.CreateFull(actor.TenantID, g); err != nil {
		writeError(w, statusFromErr(err), "create genesis failed: "+err.Error())
		return
	}

	// Best effort: bind genesis ref back to project node for traceability.
	if node, err := s.projects.GetNode(actor.TenantID, projectRef); err == nil {
		node.GenesisUTXORef = g.Ref
		node.UpdatedAt = time.Now().UTC()
		_ = s.projects.UpdateNode(actor.TenantID, node)
	}

	writeJSON(w, 201, g)
}

func (s *Server) handleSubmitEvent(w http.ResponseWriter, r *http.Request) {
	actor := actorFromCtx(r)
	var evt pc.ProjectEvent
	if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if err := s.eventApp.Submit(actor, evt); err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"result": "ok"})
}

func (s *Server) handleSettle(w http.ResponseWriter, r *http.Request) {
	actor := actorFromCtx(r)
	projectRef := pc.VRef(r.PathValue("ref"))
	if err := s.settleApp.TriggerSettle(actor, projectRef); err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}
	resp := map[string]interface{}{"result": "settled"}
	settlementRef, err := s.recordSettlementSignal(actor, projectRef)
	if err != nil {
		resp["settlement_record"] = "failed"
		resp["settlement_error"] = err.Error()
		writeJSON(w, 200, resp)
		return
	}
	resp["settlement_record"] = "ok"
	resp["settlement_ref"] = settlementRef

	linkStatus, linkedBalanceID, err := s.linkSettlementToDesignInstitute(projectRef, settlementRef)
	resp["design_institute_link"] = linkStatus
	if linkedBalanceID > 0 {
		resp["linked_balance_id"] = linkedBalanceID
	}
	if err != nil {
		resp["design_institute_error"] = err.Error()
	}
	writeJSON(w, 200, resp)
}

func (s *Server) recordSettlementSignal(actor app.Actor, projectRef pc.VRef) (pc.VRef, error) {
	if s.settlements == nil {
		return "", fmt.Errorf("settlement store is not configured")
	}
	node, err := s.projects.GetNode(actor.TenantID, projectRef)
	if err != nil {
		return "", fmt.Errorf("load project before settlement record failed: %w", err)
	}
	now := time.Now().UTC()
	settlementRef := pc.VRef(fmt.Sprintf("v://%s/settlement/%d", actor.TenantID, now.UnixNano()))
	st := &store.Settlement{
		Ref:        settlementRef,
		ProjectRef: projectRef,
		GenesisRef: node.GenesisUTXORef,
		Amount:     0,
		Status:     "SETTLED",
		TenantID:   actor.TenantID,
		CreatedAt:  now,
		ProofHash:  computeSettlementProofHash(actor.TenantID, projectRef, settlementRef, now),
	}
	if err := s.settlements.Create(actor.TenantID, st); err != nil {
		return "", fmt.Errorf("create settlement record failed: %w", err)
	}
	return settlementRef, nil
}

func (s *Server) linkSettlementToDesignInstitute(projectRef, utxoRef pc.VRef) (string, int64, error) {
	if s.diBaseURL == "" {
		return "skipped", 0, nil
	}
	reqBody, _ := json.Marshal(map[string]interface{}{
		"project_ref": string(projectRef),
		"utxo_ref":    string(utxoRef),
	})
	req, err := http.NewRequest(http.MethodPost, s.diBaseURL+"/api/v1/settlements/link-utxo", bytes.NewReader(reqBody))
	if err != nil {
		return "failed", 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "failed", 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "failed", 0, fmt.Errorf("design-institute returned %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out struct {
		Status    string `json:"status"`
		BalanceID int64  `json:"balance_id"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "failed", 0, fmt.Errorf("decode design-institute response failed: %w", err)
	}
	return "linked", out.BalanceID, nil
}

type utxoIngestRequest struct {
	UTXORef     string          `json:"utxo_ref"`
	SPURef      string          `json:"spu_ref"`
	ProjectRef  string          `json:"project_ref"`
	ParcelRef   string          `json:"parcel_ref"`
	GenesisRef  string          `json:"genesis_ref"`
	ExecutorRef string          `json:"executor_ref"`
	Kind        string          `json:"kind"`
	Status      string          `json:"status"`
	PrevHash    string          `json:"prev_hash"`
	ProofHash   string          `json:"proof_hash"`
	Payload     json.RawMessage `json:"payload"`
}

func (s *Server) handleUTXOIngest(w http.ResponseWriter, r *http.Request) {
	actor := actorFromCtx(r)
	var req utxoIngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "invalid request body: "+err.Error())
		return
	}

	req.SPURef = strings.TrimSpace(req.SPURef)
	req.ProjectRef = strings.TrimSpace(req.ProjectRef)
	req.ExecutorRef = strings.TrimSpace(req.ExecutorRef)
	if req.SPURef == "" || req.ProjectRef == "" || req.ExecutorRef == "" {
		writeError(w, 400, "spu_ref, project_ref and executor_ref are required")
		return
	}

	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		writeError(w, 400, "payload must be valid json object")
		return
	}
	if payload == nil {
		payload = map[string]interface{}{}
	}

	expectedProof := computeIngestProofHash(req.SPURef, req.ProjectRef, req.ExecutorRef, req.Payload)
	req.ProofHash = strings.TrimSpace(req.ProofHash)
	if req.ProofHash != "" && req.ProofHash != expectedProof {
		writeError(w, 400, "proof_hash mismatch")
		return
	}
	if req.ProofHash == "" {
		req.ProofHash = expectedProof
	}

	req.UTXORef = strings.TrimSpace(req.UTXORef)
	if req.UTXORef == "" {
		req.UTXORef = fmt.Sprintf("v://%s/utxo/%d", actor.TenantID, time.Now().UnixNano())
	}
	req.Kind = strings.TrimSpace(req.Kind)
	if req.Kind == "" {
		req.Kind = req.SPURef
	}
	req.Status = strings.TrimSpace(req.Status)
	if req.Status == "" {
		req.Status = "INGESTED"
	}

	executorVerify := "skipped"
	executorVerifyErr := ""
	if s.verifyOnIngest {
		if verifySummary, err := s.verifyExecutorForIngest(r.Context(), actor.TenantID, req); err != nil {
			if s.verifyFailOpen {
				executorVerify = "failed_open"
				executorVerifyErr = err.Error()
			} else {
				writeJSON(w, http.StatusForbidden, map[string]interface{}{
					"error":           "executor verification failed",
					"executor_verify": "failed",
					"detail":          err.Error(),
				})
				return
			}
		} else {
			executorVerify = "passed"
			executorVerifyErr = verifySummary
		}
	}

	u := &store.UTXO{
		Ref:        pc.VRef(req.UTXORef),
		ProjectRef: pc.VRef(req.ProjectRef),
		ParcelRef:  pc.VRef(strings.TrimSpace(req.ParcelRef)),
		GenesisRef: pc.VRef(strings.TrimSpace(req.GenesisRef)),
		Kind:       req.Kind,
		Status:     req.Status,
		TenantID:   actor.TenantID,
		CreatedAt:  time.Now().UTC(),
		ProofHash:  req.ProofHash,
		PrevHash:   strings.TrimSpace(req.PrevHash),
		Payload:    payload,
	}
	if err := s.utxos.Create(actor.TenantID, u); err != nil {
		writeError(w, statusFromErr(err), "failed to store utxo: "+err.Error())
		return
	}

	autoArchive := "skipped"
	autoArchiveErr := ""
	if state, err := s.tryAutoArchiveAfterIngest(actor, req, u.Ref); err != nil {
		autoArchive = "failed"
		autoArchiveErr = err.Error()
	} else {
		autoArchive = state
	}

	resp := map[string]interface{}{
		"utxo_ref":        u.Ref,
		"project_ref":     u.ProjectRef,
		"status":          u.Status,
		"proof_hash":      u.ProofHash,
		"auto_archive":    autoArchive,
		"executor_verify": executorVerify,
	}
	if autoArchiveErr != "" {
		resp["auto_archive_error"] = autoArchiveErr
	}
	if executorVerifyErr != "" {
		resp["executor_verify_info"] = executorVerifyErr
	}
	writeJSON(w, 201, resp)
}

func (s *Server) verifyExecutorForIngest(ctx context.Context, tenantID string, req utxoIngestRequest) (string, error) {
	if s.resolverSvc != nil {
		return s.verifyExecutorDirect(ctx, tenantID, req)
	}
	if s.diBaseURL == "" {
		if s.resolverInitErr != "" {
			return "", fmt.Errorf("direct resolver init failed: %s", s.resolverInitErr)
		}
		return "skipped: no executor verifier configured", nil
	}
	body := map[string]interface{}{
		"executor_ref": strings.TrimSpace(req.ExecutorRef),
		"project_ref":  strings.TrimSpace(req.ProjectRef),
		"spu_ref":      strings.TrimSpace(req.SPURef),
		"valid_on":     time.Now().UTC().Format("2006-01-02"),
	}
	if action := inferVerifyAction(req.SPURef); action != "" {
		body["action"] = action
	}
	rawReq, _ := json.Marshal(body)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, s.diBaseURL+"/api/v1/verify/executor", bytes.NewReader(rawReq))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	rawResp, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		msg := extractVerifyMessage(rawResp)
		if msg == "" {
			msg = strings.TrimSpace(string(rawResp))
		}
		if msg == "" {
			msg = fmt.Sprintf("design-institute verify returned %d", resp.StatusCode)
		}
		return "", fmt.Errorf("%s", msg)
	}
	return extractVerifyMessage(rawResp), nil
}

func (s *Server) verifyExecutorDirect(ctx context.Context, tenantID string, req utxoIngestRequest) (string, error) {
	if s.resolverSvc == nil {
		return "", fmt.Errorf("direct resolver is not initialized")
	}
	in := resolverpkg.VerifyInput{
		ExecutorRef: strings.TrimSpace(req.ExecutorRef),
		ProjectRef:  strings.TrimSpace(req.ProjectRef),
		SPURef:      strings.TrimSpace(req.SPURef),
		Action:      inferResolverAction(req.SPURef),
		ValidOn:     time.Now().UTC(),
		TenantID:    parseIntOrDefault(tenantID, 10000),
	}
	out, err := s.resolverSvc.Verify(ctx, in)
	if err != nil {
		return "", err
	}
	if !out.Pass {
		msg := strings.TrimSpace(out.Summary)
		if msg == "" {
			msg = "executor did not satisfy resolver requirements"
		}
		return "", fmt.Errorf(msg)
	}
	return strings.TrimSpace(out.Summary), nil
}

func (s *Server) initDirectResolver() error {
	dsn := firstNonEmptyEnv("VAULT_SERVICE_RESOLVER_PG_DSN", "VAULT_SERVICE_PG_DSN", "DATABASE_URL")
	if strings.TrimSpace(dsn) == "" {
		return fmt.Errorf("missing pg dsn in VAULT_SERVICE_RESOLVER_PG_DSN / VAULT_SERVICE_PG_DSN / DATABASE_URL")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("open pg for resolver failed: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("ping pg for resolver failed: %w", err)
	}
	tenantID := parseIntOrDefault(os.Getenv("VAULT_SERVICE_TENANT_ID"), 10000)
	headOfficeRef := strings.TrimSpace(os.Getenv("VAULT_SERVICE_HEAD_OFFICE_REF"))
	if headOfficeRef == "" {
		headOfficeRef = "v://zhongbei/executor/headquarters"
	}
	s.resolverDB = db
	s.resolverSvc = resolverpkg.NewService(resolverpkg.NewPGStore(db), tenantID, headOfficeRef)
	return nil
}

func inferResolverAction(spuRef string) resolverpkg.Action {
	ref := strings.ToLower(strings.TrimSpace(spuRef))
	switch {
	case strings.Contains(ref, "review_certificate"):
		return resolverpkg.ActionIssueReviewCert
	case strings.Contains(ref, "settlement_cert"):
		return resolverpkg.ActionIssueInvoice
	default:
		return ""
	}
}

func inferVerifyAction(spuRef string) string {
	return string(inferResolverAction(spuRef))
}

func extractVerifyMessage(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var out struct {
		Summary string `json:"summary"`
		Error   string `json:"error"`
		Pass    *bool  `json:"pass"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return ""
	}
	if strings.TrimSpace(out.Error) != "" {
		return strings.TrimSpace(out.Error)
	}
	return strings.TrimSpace(out.Summary)
}

func (s *Server) tryAutoArchiveAfterIngest(actor app.Actor, req utxoIngestRequest, utxoRef pc.VRef) (string, error) {
	if !isReviewChainCompleteSPU(req.SPURef) || strings.TrimSpace(req.ProjectRef) == "" {
		return "skipped", nil
	}
	projectRef := pc.VRef(strings.TrimSpace(req.ProjectRef))
	node, err := s.projects.GetNode(actor.TenantID, projectRef)
	if err != nil {
		return "failed", fmt.Errorf("load project node failed: %w", err)
	}
	markReviewChainMilestones(node, utxoRef, actor.Ref)
	if err := s.projects.UpdateNode(actor.TenantID, node); err != nil {
		return "failed", fmt.Errorf("update project milestones failed: %w", err)
	}
	targets := autoArchiveTransitionTargets(node.Status)
	if len(targets) == 0 {
		return "skipped", nil
	}
	for _, target := range targets {
		if err := s.projectApp.TransitionStatus(actor, projectRef, target); err != nil {
			return "failed", fmt.Errorf("auto transition to %s failed: %w", target, err)
		}
	}
	return "completed", nil
}

func isReviewChainCompleteSPU(spuRef string) bool {
	ref := strings.ToLower(strings.TrimSpace(spuRef))
	return strings.Contains(ref, "review_certificate") || strings.Contains(ref, "settlement_cert")
}

func autoArchiveTransitionTargets(status pc.LifecycleStatus) []pc.LifecycleStatus {
	switch status {
	case pc.StatusInProgress:
		return []pc.LifecycleStatus{pc.StatusDelivered, pc.StatusSettled, pc.StatusArchived}
	case pc.StatusDelivered:
		return []pc.LifecycleStatus{pc.StatusSettled, pc.StatusArchived}
	case pc.StatusSettled:
		return []pc.LifecycleStatus{pc.StatusArchived}
	default:
		return nil
	}
}

func markReviewChainMilestones(node *pc.ProjectNode, utxoRef, actorRef pc.VRef) {
	if node == nil {
		return
	}
	signer := node.PlatformRef
	if signer == "" {
		signer = actorRef
	}
	now := time.Now().UTC()
	upsertReachedMilestone(node, "REVIEW", utxoRef, signer, now)
	upsertReachedMilestone(node, "DELIVER", utxoRef, signer, now)
}

func upsertReachedMilestone(node *pc.ProjectNode, name string, utxoRef, signer pc.VRef, reachedAt time.Time) {
	for i := range node.Milestones {
		if strings.EqualFold(node.Milestones[i].Name, name) {
			node.Milestones[i].Status = "REACHED"
			node.Milestones[i].UTXORef = utxoRef
			node.Milestones[i].SignedBy = signer
			node.Milestones[i].ReachedAt = &reachedAt
			return
		}
	}
	node.Milestones = append(node.Milestones, pc.MilestoneEvent{
		ID:         fmt.Sprintf("auto-%s-%d", strings.ToLower(name), time.Now().UnixNano()),
		ProjectRef: node.Ref,
		Name:       name,
		Status:     "REACHED",
		ReachedAt:  &reachedAt,
		UTXORef:    utxoRef,
		SignedBy:   signer,
	})
}

func (s *Server) handleQueryUTXOs(w http.ResponseWriter, r *http.Request) {
	actor := actorFromCtx(r)
	ref := strings.TrimSpace(r.URL.Query().Get("ref"))
	projectRef := strings.TrimSpace(r.URL.Query().Get("project_ref"))
	parcelRef := strings.TrimSpace(r.URL.Query().Get("parcel_ref"))

	if ref != "" {
		u, err := s.utxos.Get(actor.TenantID, pc.VRef(ref))
		if err != nil {
			writeError(w, statusFromErr(err), err.Error())
			return
		}
		writeJSON(w, 200, u)
		return
	}
	if projectRef != "" {
		items, err := s.utxos.ListByProject(actor.TenantID, pc.VRef(projectRef))
		if err != nil {
			writeError(w, statusFromErr(err), err.Error())
			return
		}
		writeJSON(w, 200, map[string]interface{}{"items": items, "count": len(items)})
		return
	}
	if parcelRef != "" {
		items, err := s.utxos.ListByParcel(actor.TenantID, pc.VRef(parcelRef))
		if err != nil {
			writeError(w, statusFromErr(err), err.Error())
			return
		}
		writeJSON(w, 200, map[string]interface{}{"items": items, "count": len(items)})
		return
	}

	writeError(w, 400, "query one of ref / project_ref / parcel_ref is required")
}

func computeIngestProofHash(spuRef, projectRef, executorRef string, payload []byte) string {
	h := sha256.New()
	h.Write([]byte(spuRef))
	h.Write([]byte(projectRef))
	h.Write([]byte(executorRef))
	h.Write(payload)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func computeSettlementProofHash(tenantID string, projectRef, settlementRef pc.VRef, at time.Time) string {
	h := sha256.New()
	h.Write([]byte(tenantID))
	h.Write([]byte(projectRef))
	h.Write([]byte(settlementRef))
	h.Write([]byte(at.Format(time.RFC3339Nano)))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// Auth middleware.
type contextKey string

const actorKey contextKey = "actor"

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Simplified auth: token format is tenantID:actorRef:role1,role2.
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" {
			writeError(w, 401, "missing authorization token")
			return
		}
		actor := parseToken(token)
		if actor == nil {
			writeError(w, 401, "invalid token")
			return
		}
		// Inject actor into headers for downstream handler usage.
		r.Header.Set("X-Actor-Ref", string(actor.Ref))
		r.Header.Set("X-Tenant-ID", actor.TenantID)
		r.Header.Set("X-Roles", strings.Join(actor.Roles, ","))
		next(w, r)
	}
}

func actorFromCtx(r *http.Request) app.Actor {
	return app.Actor{
		Ref:      pc.VRef(r.Header.Get("X-Actor-Ref")),
		TenantID: r.Header.Get("X-Tenant-ID"),
		Roles:    strings.Split(r.Header.Get("X-Roles"), ","),
	}
}

func parseToken(token string) *app.Actor {
	first := strings.Index(token, ":")
	if first <= 0 {
		return nil
	}
	tenant := token[:first]
	rest := token[first+1:]
	if strings.TrimSpace(rest) == "" {
		return nil
	}

	actorRef := rest
	roles := []string(nil)
	if last := strings.LastIndex(rest, ":"); last > 0 {
		tail := strings.TrimSpace(rest[last+1:])
		if isRoleList(tail) {
			actorRef = strings.TrimSpace(rest[:last])
			if actorRef == "" {
				return nil
			}
			roles = splitRoleList(tail)
		}
	}

	actor := &app.Actor{
		TenantID: tenant,
		Ref:      pc.VRef(actorRef),
	}
	if len(roles) > 0 {
		actor.Roles = roles
	}
	return actor
}

func splitRoleList(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		role := strings.TrimSpace(p)
		if role != "" {
			out = append(out, role)
		}
	}
	return out
}

func isRoleList(raw string) bool {
	parts := strings.Split(raw, ",")
	if len(parts) == 0 {
		return false
	}
	for _, p := range parts {
		role := strings.TrimSpace(p)
		if role == "" {
			return false
		}
		for _, ch := range role {
			if (ch < 'A' || ch > 'Z') && ch != '_' {
				return false
			}
		}
	}
	return true
}

func parseBoolEnv(key string, fallback bool) bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	case "":
		return fallback
	default:
		return fallback
	}
}

func parseIntOrDefault(raw string, fallback int) int {
	v := strings.TrimSpace(raw)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func firstNonEmptyEnv(keys ...string) string {
	for _, key := range keys {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return ""
}

// Response helpers.
func writeJSON(w http.ResponseWriter, code int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func statusFromErr(err error) int {
	if err == nil {
		return 200
	}
	switch err.(type) {
	case *app.PermissionError:
		return 403
	case *pc.RuleViolationError:
		return 422
	default:
		if strings.Contains(strings.ToLower(err.Error()), "not found") {
			return 404
		}
		return 500
	}
}

type projectTreeNode struct {
	Node     *pc.ProjectNode    `json:"node"`
	Children []*projectTreeNode `json:"children"`
}

func (s *Server) buildProjectTree(tenantID string, node *pc.ProjectNode, depth int, seen map[pc.VRef]struct{}) (*projectTreeNode, error) {
	out := &projectTreeNode{
		Node:     node,
		Children: []*projectTreeNode{},
	}
	if depth == 0 {
		return out, nil
	}
	if _, ok := seen[node.Ref]; ok {
		return out, nil
	}
	seen[node.Ref] = struct{}{}

	children, err := s.projects.GetChildren(tenantID, node.Ref)
	if err != nil {
		return nil, err
	}
	for _, child := range children {
		sub, err := s.buildProjectTree(tenantID, child, depth-1, seen)
		if err != nil {
			return nil, err
		}
		out.Children = append(out.Children, sub)
	}
	return out, nil
}

// Run starts the HTTP server.
func Run(d app.Deps, addr, jwtSecret string) error {
	srv := NewServer(d, jwtSecret)
	fmt.Printf("CoordOS vault-service listening on %s\n", addr)
	return http.ListenAndServe(addr, srv)
}

func applyCORS(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Authorization,Content-Type")
	h.Set("Access-Control-Max-Age", "86400")
}
