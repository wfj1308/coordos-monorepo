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
	utxoRelations   store.UTXORelationStore
	settlements     store.SettlementStore
	audit           store.AuditStore
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
		utxoRelations:  d.UTXORelations,
		settlements:    d.Settlements,
		audit:          d.Audit,
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
	s.mux.HandleFunc("POST /api/v1/utxos/{ref}/supersede", s.auth(s.handleCreateUTXOSupersede))
	s.mux.HandleFunc("POST /api/v1/utxos/{ref}/reassign", s.auth(s.handleCreateUTXOReassign))
	s.mux.HandleFunc("POST /api/v1/utxos/{ref}/spec-upgrade", s.auth(s.handleCreateUTXOSpecUpgrade))
	s.mux.HandleFunc("GET /api/v1/utxos/{ref}/relations", s.auth(s.handleQueryUTXORelations))
	s.mux.HandleFunc("POST /api/v1/utxo-relations/backfill-evidence", s.auth(s.handleBackfillUTXORelationEvidence))

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
	InputRefs   []string        `json:"input_refs"`
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
	req.ParcelRef = strings.TrimSpace(req.ParcelRef)
	req.GenesisRef = strings.TrimSpace(req.GenesisRef)
	req.ExecutorRef = strings.TrimSpace(req.ExecutorRef)
	req.PrevHash = strings.TrimSpace(req.PrevHash)
	req.InputRefs = normalizeInputRefs(req.InputRefs)
	if req.SPURef == "" || req.ProjectRef == "" || req.ExecutorRef == "" {
		writeError(w, 400, "spu_ref, project_ref and executor_ref are required")
		return
	}
	if err := s.validateUTXOIngestSource(actor, &req); err != nil {
		writeError(w, 400, "invalid source lineage: "+err.Error())
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

	expectedProof := computeIngestProofHashV2(
		req.SPURef,
		req.ProjectRef,
		req.ExecutorRef,
		req.GenesisRef,
		req.PrevHash,
		req.InputRefs,
		req.Payload,
	)
	legacyProof := computeIngestProofHash(req.SPURef, req.ProjectRef, req.ExecutorRef, req.Payload)
	req.ProofHash = strings.TrimSpace(req.ProofHash)
	if req.ProofHash != "" && req.ProofHash != expectedProof && req.ProofHash != legacyProof {
		writeError(w, 400, "proof_hash mismatch")
		return
	}
	// Persist v2 proof hash so lineage/source fields are always bound.
	req.ProofHash = expectedProof

	status, err := normalizeIngestUTXOStatus(req.Status)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}

	req.UTXORef = strings.TrimSpace(req.UTXORef)
	if req.UTXORef == "" {
		req.UTXORef = fmt.Sprintf("v://%s/utxo/%d", actor.TenantID, time.Now().UnixNano())
	}
	req.Kind = strings.TrimSpace(req.Kind)
	if req.Kind == "" {
		req.Kind = req.SPURef
	}
	normalizedKind, err := pc.NormalizeUTXOKind(req.Kind)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	req.Kind = normalizedKind
	req.Status = status
	inputRefs := make([]pc.VRef, 0, len(req.InputRefs))
	for _, item := range req.InputRefs {
		inputRefs = append(inputRefs, pc.VRef(item))
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
		ParcelRef:  pc.VRef(req.ParcelRef),
		GenesisRef: pc.VRef(req.GenesisRef),
		InputRefs:  inputRefs,
		Kind:       req.Kind,
		Status:     req.Status,
		TenantID:   actor.TenantID,
		CreatedAt:  time.Now().UTC(),
		ProofHash:  req.ProofHash,
		PrevHash:   req.PrevHash,
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

type utxoRelationCreateRequest struct {
	ToRef         string                 `json:"to_ref"`
	ChangeUTXORef string                 `json:"change_utxo_ref"`
	Reason        string                 `json:"reason"`
	Payload       map[string]interface{} `json:"payload"`
}

type utxoRelationBackfillRequest struct {
	FallbackActorRef string `json:"fallback_actor_ref"`
}

func (s *Server) handleCreateUTXOSupersede(w http.ResponseWriter, r *http.Request) {
	s.handleCreateUTXORelation(w, r, store.UTXORelationSupersedes)
}

func (s *Server) handleCreateUTXOReassign(w http.ResponseWriter, r *http.Request) {
	s.handleCreateUTXORelation(w, r, store.UTXORelationReassigns)
}

func (s *Server) handleCreateUTXOSpecUpgrade(w http.ResponseWriter, r *http.Request) {
	s.handleCreateUTXORelation(w, r, store.UTXORelationSpecUpgrades)
}

func (s *Server) handleCreateUTXORelation(w http.ResponseWriter, r *http.Request, relationType store.UTXORelationType) {
	actor := actorFromCtx(r)
	fromRef := pc.VRef(strings.TrimSpace(r.PathValue("ref")))
	if strings.TrimSpace(string(fromRef)) == "" {
		writeError(w, 400, "path ref is required")
		return
	}

	var req utxoRelationCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "invalid request body: "+err.Error())
		return
	}
	toRef := pc.VRef(strings.TrimSpace(req.ToRef))
	if strings.TrimSpace(string(toRef)) == "" {
		writeError(w, 400, "to_ref is required")
		return
	}
	changeUTXORef := pc.VRef(strings.TrimSpace(req.ChangeUTXORef))
	if strings.TrimSpace(string(changeUTXORef)) == "" {
		writeError(w, 400, "change_utxo_ref is required")
		return
	}
	if fromRef == toRef {
		writeError(w, 400, "from_ref and to_ref must be different")
		return
	}
	reason, payload, err := normalizeUTXORelationEvidence(actor.Ref, req.Reason, req.Payload)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}

	fromUTXO, err := s.utxos.Get(actor.TenantID, fromRef)
	if err != nil {
		writeError(w, statusFromErr(err), "from utxo: "+err.Error())
		return
	}
	toUTXO, err := s.utxos.Get(actor.TenantID, toRef)
	if err != nil {
		writeError(w, statusFromErr(err), "to utxo: "+err.Error())
		return
	}
	changeUTXO, err := s.utxos.Get(actor.TenantID, changeUTXORef)
	if err != nil {
		writeError(w, statusFromErr(err), "change utxo: "+err.Error())
		return
	}
	if err := validateUTXORelationSemantics(relationType, fromUTXO, toUTXO, changeUTXO); err != nil {
		writeError(w, 400, err.Error())
		return
	}

	relation := &store.UTXORelation{
		FromRef:       fromRef,
		ToRef:         toRef,
		ChangeUTXORef: changeUTXORef,
		Type:          relationType,
		ProjectRef:    fromUTXO.ProjectRef,
		Reason:        reason,
		Payload:       payload,
		TenantID:      actor.TenantID,
		CreatedAt:     time.Now().UTC(),
	}
	if err := s.utxoRelations.Create(actor.TenantID, relation); err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}

	if s.audit != nil {
		_, _ = s.audit.RecordEvent(actor.TenantID, store.AuditEvent{
			TenantID:   actor.TenantID,
			ProjectRef: relation.ProjectRef,
			ActorRef:   actor.Ref,
			Verb:       "UTXO_RELATION_CREATE",
			Payload: map[string]interface{}{
				"relation_ref":    relation.Ref,
				"type":            relation.Type,
				"from_ref":        relation.FromRef,
				"to_ref":          relation.ToRef,
				"change_utxo_ref": relation.ChangeUTXORef,
				"reason":          relation.Reason,
			},
			Timestamp: time.Now().UTC(),
		})
	}

	writeJSON(w, 201, relation)
}

func (s *Server) handleQueryUTXORelations(w http.ResponseWriter, r *http.Request) {
	actor := actorFromCtx(r)
	ref := pc.VRef(strings.TrimSpace(r.PathValue("ref")))
	if strings.TrimSpace(string(ref)) == "" {
		writeError(w, 400, "path ref is required")
		return
	}
	if _, err := s.utxos.Get(actor.TenantID, ref); err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}
	outgoing, err := s.utxoRelations.ListByFrom(actor.TenantID, ref)
	if err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}
	incoming, err := s.utxoRelations.ListByTo(actor.TenantID, ref)
	if err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}
	asChange, err := s.utxoRelations.ListByChangeUTXO(actor.TenantID, ref)
	if err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}
	writeJSON(w, 200, map[string]interface{}{
		"utxo_ref":        ref,
		"outgoing":        outgoing,
		"incoming":        incoming,
		"as_change":       asChange,
		"count_outgoing":  len(outgoing),
		"count_incoming":  len(incoming),
		"count_as_change": len(asChange),
		"count_relations": len(outgoing) + len(incoming) + len(asChange),
	})
}

func (s *Server) handleBackfillUTXORelationEvidence(w http.ResponseWriter, r *http.Request) {
	actor := actorFromCtx(r)
	if !hasRole(actor, "PLATFORM") {
		writeError(w, 403, "platform role required")
		return
	}
	if s.utxoRelations == nil {
		writeError(w, 500, "utxo relation store is not configured")
		return
	}

	var req utxoRelationBackfillRequest
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, 400, "invalid request body: "+err.Error())
			return
		}
	}

	fallbackActorRef := pc.VRef(strings.TrimSpace(req.FallbackActorRef))
	updated, err := s.utxoRelations.BackfillAuthorizationChain(actor.TenantID, fallbackActorRef)
	if err != nil {
		writeError(w, statusFromErr(err), err.Error())
		return
	}

	if s.audit != nil {
		payload := map[string]interface{}{
			"updated_relations": updated,
		}
		if strings.TrimSpace(string(fallbackActorRef)) != "" {
			payload["fallback_actor_ref"] = fallbackActorRef
		}
		_, _ = s.audit.RecordEvent(actor.TenantID, store.AuditEvent{
			TenantID:  actor.TenantID,
			ActorRef:  actor.Ref,
			Verb:      "UTXO_RELATION_EVIDENCE_BACKFILL",
			Payload:   payload,
			Timestamp: time.Now().UTC(),
		})
	}

	writeJSON(w, 200, map[string]interface{}{
		"tenant_id":         actor.TenantID,
		"updated_relations": updated,
	})
}

func normalizeUTXORelationEvidence(actorRef pc.VRef, reason string, payload map[string]interface{}) (string, map[string]interface{}, error) {
	normalizedReason := strings.TrimSpace(reason)
	if normalizedReason == "" {
		return "", nil, fmt.Errorf("reason is required")
	}
	if len(normalizedReason) > 2048 {
		return "", nil, fmt.Errorf("reason is too long")
	}

	out := map[string]interface{}{}
	for k, v := range payload {
		out[k] = v
	}

	rawChain, ok := out["authorization_chain"]
	if !ok {
		out["authorization_chain"] = []string{string(actorRef)}
		return normalizedReason, out, nil
	}

	normalizedChain, err := normalizeAuthorizationChain(rawChain)
	if err != nil {
		return "", nil, err
	}
	out["authorization_chain"] = normalizedChain
	return normalizedReason, out, nil
}

func normalizeAuthorizationChain(raw any) ([]string, error) {
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
		if len(out) == 0 {
			return nil, fmt.Errorf("payload.authorization_chain must be a non-empty string array")
		}
		return out, nil
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("payload.authorization_chain must be a non-empty string array")
			}
			trimmed := strings.TrimSpace(text)
			if trimmed == "" {
				continue
			}
			out = append(out, trimmed)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("payload.authorization_chain must be a non-empty string array")
		}
		return out, nil
	default:
		return nil, fmt.Errorf("payload.authorization_chain must be a non-empty string array")
	}
}

func validateUTXORelationSemantics(
	relationType store.UTXORelationType,
	fromUTXO, toUTXO, changeUTXO *store.UTXO,
) error {
	if fromUTXO == nil || toUTXO == nil || changeUTXO == nil {
		return fmt.Errorf("utxo relation endpoints are required")
	}
	if !isSealedUTXOStatus(fromUTXO.Status) || !isSealedUTXOStatus(toUTXO.Status) {
		return fmt.Errorf("both from and to utxo must be SEALED")
	}
	if !isSealedUTXOStatus(changeUTXO.Status) {
		return fmt.Errorf("change_utxo_ref must be SEALED")
	}
	if fromUTXO.ProjectRef != toUTXO.ProjectRef {
		return fmt.Errorf("from/to utxo must belong to same project")
	}
	if changeUTXO.ProjectRef != fromUTXO.ProjectRef {
		return fmt.Errorf("change_utxo_ref must belong to same project")
	}
	if changeUTXO.Ref == fromUTXO.Ref || changeUTXO.Ref == toUTXO.Ref {
		return fmt.Errorf("change_utxo_ref must be different from from_ref and to_ref")
	}
	if !pc.IsAllowedChangeUTXOKindForRelation(toCoreRelationType(relationType), changeUTXO.Kind) {
		return fmt.Errorf("change_utxo_ref kind %q is invalid for %s", changeUTXO.Kind, relationType)
	}

	switch relationType {
	case store.UTXORelationSupersedes:
		if strings.TrimSpace(string(fromUTXO.GenesisRef)) == "" || strings.TrimSpace(string(toUTXO.GenesisRef)) == "" {
			return fmt.Errorf("supersede requires both utxos to have genesis_ref")
		}
		if len(fromUTXO.InputRefs) > 0 || len(toUTXO.InputRefs) > 0 {
			return fmt.Errorf("supersede requires both utxos to be chain roots (input_refs must be empty)")
		}
		if fromUTXO.GenesisRef == toUTXO.GenesisRef {
			return fmt.Errorf("supersede requires different genesis_ref values")
		}
		return nil
	case store.UTXORelationReassigns:
		if fromUTXO.ProjectRef != toUTXO.ProjectRef {
			return fmt.Errorf("reassign requires from/to in same project")
		}
		if len(toUTXO.InputRefs) != 1 || toUTXO.InputRefs[0] != fromUTXO.Ref {
			return fmt.Errorf("reassign requires to.input_refs=[from_ref]")
		}
		if strings.TrimSpace(fromUTXO.ProofHash) == "" || strings.TrimSpace(toUTXO.PrevHash) == "" {
			return fmt.Errorf("reassign requires predecessor proof linkage")
		}
		if strings.TrimSpace(toUTXO.PrevHash) != strings.TrimSpace(fromUTXO.ProofHash) {
			return fmt.Errorf("reassign prev_hash must match from proof_hash")
		}
		return nil
	case store.UTXORelationSpecUpgrades:
		if fromUTXO.ProjectRef != toUTXO.ProjectRef {
			return fmt.Errorf("spec-upgrade requires from/to in same project")
		}
		if len(toUTXO.InputRefs) != 1 || toUTXO.InputRefs[0] != fromUTXO.Ref {
			return fmt.Errorf("spec-upgrade requires to.input_refs=[from_ref]")
		}
		if strings.TrimSpace(fromUTXO.ProofHash) == "" || strings.TrimSpace(toUTXO.PrevHash) == "" {
			return fmt.Errorf("spec-upgrade requires predecessor proof linkage")
		}
		if strings.TrimSpace(toUTXO.PrevHash) != strings.TrimSpace(fromUTXO.ProofHash) {
			return fmt.Errorf("spec-upgrade prev_hash must match from proof_hash")
		}
		return nil
	default:
		return fmt.Errorf("invalid relation type: %s", relationType)
	}
}

func isSealedUTXOStatus(status string) bool {
	return strings.EqualFold(strings.TrimSpace(status), "SEALED")
}

func toCoreRelationType(relationType store.UTXORelationType) pc.UTXORelationType {
	return pc.UTXORelationType(strings.ToUpper(strings.TrimSpace(string(relationType))))
}

func (s *Server) validateUTXOIngestSource(actor app.Actor, req *utxoIngestRequest) error {
	hasGenesis := strings.TrimSpace(req.GenesisRef) != ""
	hasInput := len(req.InputRefs) > 0
	if hasGenesis == hasInput {
		return fmt.Errorf("exactly one of genesis_ref or input_refs must be provided")
	}
	if hasGenesis {
		if strings.TrimSpace(req.PrevHash) != "" {
			return fmt.Errorf("prev_hash must be empty when source is genesis_ref")
		}
		if _, err := s.genesis.GetFull(actor.TenantID, pc.VRef(req.GenesisRef)); err != nil {
			return fmt.Errorf("genesis_ref not found: %w", err)
		}
		return nil
	}
	// Current architecture enforces single-parent lineage for settlement legality.
	if len(req.InputRefs) != 1 {
		return fmt.Errorf("input_refs must contain exactly 1 predecessor utxo_ref")
	}
	if strings.TrimSpace(req.PrevHash) == "" {
		return fmt.Errorf("prev_hash is required when source is input_refs")
	}
	parentRef := pc.VRef(req.InputRefs[0])
	parent, err := s.utxos.Get(actor.TenantID, parentRef)
	if err != nil {
		return fmt.Errorf("predecessor utxo not found: %s", parentRef)
	}
	if !strings.EqualFold(strings.TrimSpace(parent.Status), "SEALED") {
		return fmt.Errorf("predecessor utxo must be SEALED: %s", parentRef)
	}
	if strings.TrimSpace(parent.ProofHash) == "" {
		return fmt.Errorf("predecessor utxo has empty proof_hash: %s", parentRef)
	}
	if strings.TrimSpace(req.PrevHash) != strings.TrimSpace(parent.ProofHash) {
		return fmt.Errorf("prev_hash mismatch with predecessor proof_hash")
	}
	return nil
}

func normalizeInputRefs(input []string) []string {
	if len(input) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(input))
	out := make([]string, 0, len(input))
	for _, item := range input {
		ref := strings.TrimSpace(item)
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	return out
}

func normalizeIngestUTXOStatus(raw string) (string, error) {
	status := strings.ToUpper(strings.TrimSpace(raw))
	if status == "" {
		return "SEALED", nil
	}
	if status == "INGESTED" {
		return "SEALED", nil
	}
	allowed := map[string]bool{
		"DRAFT":       true,
		"REVIEWED":    true,
		"APPROVED":    true,
		"SEALED":      true,
		"DELIVERABLE": true,
	}
	if !allowed[status] {
		return "", fmt.Errorf("invalid status: %s", raw)
	}
	return status, nil
}

func computeIngestProofHash(spuRef, projectRef, executorRef string, payload []byte) string {
	h := sha256.New()
	h.Write([]byte(spuRef))
	h.Write([]byte(projectRef))
	h.Write([]byte(executorRef))
	h.Write(payload)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func computeIngestProofHashV2(
	spuRef, projectRef, executorRef, genesisRef, prevHash string,
	inputRefs []string,
	payload []byte,
) string {
	h := sha256.New()
	h.Write([]byte(spuRef))
	h.Write([]byte{0})
	h.Write([]byte(projectRef))
	h.Write([]byte{0})
	h.Write([]byte(executorRef))
	h.Write([]byte{0})
	h.Write([]byte(strings.TrimSpace(genesisRef)))
	h.Write([]byte{0})
	h.Write([]byte(strings.TrimSpace(prevHash)))
	h.Write([]byte{0})
	for _, ref := range inputRefs {
		h.Write([]byte(strings.TrimSpace(ref)))
		h.Write([]byte{0})
	}
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

func hasRole(actor app.Actor, target string) bool {
	want := strings.ToUpper(strings.TrimSpace(target))
	if want == "" {
		return false
	}
	for _, role := range actor.Roles {
		if strings.EqualFold(strings.TrimSpace(role), want) {
			return true
		}
	}
	return false
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
	msg := strings.ToLower(err.Error())
	switch err.(type) {
	case *app.PermissionError:
		return 403
	case *pc.RuleViolationError:
		return 422
	default:
		if strings.Contains(msg, "not found") {
			return 404
		}
		if strings.Contains(msg, "already exists") || strings.Contains(msg, "duplicate") {
			return 409
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
