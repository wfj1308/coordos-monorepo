// Package api implements the HTTP server for CoordOS vault service.
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	pc "coordos/project-core"
	"coordos/vault-service/app"
	"coordos/vault-service/infra/store"
)

// Server is the HTTP entrypoint of vault-service.
type Server struct {
	projectApp *app.ProjectApp
	fissionApp *app.FissionApp
	eventApp   *app.EventApp
	settleApp  *app.SettleApp
	projects   store.ProjectTreeStore
	genesis    store.GenesisStore
	jwtSecret  []byte
	mux        *http.ServeMux
}

func NewServer(d app.Deps, jwtSecret string) *Server {
	s := &Server{
		projectApp: app.NewProjectApp(d),
		fissionApp: app.NewFissionApp(d),
		eventApp:   app.NewEventApp(d),
		settleApp:  app.NewSettleApp(d),
		projects:   d.Projects,
		genesis:    d.Genesis,
		jwtSecret:  []byte(jwtSecret),
		mux:        http.NewServeMux(),
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
	s.mux.HandleFunc("POST /api/v1/genesis/{ref}/fission", s.auth(s.handleFission))
	s.mux.HandleFunc("GET /api/v1/genesis/{ref}", s.auth(s.handleGetGenesis))

	// Unified event ingestion.
	s.mux.HandleFunc("POST /api/v1/events", s.auth(s.handleSubmitEvent))

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
	writeJSON(w, 200, map[string]string{"result": "settled"})
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
	parts := strings.SplitN(token, ":", 3)
	if len(parts) < 2 {
		return nil
	}
	actor := &app.Actor{
		TenantID: parts[0],
		Ref:      pc.VRef(parts[1]),
	}
	if len(parts) == 3 {
		actor.Roles = strings.Split(parts[2], ",")
	}
	return actor
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
