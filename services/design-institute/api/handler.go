package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"coordos/design-institute/achievement"
	"coordos/design-institute/achievementprofile"
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
)

type Services struct {
	Project            *project.Service
	Contract           *contract.Service
	Gathering          *gathering.Service
	Settlement         *settlement.Service
	Invoice            *invoice.Service
	CostTicket         *costticket.Service
	Payment            *payment.Service
	Company            *company.Service
	Employee           *employee.Service
	Achievement        *achievement.Service
	Approve            *approve.Service
	Report             *report.Service
	Qualification      *qualification.Service
	AchievementProfile *achievementprofile.Service
	Resolve            *resolve.Service
	Right              *rights.Service
	Namespace          *namespace.Service
	Publishing         *publishing.Service
	PublicAPI          *publicapi.Service
	Bidding            *bidding.Service
	Compliance         *compliance.Service
	ResourceBinding    *resourcebinding.Service
}

type Handler struct {
	projectSvc            *project.Service
	contractSvc           *contract.Service
	gatheringSvc          *gathering.Service
	settlementSvc         *settlement.Service
	invoiceSvc            *invoice.Service
	costTicketSvc         *costticket.Service
	paymentSvc            *payment.Service
	companySvc            *company.Service
	employeeSvc           *employee.Service
	achievementSvc        *achievement.Service
	approveSvc            *approve.Service
	reportSvc             *report.Service
	qualificationSvc      *qualification.Service
	achievementProfileSvc *achievementprofile.Service
	resolveSvc            *resolve.Service
	rightSvc              *rights.Service
	namespaceSvc          *namespace.Service
	publishingSvc         *publishing.Service
	publicAPISvc          *publicapi.Service
	biddingSvc            *bidding.Service
	complianceSvc         *compliance.Service
	resourceBindingSvc    *resourcebinding.Service
	mux                   *http.ServeMux
}

func NewHandler(s Services) *Handler {
	h := &Handler{
		projectSvc:            s.Project,
		contractSvc:           s.Contract,
		gatheringSvc:          s.Gathering,
		settlementSvc:         s.Settlement,
		invoiceSvc:            s.Invoice,
		costTicketSvc:         s.CostTicket,
		paymentSvc:            s.Payment,
		companySvc:            s.Company,
		employeeSvc:           s.Employee,
		achievementSvc:        s.Achievement,
		approveSvc:            s.Approve,
		reportSvc:             s.Report,
		qualificationSvc:      s.Qualification,
		achievementProfileSvc: s.AchievementProfile,
		resolveSvc:            s.Resolve,
		rightSvc:              s.Right,
		namespaceSvc:          s.Namespace,
		publishingSvc:         s.Publishing,
		publicAPISvc:          s.PublicAPI,
		biddingSvc:            s.Bidding,
		complianceSvc:         s.Compliance,
		resourceBindingSvc:    s.ResourceBinding,
		mux:                   http.NewServeMux(),
	}
	h.registerRoutes()
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// UI console runs on a different local origin (including file://).
	// Handle CORS + preflight here so browser requests can reach handlers.
	applyCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) registerRoutes() {
	h.mux.HandleFunc("GET /health", h.handleHealth)

	h.mux.HandleFunc("POST /api/v1/projects", h.handleProjectCreateRoot)
	h.mux.HandleFunc("POST /api/v1/projects/children", h.handleProjectCreateChild)
	h.mux.HandleFunc("GET /api/v1/projects", h.handleProjectList)
	h.mux.HandleFunc("GET /api/v1/projects/get", h.handleProjectGet)
	h.mux.HandleFunc("PUT /api/v1/projects/status", h.handleProjectStatus)
	h.mux.HandleFunc("GET /api/v1/projects/{ref}/evidence-pack", h.handleProjectEvidencePack)

	h.mux.HandleFunc("POST /api/v1/contracts", h.handleContractCreate)
	h.mux.HandleFunc("GET /api/v1/contracts", h.handleContractList)
	h.mux.HandleFunc("GET /api/v1/contracts/{id}", h.handleContractGet)
	h.mux.HandleFunc("PUT /api/v1/contracts/{id}/approve", h.handleContractApprove)
	h.mux.HandleFunc("PUT /api/v1/contracts/{id}/void", h.handleContractVoid)
	h.mux.HandleFunc("GET /api/v1/contracts/{id}/finance-summary", h.handleContractFinanceSummary)

	h.mux.HandleFunc("POST /api/v1/gatherings", h.handleGatheringCreate)
	h.mux.HandleFunc("GET /api/v1/gatherings", h.handleGatheringList)
	h.mux.HandleFunc("GET /api/v1/gatherings/{id}", h.handleGatheringGet)
	h.mux.HandleFunc("PUT /api/v1/gatherings/{id}/approve", h.handleGatheringApprove)

	h.mux.HandleFunc("POST /api/v1/settlements", h.handleSettlementCreate)
	h.mux.HandleFunc("GET /api/v1/settlements", h.handleSettlementList)
	h.mux.HandleFunc("GET /api/v1/settlements/{id}", h.handleSettlementGet)
	h.mux.HandleFunc("PUT /api/v1/settlements/{id}/submit", h.handleSettlementSubmit)
	h.mux.HandleFunc("PUT /api/v1/settlements/{id}/approve", h.handleSettlementApprove)
	h.mux.HandleFunc("PUT /api/v1/settlements/{id}/pay", h.handleSettlementPay)
	h.mux.HandleFunc("POST /api/v1/settlements/link-utxo", h.handleSettlementLinkUTXO)

	h.mux.HandleFunc("POST /api/v1/invoices", h.handleInvoiceCreate)
	h.mux.HandleFunc("GET /api/v1/invoices", h.handleInvoiceList)
	h.mux.HandleFunc("GET /api/v1/invoices/{id}", h.handleInvoiceGet)
	h.mux.HandleFunc("PUT /api/v1/invoices/{id}/submit", h.handleInvoiceSubmit)
	h.mux.HandleFunc("PUT /api/v1/invoices/{id}/approve", h.handleInvoiceApprove)
	h.mux.HandleFunc("PUT /api/v1/invoices/{id}/issue", h.handleInvoiceIssue)
	h.mux.HandleFunc("PUT /api/v1/invoices/{id}/scrap", h.handleInvoiceScrap)

	h.mux.HandleFunc("POST /api/v1/costtickets", h.handleCostTicketCreate)
	h.mux.HandleFunc("GET /api/v1/costtickets", h.handleCostTicketList)
	h.mux.HandleFunc("GET /api/v1/costtickets/{id}", h.handleCostTicketGet)
	h.mux.HandleFunc("PUT /api/v1/costtickets/{id}/pay", h.handleCostTicketPay)

	h.mux.HandleFunc("POST /api/v1/payments", h.handlePaymentCreate)
	h.mux.HandleFunc("GET /api/v1/payments/{id}", h.handlePaymentGet)
	h.mux.HandleFunc("PUT /api/v1/payments/{id}/approve", h.handlePaymentApprove)
	h.mux.HandleFunc("PUT /api/v1/payments/{id}/pay", h.handlePaymentPay)

	h.mux.HandleFunc("GET /api/v1/companies", h.handleCompanyList)
	h.mux.HandleFunc("GET /api/v1/companies/{id}", h.handleCompanyGet)
	h.mux.HandleFunc("GET /api/v1/companies/{id}/branches", h.handleCompanyBranches)
	h.mux.HandleFunc("PUT /api/v1/companies/{id}/bind-executor", h.handleCompanyBindExecutor)

	h.mux.HandleFunc("POST /api/v1/employees", h.handleEmployeeCreate)
	h.mux.HandleFunc("GET /api/v1/employees", h.handleEmployeeList)
	h.mux.HandleFunc("GET /api/v1/employees/{id}", h.handleEmployeeGet)
	h.mux.HandleFunc("PUT /api/v1/employees/{id}/resign", h.handleEmployeeResign)
	h.mux.HandleFunc("PUT /api/v1/employees/{id}/bind-person", h.handleEmployeeBindPerson)

	h.mux.HandleFunc("GET /api/v1/achievements/{id}", h.handleAchievementGet)
	h.mux.HandleFunc("GET /api/v1/achievements", h.handleAchievementList)
	h.mux.HandleFunc("POST /api/v1/achievements/manual", h.handleAchievementCreateManual)

	h.mux.HandleFunc("POST /api/v1/approvals", h.handleApproveSubmit)
	h.mux.HandleFunc("POST /api/v1/approvals/act", h.handleApproveAct)
	h.mux.HandleFunc("GET /api/v1/approvals/{id}", h.handleApproveGet)
	h.mux.HandleFunc("GET /api/v1/approvals/{id}/detail", h.handleApproveDetail)
	h.mux.HandleFunc("GET /api/v1/approvals/pending", h.handleApprovePending)
	h.mux.HandleFunc("GET /api/v1/approvals/biz", h.handleApproveByBiz)

	h.mux.HandleFunc("GET /api/v1/reports/overview", h.handleReportOverview)
	h.mux.HandleFunc("GET /api/v1/reports/company", h.handleReportCompany)
	h.mux.HandleFunc("GET /api/v1/reports/contracts/{id}", h.handleReportContract)
	h.mux.HandleFunc("GET /api/v1/reports/gathering-progress", h.handleReportGatheringProgress)
	h.mux.HandleFunc("GET /api/v1/reports/employees", h.handleReportEmployee)
	h.mux.HandleFunc("GET /api/v1/reports/qualification", h.handleReportQualification)
	h.mux.HandleFunc("GET /api/v1/reports/risk-events", h.handleReportRiskEvents)

	h.mux.HandleFunc("POST /api/v1/resolve/verify", h.handleResolveVerify)
	h.mux.HandleFunc("POST /api/v1/resolve/candidates", h.handleResolveCandidates)
	h.mux.HandleFunc("GET /api/v1/resolve/occupied/{ref}", h.handleResolveOccupied)
	// Compatibility aliases used by external docs/tools.
	h.mux.HandleFunc("POST /api/v1/verify/executor", h.handleResolveVerify)
	h.mux.HandleFunc("POST /api/v1/resolve/executor", h.handleResolveCandidates)
	h.mux.HandleFunc("GET /api/v1/projects/{ref}/resources", h.handleProjectEvidencePack)
	h.mux.HandleFunc("POST /api/v1/rights", h.handleRightCreate)
	h.mux.HandleFunc("GET /api/v1/rights", h.handleRightList)

	h.mux.HandleFunc("POST /api/v1/namespaces", h.handleNamespaceCreate)
	h.mux.HandleFunc("GET /api/v1/namespaces/{ref}", h.handleNamespaceGet)
	h.mux.HandleFunc("GET /api/v1/namespaces/{ref}/children", h.handleNamespaceChildren)
	h.mux.HandleFunc("GET /api/v1/namespaces/{ref}/network", h.handleNamespaceNetwork)
	h.mux.HandleFunc("POST /api/v1/namespaces/delegations", h.handleNamespaceDelegate)
	h.mux.HandleFunc("POST /api/v1/namespaces/route", h.handleNamespaceRoute)
	// Query aliases for refs that contain "/" and are not URL-encoded in callers.
	h.mux.HandleFunc("GET /api/v1/namespaces/get", h.handleNamespaceGet)
	h.mux.HandleFunc("GET /api/v1/namespaces/children", h.handleNamespaceChildren)
	h.mux.HandleFunc("GET /api/v1/namespaces/network", h.handleNamespaceNetwork)

	h.mux.HandleFunc("POST /api/v1/publishing/review-cert", h.handlePublishingIssueReviewCert)
	h.mux.HandleFunc("POST /api/v1/publishing/publish", h.handlePublishingPublish)
	h.mux.HandleFunc("GET /api/v1/publishing/drawings/{no}/current", h.handlePublishingCurrent)
	h.mux.HandleFunc("GET /api/v1/publishing/drawings/{no}/chain", h.handlePublishingChain)
	h.mux.HandleFunc("GET /api/v1/publishing/projects/{ref}/drawings", h.handlePublishingProjectDrawings)
	// Query alias for refs that include "/".
	h.mux.HandleFunc("GET /api/v1/publishing/projects/drawings", h.handlePublishingProjectDrawings)

	h.mux.HandleFunc("GET /public/v1/capabilities", h.handlePublicCapabilities)
	h.mux.HandleFunc("GET /public/v1/products", h.handlePublicProducts)
	h.mux.HandleFunc("GET /public/v1/achievements", h.handlePublicAchievements)

	h.mux.HandleFunc("POST /api/v1/bidding/profiles", h.handleBiddingProfileCreate)
	h.mux.HandleFunc("GET /api/v1/bidding/profiles", h.handleBiddingProfileList)
	h.mux.HandleFunc("GET /api/v1/bidding/profiles/{id}", h.handleBiddingProfileGet)
	h.mux.HandleFunc("PUT /api/v1/bidding/profiles/{id}/publish", h.handleBiddingProfilePublish)

	h.mux.HandleFunc("POST /api/v1/violations", h.handleViolationCreate)
	h.mux.HandleFunc("GET /api/v1/violations", h.handleViolationList)
	h.mux.HandleFunc("GET /api/v1/executors/{ref}/stats", h.handleExecutorStatsGet)
	h.mux.HandleFunc("GET /api/v1/executors/stats", h.handleExecutorStatsGet)

	h.mux.HandleFunc("POST /api/v1/resource-bindings", h.handleResourceBindingCreate)
	h.mux.HandleFunc("GET /api/v1/resource-bindings", h.handleResourceBindingList)
	h.mux.HandleFunc("PUT /api/v1/resource-bindings/{id}/release", h.handleResourceBindingRelease)

	h.mux.HandleFunc("POST /api/v1/qualifications", h.handleQualificationCreate)
	h.mux.HandleFunc("POST /api/v1/qualifications/assignments", h.handleQualificationAssignmentCreate)
	h.mux.HandleFunc("GET /api/v1/qualifications/{id}", h.handleQualificationGet)
	h.mux.HandleFunc("GET /api/v1/qualifications", h.handleQualificationList)
	h.mux.HandleFunc("PUT /api/v1/qualifications/{id}", h.handleQualificationUpdate)
	h.mux.HandleFunc("PUT /api/v1/qualifications/{id}/revoke", h.handleQualificationRevoke)
	h.mux.HandleFunc("GET /api/v1/qualifications/warnings", h.handleQualificationWarnings)
	h.mux.HandleFunc("GET /api/v1/qualifications/summary", h.handleQualificationSummary)
	h.mux.HandleFunc("GET /api/v1/qualifications/check-rule002", h.handleQualificationCheckRule002)
	h.mux.HandleFunc("GET /api/v1/projects/{ref}/qualification-assignments", h.handleProjectQualificationAssignments)

	h.mux.HandleFunc("POST /api/v1/profiles", h.handleProfileCreate)
	h.mux.HandleFunc("GET /api/v1/profiles/{id}", h.handleProfileGet)
	h.mux.HandleFunc("GET /api/v1/profiles", h.handleProfileList)
	h.mux.HandleFunc("POST /api/v1/profiles/{id}/personnel", h.handleProfileAddPersonnel)
	h.mux.HandleFunc("POST /api/v1/profiles/{id}/attachments", h.handleProfileAddAttachment)
	h.mux.HandleFunc("GET /api/v1/profiles/personal/{employee_id}", h.handleProfilePersonal)
	h.mux.HandleFunc("POST /api/v1/profiles/bidding-package", h.handleProfileBiddingPackage)
	h.mux.HandleFunc("GET /api/v1/professional/{ref}/profile", h.handleProfessionalProfile)
	h.mux.HandleFunc("GET /api/v1/professional/{ref}/capacity", h.handleProfessionalCapacity)
}

func (h *Handler) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"service":   "design-institute",
		"timestamp": time.Now().UTC(),
	})
}

func (h *Handler) handleProjectCreateRoot(w http.ResponseWriter, r *http.Request) {
	var in project.CreateNodeInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.projectSvc.CreateRoot(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleProjectCreateChild(w http.ResponseWriter, r *http.Request) {
	var in project.CreateNodeInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if ref := queryString(r, "parent_ref"); ref != "" {
		in.ParentRef = &ref
	}
	out, err := h.projectSvc.CreateChild(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleProjectGet(w http.ResponseWriter, r *http.Request) {
	ref := queryString(r, "ref")
	if ref == "" {
		writeError(w, http.StatusBadRequest, "missing ref")
		return
	}
	out, err := h.projectSvc.Get(r.Context(), ref)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleProjectList(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := pagination(r, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var statusPtr *project.Status
	if s := queryString(r, "status"); s != "" {
		x := project.Status(s)
		statusPtr = &x
	}
	items, total, err := h.projectSvc.List(r.Context(), statusPtr, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

func (h *Handler) handleProjectStatus(w http.ResponseWriter, r *http.Request) {
	ref := queryString(r, "ref")
	if ref == "" {
		writeError(w, http.StatusBadRequest, "missing ref")
		return
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.projectSvc.Transition(r.Context(), ref, project.Status(body.Status)); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleProjectEvidencePack(w http.ResponseWriter, r *http.Request) {
	ref := r.PathValue("ref")
	if ref == "" {
		writeError(w, http.StatusBadRequest, "missing ref")
		return
	}
	if h.projectSvc == nil {
		writeError(w, http.StatusNotImplemented, "project service is disabled")
		return
	}

	proj, err := h.projectSvc.Get(r.Context(), ref)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var achievements []*achievement.AchievementUTXO
	if h.achievementSvc != nil {
		achievements, err = h.achievementSvc.ListByProject(r.Context(), ref)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	var gatherings []*gathering.Gathering
	if h.gatheringSvc != nil {
		items, _, err := h.gatheringSvc.List(r.Context(), gathering.GatheringFilter{
			ProjectRef: &ref,
			Limit:      200,
			Offset:     0,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		gatherings = items
	}

	var settlements []*settlement.Balance
	if h.settlementSvc != nil {
		items, _, err := h.settlementSvc.List(r.Context(), settlement.BalanceFilter{
			ProjectRef: &ref,
			Limit:      200,
			Offset:     0,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		settlements = items
	}

	var invoices []*invoice.Invoice
	if h.invoiceSvc != nil {
		items, _, err := h.invoiceSvc.List(r.Context(), invoice.InvoiceFilter{
			ProjectRef: &ref,
			Limit:      200,
			Offset:     0,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		invoices = items
	}

	contractIDs := make(map[int64]struct{})
	for _, it := range achievements {
		if it != nil && it.ContractID != nil {
			contractIDs[*it.ContractID] = struct{}{}
		}
	}
	for _, it := range settlements {
		if it != nil && it.ContractID != nil {
			contractIDs[*it.ContractID] = struct{}{}
		}
	}
	for _, it := range invoices {
		if it != nil && it.ContractID != nil {
			contractIDs[*it.ContractID] = struct{}{}
		}
	}
	if proj.ContractRef != nil {
		if id, ok := extractContractIDFromRef(*proj.ContractRef); ok {
			contractIDs[id] = struct{}{}
		}
	}

	contractIDList := make([]int64, 0, len(contractIDs))
	for id := range contractIDs {
		contractIDList = append(contractIDList, id)
	}
	sort.Slice(contractIDList, func(i, j int) bool { return contractIDList[i] < contractIDList[j] })

	contracts := make([]*contract.Contract, 0, len(contractIDList))
	if h.contractSvc != nil {
		for _, id := range contractIDList {
			item, err := h.contractSvc.Get(r.Context(), id)
			if err == nil && item != nil {
				contracts = append(contracts, item)
			}
		}
	}

	qualsByExecutor := map[string][]*qualification.Qualification{}
	if h.qualificationSvc != nil {
		executorSet := map[string]struct{}{}
		for _, it := range achievements {
			if it != nil && it.ExecutorRef != "" {
				executorSet[it.ExecutorRef] = struct{}{}
			}
		}
		for ref := range executorSet {
			items, err := h.qualificationSvc.ListByExecutorRef(r.Context(), ref)
			if err == nil {
				qualsByExecutor[ref] = items
			}
		}
	}

	projectRights := make([]*rights.Right, 0)
	if h.rightSvc != nil {
		holderSet := map[string]struct{}{}
		for _, holder := range []string{proj.OwnerRef, proj.ContractorRef, proj.ExecutorRef, proj.PlatformRef} {
			holder = strings.TrimSpace(holder)
			if holder != "" {
				holderSet[holder] = struct{}{}
			}
		}
		for _, it := range achievements {
			if it == nil {
				continue
			}
			holder := strings.TrimSpace(it.ExecutorRef)
			if holder != "" {
				holderSet[holder] = struct{}{}
			}
		}
		holders := make([]string, 0, len(holderSet))
		for holder := range holderSet {
			holders = append(holders, holder)
		}
		sort.Strings(holders)
		items, err := h.rightSvc.ListByHolderRefs(r.Context(), holders)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		projectRights = items
	}
	qualAssignments := make([]*resolve.QualificationAssignment, 0)
	if h.resolveSvc != nil {
		items, err := h.resolveSvc.ListQualificationAssignmentsByProject(r.Context(), ref)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		qualAssignments = items
	}
	resourceBindings := make([]*resourcebinding.Binding, 0)
	if h.resourceBindingSvc != nil {
		items, _, err := h.resourceBindingSvc.List(r.Context(), resourcebinding.Filter{
			ProjectRef: &ref,
			Limit:      500,
			Offset:     0,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		resourceBindings = items
	}

	resourceRefs := buildProjectResourceRefs(
		ref,
		proj.TenantID,
		contracts,
		achievements,
		gatherings,
		settlements,
		invoices,
		qualsByExecutor,
		projectRights,
		qualAssignments,
		resourceBindings,
	)

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at":               time.Now().UTC(),
		"project_ref":                ref,
		"project":                    proj,
		"contracts":                  contracts,
		"achievements":               achievements,
		"gatherings":                 gatherings,
		"settlements":                settlements,
		"invoices":                   invoices,
		"rights":                     projectRights,
		"qualification_assignments":  qualAssignments,
		"resource_bindings":          resourceBindings,
		"qualifications_by_executor": qualsByExecutor,
		"resource_refs":              resourceRefs,
	})
}

func (h *Handler) handleContractCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		contract.CreateContractInput
		ProjectRefSnake *string `json:"project_ref"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	in := body.CreateContractInput
	if in.ProjectRef == nil && body.ProjectRefSnake != nil {
		in.ProjectRef = body.ProjectRefSnake
	}
	out, err := h.contractSvc.Create(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Optional linkage: keep contract.project_ref and project.contract_ref in sync.
	if in.ProjectRef != nil {
		projectRef := strings.TrimSpace(*in.ProjectRef)
		if projectRef != "" {
			if err := h.contractSvc.BindProjectRef(r.Context(), out.ID, projectRef); err != nil {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("contract created but failed to bind project_ref: %v", err))
				return
			}
			if h.projectSvc != nil {
				contractRef := buildContractRef(out.TenantID, out.ID)
				if err := h.projectSvc.BindContract(r.Context(), projectRef, contractRef); err != nil {
					writeError(w, http.StatusInternalServerError, fmt.Sprintf("contract created but failed to bind project node: %v", err))
					return
				}
			}
		}
	}

	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleContractGet(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.contractSvc.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleContractList(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := pagination(r, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	f := contract.ContractFilter{
		Keyword: queryString(r, "keyword"),
		Limit:   limit,
		Offset:  offset,
	}
	if v := queryString(r, "company_id"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid company_id")
			return
		}
		f.CompanyID = &n
	}
	if v := queryString(r, "employee_id"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid employee_id")
			return
		}
		f.EmployeeID = &n
	}
	if v := queryString(r, "parent_id"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid parent_id")
			return
		}
		f.ParentID = &n
	}
	if v := queryString(r, "state"); v != "" {
		s := contract.State(v)
		f.State = &s
	}
	items, total, err := h.contractSvc.List(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

func (h *Handler) handleContractApprove(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.contractSvc.Approve(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleContractVoid(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var body struct {
		Reason string `json:"reason"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.contractSvc.Void(r.Context(), id, body.Reason); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleContractFinanceSummary(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.contractSvc.FinanceSummary(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleGatheringCreate(w http.ResponseWriter, r *http.Request) {
	var in gathering.CreateGatheringInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.gatheringSvc.Create(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleGatheringGet(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.gatheringSvc.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleGatheringList(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := pagination(r, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	f := gathering.GatheringFilter{Limit: limit, Offset: offset}
	if v := queryString(r, "contract_id"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid contract_id")
			return
		}
		f.ContractID = &n
	}
	if v := queryString(r, "project_ref"); v != "" {
		f.ProjectRef = &v
	}
	if v := queryString(r, "state"); v != "" {
		s := gathering.State(v)
		f.State = &s
	}
	items, total, err := h.gatheringSvc.List(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

func (h *Handler) handleGatheringApprove(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.gatheringSvc.Approve(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleSettlementCreate(w http.ResponseWriter, r *http.Request) {
	var in settlement.CreateBalanceInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.settlementSvc.Create(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleSettlementGet(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.settlementSvc.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleSettlementList(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := pagination(r, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	f := settlement.BalanceFilter{Limit: limit, Offset: offset}
	if v := queryString(r, "contract_id"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid contract_id")
			return
		}
		f.ContractID = &n
	}
	if v := queryString(r, "project_ref"); v != "" {
		f.ProjectRef = &v
	}
	if v := queryString(r, "state"); v != "" {
		s := settlement.State(v)
		f.State = &s
	}
	items, total, err := h.settlementSvc.List(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

func (h *Handler) handleSettlementSubmit(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.settlementSvc.Submit(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleSettlementApprove(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.settlementSvc.Approve(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleSettlementPay(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var body struct {
		BankID  int64  `json:"bank_id"`
		PayDate string `json:"pay_date"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	payDate := time.Now()
	if body.PayDate != "" {
		payDate, err = parseTime(body.PayDate)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid pay_date")
			return
		}
	}
	if err := h.settlementSvc.MarkPaid(r.Context(), id, payDate, body.BankID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleSettlementLinkUTXO(w http.ResponseWriter, r *http.Request) {
	var body struct {
		BalanceID  *int64 `json:"balance_id"`
		ProjectRef string `json:"project_ref"`
		UTXORef    string `json:"utxo_ref"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	body.ProjectRef = strings.TrimSpace(body.ProjectRef)
	body.UTXORef = strings.TrimSpace(body.UTXORef)
	if body.UTXORef == "" {
		writeError(w, http.StatusBadRequest, "missing utxo_ref")
		return
	}
	if body.BalanceID == nil && body.ProjectRef == "" {
		writeError(w, http.StatusBadRequest, "one of balance_id/project_ref is required")
		return
	}
	var linkedID int64
	if body.BalanceID != nil {
		if err := h.settlementSvc.TriggerFromUTXO(r.Context(), *body.BalanceID, body.UTXORef); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		linkedID = *body.BalanceID
	} else {
		id, err := h.settlementSvc.TriggerFromUTXOByProject(r.Context(), body.ProjectRef, body.UTXORef)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		linkedID = id
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"balance_id": linkedID,
		"utxo_ref":   body.UTXORef,
	})
}

func (h *Handler) handleInvoiceCreate(w http.ResponseWriter, r *http.Request) {
	var in invoice.CreateInvoiceInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.invoiceSvc.Create(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleInvoiceGet(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.invoiceSvc.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleInvoiceList(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := pagination(r, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	f := invoice.InvoiceFilter{Limit: limit, Offset: offset}
	if v := queryString(r, "contract_id"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid contract_id")
			return
		}
		f.ContractID = &n
	}
	if v := queryString(r, "project_ref"); v != "" {
		f.ProjectRef = &v
	}
	if v := queryString(r, "state"); v != "" {
		s := invoice.State(v)
		f.State = &s
	}
	items, total, err := h.invoiceSvc.List(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

func (h *Handler) handleInvoiceSubmit(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.invoiceSvc.Submit(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleInvoiceApprove(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.invoiceSvc.Approve(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleInvoiceIssue(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var body struct {
		Code   string `json:"code"`
		Number string `json:"number"`
		Date   string `json:"date"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.invoiceSvc.Issue(r.Context(), id, body.Code, body.Number, body.Date); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleInvoiceScrap(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var body struct {
		Reason string `json:"reason"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.invoiceSvc.Scrap(r.Context(), id, body.Reason); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleCostTicketCreate(w http.ResponseWriter, r *http.Request) {
	var in costticket.CreateInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.costTicketSvc.Create(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleCostTicketGet(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.costTicketSvc.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleCostTicketList(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := pagination(r, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var state *costticket.State
	if v := queryString(r, "state"); v != "" {
		x := costticket.State(v)
		state = &x
	}
	items, total, err := h.costTicketSvc.List(r.Context(), state, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

func (h *Handler) handleCostTicketPay(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var body struct {
		PayDate string `json:"pay_date"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	payDate := time.Now()
	if body.PayDate != "" {
		payDate, err = parseTime(body.PayDate)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid pay_date")
			return
		}
	}
	out, err := h.costTicketSvc.MarkPaid(r.Context(), id, payDate)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handlePaymentCreate(w http.ResponseWriter, r *http.Request) {
	var in payment.CreatePaymentInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.paymentSvc.Create(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handlePaymentGet(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.paymentSvc.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handlePaymentApprove(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.paymentSvc.Approve(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handlePaymentPay(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var body struct {
		PayDate string `json:"pay_date"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	payDate := time.Now()
	if body.PayDate != "" {
		payDate, err = parseTime(body.PayDate)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid pay_date")
			return
		}
	}
	if err := h.paymentSvc.MarkPaid(r.Context(), id, payDate); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleCompanyList(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := pagination(r, 50)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var t *company.CompanyType
	if v := queryString(r, "company_type"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid company_type")
			return
		}
		x := company.CompanyType(n)
		t = &x
	}
	items, total, err := h.companySvc.List(r.Context(), t, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

func (h *Handler) handleCompanyGet(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.companySvc.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleCompanyBranches(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	items, err := h.companySvc.GetBranches(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) handleCompanyBindExecutor(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.companySvc.BindExecutorRef(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleEmployeeCreate(w http.ResponseWriter, r *http.Request) {
	var in employee.CreateEmployeeInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.employeeSvc.Create(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleEmployeeList(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := pagination(r, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	f := employee.EmployeeFilter{
		Keyword: queryString(r, "keyword"),
		Limit:   limit,
		Offset:  offset,
	}
	if v := queryString(r, "company_id"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid company_id")
			return
		}
		f.CompanyID = &n
	}
	items, total, err := h.employeeSvc.List(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

func (h *Handler) handleEmployeeGet(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.employeeSvc.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleEmployeeResign(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var body struct {
		EndDate string `json:"end_date"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	endDate := time.Now()
	if body.EndDate != "" {
		endDate, err = parseTime(body.EndDate)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid end_date")
			return
		}
	}
	if err := h.employeeSvc.Resign(r.Context(), id, endDate); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleEmployeeBindPerson(w http.ResponseWriter, r *http.Request) {
	if h.employeeSvc == nil {
		writeError(w, http.StatusNotImplemented, "employee service is disabled")
		return
	}
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var body struct {
		PersonIdentity    string `json:"person_identity"`
		ExecutorRef       string `json:"executor_ref"`
		PersonIdentityPas string `json:"PersonIdentity"`
		ExecutorRefPas    string `json:"ExecutorRef"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	personIdentity := strings.TrimSpace(body.PersonIdentity)
	if personIdentity == "" {
		personIdentity = strings.TrimSpace(body.PersonIdentityPas)
	}
	executorRef := strings.TrimSpace(body.ExecutorRef)
	if executorRef == "" {
		executorRef = strings.TrimSpace(body.ExecutorRefPas)
	}

	var boundRef string
	if personIdentity != "" {
		boundRef, err = h.employeeSvc.BindPersonIdentity(r.Context(), id, personIdentity)
	} else {
		boundRef, err = h.employeeSvc.BindExecutorRef(r.Context(), id, executorRef)
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":       "ok",
		"employee_id":  id,
		"executor_ref": boundRef,
	})
}

func (h *Handler) handleAchievementGet(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.achievementSvc.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleAchievementList(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := pagination(r, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if v := queryString(r, "executor_ref"); v != "" {
		items, total, err := h.achievementSvc.ListByExecutor(r.Context(), v, limit, offset)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
		return
	}
	if v := queryString(r, "project_ref"); v != "" {
		items, err := h.achievementSvc.ListByProject(r.Context(), v)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, items)
		return
	}
	if v := queryString(r, "contract_id"); v != "" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid contract_id")
			return
		}
		items, err := h.achievementSvc.ListByContract(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, items)
		return
	}

	items, total, err := h.achievementSvc.List(r.Context(), limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

func (h *Handler) handleAchievementCreateManual(w http.ResponseWriter, r *http.Request) {
	if h.achievementSvc == nil {
		writeError(w, http.StatusNotImplemented, "achievement service is disabled")
		return
	}
	var in achievement.CreateManualInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.achievementSvc.CreateManual(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.autoArchiveAfterReviewChain(r.Context(), out.ProjectRef, out.SPURef); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("achievement created but auto-archive failed: %v", err))
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) autoArchiveAfterReviewChain(ctx context.Context, projectRef, spuRef string) error {
	if h.projectSvc == nil || projectRef == "" || !isReviewChainCompleteSPU(spuRef) {
		return nil
	}
	node, err := h.projectSvc.Get(ctx, projectRef)
	if err != nil {
		return err
	}
	for _, target := range autoArchiveTransitionTargets(node.Status) {
		if err := h.projectSvc.Transition(ctx, projectRef, target); err != nil {
			return err
		}
	}
	return nil
}

func isReviewChainCompleteSPU(spuRef string) bool {
	ref := strings.ToLower(strings.TrimSpace(spuRef))
	return strings.Contains(ref, "review_certificate") || strings.Contains(ref, "settlement_cert")
}

func autoArchiveTransitionTargets(status project.Status) []project.Status {
	switch status {
	case project.StatusInProgress:
		return []project.Status{project.StatusDelivered, project.StatusSettled, project.StatusArchived}
	case project.StatusDelivered:
		return []project.Status{project.StatusSettled, project.StatusArchived}
	case project.StatusSettled:
		return []project.Status{project.StatusArchived}
	default:
		return nil
	}
}

func (h *Handler) handleApproveSubmit(w http.ResponseWriter, r *http.Request) {
	var in approve.SubmitInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.approveSvc.Submit(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleApproveAct(w http.ResponseWriter, r *http.Request) {
	var in approve.ActInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.approveSvc.Act(r.Context(), in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleApproveGet(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.approveSvc.GetFlow(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleApproveDetail(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	flow, tasks, records, err := h.approveSvc.GetFlowDetail(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"flow":    flow,
		"tasks":   tasks,
		"records": records,
	})
}

func (h *Handler) handleApprovePending(w http.ResponseWriter, r *http.Request) {
	ref := queryString(r, "approver_ref")
	if ref == "" {
		writeError(w, http.StatusBadRequest, "missing approver_ref")
		return
	}
	items, err := h.approveSvc.ListPending(r.Context(), ref)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) handleApproveByBiz(w http.ResponseWriter, r *http.Request) {
	bizType := queryString(r, "biz_type")
	bizIDStr := queryString(r, "biz_id")
	if bizType == "" || bizIDStr == "" {
		writeError(w, http.StatusBadRequest, "missing biz_type/biz_id")
		return
	}
	bizID, err := strconv.ParseInt(bizIDStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid biz_id")
		return
	}
	items, err := h.approveSvc.ListByBiz(r.Context(), approve.BizType(bizType), bizID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) handleReportOverview(w http.ResponseWriter, r *http.Request) {
	from, to, err := rangeFromQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.reportSvc.GetOverview(r.Context(), from, to)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleReportCompany(w http.ResponseWriter, r *http.Request) {
	from, to, err := rangeFromQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.reportSvc.GetCompanyReport(r.Context(), from, to)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleReportContract(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.reportSvc.GetContractAnalysis(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleReportGatheringProgress(w http.ResponseWriter, r *http.Request) {
	year := time.Now().Year()
	if v := queryString(r, "year"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid year")
			return
		}
		year = n
	}
	out, err := h.reportSvc.GetGatheringProgress(r.Context(), year)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleReportEmployee(w http.ResponseWriter, r *http.Request) {
	from, to, err := rangeFromQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.reportSvc.GetEmployeeReport(r.Context(), from, to)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleReportQualification(w http.ResponseWriter, r *http.Request) {
	if h.reportSvc == nil {
		writeError(w, http.StatusNotImplemented, "report service is disabled")
		return
	}
	year := time.Now().Year()
	if v := queryString(r, "year"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid year")
			return
		}
		year = n
	}
	out, err := h.reportSvc.GetQualificationReport(r.Context(), year)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleReportRiskEvents(w http.ResponseWriter, r *http.Request) {
	if h.reportSvc == nil {
		writeError(w, http.StatusNotImplemented, "report service is disabled")
		return
	}
	days := 30
	if v := queryString(r, "days"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "invalid days")
			return
		}
		days = n
	}
	out, err := h.reportSvc.GetRiskEvents(r.Context(), days)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"days":  days,
		"total": len(out),
		"data":  out,
	})
}

func (h *Handler) handleResolveVerify(w http.ResponseWriter, r *http.Request) {
	if h.resolveSvc == nil {
		writeError(w, http.StatusNotImplemented, "resolve service is disabled")
		return
	}
	var body struct {
		ExecutorRef string         `json:"executor_ref"`
		ProjectRef  string         `json:"project_ref"`
		SPURef      string         `json:"spu_ref"`
		Action      resolve.Action `json:"action"`
		ValidOn     string         `json:"valid_on"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.ExecutorRef == "" {
		writeError(w, http.StatusBadRequest, "executor_ref is required")
		return
	}
	in := resolve.VerifyInput{
		ExecutorRef: body.ExecutorRef,
		ProjectRef:  body.ProjectRef,
		SPURef:      body.SPURef,
		Action:      body.Action,
	}
	if body.ValidOn != "" {
		if t, err := time.Parse("2006-01-02", body.ValidOn); err == nil {
			in.ValidOn = t
		}
	}
	out, err := h.resolveSvc.Verify(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if out.Pass {
		writeJSON(w, http.StatusOK, out)
		return
	}
	writeJSON(w, http.StatusForbidden, out)
}

func (h *Handler) handleResolveCandidates(w http.ResponseWriter, r *http.Request) {
	if h.resolveSvc == nil {
		writeError(w, http.StatusNotImplemented, "resolve service is disabled")
		return
	}
	var body struct {
		Tenant         string         `json:"tenant"`
		ProjectRef     string         `json:"project_ref"`
		SPURef         string         `json:"spu_ref"`
		Role           string         `json:"role"`
		Action         resolve.Action `json:"action"`
		HeadOfficeOnly bool           `json:"head_office_only"`
		ValidOn        string         `json:"valid_on"`
		Limit          int            `json:"limit"`
		Constraints    struct {
			NeedsCompanyQualTypes []string `json:"needs_company_qual_types"`
			NeedsPersonQualTypes  []string `json:"needs_person_qual_types"`
			ValidOn               string   `json:"valid_on"`
		} `json:"constraints"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	in := resolve.ResolveInput{
		Tenant:         body.Tenant,
		ProjectRef:     body.ProjectRef,
		SPURef:         body.SPURef,
		Role:           body.Role,
		Action:         body.Action,
		HeadOfficeOnly: body.HeadOfficeOnly,
		Limit:          body.Limit,
		NeedQualTypes:  parseResolveConstraintQualTypes(body.Constraints.NeedsCompanyQualTypes, body.Constraints.NeedsPersonQualTypes),
	}
	validOn := strings.TrimSpace(body.ValidOn)
	if validOn == "" {
		validOn = strings.TrimSpace(body.Constraints.ValidOn)
	}
	if validOn != "" {
		if t, err := time.Parse("2006-01-02", validOn); err == nil {
			in.ValidOn = t
		}
	}
	out, rejected, err := h.resolveSvc.ResolveWithDetails(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if strings.HasSuffix(r.URL.Path, "/api/v1/resolve/executor") {
		if rejected == nil {
			rejected = make([]*resolve.RejectedCandidate, 0)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"data":           out,
			"total":          len(out),
			"rejected":       rejected,
			"rejected_total": len(rejected),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": out, "total": len(out)})
}

func (h *Handler) handleResolveOccupied(w http.ResponseWriter, r *http.Request) {
	if h.resolveSvc == nil {
		writeError(w, http.StatusNotImplemented, "resolve service is disabled")
		return
	}
	ref := r.PathValue("ref")
	if ref == "" {
		writeError(w, http.StatusBadRequest, "missing ref")
		return
	}
	out, err := h.resolveSvc.Occupied(r.Context(), ref)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleRightCreate(w http.ResponseWriter, r *http.Request) {
	if h.rightSvc == nil {
		writeError(w, http.StatusNotImplemented, "right service is disabled")
		return
	}
	var body struct {
		Ref        string `json:"ref"`
		RightType  string `json:"right_type"`
		HolderRef  string `json:"holder_ref"`
		Scope      string `json:"scope"`
		Status     string `json:"status"`
		ValidFrom  string `json:"valid_from"`
		ValidUntil string `json:"valid_until"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	in := rights.CreateInput{
		Ref:       strings.TrimSpace(body.Ref),
		RightType: rights.RightType(strings.TrimSpace(body.RightType)),
		HolderRef: strings.TrimSpace(body.HolderRef),
		Scope:     strings.TrimSpace(body.Scope),
		Status:    rights.Status(strings.TrimSpace(body.Status)),
	}
	if body.ValidFrom != "" {
		t, err := parseTime(body.ValidFrom)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid valid_from")
			return
		}
		in.ValidFrom = &t
	}
	if body.ValidUntil != "" {
		t, err := parseTime(body.ValidUntil)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid valid_until")
			return
		}
		in.ValidUntil = &t
	}
	out, err := h.rightSvc.Create(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleRightList(w http.ResponseWriter, r *http.Request) {
	if h.rightSvc == nil {
		writeError(w, http.StatusNotImplemented, "right service is disabled")
		return
	}
	limit, offset, err := pagination(r, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	f := rights.Filter{Limit: limit, Offset: offset}
	if v := queryString(r, "holder_ref"); v != "" {
		f.HolderRef = &v
	}
	if v := queryString(r, "right_type"); v != "" {
		rt := rights.RightType(v)
		f.RightType = &rt
	}
	if v := queryString(r, "status"); v != "" {
		st := rights.Status(v)
		f.Status = &st
	}
	items, total, err := h.rightSvc.List(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data":   items,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (h *Handler) handleNamespaceCreate(w http.ResponseWriter, r *http.Request) {
	if h.namespaceSvc == nil {
		writeError(w, http.StatusNotImplemented, "namespace service is disabled")
		return
	}
	var in namespace.CreateNamespaceInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.namespaceSvc.Register(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleNamespaceGet(w http.ResponseWriter, r *http.Request) {
	if h.namespaceSvc == nil {
		writeError(w, http.StatusNotImplemented, "namespace service is disabled")
		return
	}
	ref := pathOrQueryRef(r, "ref", "ref")
	if ref == "" {
		writeError(w, http.StatusBadRequest, "missing ref")
		return
	}
	out, err := h.namespaceSvc.Get(r.Context(), ref)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleNamespaceChildren(w http.ResponseWriter, r *http.Request) {
	if h.namespaceSvc == nil {
		writeError(w, http.StatusNotImplemented, "namespace service is disabled")
		return
	}
	ref := pathOrQueryRef(r, "ref", "ref")
	if ref == "" {
		writeError(w, http.StatusBadRequest, "missing ref")
		return
	}
	out, err := h.namespaceSvc.Children(r.Context(), ref)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data":  out,
		"total": len(out),
	})
}

func (h *Handler) handleNamespaceNetwork(w http.ResponseWriter, r *http.Request) {
	if h.namespaceSvc == nil {
		writeError(w, http.StatusNotImplemented, "namespace service is disabled")
		return
	}
	ref := pathOrQueryRef(r, "ref", "ref")
	if ref == "" {
		writeError(w, http.StatusBadRequest, "missing ref")
		return
	}
	out, err := h.namespaceSvc.Network(r.Context(), ref)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleNamespaceDelegate(w http.ResponseWriter, r *http.Request) {
	if h.namespaceSvc == nil {
		writeError(w, http.StatusNotImplemented, "namespace service is disabled")
		return
	}
	var in namespace.CreateDelegationInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.namespaceSvc.Authorize(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleNamespaceRoute(w http.ResponseWriter, r *http.Request) {
	if h.namespaceSvc == nil {
		writeError(w, http.StatusNotImplemented, "namespace service is disabled")
		return
	}
	var in namespace.RouteInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.namespaceSvc.Route(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if out.Allowed {
		writeJSON(w, http.StatusOK, out)
		return
	}
	writeJSON(w, http.StatusForbidden, out)
}

func (h *Handler) handlePublishingIssueReviewCert(w http.ResponseWriter, r *http.Request) {
	if h.publishingSvc == nil {
		writeError(w, http.StatusNotImplemented, "publishing service is disabled")
		return
	}
	var in publishing.IssueReviewCertInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.publishingSvc.IssueReviewCert(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handlePublishingPublish(w http.ResponseWriter, r *http.Request) {
	if h.publishingSvc == nil {
		writeError(w, http.StatusNotImplemented, "publishing service is disabled")
		return
	}
	var in publishing.PublishDrawingInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.publishingSvc.Publish(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handlePublishingCurrent(w http.ResponseWriter, r *http.Request) {
	if h.publishingSvc == nil {
		writeError(w, http.StatusNotImplemented, "publishing service is disabled")
		return
	}
	drawingNo := pathOrQueryRef(r, "no", "drawing_no")
	if drawingNo == "" {
		writeError(w, http.StatusBadRequest, "missing drawing_no")
		return
	}
	out, err := h.publishingSvc.Current(r.Context(), drawingNo)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if out == nil {
		writeError(w, http.StatusNotFound, "drawing not found")
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handlePublishingChain(w http.ResponseWriter, r *http.Request) {
	if h.publishingSvc == nil {
		writeError(w, http.StatusNotImplemented, "publishing service is disabled")
		return
	}
	drawingNo := pathOrQueryRef(r, "no", "drawing_no")
	if drawingNo == "" {
		writeError(w, http.StatusBadRequest, "missing drawing_no")
		return
	}
	out, err := h.publishingSvc.Chain(r.Context(), drawingNo)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"drawing_no": drawingNo,
		"data":       out,
		"total":      len(out),
	})
}

func (h *Handler) handlePublishingProjectDrawings(w http.ResponseWriter, r *http.Request) {
	if h.publishingSvc == nil {
		writeError(w, http.StatusNotImplemented, "publishing service is disabled")
		return
	}
	projectRef := pathOrQueryRef(r, "ref", "project_ref")
	if projectRef == "" {
		writeError(w, http.StatusBadRequest, "missing project_ref")
		return
	}
	out, err := h.publishingSvc.ListByProject(r.Context(), projectRef)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"project_ref": projectRef,
		"data":        out,
		"total":       len(out),
	})
}

func (h *Handler) handlePublicCapabilities(w http.ResponseWriter, r *http.Request) {
	if h.publicAPISvc == nil {
		writeError(w, http.StatusNotImplemented, "public api service is disabled")
		return
	}
	out, err := h.publicAPISvc.Capabilities(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handlePublicProducts(w http.ResponseWriter, r *http.Request) {
	if h.publicAPISvc == nil {
		writeError(w, http.StatusNotImplemented, "public api service is disabled")
		return
	}
	out, err := h.publicAPISvc.Products()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data":  out,
		"total": len(out),
	})
}

func (h *Handler) handlePublicAchievements(w http.ResponseWriter, r *http.Request) {
	if h.publicAPISvc == nil {
		writeError(w, http.StatusNotImplemented, "public api service is disabled")
		return
	}
	limit, offset, err := pagination(r, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	items, total, err := h.publicAPISvc.Achievements(r.Context(), publicapi.AchievementFilter{
		ProjectRef: strings.TrimSpace(queryString(r, "project_ref")),
		SPURef:     strings.TrimSpace(queryString(r, "spu_ref")),
		Limit:      limit,
		Offset:     offset,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (h *Handler) handleBiddingProfileCreate(w http.ResponseWriter, r *http.Request) {
	if h.biddingSvc == nil {
		writeError(w, http.StatusNotImplemented, "bidding service is disabled")
		return
	}
	var in bidding.CreateInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.biddingSvc.Create(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleBiddingProfileGet(w http.ResponseWriter, r *http.Request) {
	if h.biddingSvc == nil {
		writeError(w, http.StatusNotImplemented, "bidding service is disabled")
		return
	}
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.biddingSvc.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if out == nil {
		writeError(w, http.StatusNotFound, "profile not found")
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleBiddingProfileList(w http.ResponseWriter, r *http.Request) {
	if h.biddingSvc == nil {
		writeError(w, http.StatusNotImplemented, "bidding service is disabled")
		return
	}
	limit, offset, err := pagination(r, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	f := bidding.Filter{
		Keyword: queryString(r, "keyword"),
		Limit:   limit,
		Offset:  offset,
	}
	if v := strings.TrimSpace(queryString(r, "project_ref")); v != "" {
		f.ProjectRef = &v
	}
	if v := strings.TrimSpace(queryString(r, "status")); v != "" {
		st := bidding.Status(strings.ToUpper(v))
		f.Status = &st
	}
	items, total, err := h.biddingSvc.List(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (h *Handler) handleBiddingProfilePublish(w http.ResponseWriter, r *http.Request) {
	if h.biddingSvc == nil {
		writeError(w, http.StatusNotImplemented, "bidding service is disabled")
		return
	}
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.biddingSvc.Publish(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleViolationCreate(w http.ResponseWriter, r *http.Request) {
	if h.complianceSvc == nil {
		writeError(w, http.StatusNotImplemented, "compliance service is disabled")
		return
	}
	var in compliance.CreateViolationInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.complianceSvc.ReportViolation(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleViolationList(w http.ResponseWriter, r *http.Request) {
	if h.complianceSvc == nil {
		writeError(w, http.StatusNotImplemented, "compliance service is disabled")
		return
	}
	limit, offset, err := pagination(r, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	f := compliance.ViolationFilter{Limit: limit, Offset: offset}
	if v := strings.TrimSpace(queryString(r, "executor_ref")); v != "" {
		f.ExecutorRef = &v
	}
	if v := strings.TrimSpace(queryString(r, "project_ref")); v != "" {
		f.ProjectRef = &v
	}
	if v := strings.TrimSpace(queryString(r, "rule_code")); v != "" {
		f.RuleCode = &v
	}
	if v := strings.TrimSpace(queryString(r, "severity")); v != "" {
		f.Severity = &v
	}
	items, total, err := h.complianceSvc.ListViolations(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (h *Handler) handleExecutorStatsGet(w http.ResponseWriter, r *http.Request) {
	if h.complianceSvc == nil {
		writeError(w, http.StatusNotImplemented, "compliance service is disabled")
		return
	}
	ref := pathOrQueryRef(r, "ref", "ref")
	if ref == "" {
		writeError(w, http.StatusBadRequest, "missing ref")
		return
	}
	out, err := h.complianceSvc.GetExecutorStats(r.Context(), ref)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleResourceBindingCreate(w http.ResponseWriter, r *http.Request) {
	if h.resourceBindingSvc == nil {
		writeError(w, http.StatusNotImplemented, "resource binding service is disabled")
		return
	}
	var in resourcebinding.CreateInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.resourceBindingSvc.Create(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleResourceBindingList(w http.ResponseWriter, r *http.Request) {
	if h.resourceBindingSvc == nil {
		writeError(w, http.StatusNotImplemented, "resource binding service is disabled")
		return
	}
	limit, offset, err := pagination(r, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	f := resourcebinding.Filter{
		Limit:  limit,
		Offset: offset,
	}
	if v := strings.TrimSpace(queryString(r, "resource_ref")); v != "" {
		f.ResourceRef = &v
	}
	if v := strings.TrimSpace(queryString(r, "project_ref")); v != "" {
		f.ProjectRef = &v
	}
	if v := strings.TrimSpace(queryString(r, "executor_ref")); v != "" {
		f.ExecutorRef = &v
	}
	if v := strings.TrimSpace(queryString(r, "status")); v != "" {
		st := resourcebinding.Status(strings.ToUpper(v))
		f.Status = &st
	}
	items, total, err := h.resourceBindingSvc.List(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (h *Handler) handleResourceBindingRelease(w http.ResponseWriter, r *http.Request) {
	if h.resourceBindingSvc == nil {
		writeError(w, http.StatusNotImplemented, "resource binding service is disabled")
		return
	}
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.resourceBindingSvc.Release(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleQualificationCreate(w http.ResponseWriter, r *http.Request) {
	if h.qualificationSvc == nil {
		writeError(w, http.StatusNotImplemented, "qualification service is disabled")
		return
	}
	var in qualification.CreateInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.qualificationSvc.Create(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleQualificationGet(w http.ResponseWriter, r *http.Request) {
	if h.qualificationSvc == nil {
		writeError(w, http.StatusNotImplemented, "qualification service is disabled")
		return
	}
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.qualificationSvc.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleQualificationList(w http.ResponseWriter, r *http.Request) {
	if h.qualificationSvc == nil {
		writeError(w, http.StatusNotImplemented, "qualification service is disabled")
		return
	}
	limit, offset, err := pagination(r, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	f := qualification.Filter{Limit: limit, Offset: offset}
	if v := queryString(r, "holder_type"); v != "" {
		ht := qualification.HolderType(v)
		f.HolderType = &ht
	}
	if v := queryString(r, "holder_id"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid holder_id")
			return
		}
		f.HolderID = &n
	}
	if v := queryString(r, "qual_type"); v != "" {
		qt := qualification.QualType(v)
		f.QualType = &qt
	}
	if v := queryString(r, "status"); v != "" {
		st := qualification.CertStatus(v)
		f.Status = &st
	}
	out, total, err := h.qualificationSvc.List(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": out, "total": total})
}

func (h *Handler) handleQualificationUpdate(w http.ResponseWriter, r *http.Request) {
	if h.qualificationSvc == nil {
		writeError(w, http.StatusNotImplemented, "qualification service is disabled")
		return
	}
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var in qualification.UpdateInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.qualificationSvc.Update(r.Context(), id, in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "updated": true})
}

func (h *Handler) handleQualificationRevoke(w http.ResponseWriter, r *http.Request) {
	if h.qualificationSvc == nil {
		writeError(w, http.StatusNotImplemented, "qualification service is disabled")
		return
	}
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var body struct {
		Reason string `json:"reason"`
	}
	_ = decodeJSON(r, &body)
	if err := h.qualificationSvc.Revoke(r.Context(), id, body.Reason); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "revoked": true})
}

func (h *Handler) handleQualificationWarnings(w http.ResponseWriter, r *http.Request) {
	if h.qualificationSvc == nil {
		writeError(w, http.StatusNotImplemented, "qualification service is disabled")
		return
	}
	days := 90
	if v := queryString(r, "days"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "invalid days")
			return
		}
		days = n
	}
	out, err := h.qualificationSvc.GetExpiryWarnings(r.Context(), days)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": out, "total": len(out)})
}

func (h *Handler) handleQualificationSummary(w http.ResponseWriter, r *http.Request) {
	if h.qualificationSvc == nil {
		writeError(w, http.StatusNotImplemented, "qualification service is disabled")
		return
	}
	v := queryString(r, "company_id")
	if v == "" {
		writeError(w, http.StatusBadRequest, "missing company_id")
		return
	}
	companyID, err := strconv.Atoi(v)
	if err != nil || companyID <= 0 {
		writeError(w, http.StatusBadRequest, "invalid company_id")
		return
	}
	out, err := h.qualificationSvc.SummaryByCompany(r.Context(), companyID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleQualificationCheckRule002(w http.ResponseWriter, r *http.Request) {
	if h.qualificationSvc == nil {
		writeError(w, http.StatusNotImplemented, "qualification service is disabled")
		return
	}
	executorRef := queryString(r, "executor_ref")
	if executorRef == "" {
		writeError(w, http.StatusBadRequest, "missing executor_ref")
		return
	}
	ok, err := h.qualificationSvc.CheckValidForRule002(r.Context(), executorRef)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"executor_ref":  executorRef,
		"rule002_valid": ok,
	})
}

func (h *Handler) handleQualificationAssignmentCreate(w http.ResponseWriter, r *http.Request) {
	if h.resolveSvc == nil {
		writeError(w, http.StatusNotImplemented, "resolve service is disabled")
		return
	}
	var body struct {
		QualificationID int64  `json:"qualification_id"`
		ExecutorRef     string `json:"executor_ref"`
		ProjectRef      string `json:"project_ref"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.resolveSvc.BindQualification(r.Context(), resolve.AssignQualificationInput{
		QualificationID: body.QualificationID,
		ExecutorRef:     body.ExecutorRef,
		ProjectRef:      body.ProjectRef,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleProjectQualificationAssignments(w http.ResponseWriter, r *http.Request) {
	if h.resolveSvc == nil {
		writeError(w, http.StatusNotImplemented, "resolve service is disabled")
		return
	}
	ref := strings.TrimSpace(r.PathValue("ref"))
	if ref == "" {
		writeError(w, http.StatusBadRequest, "missing ref")
		return
	}
	items, err := h.resolveSvc.ListQualificationAssignmentsByProject(r.Context(), ref)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"project_ref": ref,
		"data":        items,
		"total":       len(items),
	})
}

func (h *Handler) handleProfileCreate(w http.ResponseWriter, r *http.Request) {
	if h.achievementProfileSvc == nil {
		writeError(w, http.StatusNotImplemented, "profile service is disabled")
		return
	}
	var in achievementprofile.CreateInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.achievementProfileSvc.Create(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleProfileGet(w http.ResponseWriter, r *http.Request) {
	if h.achievementProfileSvc == nil {
		writeError(w, http.StatusNotImplemented, "profile service is disabled")
		return
	}
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.achievementProfileSvc.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleProfileList(w http.ResponseWriter, r *http.Request) {
	if h.achievementProfileSvc == nil {
		writeError(w, http.StatusNotImplemented, "profile service is disabled")
		return
	}
	limit, offset, err := pagination(r, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	f := achievementprofile.ProfileFilter{
		Keyword: queryString(r, "keyword"),
		Limit:   limit,
		Offset:  offset,
	}
	if v := queryString(r, "company_id"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid company_id")
			return
		}
		f.CompanyID = &n
	}
	if v := queryString(r, "project_type"); v != "" {
		t := achievementprofile.ProjectType(v)
		f.ProjectType = &t
	}
	if v := queryString(r, "status"); v != "" {
		s := achievementprofile.ProfileStatus(v)
		f.Status = &s
	}
	if v := queryString(r, "year_from"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid year_from")
			return
		}
		f.YearFrom = &n
	}
	if v := queryString(r, "year_to"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid year_to")
			return
		}
		f.YearTo = &n
	}
	out, total, err := h.achievementProfileSvc.List(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": out, "total": total})
}

func (h *Handler) handleProfileAddPersonnel(w http.ResponseWriter, r *http.Request) {
	if h.achievementProfileSvc == nil {
		writeError(w, http.StatusNotImplemented, "profile service is disabled")
		return
	}
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var in achievementprofile.ProfilePersonnel
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	in.ProfileID = id
	out, err := h.achievementProfileSvc.AddPersonnel(r.Context(), &in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleProfileAddAttachment(w http.ResponseWriter, r *http.Request) {
	if h.achievementProfileSvc == nil {
		writeError(w, http.StatusNotImplemented, "profile service is disabled")
		return
	}
	id, err := pathInt64(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var in achievementprofile.ProfileAttachment
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	in.ProfileID = id
	out, err := h.achievementProfileSvc.AddAttachment(r.Context(), &in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *Handler) handleProfilePersonal(w http.ResponseWriter, r *http.Request) {
	if h.achievementProfileSvc == nil {
		writeError(w, http.StatusNotImplemented, "profile service is disabled")
		return
	}
	employeeID, err := pathInt64(r, "employee_id")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.achievementProfileSvc.GetPersonalProfile(r.Context(), employeeID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleProfileBiddingPackage(w http.ResponseWriter, r *http.Request) {
	if h.achievementProfileSvc == nil {
		writeError(w, http.StatusNotImplemented, "profile service is disabled")
		return
	}
	var body struct {
		CompanyID   int                             `json:"company_id"`
		ProjectType *achievementprofile.ProjectType `json:"project_type"`
		YearFrom    *int                            `json:"year_from"`
		YearTo      *int                            `json:"year_to"`
		Keyword     string                          `json:"keyword"`
		Desc        string                          `json:"desc"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	f := achievementprofile.ProfileFilter{
		CompanyID:   &body.CompanyID,
		ProjectType: body.ProjectType,
		YearFrom:    body.YearFrom,
		YearTo:      body.YearTo,
		Keyword:     body.Keyword,
	}
	out, err := h.achievementProfileSvc.BuildBiddingPackage(r.Context(), f, body.Desc)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) handleProfessionalProfile(w http.ResponseWriter, r *http.Request) {
	if h.achievementSvc == nil {
		writeError(w, http.StatusNotImplemented, "achievement service is disabled")
		return
	}
	ref := r.PathValue("ref")
	if ref == "" {
		writeError(w, http.StatusBadRequest, "missing ref")
		return
	}
	tenantIDs, err := parseTenantIDs(queryString(r, "tenant_ids"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	limit := 200
	offset := 0
	total := 0
	items := make([]*achievement.AchievementUTXO, 0)
	for {
		var page []*achievement.AchievementUTXO
		var pageTotal int
		if len(tenantIDs) > 0 {
			page, pageTotal, err = h.achievementSvc.ListByExecutorAcrossTenants(r.Context(), ref, tenantIDs, limit, offset)
		} else {
			page, pageTotal, err = h.achievementSvc.ListByExecutor(r.Context(), ref, limit, offset)
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if total == 0 {
			total = pageTotal
		}
		if len(page) == 0 {
			break
		}
		items = append(items, page...)
		offset += len(page)
		if len(items) >= pageTotal {
			break
		}
	}

	bySPU := map[string]int{}
	projectSet := map[string]struct{}{}
	var firstAt *time.Time
	var lastAt *time.Time
	for _, it := range items {
		if it == nil {
			continue
		}
		bySPU[it.SPURef]++
		projectSet[it.ProjectRef] = struct{}{}
		if firstAt == nil || it.IngestedAt.Before(*firstAt) {
			t := it.IngestedAt
			firstAt = &t
		}
		if lastAt == nil || it.IngestedAt.After(*lastAt) {
			t := it.IngestedAt
			lastAt = &t
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"executor_ref":      ref,
		"tenant_ids":        tenantIDs,
		"total_utxos":       total,
		"total_projects":    len(projectSet),
		"first_ingested_at": firstAt,
		"last_ingested_at":  lastAt,
		"spu_distribution":  bySPU,
		"items":             items,
	})
}

func (h *Handler) handleProfessionalCapacity(w http.ResponseWriter, r *http.Request) {
	if h.resolveSvc == nil {
		writeError(w, http.StatusNotImplemented, "resolve service is disabled")
		return
	}
	ref := r.PathValue("ref")
	if ref == "" {
		writeError(w, http.StatusBadRequest, "missing ref")
		return
	}
	out, err := h.resolveSvc.Occupied(r.Context(), ref)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"executor_ref":    out.ExecutorRef,
		"active_projects": out.ActiveProjects,
		"limit":           out.ProjectLimit,
		"available":       out.Available,
		"projects":        out.Projects,
	})
}

func buildProjectResourceRefs(
	projectRef string,
	tenantID int,
	contracts []*contract.Contract,
	achievements []*achievement.AchievementUTXO,
	gatherings []*gathering.Gathering,
	settlements []*settlement.Balance,
	invoices []*invoice.Invoice,
	qualsByExecutor map[string][]*qualification.Qualification,
	projectRights []*rights.Right,
	qualAssignments []*resolve.QualificationAssignment,
	resourceBindings []*resourcebinding.Binding,
) map[string][]string {
	tenant := tenantSegmentFromRef(projectRef, tenantID)
	buckets := map[string]map[string]struct{}{}
	add := func(kind, value string) {
		v := strings.TrimSpace(value)
		if v == "" {
			return
		}
		if _, ok := buckets[kind]; !ok {
			buckets[kind] = map[string]struct{}{}
		}
		buckets[kind][v] = struct{}{}
	}
	segment := func(raw string) string {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return "unknown"
		}
		return url.PathEscape(raw)
	}

	add("projects", projectRef)
	for _, it := range contracts {
		if it == nil || it.ID <= 0 {
			continue
		}
		add("contracts", fmt.Sprintf("v://%s/finance/contract/%d@v1", tenant, it.ID))
	}
	for _, it := range achievements {
		if it == nil {
			continue
		}
		token := strings.TrimSpace(it.UTXORef)
		if token == "" {
			token = strconv.FormatInt(it.ID, 10)
		}
		add("experiences", fmt.Sprintf(
			"v://%s/experience/project/%s/%s@v1",
			tenant,
			segment(it.ProjectRef),
			segment(token),
		))
	}
	for _, it := range gatherings {
		if it == nil || it.ID <= 0 {
			continue
		}
		add("gatherings", fmt.Sprintf("v://%s/finance/gathering/%d@v1", tenant, it.ID))
	}
	for _, it := range settlements {
		if it == nil || it.ID <= 0 {
			continue
		}
		add("settlements", fmt.Sprintf("v://%s/finance/settlement/%d@v1", tenant, it.ID))
	}
	for _, it := range invoices {
		if it == nil || it.ID <= 0 {
			continue
		}
		add("invoices", fmt.Sprintf("v://%s/finance/invoice/%d@v1", tenant, it.ID))
	}
	for _, items := range qualsByExecutor {
		for _, q := range items {
			if q == nil {
				continue
			}
			holderKind := "company"
			holderKey := fmt.Sprintf("company-%d", q.HolderID)
			if q.HolderType == qualification.HolderPerson {
				holderKind = "person"
				holderKey = fmt.Sprintf("person-%d", q.HolderID)
				if identity, ok := canonicalPersonIdentityFromExecutorRef(q.ExecutorRef); ok {
					holderKey = identity
				}
			}
			certNo := strings.TrimSpace(q.CertNo)
			if certNo == "" {
				certNo = strconv.FormatInt(q.ID, 10)
			}
			add("credentials", fmt.Sprintf(
				"v://%s/credential/%s/%s/%s/%s@v1",
				tenant,
				holderKind,
				segment(holderKey),
				segment(string(q.QualType)),
				segment(certNo),
			))
		}
	}
	for _, it := range projectRights {
		if it == nil {
			continue
		}
		ref := strings.TrimSpace(it.Ref)
		if ref == "" {
			ref = rights.BuildRef(tenantID, rights.RightType(strings.ToUpper(strings.TrimSpace(string(it.RightType)))), it.HolderRef)
		}
		add("rights", ref)
	}
	for _, it := range qualAssignments {
		if it == nil || it.ID <= 0 {
			continue
		}
		add("qualification_assignments", fmt.Sprintf("v://%s/assignment/qualification/%d@v1", tenant, it.ID))
	}
	for _, it := range resourceBindings {
		if it == nil {
			continue
		}
		add("resource_bindings", it.ResourceRef)
	}

	out := make(map[string][]string, len(buckets))
	for kind, values := range buckets {
		list := make([]string, 0, len(values))
		for v := range values {
			list = append(list, v)
		}
		sort.Strings(list)
		out[kind] = list
	}
	return out
}

func tenantSegmentFromRef(ref string, fallback int) string {
	raw := strings.TrimSpace(ref)
	if strings.HasPrefix(raw, "v://") {
		rest := strings.TrimPrefix(raw, "v://")
		if idx := strings.Index(rest, "/"); idx > 0 {
			return rest[:idx]
		}
		if rest != "" {
			return rest
		}
	}
	if fallback > 0 {
		return strconv.Itoa(fallback)
	}
	return "unknown"
}

func canonicalPersonIdentityFromExecutorRef(ref string) (string, bool) {
	ref = strings.TrimSpace(ref)
	if !strings.HasPrefix(ref, "v://person/") || !strings.HasSuffix(ref, "/executor") {
		return "", false
	}
	identity := strings.TrimSuffix(strings.TrimPrefix(ref, "v://person/"), "/executor")
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return "", false
	}
	return identity, true
}

func decodeJSON(r *http.Request, out any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func pathInt64(r *http.Request, name string) (int64, error) {
	n, err := strconv.ParseInt(r.PathValue(name), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return n, nil
}

func pathInt(r *http.Request, name string) (int, error) {
	n64, err := pathInt64(r, name)
	if err != nil {
		return 0, err
	}
	return int(n64), nil
}

func queryString(r *http.Request, key string) string {
	return r.URL.Query().Get(key)
}

func pathOrQueryRef(r *http.Request, pathKey, queryKey string) string {
	v := strings.TrimSpace(r.PathValue(pathKey))
	if v == "" && queryKey != "" {
		v = strings.TrimSpace(queryString(r, queryKey))
	}
	if v == "" {
		return ""
	}
	if decoded, err := url.PathUnescape(v); err == nil {
		return strings.TrimSpace(decoded)
	}
	return v
}

func parseTenantIDs(raw string) ([]int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	seen := make(map[int]struct{}, len(parts))
	ids := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.Atoi(part)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("invalid tenant_ids")
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("invalid tenant_ids")
	}
	sort.Ints(ids)
	return ids, nil
}

func parseResolveConstraintQualTypes(companyQualTypes []string, personQualTypes []string) []qualification.QualType {
	normalize := func(raw string) qualification.QualType {
		v := strings.ToUpper(strings.TrimSpace(raw))
		switch v {
		case "", "NONE":
			return ""
		case "REG_STRUCT", "REG_STRUCTURE":
			return qualification.QualRegStructure
		case "REG_ARCH":
			return qualification.QualRegArch
		case "REG_CIVIL":
			return qualification.QualRegCivil
		case "REG_ELECTRIC", "REG_ELEC":
			return qualification.QualRegElectric
		case "REG_MECH":
			return qualification.QualRegMech
		case "REG_COST":
			return qualification.QualRegCost
		case "REG_SAFETY":
			return qualification.QualRegSafety
		case "COMP_COMPREHENSIVE_A":
			return qualification.QualComprehensiveA
		case "INDUSTRY_ARCH_A", "COMP_INDUSTRY_A":
			return qualification.QualIndustryA
		case "COMP_INDUSTRY_B":
			return qualification.QualIndustryB
		default:
			return qualification.QualType(v)
		}
	}

	seen := map[qualification.QualType]struct{}{}
	out := make([]qualification.QualType, 0, len(companyQualTypes)+len(personQualTypes))
	for _, raw := range append(companyQualTypes, personQualTypes...) {
		qt := normalize(raw)
		if qt == "" {
			continue
		}
		if _, ok := seen[qt]; ok {
			continue
		}
		seen[qt] = struct{}{}
		out = append(out, qt)
	}
	return out
}

func pagination(r *http.Request, defaultLimit int) (int, int, error) {
	limit := defaultLimit
	offset := 0

	if v := queryString(r, "limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return 0, 0, fmt.Errorf("invalid limit")
		}
		limit = n
	}
	if v := queryString(r, "offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return 0, 0, fmt.Errorf("invalid offset")
		}
		offset = n
	}
	return limit, offset, nil
}

func parseTime(v string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", v); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid time")
}

func rangeFromQuery(r *http.Request) (time.Time, time.Time, error) {
	from := queryString(r, "from")
	to := queryString(r, "to")
	if from == "" || to == "" {
		now := time.Now()
		start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
		return start, now, nil
	}
	f, err := parseTime(from)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid from")
	}
	t, err := parseTime(to)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid to")
	}
	return f, t, nil
}

func applyCORS(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Authorization,Content-Type")
	h.Set("Access-Control-Max-Age", "86400")
}

func extractContractIDFromRef(ref string) (int64, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return 0, false
	}
	parts := strings.Split(ref, "/")
	if len(parts) == 0 {
		return 0, false
	}
	last := parts[len(parts)-1]
	id, err := strconv.ParseInt(last, 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

func buildContractRef(tenantID int, contractID int64) string {
	if tenantID <= 0 {
		tenantID = 10000
	}
	return fmt.Sprintf("v://%d/contract/%d", tenantID, contractID)
}
