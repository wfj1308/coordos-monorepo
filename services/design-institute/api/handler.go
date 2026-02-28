package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"coordos/design-institute/achievement"
	"coordos/design-institute/approve"
	"coordos/design-institute/company"
	"coordos/design-institute/contract"
	"coordos/design-institute/costticket"
	"coordos/design-institute/employee"
	"coordos/design-institute/gathering"
	"coordos/design-institute/invoice"
	"coordos/design-institute/payment"
	"coordos/design-institute/project"
	"coordos/design-institute/report"
	"coordos/design-institute/settlement"
)

type Services struct {
	Project     *project.Service
	Contract    *contract.Service
	Gathering   *gathering.Service
	Settlement  *settlement.Service
	Invoice     *invoice.Service
	CostTicket  *costticket.Service
	Payment     *payment.Service
	Company     *company.Service
	Employee    *employee.Service
	Achievement *achievement.Service
	Approve     *approve.Service
	Report      *report.Service
}

type Handler struct {
	projectSvc     *project.Service
	contractSvc    *contract.Service
	gatheringSvc   *gathering.Service
	settlementSvc  *settlement.Service
	invoiceSvc     *invoice.Service
	costTicketSvc  *costticket.Service
	paymentSvc     *payment.Service
	companySvc     *company.Service
	employeeSvc    *employee.Service
	achievementSvc *achievement.Service
	approveSvc     *approve.Service
	reportSvc      *report.Service
	mux            *http.ServeMux
}

func NewHandler(s Services) *Handler {
	h := &Handler{
		projectSvc:     s.Project,
		contractSvc:    s.Contract,
		gatheringSvc:   s.Gathering,
		settlementSvc:  s.Settlement,
		invoiceSvc:     s.Invoice,
		costTicketSvc:  s.CostTicket,
		paymentSvc:     s.Payment,
		companySvc:     s.Company,
		employeeSvc:    s.Employee,
		achievementSvc: s.Achievement,
		approveSvc:     s.Approve,
		reportSvc:      s.Report,
		mux:            http.NewServeMux(),
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

	h.mux.HandleFunc("GET /api/v1/achievements/{id}", h.handleAchievementGet)
	h.mux.HandleFunc("GET /api/v1/achievements", h.handleAchievementList)

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

func (h *Handler) handleContractCreate(w http.ResponseWriter, r *http.Request) {
	var in contract.CreateContractInput
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := h.contractSvc.Create(r.Context(), in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
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
	if v := queryString(r, "executor_ref"); v != "" {
		limit, offset, err := pagination(r, 20)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
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
	writeError(w, http.StatusBadRequest, "query executor_ref/project_ref/contract_id is required")
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
