package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"time"

	"coordos/design-institute/achievement"
	"coordos/design-institute/api"
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

	_ "github.com/lib/pq"
)

type config struct {
	Addr     string
	PGDSN    string
	TenantID int
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

	approveSvc.SetCallbacks(
		func(context.Context, approve.BizType, int64) error { return nil },
		func(context.Context, approve.BizType, int64, string) error { return nil },
	)

	handler := api.NewHandler(api.Services{
		Project:     projectSvc,
		Contract:    contractSvc,
		Gathering:   gatheringSvc,
		Settlement:  settlementSvc,
		Invoice:     invoiceSvc,
		CostTicket:  costTicketSvc,
		Payment:     paymentSvc,
		Company:     companySvc,
		Employee:    employeeSvc,
		Achievement: achievementSvc,
		Approve:     approveSvc,
		Report:      reportSvc,
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
