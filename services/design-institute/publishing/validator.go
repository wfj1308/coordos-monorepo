package publishing

import (
	"context"
	"database/sql"
)

type Rule002Validator struct {
	qualSvc    QualificationChecker
	db         *sql.DB
	tenantID   int
	headOffice string
}

type QualificationChecker interface {
	CheckValidForRule002(ctx context.Context, executorRef string) (bool, error)
}

func NewRule002Validator(qualSvc QualificationChecker, db *sql.DB, tenantID int, headOffice string) *Rule002Validator {
	return &Rule002Validator{
		qualSvc:    qualSvc,
		db:         db,
		tenantID:   tenantID,
		headOffice: headOffice,
	}
}

func (v *Rule002Validator) CheckValidForRule002(ctx context.Context, executorRef string) (bool, error) {
	return v.qualSvc.CheckValidForRule002(ctx, executorRef)
}

func (v *Rule002Validator) GetReviewOpinionStats(ctx context.Context, projectRef, drawingNo string) (*ReviewOpinionStats, error) {
	stats := &ReviewOpinionStats{}

	err := v.db.QueryRowContext(ctx, `
		SELECT 
			COALESCE(SUM(total_opinions), 0),
			COALESCE(SUM(processed_opinions), 0),
			COALESCE(SUM(major_opinions), 0)
		FROM review_opinions
		WHERE tenant_id = $1 AND project_ref = $2 AND drawing_no = $3
	`, v.tenantID, projectRef, drawingNo).Scan(&stats.TotalOpinions, &stats.ProcessedOpinions, &stats.MajorOpinions)

	if err == sql.ErrNoRows {
		stats.ProcessingRate = 100
		return stats, nil
	}
	if err != nil {
		return nil, err
	}

	if stats.TotalOpinions > 0 {
		stats.ProcessingRate = (stats.ProcessedOpinions * 100) / stats.TotalOpinions
	} else {
		stats.ProcessingRate = 100
	}

	return stats, nil
}
