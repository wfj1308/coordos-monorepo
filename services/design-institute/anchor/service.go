package anchor

import (
	"context"
	"log"
	"time"
)

const defaultBatchSize = 100

type Service struct {
	repo   *Repository
	logger *log.Logger
}

func NewService(repo *Repository, logger *log.Logger) *Service {
	return &Service{repo: repo, logger: logger}
}

func (s *Service) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	s.logger.Printf("proof anchor scanner started, interval=%s", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Printf("proof anchor scanner stopped")
			return
		case <-ticker.C:
			if err := s.scanAndQueue(ctx); err != nil {
				s.logger.Printf("proof anchor scan failed: %v", err)
			}
		}
	}
}

func (s *Service) scanAndQueue(ctx context.Context) error {
	items, err := s.repo.FindUnanchoredAchievementHashes(ctx, defaultBatchSize)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}
	count, err := s.repo.CreatePendingAnchors(ctx, items)
	if err != nil {
		return err
	}
	s.logger.Printf("proof anchor queued %d hashes", count)
	return nil
}
