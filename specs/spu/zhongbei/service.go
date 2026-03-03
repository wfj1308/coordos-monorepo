package anchor

import (
	"context"
	"log"
	"time"
)

const (
	defaultScanInterval = 1 * time.Minute
	defaultBatchSize    = 100
)

// Service 是后台锚定服务
type Service struct {
	repo   *Repository
	logger *log.Logger
}

// NewService 创建一个新的锚定服务实例
func NewService(repo *Repository, logger *log.Logger) *Service {
	return &Service{
		repo:   repo,
		logger: logger,
	}
}

// Run 启动服务的主循环。它会阻塞直到上下文被取消。
func (s *Service) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = defaultScanInterval
	}
	s.logger.Printf("Anchor service starting, scan interval: %v", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Println("Anchor service shutting down.")
			return
		case <-ticker.C:
			s.logger.Println("Scanning for new hashes to anchor...")
			if err := s.scanAndProcess(ctx); err != nil {
				s.logger.Printf("Error during scan and process cycle: %v", err)
			}
		}
	}
}

// scanAndProcess 执行一次扫描和处理周期
func (s *Service) scanAndProcess(ctx context.Context) error {
	// 1. 从 achievement_utxos 查找尚未锚定的哈希
	// TODO: 将来可以扩展到查找其他类型的UTXO，如 genesis_utxos, qualification_utxos 等
	items, err := s.repo.FindUnanchoredAchievementHashes(ctx, defaultBatchSize)
	if err != nil {
		return err
	}

	if len(items) == 0 {
		s.logger.Println("No new hashes found.")
		return nil
	}

	s.logger.Printf("Found %d new hashes to anchor.", len(items))

	// 2. 将这些哈希以 PENDING 状态写入 proof_anchors 表
	count, err := s.repo.CreatePendingAnchors(ctx, items)
	if err != nil {
		return err
	}

	s.logger.Printf("Successfully created %d pending anchor records.", count)

	// 3. TODO: 下一步，实现一个函数来处理 PENDING 记录，将它们打包并提交到区块链。
	//    err = s.submitToBlockchain(ctx)

	return nil
}