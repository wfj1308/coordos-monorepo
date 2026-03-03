package resolve

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"coordos/design-institute/qualification"
)

// VRPResponse defines the v:// resolver response payload.
type VRPResponse struct {
	Addr      string          `json:"addr"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	ProofHash string          `json:"proof_hash"`
	Anchor    *AnchorInfo     `json:"anchor,omitempty"`
	Resolver  string          `json:"resolver"`
	TTL       int             `json:"ttl"`
}

// AnchorInfo stores optional L0 anchor info.
type AnchorInfo struct {
	Chain       string    `json:"chain"`
	TxHash      string    `json:"tx_hash"`
	Block       int64     `json:"block"`
	ConfirmedAt time.Time `json:"confirmed_at"`
}

// Service serves both vlink resolve and candidate scheduling APIs.
type Service struct {
	repo              *Repository
	db                *sql.DB
	specBasePath      string
	tenantID          int
	headOfficeRefBase string
	qualificationSvc  *qualification.Service
}

// NewService supports both constructors:
// 1) NewService(db, specBasePath)
// 2) NewService(qualificationSvc, db, tenantID, headOfficeRefBase)
func NewService(args ...any) *Service {
	s := &Service{
		specBasePath:      "specs",
		tenantID:          10000,
		headOfficeRefBase: "v://zhongbei/executor/headquarters",
	}

	switch len(args) {
	case 2:
		if db, ok := args[0].(*sql.DB); ok {
			s.db = db
			s.repo = NewRepository(db)
		}
		if specBasePath, ok := args[1].(string); ok && strings.TrimSpace(specBasePath) != "" {
			s.specBasePath = strings.TrimSpace(specBasePath)
		}
	case 4:
		if qsvc, ok := args[0].(*qualification.Service); ok {
			s.qualificationSvc = qsvc
		}
		if db, ok := args[1].(*sql.DB); ok {
			s.db = db
			s.repo = NewRepository(db)
		}
		if tenantID, ok := args[2].(int); ok && tenantID > 0 {
			s.tenantID = tenantID
		}
		if headOfficeRefBase, ok := args[3].(string); ok && strings.TrimSpace(headOfficeRefBase) != "" {
			s.headOfficeRefBase = strings.TrimSpace(headOfficeRefBase)
		}
	default:
		for _, arg := range args {
			switch v := arg.(type) {
			case *qualification.Service:
				s.qualificationSvc = v
			case *sql.DB:
				s.db = v
				s.repo = NewRepository(v)
			case int:
				if v > 0 {
					s.tenantID = v
				}
			case string:
				val := strings.TrimSpace(v)
				if val == "" {
					continue
				}
				if strings.HasPrefix(val, "v://") {
					s.headOfficeRefBase = val
				} else {
					s.specBasePath = val
				}
			}
		}
	}

	return s
}

// Resolve resolves one v:// address.
func (s *Service) Resolve(ctx context.Context, addr string) (*VRPResponse, error) {
	addr = strings.TrimSpace(addr)
	if !strings.HasPrefix(addr, "v://") {
		return nil, fmt.Errorf("invalid v:// address format: %s", addr)
	}

	parts := strings.Split(strings.TrimPrefix(addr, "v://"), "/")
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid v:// address path: %s", addr)
	}

	resourceType := parts[1]
	switch resourceType {
	case "utxo":
		if len(parts) > 2 && parts[2] == "achievement" {
			return s.resolveAchievement(ctx, addr)
		}
	case "genesis":
		return s.resolveGenesisUTXO(ctx, addr)
	case "executor":
		return s.resolveExecutor(ctx, addr)
	case "spu":
		return s.resolveSPU(ctx, addr)
	default:
		return nil, fmt.Errorf("unsupported resource type for resolution: %s", resourceType)
	}

	return nil, fmt.Errorf("resource not found: %s", addr)
}

func (s *Service) resolveAchievement(ctx context.Context, addr string) (*VRPResponse, error) {
	item, err := s.repo.FindAchievementByRef(ctx, addr)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("achievement not found: %s", addr)
		}
		return nil, fmt.Errorf("database error resolving achievement: %w", err)
	}

	payload, err := json.Marshal(item.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal achievement payload: %w", err)
	}

	resp := &VRPResponse{
		Addr:      item.Ref,
		Type:      "achievement_utxo",
		Payload:   payload,
		ProofHash: item.ProofHash,
		Resolver:  item.NamespaceRef,
		TTL:       3600,
	}

	if item.AnchorChain.Valid && item.AnchorTxHash.Valid && item.AnchorBlock.Valid {
		resp.Anchor = &AnchorInfo{
			Chain:       item.AnchorChain.String,
			TxHash:      item.AnchorTxHash.String,
			Block:       item.AnchorBlock.Int64,
			ConfirmedAt: item.AnchoredAt.Time,
		}
	}

	return resp, nil
}

func (s *Service) resolveSPU(ctx context.Context, addr string) (*VRPResponse, error) {
	if s.specBasePath == "" {
		return nil, fmt.Errorf("SPU spec path is not configured")
	}
	_ = ctx

	parts := strings.Split(strings.TrimPrefix(addr, "v://"), "/")
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid SPU address path: %s", addr)
	}

	spuPath := strings.Join(parts[2:], "/")
	atIndex := strings.LastIndex(spuPath, "@")
	if atIndex == -1 {
		return nil, fmt.Errorf("invalid SPU address: missing version anchor '@': %s", addr)
	}

	basePath := spuPath[:atIndex]
	version := spuPath[atIndex+1:]
	fileName := fmt.Sprintf("%s.%s.json", basePath, version)
	filePath := filepath.Join(s.specBasePath, "spu", fileName)

	payload, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("spu specification not found at path %s for address %s", filePath, addr)
		}
		return nil, fmt.Errorf("failed to read spu spec file %s: %w", filePath, err)
	}

	hash := sha256.Sum256(payload)
	proofHash := "sha256:" + hex.EncodeToString(hash[:])
	resolverRef := "v://" + parts[0]

	return &VRPResponse{
		Addr:      addr,
		Type:      "spu_specification",
		Payload:   payload,
		ProofHash: proofHash,
		Resolver:  resolverRef,
		TTL:       86400,
	}, nil
}

func (s *Service) resolveExecutor(ctx context.Context, addr string) (*VRPResponse, error) {
	item, err := s.repo.FindExecutorByRef(ctx, addr)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("executor not found: %s", addr)
		}
		return nil, fmt.Errorf("database error resolving executor: %w", err)
	}

	payload, err := json.Marshal(item)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal executor payload: %w", err)
	}

	hash := sha256.Sum256(payload)
	proofHash := "sha256:" + hex.EncodeToString(hash[:])

	parts := strings.Split(strings.TrimPrefix(addr, "v://"), "/")
	resolverRef := "v://" + parts[0]

	resp := &VRPResponse{
		Addr:      item.Ref,
		Type:      "executor",
		Payload:   payload,
		ProofHash: proofHash,
		Resolver:  resolverRef,
		TTL:       3600,
	}
	return resp, nil
}

func (s *Service) resolveGenesisUTXO(ctx context.Context, addr string) (*VRPResponse, error) {
	item, err := s.repo.FindGenesisByRef(ctx, addr)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("genesis utxo not found: %s", addr)
		}
		return nil, fmt.Errorf("database error resolving genesis utxo: %w", err)
	}

	payload, err := json.Marshal(item.ConstraintJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal genesis payload: %w", err)
	}

	parts := strings.Split(strings.TrimPrefix(addr, "v://"), "/")
	resolverRef := "v://" + parts[0]

	resp := &VRPResponse{
		Addr:      item.Ref,
		Type:      "genesis_utxo",
		Payload:   payload,
		ProofHash: item.ProofHash,
		Resolver:  resolverRef,
		TTL:       86400,
	}

	if item.AnchorChain.Valid && item.AnchorTxHash.Valid && item.AnchorBlock.Valid {
		resp.Anchor = &AnchorInfo{
			Chain:       item.AnchorChain.String,
			TxHash:      item.AnchorTxHash.String,
			Block:       item.AnchorBlock.Int64,
			ConfirmedAt: item.AnchoredAt.Time,
		}
	}

	return resp, nil
}
