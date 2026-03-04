package main

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type fileConfig struct {
	Server struct {
		Addr string `yaml:"addr"`
	} `yaml:"server"`
	SPU struct {
		CatalogPath string `yaml:"catalog_path"`
	} `yaml:"spu"`
	Resolve struct {
		HeadOfficeRef string `yaml:"head_office_ref"`
	} `yaml:"resolve"`
	Tenant struct {
		ID int `yaml:"id"`
	} `yaml:"tenant"`
	Postgres struct {
		DSN string `yaml:"dsn"`
	} `yaml:"postgres"`
	ProofAnchor struct {
		Enabled            *bool `yaml:"enabled"`
		ScanIntervalSecond int   `yaml:"scan_interval_second"`
	} `yaml:"proof_anchor"`
}

func loadConfig() config {
	cfg := config{
		Addr:                    ":8090",
		PGDSN:                   "postgres://coordos:coordos@localhost:5432/coordos?sslmode=disable",
		TenantID:                10000,
		HeadOfficeRefBase:       "v://cn.zhongbei/executor/headquarters",
		SPUCatalogPath:          "specs/spu/bridge/catalog.v1.json",
		ProofAnchorEnabled:      true,
		ProofAnchorScanInterval: time.Minute,
	}

	fc, path, err := loadConfigFile()
	if err != nil {
		log.Fatalf("load config file failed: %v", err)
	}
	if path != "" {
		if fc.Server.Addr != "" {
			cfg.Addr = fc.Server.Addr
		}
		if fc.Postgres.DSN != "" {
			cfg.PGDSN = fc.Postgres.DSN
		}
		if fc.Tenant.ID > 0 {
			cfg.TenantID = fc.Tenant.ID
		}
		if v := strings.TrimSpace(fc.Resolve.HeadOfficeRef); v != "" {
			cfg.HeadOfficeRefBase = v
		}
		if v := strings.TrimSpace(fc.SPU.CatalogPath); v != "" {
			cfg.SPUCatalogPath = v
		}
		if fc.ProofAnchor.Enabled != nil {
			cfg.ProofAnchorEnabled = *fc.ProofAnchor.Enabled
		}
		if fc.ProofAnchor.ScanIntervalSecond > 0 {
			cfg.ProofAnchorScanInterval = time.Duration(fc.ProofAnchor.ScanIntervalSecond) * time.Second
		}
		log.Printf("config file loaded: %s", path)
	}

	// Preferred env vars.
	if v := strings.TrimSpace(os.Getenv("DESIGN_INSTITUTE_HTTP_ADDR")); v != "" {
		cfg.Addr = normalizeHTTPAddr(v)
	}
	if v := strings.TrimSpace(os.Getenv("DESIGN_INSTITUTE_PG_DSN")); v != "" {
		cfg.PGDSN = v
	}
	if v := strings.TrimSpace(os.Getenv("DESIGN_INSTITUTE_HEAD_OFFICE_REF")); v != "" {
		cfg.HeadOfficeRefBase = v
	}
	if v := strings.TrimSpace(os.Getenv("DESIGN_INSTITUTE_SPU_CATALOG_PATH")); v != "" {
		cfg.SPUCatalogPath = v
	}
	cfg.TenantID = envIntOrDefault("DESIGN_INSTITUTE_TENANT_ID", cfg.TenantID)
	cfg.ProofAnchorEnabled = envBoolOrDefault("DESIGN_INSTITUTE_PROOF_ANCHOR_ENABLED", cfg.ProofAnchorEnabled)
	if sec := envIntOrDefault("DESIGN_INSTITUTE_PROOF_ANCHOR_SCAN_SECONDS", int(cfg.ProofAnchorScanInterval.Seconds())); sec > 0 {
		cfg.ProofAnchorScanInterval = time.Duration(sec) * time.Second
	}

	// Compatibility with older docs/scripts.
	// 1) DATABASE_URL for Postgres DSN.
	if v := strings.TrimSpace(os.Getenv("DATABASE_URL")); v != "" {
		if strings.TrimSpace(os.Getenv("DESIGN_INSTITUTE_PG_DSN")) == "" {
			cfg.PGDSN = v
		}
		// Keep test-1 behavior: when only DATABASE_URL is provided, bind on :8081.
		if strings.TrimSpace(os.Getenv("DESIGN_INSTITUTE_HTTP_ADDR")) == "" &&
			strings.TrimSpace(os.Getenv("PORT")) == "" &&
			cfg.Addr == ":8090" {
			cfg.Addr = ":8081"
		}
	}
	// 2) PORT for HTTP bind.
	if v := strings.TrimSpace(os.Getenv("PORT")); v != "" &&
		strings.TrimSpace(os.Getenv("DESIGN_INSTITUTE_HTTP_ADDR")) == "" {
		cfg.Addr = normalizeHTTPAddr(v)
	}
	// 3) TENANT_ID fallback.
	if strings.TrimSpace(os.Getenv("DESIGN_INSTITUTE_TENANT_ID")) == "" {
		cfg.TenantID = envIntOrDefault("TENANT_ID", cfg.TenantID)
	}

	validateConfig(cfg)
	return cfg
}

func validateConfig(cfg config) {
	if strings.TrimSpace(cfg.Addr) == "" {
		log.Fatalf("invalid config: empty server.addr")
	}
	if strings.TrimSpace(cfg.PGDSN) == "" {
		log.Fatalf("invalid config: empty postgres.dsn")
	}
	if cfg.TenantID <= 0 {
		log.Fatalf("invalid config: tenant.id must be positive")
	}
	if strings.TrimSpace(cfg.HeadOfficeRefBase) == "" {
		log.Fatalf("invalid config: resolve.head_office_ref is empty")
	}
	if strings.TrimSpace(cfg.SPUCatalogPath) == "" {
		log.Fatalf("invalid config: spu.catalog_path is empty")
	}
	if cfg.ProofAnchorScanInterval <= 0 {
		log.Fatalf("invalid config: proof anchor scan interval must be positive")
	}
}

func loadConfigFile() (fileConfig, string, error) {
	var fc fileConfig
	candidates := candidateConfigPaths(
		os.Getenv("DESIGN_INSTITUTE_CONFIG"),
		"config.yaml",
		filepath.Join("services", "design-institute", "config.yaml"),
	)

	for _, p := range candidates {
		data, err := os.ReadFile(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fc, "", err
		}

		dec := yaml.NewDecoder(bytes.NewReader(data))
		dec.KnownFields(true)
		if err := dec.Decode(&fc); err != nil {
			return fc, "", err
		}
		return fc, p, nil
	}
	return fc, "", nil
}

func candidateConfigPaths(paths ...string) []string {
	out := make([]string, 0, len(paths))
	seen := map[string]struct{}{}
	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

func envOrDefault(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func envIntOrDefault(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("invalid %s=%q, fallback=%d", key, v, fallback)
		return fallback
	}
	return n
}

func envBoolOrDefault(key string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return fallback
	}
	switch v {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		log.Printf("invalid %s=%q, fallback=%v", key, v, fallback)
		return fallback
	}
}

func normalizeHTTPAddr(raw string) string {
	v := strings.TrimSpace(raw)
	if v == "" {
		return v
	}
	if strings.HasPrefix(v, ":") {
		return v
	}
	if _, err := strconv.Atoi(v); err == nil {
		return ":" + v
	}
	return v
}
