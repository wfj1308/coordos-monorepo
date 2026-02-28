package main

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type fileConfig struct {
	Server struct {
		Addr string `yaml:"addr"`
	} `yaml:"server"`
	Tenant struct {
		ID int `yaml:"id"`
	} `yaml:"tenant"`
	Postgres struct {
		DSN string `yaml:"dsn"`
	} `yaml:"postgres"`
}

func loadConfig() config {
	cfg := config{
		Addr:     ":8090",
		PGDSN:    "postgres://coordos:coordos@localhost:5432/coordos?sslmode=disable",
		TenantID: 10000,
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
		log.Printf("config file loaded: %s", path)
	}

	cfg.Addr = envOrDefault("DESIGN_INSTITUTE_HTTP_ADDR", cfg.Addr)
	cfg.PGDSN = envOrDefault("DESIGN_INSTITUTE_PG_DSN", cfg.PGDSN)
	cfg.TenantID = envIntOrDefault("DESIGN_INSTITUTE_TENANT_ID", cfg.TenantID)

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
