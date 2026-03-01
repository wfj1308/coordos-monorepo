package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server struct {
		Addr      string `yaml:"addr"`
		JWTSecret string `yaml:"jwt_secret"`
	} `yaml:"server"`
	Tenant struct {
		ID string `yaml:"id"`
	} `yaml:"tenant"`
	Postgres struct {
		DSN string `yaml:"dsn"`
	} `yaml:"postgres"`
	MySQL struct {
		DSN string `yaml:"dsn"`
	} `yaml:"mysql"`
	Storage struct {
		Backend   string `yaml:"backend"`
		RocksPath string `yaml:"rocks_path"`
	} `yaml:"storage"`
}

func Load() (Config, error) {
	cfg := defaultConfig()

	fileCfg, filePath, err := loadConfigFile()
	if err != nil {
		return cfg, err
	}
	if filePath != "" {
		mergeFromFile(&cfg, fileCfg)
	}

	applyEnv(&cfg)
	if err := validate(cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func defaultConfig() Config {
	var c Config
	c.Server.Addr = ":8080"
	c.Server.JWTSecret = "CHANGE_ME"
	c.Tenant.ID = "10000"
	c.Postgres.DSN = "postgres://coordos:coordos@localhost:5432/coordos?sslmode=disable"
	c.MySQL.DSN = "root:123.com@tcp(127.0.0.1:3306)/icrm?charset=utf8mb4&parseTime=true&loc=Local"
	c.Storage.Backend = "rocksdb"
	c.Storage.RocksPath = "./coordos_vault_rocks_native.json"
	return c
}

func mergeFromFile(dst *Config, src Config) {
	if src.Server.Addr != "" {
		dst.Server.Addr = src.Server.Addr
	}
	if src.Server.JWTSecret != "" {
		dst.Server.JWTSecret = src.Server.JWTSecret
	}
	if src.Tenant.ID != "" {
		dst.Tenant.ID = src.Tenant.ID
	}
	if src.Postgres.DSN != "" {
		dst.Postgres.DSN = src.Postgres.DSN
	}
	if src.MySQL.DSN != "" {
		dst.MySQL.DSN = src.MySQL.DSN
	}
	if src.Storage.Backend != "" {
		dst.Storage.Backend = src.Storage.Backend
	}
	if src.Storage.RocksPath != "" {
		dst.Storage.RocksPath = src.Storage.RocksPath
	}
}

func applyEnv(c *Config) {
	c.Server.Addr = envOrDefault("VAULT_SERVICE_HTTP_ADDR", c.Server.Addr)
	c.Server.JWTSecret = envOrDefault("VAULT_SERVICE_JWT_SECRET", c.Server.JWTSecret)
	c.Tenant.ID = envOrDefault("VAULT_SERVICE_TENANT_ID", c.Tenant.ID)
	c.Postgres.DSN = envOrDefault("VAULT_SERVICE_PG_DSN", c.Postgres.DSN)
	c.MySQL.DSN = envOrDefault("VAULT_SERVICE_MYSQL_DSN", c.MySQL.DSN)
	c.Storage.Backend = envOrDefault("VAULT_SERVICE_STORAGE_BACKEND", c.Storage.Backend)
	c.Storage.RocksPath = envOrDefault("VAULT_SERVICE_ROCKS_PATH", c.Storage.RocksPath)
}

func validate(c Config) error {
	if strings.TrimSpace(c.Server.Addr) == "" {
		return fmt.Errorf("invalid config: server.addr is empty")
	}
	if strings.TrimSpace(c.Server.JWTSecret) == "" {
		return fmt.Errorf("invalid config: server.jwt_secret is empty")
	}
	if strings.TrimSpace(c.Tenant.ID) == "" {
		return fmt.Errorf("invalid config: tenant.id is empty")
	}
	switch strings.ToLower(strings.TrimSpace(c.Storage.Backend)) {
	case "rocksdb":
	default:
		return fmt.Errorf("invalid config: storage.backend must be rocksdb")
	}
	if strings.TrimSpace(c.Storage.RocksPath) == "" {
		return fmt.Errorf("invalid config: storage.rocks_path is empty while backend=rocksdb")
	}
	return nil
}

func loadConfigFile() (Config, string, error) {
	var c Config
	candidates := candidatePaths(
		os.Getenv("VAULT_SERVICE_CONFIG"),
		"config.yaml",
		filepath.Join("services", "vault-service", "config.yaml"),
	)
	for _, p := range candidates {
		data, err := os.ReadFile(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return c, "", err
		}
		dec := yaml.NewDecoder(bytes.NewReader(data))
		dec.KnownFields(true)
		if err := dec.Decode(&c); err != nil {
			return c, "", err
		}
		return c, p, nil
	}
	return c, "", nil
}

func candidatePaths(paths ...string) []string {
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

