package rocksdb

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizePath_Empty(t *testing.T) {
	got, err := normalizePath("")
	if err != nil {
		t.Fatalf("normalizePath returned error: %v", err)
	}
	if !strings.HasPrefix(got, "file:") {
		t.Fatalf("dsn should start with file:, got %q", got)
	}
	if !strings.Contains(got, defaultDBFile) {
		t.Fatalf("dsn should contain default file name, got %q", got)
	}
}

func TestNormalizePath_DirectoryStyle(t *testing.T) {
	base := filepath.Join(t.TempDir(), "rocksdb-data")
	got, err := normalizePath(base)
	if err != nil {
		t.Fatalf("normalizePath returned error: %v", err)
	}
	if !strings.Contains(got, "rocksdb-data/"+defaultDBFile) {
		t.Fatalf("expected dsn to include directory + file, got %q", got)
	}
}

func TestOpenClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backend-dir")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	if db.Path() == "" {
		t.Fatal("Path should not be empty")
	}
	if db.ProjectTree() == nil || db.Genesis() == nil || db.Audit() == nil {
		t.Fatal("store accessors should not return nil")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}
