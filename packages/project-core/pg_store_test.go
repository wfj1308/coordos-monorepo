package projectcore

import "testing"

func TestTenantFromRef(t *testing.T) {
	tid, err := tenantFromRef(VRef("v://coordos/project/demo"))
	if err != nil {
		t.Fatalf("tenantFromRef returned error: %v", err)
	}
	if tid != "coordos" {
		t.Fatalf("unexpected tenant: %s", tid)
	}
}

func TestTenantFromRefInvalid(t *testing.T) {
	if _, err := tenantFromRef(VRef("bad-ref")); err == nil {
		t.Fatal("expected error for invalid ref")
	}
}
