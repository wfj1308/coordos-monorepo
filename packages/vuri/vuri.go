// Package vuri implements the v:// unified resource reference protocol.
//
// Format:  v://{tenant}/{kind}/{path...}
// Example: v://zhongbei/project/highway-001/design/structure
package vuri

import (
	"fmt"
	"strings"
)

// VRef is a v:// resource reference (immutable, comparable)
type VRef string

// Parts of a parsed VRef
type VRefParts struct {
	Raw    string
	Tenant string
	Kind   string
	Path   string   // full path after kind
	Segs   []string // path segments
}

// Parse parses a VRef into its components.
func Parse(ref VRef) (*VRefParts, error) {
	s := string(ref)
	if !strings.HasPrefix(s, "v://") {
		return nil, fmt.Errorf("invalid vref: must start with v://: %q", s)
	}
	rest := s[4:] // strip "v://"
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid vref: missing tenant or kind: %q", s)
	}
	p := &VRefParts{
		Raw:    s,
		Tenant: parts[0],
		Kind:   parts[1],
	}
	if len(parts) == 3 {
		p.Path = parts[2]
		p.Segs = strings.Split(parts[2], "/")
	}
	return p, nil
}

// New constructs a VRef from components.
func New(tenant, kind string, segs ...string) VRef {
	if len(segs) == 0 {
		return VRef(fmt.Sprintf("v://%s/%s", tenant, kind))
	}
	return VRef(fmt.Sprintf("v://%s/%s/%s", tenant, kind, strings.Join(segs, "/")))
}

// Child appends a segment to an existing VRef.
func Child(parent VRef, seg string) VRef {
	return VRef(string(parent) + "/" + seg)
}

// Tenant extracts the tenant from a VRef (returns "" on error).
func Tenant(ref VRef) string {
	p, err := Parse(ref)
	if err != nil {
		return ""
	}
	return p.Tenant
}

// Depth returns the number of path segments (0 = root kind only).
func Depth(ref VRef) int {
	p, err := Parse(ref)
	if err != nil || p.Path == "" {
		return 0
	}
	return len(p.Segs)
}

// SameTenant reports whether two refs belong to the same tenant.
func SameTenant(a, b VRef) bool {
	return Tenant(a) == Tenant(b) && Tenant(a) != ""
}

// IsAncestor reports whether a is a strict ancestor of b.
func IsAncestor(a, b VRef) bool {
	as, bs := string(a), string(b)
	return strings.HasPrefix(bs, as+"/")
}
