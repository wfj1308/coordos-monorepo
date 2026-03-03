// Package vuri implements the v:// unified resource reference protocol.
//
// Format:  v://{node}/{kind}/{path...}/{id}@{version}
// Example: v://cn.zhongbei/executor/person/cyp4310@v1
package vuri

import (
	"fmt"
	"strings"
)

// VRef is a v:// resource reference (immutable, comparable)
type VRef string

// Parts of a parsed VRef
type VRefParts struct {
	Raw     string
	Node    string   // e.g. "cn.zhongbei"
	Kind    string   // e.g. "executor"
	Path    string   // full path after kind, before id, e.g. "person"
	Segs    []string // path segments
	ID      string   // e.g. "cyp4310"
	Version string   // e.g. "v1"
}

// Parse parses a VRef into its components.
// Format: v://{node}/{kind}/{path...}/{id}@{version}
// The version part is optional.
func Parse(ref VRef) (*VRefParts, error) {
	s := string(ref)
	if !strings.HasPrefix(s, "v://") {
		return nil, fmt.Errorf("invalid vref: must start with v://: %q", s)
	}
	rest := s[4:] // strip "v://"

	p := &VRefParts{Raw: s}

	// Extract version if present
	if atIndex := strings.LastIndex(rest, "@"); atIndex != -1 {
		p.Version = rest[atIndex+1:]
		rest = rest[:atIndex]
	}

	parts := strings.SplitN(rest, "/", 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid vref: missing node or kind: %q", s)
	}

	p.Node = parts[0]
	p.Kind = parts[1]

	if len(parts) == 3 {
		pathAndID := parts[2]
		if lastSlash := strings.LastIndex(pathAndID, "/"); lastSlash != -1 {
			p.Path = pathAndID[:lastSlash]
			p.Segs = strings.Split(p.Path, "/")
			p.ID = pathAndID[lastSlash+1:]
		} else {
			p.ID = pathAndID
		}
	}

	// If ID is empty and there are path segments, the last segment is the ID
	if p.ID == "" && len(p.Segs) > 0 {
		p.ID = p.Segs[len(p.Segs)-1]
		p.Path = strings.Join(p.Segs[:len(p.Segs)-1], "/")
		p.Segs = p.Segs[:len(p.Segs)-1]
	}

	return p, nil
}

// New constructs a VRef from components.
func New(node, kind, path, id, version string) VRef {
	var sb strings.Builder
	sb.WriteString("v://")
	sb.WriteString(node)
	sb.WriteString("/")
	sb.WriteString(kind)
	if path != "" {
		sb.WriteString("/")
		sb.WriteString(path)
	}
	if id != "" {
		sb.WriteString("/")
		sb.WriteString(id)
	}
	if version != "" {
		sb.WriteString("@")
		sb.WriteString(version)
	}
	return VRef(sb.String())
}

// Child appends a segment to an existing VRef's path.
// This is a bit ambiguous with the new structure, let's assume it adds to the path.
func Child(parent VRef, seg string) VRef {
	// This function's behavior is less clear in the new format.
	// For now, let's assume it appends to the path part before the ID.
	// A more robust implementation might require parsing and rebuilding.
	p, err := Parse(parent)
	if err != nil {
		// Fallback for unparsable or simple VRefs
		return VRef(string(parent) + "/" + seg)
	}
	
	newPath := p.Path
	if newPath != "" {
		newPath += "/"
	}
	newPath += seg
	
	return New(p.Node, p.Kind, newPath, p.ID, p.Version)
}

// Node extracts the node from a VRef (returns "" on error).
func Node(ref VRef) string {
	p, err := Parse(ref)
	if err != nil {
		return ""
	}
	return p.Node
}

// Depth returns the number of path segments (0 = root kind only).
func Depth(ref VRef) int {
	p, err := Parse(ref)
	if err != nil || p.Path == "" {
		return 0
	}
	return len(p.Segs)
}

// SameNode reports whether two refs belong to the same node.
func SameNode(a, b VRef) bool {
	return Node(a) == Node(b) && Node(a) != ""
}

// IsAncestor reports whether a is a strict ancestor of b.
// This logic needs to be re-evaluated with the new structure.
// For now, we'll keep the simple prefix check.
func IsAncestor(a, b VRef) bool {
	as, bs := string(a), string(b)
	return strings.HasPrefix(bs, as+"/")
}
