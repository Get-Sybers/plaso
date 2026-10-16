// Package dfpath implements the minimal dfVFS path-specification handling the
// dynamic output module needs: decoding the serialized path_spec and deriving
// a display name, mirroring plaso path_helper.GetDisplayNameForPathSpec for the
// common cases.
package dfpath

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Get-Sybers/plaso/goplaso/internal/container"
)

// PathSpec is a decoded dfVFS path specification (a chain via Parent).
type PathSpec struct {
	TypeIndicator string
	Location      string
	Parent        *PathSpec
	raw           map[string]any
}

// Parse decodes a serialized path_spec JSON object.
func Parse(jsonStr string) (*PathSpec, error) {
	if strings.TrimSpace(jsonStr) == "" {
		return nil, nil
	}
	obj, err := container.DecodeJSONObject([]byte(jsonStr))
	if err != nil {
		return nil, err
	}
	return fromMap(obj), nil
}

func fromMap(m map[string]any) *PathSpec {
	if m == nil {
		return nil
	}
	ps := &PathSpec{raw: m}
	ps.TypeIndicator, _ = m["type_indicator"].(string)
	ps.Location, _ = m["location"].(string)
	if parent, ok := m["parent"].(map[string]any); ok {
		ps.Parent = fromMap(parent)
	}
	return ps
}

// DisplayName derives the human-readable display name, e.g. "OS:/path/to/file".
// For the first slice this covers the flat (parent-less) case byte-exactly;
// nested specs (images, VSS, compressed streams) are refined in a later
// milestone.
func (p *PathSpec) DisplayName() string {
	if p == nil {
		return "-"
	}
	if p.Location != "" {
		return p.TypeIndicator + ":" + p.Location
	}
	if p.Parent != nil {
		return p.Parent.DisplayName()
	}
	return "-"
}

// Comparable mirrors dfVFS PathSpec.comparable: one "type: X, key: value\n" line
// per path specification in the chain (parent first), with the comparable
// attributes sorted by name. This covers the flat OS/location case exercised by
// psort's native output; the attribute set matches dfVFS for those specs.
func (p *PathSpec) Comparable() string {
	if p == nil {
		return ""
	}
	var b strings.Builder
	if p.Parent != nil {
		b.WriteString(p.Parent.Comparable())
	}
	b.WriteString("type: ")
	b.WriteString(p.TypeIndicator)
	keys := make([]string, 0, len(p.raw))
	for k := range p.raw {
		switch k {
		case "parent", "type_indicator", "__type__":
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString(", ")
		b.WriteString(k)
		b.WriteString(": ")
		b.WriteString(fmt.Sprintf("%v", p.raw[k]))
	}
	b.WriteByte('\n')
	return b.String()
}
