package output

import (
	"bytes"
	"encoding/json"
	"strings"
)

// jsonlObject renders the dynamic field set as one JSON object followed by a
// newline — the "dynamic as JSONL" deliverable. Values are the raw (un-DSV-
// sanitized) field strings and the field order is preserved.
func jsonlObject(names, raw []string) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(jsonString(n))
		b.WriteByte(':')
		b.WriteString(jsonString(raw[i]))
	}
	b.WriteString("}\n")
	return b.String()
}

// jsonString encodes s as a JSON string without HTML escaping, preserving UTF-8.
func jsonString(s string) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	return strings.TrimRight(b.String(), "\n")
}
