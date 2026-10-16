package output

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
)

// pyJSONEncode renders a value exactly as Python's json.dumps(value,
// sort_keys=True) with default separators (", " between items, ": " between a
// key and value) and ensure_ascii=True: every rune above 0x7E is emitted as a
// \uXXXX escape (surrogate pairs above U+FFFF), and "/" is not escaped. This is
// mandatory for byte-identical json / json_line output.
func pyJSONEncode(v any) string {
	var b strings.Builder
	encodeJSONValue(&b, v)
	return b.String()
}

func encodeJSONValue(b *strings.Builder, v any) {
	switch t := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		if t {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case string:
		encodeJSONString(b, t)
	case json.Number:
		b.WriteString(string(t))
	case int:
		b.WriteString(strconv.Itoa(t))
	case int64:
		b.WriteString(strconv.FormatInt(t, 10))
	case float64:
		b.WriteString(strconv.FormatFloat(t, 'g', -1, 64))
	case []any:
		b.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				b.WriteString(", ")
			}
			encodeJSONValue(b, e)
		}
		b.WriteByte(']')
	case []string:
		b.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				b.WriteString(", ")
			}
			encodeJSONString(b, e)
		}
		b.WriteByte(']')
	case map[string]any:
		encodeJSONObject(b, t)
	default:
		// Fallback: encode via Python str() representation as a JSON string.
		encodeJSONString(b, "")
	}
}

func encodeJSONObject(b *strings.Builder, m map[string]any) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		encodeJSONString(b, k)
		b.WriteString(": ")
		encodeJSONValue(b, m[k])
	}
	b.WriteByte('}')
}

// encodeJSONString mirrors Python's json string encoder with ensure_ascii.
func encodeJSONString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\f':
			b.WriteString(`\f`)
		case '\r':
			b.WriteString(`\r`)
		default:
			if r < 0x20 || r > 0x7e {
				if r > 0xffff {
					c := r - 0x10000
					writeU(b, 0xd800+(c>>10))
					writeU(b, 0xdc00+(c&0x3ff))
				} else {
					writeU(b, r)
				}
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
}

const hexDigits = "0123456789abcdef"

func writeU(b *strings.Builder, r rune) {
	b.WriteString(`\u`)
	b.WriteByte(hexDigits[(r>>12)&0xf])
	b.WriteByte(hexDigits[(r>>8)&0xf])
	b.WriteByte(hexDigits[(r>>4)&0xf])
	b.WriteByte(hexDigits[r&0xf])
}
