package formatters

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// pyFormatError signals a Python-style KeyError: a referenced field name was
// absent from the values map. The caller reproduces plaso's fallback message.
type pyFormatError struct{ key string }

func (e *pyFormatError) Error() string { return "KeyError: " + e.key }

// pyFormat emulates Python's str.format(**values) for the subset of the
// format mini-language used by plaso's formatter templates:
//
//	{name}, {name:spec}, {name!conv}, {name!conv:spec}, plus {{ and }} escapes.
//
// A missing field name returns a *pyFormatError (Python raises KeyError).
func pyFormat(template string, values map[string]any) (string, error) {
	var b strings.Builder
	i := 0
	n := len(template)
	for i < n {
		c := template[i]
		if c == '{' {
			if i+1 < n && template[i+1] == '{' {
				b.WriteByte('{')
				i += 2
				continue
			}
			end := strings.IndexByte(template[i+1:], '}')
			if end < 0 {
				// Unterminated field: emit verbatim (defensive).
				b.WriteString(template[i:])
				break
			}
			field := template[i+1 : i+1+end]
			i = i + 1 + end + 1
			out, err := formatField(field, values)
			if err != nil {
				return "", err
			}
			b.WriteString(out)
			continue
		}
		if c == '}' {
			if i+1 < n && template[i+1] == '}' {
				b.WriteByte('}')
				i += 2
				continue
			}
			b.WriteByte('}')
			i++
			continue
		}
		b.WriteByte(c)
		i++
	}
	return b.String(), nil
}

// formatField renders one replacement field "name[!conv][:spec]".
func formatField(field string, values map[string]any) (string, error) {
	name := field
	conv := ""
	spec := ""

	if colon := strings.IndexByte(name, ':'); colon >= 0 {
		spec = name[colon+1:]
		name = name[:colon]
	}
	if bang := strings.IndexByte(name, '!'); bang >= 0 {
		conv = name[bang+1:]
		name = name[:bang]
	}

	value, ok := values[name]
	if !ok {
		return "", &pyFormatError{key: name}
	}

	// Conversions: !s uses str(), !r/!a use repr()-like.
	if conv == "r" || conv == "a" {
		return applySpec(pyRepr(value), spec, value, true)
	}
	if conv == "s" {
		return applySpec(pyStr(value), spec, value, true)
	}
	return applySpec(pyStr(value), spec, value, false)
}

// pyStr mirrors Python str() for the value types that appear in event values.
func pyStr(v any) string {
	switch t := v.(type) {
	case nil:
		return "None"
	case string:
		return t
	case bool:
		if t {
			return "True"
		}
		return "False"
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case []any:
		return pyReprList(t)
	case []string:
		items := make([]any, len(t))
		for i, s := range t {
			items[i] = s
		}
		return pyReprList(items)
	default:
		return fmt.Sprintf("%v", t)
	}
}

// pyRepr is a minimal Python repr() for the value types that appear in event
// values (strings, numbers, booleans, lists).
func pyRepr(v any) string {
	switch t := v.(type) {
	case string:
		return pyReprString(t)
	case []any:
		return pyReprList(t)
	default:
		return pyStr(v)
	}
}

// pyReprList mirrors Python repr() of a list: "[elem, ...]" with each element
// rendered via repr (strings single-quoted).
func pyReprList(items []any) string {
	parts := make([]string, len(items))
	for i, e := range items {
		parts[i] = pyRepr(e)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// pyReprString mirrors Python repr() of a string: single quotes by default,
// double quotes if the string contains a single but no double quote, with the
// common escape sequences.
func pyReprString(s string) string {
	quote := byte('\'')
	if strings.Contains(s, "'") && !strings.Contains(s, "\"") {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for _, r := range s {
		switch r {
		case rune(quote):
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\\':
			b.WriteString("\\\\")
		case '\n':
			b.WriteString("\\n")
		case '\r':
			b.WriteString("\\r")
		case '\t':
			b.WriteString("\\t")
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte(quote)
	return b.String()
}

// applySpec applies the Python format spec mini-language subset used by plaso.
// hadConv indicates a !conv already produced a string (so numeric type codes
// no longer apply and spec is treated as a string spec).
func applySpec(strValue, spec string, value any, hadConv bool) (string, error) {
	if spec == "" {
		return strValue, nil
	}

	ps, err := parseFormatSpec(spec)
	if err != nil {
		return "", err
	}

	// Numeric presentation types operate on the underlying value.
	if !hadConv && strings.ContainsAny(ps.typ, "dboxXeEfFgGc%") {
		rendered, err := renderNumeric(value, ps)
		if err != nil {
			return "", err
		}
		return padAlign(rendered, ps, true), nil
	}

	// String spec (type 's' or empty).
	s := strValue
	if ps.precision >= 0 {
		if ps.precision < len(s) {
			s = s[:ps.precision]
		}
	}
	return padAlign(s, ps, false), nil
}

type formatSpec struct {
	fill      byte
	align     byte // '<','>','^','='; 0 if unset
	sign      byte // '+','-',' '; 0 if unset
	alt       bool // '#'
	zero      bool // '0'
	width     int
	grouping  byte // ',' or '_'; 0 if unset
	precision int  // -1 if unset
	typ       string
}

func parseFormatSpec(spec string) (formatSpec, error) {
	ps := formatSpec{precision: -1}
	i := 0
	// [[fill]align]
	if len(spec) >= 2 && isAlign(spec[1]) {
		ps.fill = spec[0]
		ps.align = spec[1]
		i += 2
	} else if len(spec) >= 1 && isAlign(spec[0]) {
		ps.align = spec[0]
		i++
	}
	// [sign]
	if i < len(spec) && (spec[i] == '+' || spec[i] == '-' || spec[i] == ' ') {
		ps.sign = spec[i]
		i++
	}
	// [#]
	if i < len(spec) && spec[i] == '#' {
		ps.alt = true
		i++
	}
	// [0]
	if i < len(spec) && spec[i] == '0' {
		ps.zero = true
		if ps.align == 0 {
			ps.align = '='
			ps.fill = '0'
		}
		i++
	}
	// [width]
	start := i
	for i < len(spec) && spec[i] >= '0' && spec[i] <= '9' {
		i++
	}
	if i > start {
		w, _ := strconv.Atoi(spec[start:i])
		ps.width = w
	}
	// [grouping]
	if i < len(spec) && (spec[i] == ',' || spec[i] == '_') {
		ps.grouping = spec[i]
		i++
	}
	// [.precision]
	if i < len(spec) && spec[i] == '.' {
		i++
		ps2 := i
		for i < len(spec) && spec[i] >= '0' && spec[i] <= '9' {
			i++
		}
		p, _ := strconv.Atoi(spec[ps2:i])
		ps.precision = p
	}
	// [type]
	if i < len(spec) {
		ps.typ = spec[i:]
	}
	if ps.fill == 0 {
		ps.fill = ' '
	}
	return ps, nil
}

func isAlign(b byte) bool { return b == '<' || b == '>' || b == '^' || b == '=' }

// renderNumeric formats a numeric value for integer/float presentation types.
func renderNumeric(value any, ps formatSpec) (string, error) {
	switch ps.typ {
	case "d", "b", "o", "x", "X", "c":
		iv, ok := toInt64(value)
		if !ok {
			return "", fmt.Errorf("value %v not an integer for %q", value, ps.typ)
		}
		return formatInt(iv, ps), nil
	case "f", "F", "e", "E", "g", "G", "%":
		fv, ok := toFloat64(value)
		if !ok {
			return "", fmt.Errorf("value %v not a float for %q", value, ps.typ)
		}
		prec := ps.precision
		if prec < 0 {
			prec = 6
		}
		switch ps.typ {
		case "f", "F":
			return strconv.FormatFloat(fv, 'f', prec, 64), nil
		case "e":
			return strconv.FormatFloat(fv, 'e', prec, 64), nil
		case "E":
			return strconv.FormatFloat(fv, 'E', prec, 64), nil
		case "g":
			return strconv.FormatFloat(fv, 'g', prec, 64), nil
		case "G":
			return strconv.FormatFloat(fv, 'G', prec, 64), nil
		case "%":
			return strconv.FormatFloat(fv*100, 'f', prec, 64) + "%", nil
		}
	}
	return "", fmt.Errorf("unsupported numeric type %q", ps.typ)
}

func formatInt(iv int64, ps formatSpec) string {
	neg := iv < 0
	u := iv
	if neg {
		u = -iv
	}
	var digits string
	switch ps.typ {
	case "d":
		digits = strconv.FormatInt(u, 10)
	case "b":
		digits = strconv.FormatInt(u, 2)
	case "o":
		digits = strconv.FormatInt(u, 8)
	case "x":
		digits = strconv.FormatInt(u, 16)
	case "X":
		digits = strings.ToUpper(strconv.FormatInt(u, 16))
	case "c":
		return string(rune(iv))
	}
	prefix := ""
	if ps.alt {
		switch ps.typ {
		case "b":
			prefix = "0b"
		case "o":
			prefix = "0o"
		case "x":
			prefix = "0x"
		case "X":
			prefix = "0X"
		}
	}
	sign := ""
	if neg {
		sign = "-"
	} else if ps.sign == '+' {
		sign = "+"
	} else if ps.sign == ' ' {
		sign = " "
	}
	body := sign + prefix + digits
	// Zero padding with '=' alignment pads between sign/prefix and digits.
	if ps.align == '=' && ps.width > len(body) {
		pad := strings.Repeat(string(ps.fill), ps.width-len(body))
		return sign + prefix + pad + digits
	}
	return body
}

// padAlign applies width/alignment. numeric indicates default right alignment.
func padAlign(s string, ps formatSpec, numeric bool) string {
	if ps.width <= len([]rune(s)) {
		return s
	}
	padN := ps.width - len([]rune(s))
	pad := strings.Repeat(string(ps.fill), padN)
	align := ps.align
	if align == 0 || align == '=' {
		if numeric {
			align = '>'
		} else {
			align = '<'
		}
	}
	switch align {
	case '>':
		return pad + s
	case '^':
		left := padN / 2
		right := padN - left
		return strings.Repeat(string(ps.fill), left) + s + strings.Repeat(string(ps.fill), right)
	default: // '<'
		return s + pad
	}
}

func toInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, true
		}
		if f, err := t.Float64(); err == nil {
			return int64(f), true
		}
		// Very large integers.
		if bi, ok := new(big.Int).SetString(t.String(), 10); ok {
			return bi.Int64(), true
		}
	case int:
		return int64(t), true
	case int64:
		return t, true
	case float64:
		return int64(t), true
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// toInt64FromKey parses a YAML map key (decimal or 0x-hex) to int64.
func toInt64FromKey(k string) (int64, bool) {
	k = strings.TrimSpace(k)
	if strings.HasPrefix(k, "0x") || strings.HasPrefix(k, "0X") {
		if i, err := strconv.ParseInt(k[2:], 16, 64); err == nil {
			return i, true
		}
	}
	if i, err := strconv.ParseInt(k, 10, 64); err == nil {
		return i, true
	}
	return 0, false
}

func toFloat64(v any) (float64, bool) {
	switch t := v.(type) {
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return f, true
		}
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	}
	return 0, false
}
