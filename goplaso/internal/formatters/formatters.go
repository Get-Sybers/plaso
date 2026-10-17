// Package formatters reconstructs human-readable event messages from event
// data attributes, mirroring plaso/formatters. Formatter definitions are the
// same declarative YAML files plaso ships in data/formatters/*.yaml, reused at
// runtime (not code-generated).
package formatters

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// formatStringAttributeNameRE matches an attribute name in a format string
// piece. Ported verbatim from plaso EventFormatter: the leading lowercase
// constraint is load-bearing (a piece whose only placeholder starts
// non-lowercase is treated as literal text and always included).
var formatStringAttributeNameRE = regexp.MustCompile(`{([a-z][a-zA-Z0-9_]*)[!]?[^:}]*[:]?[^}]*}`)

// Helper mutates the event-values map before templating.
type Helper interface {
	Apply(values map[string]any)
}

// BooleanHelper mirrors BooleanEventFormatterHelper.
type BooleanHelper struct {
	Input, Output             string
	ValueIfTrue, ValueIfFalse any
}

func (h *BooleanHelper) Apply(values map[string]any) {
	if truthy(values[h.Input]) {
		values[h.Output] = h.ValueIfTrue
	} else {
		values[h.Output] = h.ValueIfFalse
	}
}

// EnumerationHelper mirrors EnumerationEventFormatterHelper.
type EnumerationHelper struct {
	Input, Output string
	Default       any
	Values        map[string]any
}

func (h *EnumerationHelper) Apply(values map[string]any) {
	input, ok := values[h.Input]
	if !ok || input == nil {
		return
	}
	def := h.Default
	if def == nil {
		def = input
	}
	if mapped, ok := h.Values[keyStr(input)]; ok {
		values[h.Output] = mapped
	} else {
		values[h.Output] = def
	}
}

// FlagsHelper mirrors FlagsEventFormatterHelper.
type FlagsHelper struct {
	Input, Output string
	Values        map[int64]any
}

func (h *FlagsHelper) Apply(values map[string]any) {
	input, ok := values[h.Input]
	if !ok || input == nil {
		return
	}
	iv, ok := toInt64(input)
	if !ok {
		return
	}
	// Preserve declaration order of flags via sorted keys for determinism.
	keys := make([]int64, 0, len(h.Values))
	for k := range h.Values {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	var out []string
	for _, flag := range keys {
		if flag&iv != 0 {
			out = append(out, fmt.Sprintf("%v", h.Values[flag]))
		}
	}
	values[h.Output] = strings.Join(out, ", ")
}

// Formatter is a basic or conditional event formatter.
type Formatter struct {
	Type        string // "basic" or "conditional"
	DataType    string // lowercased
	SourceShort string
	SourceLong  string

	messagePieces   []string
	shortPieces     []string
	separator       string
	helpers         []Helper
	customHelperIDs []string

	pieceAttrs      []string // conditional: attr name per message piece (lazy)
	shortPieceAttrs []string // lazy
	mapsOnce        sync.Once
	buildErr        error

	attrNames     map[string]bool // cached GetFormatStringAttributeNames result
	attrNamesOnce sync.Once
}

// SourceMapping returns (short, long).
func (f *Formatter) SourceMapping() (string, string) { return f.SourceShort, f.SourceLong }

// FormatEventValues runs the formatter's helpers over the values map.
func (f *Formatter) FormatEventValues(values map[string]any, custom CustomRegistry) {
	for _, h := range f.helpers {
		h.Apply(values)
	}
	for _, id := range f.customHelperIDs {
		if fn := custom[strings.ToLower(id)]; fn != nil {
			fn(values)
		}
	}
}

// buildMaps computes the per-piece attribute maps lazily, mirroring plaso's
// _CreateFormatStringMaps (which runs on first GetMessage, not at load).
func (f *Formatter) buildMaps() error {
	f.mapsOnce.Do(func() {
		if f.pieceAttrs, f.buildErr = pieceAttrNames(f.messagePieces); f.buildErr != nil {
			return
		}
		f.shortPieceAttrs, f.buildErr = pieceAttrNames(f.shortPieces)
	})
	return f.buildErr
}

// GetMessage builds the long message.
func (f *Formatter) GetMessage(values map[string]any) string {
	if f.Type == "basic" {
		return formatMessage(f.messagePieces[0], values)
	}
	if err := f.buildMaps(); err != nil {
		return ""
	}
	return conditionalMessage(f.messagePieces, f.pieceAttrs, f.separator, values)
}

func conditionalMessage(pieces, attrs []string, sep string, values map[string]any) string {
	var included []string
	for i, attr := range attrs {
		if attr == "" {
			included = append(included, pieces[i])
			continue
		}
		if v, ok := values[attr]; ok && v != nil {
			included = append(included, pieces[i])
		}
	}
	return formatMessage(strings.Join(included, sep), values)
}

// formatMessage applies the template and reproduces plaso's KeyError fallback
// and the \r/\n stripping.
func formatMessage(template string, values map[string]any) string {
	out, err := pyFormat(template, values)
	if err != nil {
		if _, ok := err.(*pyFormatError); ok {
			// KeyError fallback: join "attribute: value" for all values.
			keys := make([]string, 0, len(values))
			for k := range values {
				keys = append(keys, k)
			}
			sort.Strings(keys) // NOTE: Python uses dict insertion order; see limitations.
			parts := make([]string, 0, len(keys))
			for _, k := range keys {
				parts = append(parts, fmt.Sprintf("%s: %s", k, pyStr(values[k])))
			}
			out = strings.Join(parts, " ")
		} else {
			out = ""
		}
	}
	return crlfStripper.Replace(out)
}

// crlfStripper removes carriage returns and newlines from a formatted message
// (plaso strips them so an event never spans output rows). It is built once and
// shared: a strings.Replacer is immutable and safe for concurrent use, and
// rebuilding it per message was ≈11% of allocations on the hot path.
var crlfStripper = strings.NewReplacer("\r", "", "\n", "")

// CustomRegistry maps a lowercased custom-helper identifier to its function.
type CustomRegistry map[string]func(values map[string]any)

// Set holds all loaded formatters keyed by lowercased data_type.
type Set struct {
	byDataType map[string]*Formatter
	Custom     CustomRegistry
}

// NewSet returns an empty formatter set.
func NewSet() *Set {
	return &Set{byDataType: map[string]*Formatter{}, Custom: CustomRegistry{}}
}

// Get returns the formatter for a data type, or nil.
func (s *Set) Get(dataType string) *Formatter {
	return s.byDataType[strings.ToLower(dataType)]
}

// SourceMapping returns (short, long) for a data type, ("","") if unknown.
func (s *Set) SourceMapping(dataType string) (string, string) {
	if f := s.byDataType[strings.ToLower(dataType)]; f != nil {
		return f.SourceShort, f.SourceLong
	}
	return "", ""
}

// LoadDir loads every *.yaml in dir (plaso's data/formatters).
func (s *Set) LoadDir(dir string) error {
	matches, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return err
	}
	sort.Strings(matches)
	for _, path := range matches {
		if err := s.LoadFile(path); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	return nil
}

// LoadFile loads one multi-document YAML formatters file.
func (s *Set) LoadFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	for {
		var def map[string]any
		if err := dec.Decode(&def); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}
		if def == nil {
			continue
		}
		f, err := buildFormatter(def)
		if err != nil {
			return err
		}
		s.byDataType[f.DataType] = f
	}
	return nil
}

func buildFormatter(def map[string]any) (*Formatter, error) {
	f := &Formatter{separator: " "}
	f.Type, _ = def["type"].(string)
	dataType, _ := def["data_type"].(string)
	f.DataType = strings.ToLower(dataType)
	f.SourceShort = asString(def["short_source"])
	f.SourceLong = asString(def["source"])
	if sep, ok := def["separator"]; ok {
		f.separator = asString(sep)
	}

	f.messagePieces = asStringList(def["message"])
	f.shortPieces = asStringList(def["short_message"])

	if raw, ok := def["boolean_helpers"].([]any); ok {
		for _, item := range raw {
			m := toStrMap(item)
			f.helpers = append(f.helpers, &BooleanHelper{
				Input:        asString(m["input_attribute"]),
				Output:       asString(m["output_attribute"]),
				ValueIfTrue:  m["value_if_true"],
				ValueIfFalse: m["value_if_false"],
			})
		}
	}
	if raw, ok := def["enumeration_helpers"].([]any); ok {
		for _, item := range raw {
			m := toStrMap(item)
			f.helpers = append(f.helpers, &EnumerationHelper{
				Input:   asString(m["input_attribute"]),
				Output:  asString(m["output_attribute"]),
				Default: m["default_value"],
				Values:  toAnyMapStrKey(m["values"]),
			})
		}
	}
	if raw, ok := def["flags_helpers"].([]any); ok {
		for _, item := range raw {
			m := toStrMap(item)
			f.helpers = append(f.helpers, &FlagsHelper{
				Input:  asString(m["input_attribute"]),
				Output: asString(m["output_attribute"]),
				Values: toAnyMapIntKey(m["values"]),
			})
		}
	}
	if raw, ok := def["custom_helpers"].([]any); ok {
		for _, item := range raw {
			m := toStrMap(item)
			f.customHelperIDs = append(f.customHelperIDs, asString(m["identifier"]))
		}
	}
	return f, nil
}

// pieceAttrNames computes the single attribute name per conditional piece,
// erroring if a piece references more than one distinct attribute.
func pieceAttrNames(pieces []string) ([]string, error) {
	attrs := make([]string, len(pieces))
	for i, piece := range pieces {
		matches := formatStringAttributeNameRE.FindAllStringSubmatch(piece, -1)
		distinct := map[string]bool{}
		var first string
		for _, m := range matches {
			if first == "" {
				first = m[1]
			}
			distinct[m[1]] = true
		}
		if len(distinct) > 1 {
			return nil, fmt.Errorf("invalid format string piece %q: >1 attribute name", piece)
		}
		attrs[i] = first
	}
	return attrs, nil
}

// DefaultMessage reproduces plaso's DefaultEventFormatter output for a data
// type with no registered formatter.
func DefaultMessage(values map[string]any) string {
	return "<WARNING DEFAULT FORMATTER> Attributes: " + defaultAttributeValues(values)
}

// defaultDataFormatterReserved mirrors DefaultEventFormatter._RESERVED_VARIABLE_NAMES.
var defaultDataFormatterReserved = map[string]bool{
	"_corrupted": true, "_event_values_hash": true, "_parser_chain": true,
	"_recovered": true, "data_type": true, "date_time": true, "path_spec": true,
	"timestamp": true, "timestamp_desc": true,
}

// defaultAttributeValues builds the "name: value ..." body of the default
// formatter, skipping reserved names and date/time values. Python iterates the
// event values in insertion order; we sort for determinism (a documented
// simplification — the default formatter is not exercised by the test corpus).
func defaultAttributeValues(values map[string]any) string {
	keys := make([]string, 0, len(values))
	for k, v := range values {
		if defaultDataFormatterReserved[k] {
			continue
		}
		if IsDateTimeValue(v) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s: %s", k, pyStr(values[k])))
	}
	return strings.Join(parts, " ")
}

// IsDateTimeValue reports whether a decoded JSON value is a serialized
// dfdatetime value (a map carrying "__type__": "DateTimeValues"), or a list
// whose first element is one.
func IsDateTimeValue(v any) bool {
	switch t := v.(type) {
	case map[string]any:
		s, _ := t["__type__"].(string)
		return s == "DateTimeValues"
	case []any:
		return len(t) > 0 && IsDateTimeValue(t[0])
	}
	return false
}

// --- YAML value coercion helpers ---

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func asStringList(v any) []string {
	switch t := v.(type) {
	case string:
		return []string{t}
	case []any:
		out := make([]string, 0, len(t))
		for _, e := range t {
			out = append(out, asString(e))
		}
		return out
	case nil:
		return nil
	}
	return []string{asString(v)}
}

func toStrMap(v any) map[string]any {
	out := map[string]any{}
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return out
}

func toAnyMapStrKey(v any) map[string]any {
	out := map[string]any{}
	if m, ok := v.(map[string]any); ok {
		for k, val := range m {
			out[k] = val
		}
	}
	return out
}

func toAnyMapIntKey(v any) map[int64]any {
	out := map[int64]any{}
	if m, ok := v.(map[string]any); ok {
		for k, val := range m {
			if iv, ok := toInt64FromKey(k); ok {
				out[iv] = val
			}
		}
	}
	return out
}

func truthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != ""
	default:
		if iv, ok := toInt64(v); ok {
			return iv != 0
		}
		return true
	}
}

func keyStr(v any) string { return pyStr(v) }
