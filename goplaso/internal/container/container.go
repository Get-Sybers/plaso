// Package container defines the in-memory plaso attribute-container model
// (events, event data, event data streams, event tags) and the dfDateTime
// value representation, mirroring plaso/containers and dfdatetime.
package container

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	// goccy/go-json decodes ~2.4x faster than encoding/json for the attribute-
	// container blobs (the export hot path) and is a drop-in: its UseNumber
	// yields encoding/json.Number, so large nanosecond timestamps stay exact.
	gojson "github.com/goccy/go-json"
)

// Identifier is a plaso attribute-container identifier of the form
// "<container_type>.<sequence_number>", e.g. "event_data.2".
type Identifier struct {
	ContainerType  string
	SequenceNumber int64
}

// ParseIdentifier parses a serialized identifier such as "event_data.2".
// It returns ok=false for the empty string.
func ParseIdentifier(s string) (Identifier, bool) {
	if s == "" {
		return Identifier{}, false
	}
	dot := strings.LastIndexByte(s, '.')
	if dot <= 0 || dot == len(s)-1 {
		return Identifier{}, false
	}
	seq, err := strconv.ParseInt(s[dot+1:], 10, 64)
	if err != nil {
		return Identifier{}, false
	}
	return Identifier{ContainerType: s[:dot], SequenceNumber: seq}, true
}

// Event is a row from the schema-based `event` table.
type Event struct {
	Identifier          int64
	EventDataIdentifier string // raw ref, e.g. "event_data.2"
	DateTime            *DateTimeValues
	Timestamp           int64 // microseconds since 1970-01-01 UTC; the sort key
	TimestampDesc       string

	// Populated by the reader when joining related containers.
	EventData *EventData
	Tags      []string
}

// EventData is a decoded row from the `event_data` BLOB table.
type EventData struct {
	Identifier                int64
	EventDataStreamIdentifier string // raw ref, e.g. "event_data_stream.1"
	ParserChain               string // _parser_chain
	DataType                  string // data_type
	EventValuesHash           string // _event_values_hash (dedup/MACB key)
	// Attributes holds every event-data attribute other than the reserved
	// __type__/__container_type__/_parser_chain/data_type keys. Values keep
	// JSON fidelity: numbers are json.Number, nested dfdatetime objects are
	// map[string]any, etc.
	Attributes map[string]any

	// Populated by the reader when joining.
	Stream *EventDataStream
}

// CopyToDict builds the event-values map the message formatters consume,
// mirroring plaso's EventData.CopyToDict: the public attributes plus the
// reserved keys the formatters and their KeyError fallback reference. The
// returned map is a fresh copy so formatter helpers may mutate it safely.
func (e *EventData) CopyToDict() map[string]any {
	values := make(map[string]any, len(e.Attributes)+4)
	for k, v := range e.Attributes {
		if v != nil {
			values[k] = v
		}
	}
	if e.DataType != "" {
		values["data_type"] = e.DataType
	}
	if e.ParserChain != "" {
		values["_parser_chain"] = e.ParserChain
	}
	if e.EventValuesHash != "" {
		values["_event_values_hash"] = e.EventValuesHash
	}
	if e.EventDataStreamIdentifier != "" {
		values["_event_data_stream_identifier"] = e.EventDataStreamIdentifier
	}
	return values
}

// Attr returns an attribute value and whether it was present.
func (e *EventData) Attr(name string) (any, bool) {
	v, ok := e.Attributes[name]
	return v, ok
}

// StringAttr returns a string attribute, or "" if absent/non-string.
func (e *EventData) StringAttr(name string) string {
	if v, ok := e.Attributes[name]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// EventDataStream is a row from the schema-based `event_data_stream` table.
type EventDataStream struct {
	Identifier  int64
	PathSpec    string
	MD5Hash     string
	SHA1Hash    string
	SHA256Hash  string
	FileEntropy string
	YaraMatch   string

	// ParsedPathSpec caches the decoded PathSpec, parsed once by the reader when
	// the stream is loaded (before any worker goroutine exists) and shared
	// read-only across workers. Typed as any to avoid a container→dfpath import
	// cycle; it holds a *dfpath.PathSpec (nil when PathSpec is empty).
	ParsedPathSpec any
}

// DateTimeValues is a decoded dfdatetime value. ClassName discriminates the
// concrete dfdatetime subclass; Raw preserves the full decoded JSON object so
// no information is lost for subclasses not yet modeled explicitly.
type DateTimeValues struct {
	ClassName         string
	TimeElementsTuple []int          // TimeElements family
	Timestamp         *json.Number   // Posix*/Filetime families (kept lossless)
	TimeZoneOffset    *int           // minutes
	Raw               map[string]any // full decoded object
}

// ParseDateTime decodes a dfdatetime JSON object (the `date_time` column, or a
// nested attribute). It returns nil for empty input.
func ParseDateTime(raw map[string]any) *DateTimeValues {
	if raw == nil {
		return nil
	}
	dt := &DateTimeValues{Raw: raw}
	if s, ok := raw["__class_name__"].(string); ok {
		dt.ClassName = s
	}
	if t, ok := raw["time_elements_tuple"].([]any); ok {
		dt.TimeElementsTuple = make([]int, 0, len(t))
		for _, e := range t {
			dt.TimeElementsTuple = append(dt.TimeElementsTuple, toInt(e))
		}
	}
	if n, ok := raw["timestamp"].(json.Number); ok {
		dt.Timestamp = &n
	}
	if v, ok := raw["time_zone_offset"]; ok {
		off := toInt(v)
		dt.TimeZoneOffset = &off
	}
	return dt
}

func toInt(v any) int {
	switch n := v.(type) {
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	case float64:
		return int(n)
	case int:
		return n
	}
	return 0
}

// DecodeJSONObject unmarshals a JSON object using json.Number for all numbers,
// so large integers (e.g. nanosecond timestamps exceeding 2^53) keep full
// precision instead of being coerced to float64.
func DecodeJSONObject(data []byte) (map[string]any, error) {
	return DecodeJSONObjectFrom(bytes.NewReader(data))
}

// DecodeJSONObjectFrom decodes a JSON object directly from a reader (json.Number
// preserved). Decoding straight from the zlib stream avoids buffering the whole
// decompressed blob a second time (the decoder keeps its own read buffer), which
// removed the io.ReadAll allocation from the per-event decode hot path.
func DecodeJSONObjectFrom(r io.Reader) (map[string]any, error) {
	dec := gojson.NewDecoder(r)
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("decode json object: %w", err)
	}
	return m, nil
}

// DecodeJSONArray unmarshals a JSON array using json.Number for all numbers.
func DecodeJSONArray(data []byte) ([]any, error) {
	dec := gojson.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var a []any
	if err := dec.Decode(&a); err != nil {
		return nil, fmt.Errorf("decode json array: %w", err)
	}
	return a, nil
}
