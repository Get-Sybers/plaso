package output

import (
	"bufio"
	"encoding/json"
	"strconv"

	"github.com/Get-Sybers/plaso/goplaso/internal/container"
	"github.com/Get-Sybers/plaso/goplaso/internal/mediator"
)

// JSONModule implements the json_line and json output modules
// (plaso/output/shared_json.py + json_line.py / json_out.py). Both serialize
// the full event attribute container; json_line writes one object per line and
// json wraps the objects in a single {"event_N": ...} object.
type JSONModule struct {
	fc     *FieldComputer
	framed bool // true = json (framed), false = json_line
}

// NewJSONLine returns the json_line output module.
func NewJSONLine(med *mediator.Mediator) *JSONModule {
	return &JSONModule{fc: NewFieldComputer(med), framed: false}
}

// NewJSON returns the json output module.
func NewJSON(med *mediator.Mediator) *JSONModule {
	return &JSONModule{fc: NewFieldComputer(med), framed: true}
}

// Prepare assembles the event's field-values object and encodes it as Python-
// exact JSON (sorted keys). The newline/framing is added by the sink.
func (m *JSONModule) Prepare(ev *container.Event) *Unit {
	fv := m.fieldValues(ev)
	return &Unit{
		TS:   ev.Timestamp,
		Hash: hashOf(ev),
		Desc: ev.TimestampDesc,
		Line: pyJSONEncode(fv),
	}
}

// NewSink returns the json_line (direct, newline-terminated) or json (framed)
// sink.
func (m *JSONModule) NewSink(bw *bufio.Writer) Sink {
	if m.framed {
		return &jsonSink{bw: bw}
	}
	return &jsonLineSink{bw: bw}
}

// fieldValues mirrors SharedJSONOutputModule.GetFieldValues.
func (m *JSONModule) fieldValues(ev *container.Event) map[string]any {
	ed := ev.EventData
	fv := map[string]any{
		"__container_type__": "event",
		"__type__":           "AttributeContainer",
	}

	if ed != nil {
		for _, a := range eventDataAttributes(ed) {
			name := a.name
			value := m.jsonFieldValue(name, a.value, ev)
			if name == "_parser_chain" {
				name = "parser"
			}
			fv[name] = value
		}
		if ed.Stream != nil {
			for _, a := range eventDataStreamAttributes(ed.Stream) {
				name := a.name
				value := a.value
				if name == "path_spec" {
					name = "pathspec"
					value = pathSpecDict(ed.Stream.PathSpec)
				}
				fv[name] = value
			}
		}
	}

	if ev.DateTime != nil {
		fv["date_time"] = serializeDateTime(ev.DateTime.Raw)
	}
	fv["timestamp"] = ev.Timestamp
	fv["timestamp_desc"] = ev.TimestampDesc

	if _, ok := fv["display_name"]; !ok {
		fv["display_name"] = m.fc.displayName(ev)
	}

	fv["message"] = m.fc.message(ev)

	if h := m.fc.med.GetHostname(ed, ""); h != "" {
		fv["hostname"] = h
	}
	if u := m.fc.med.GetUsername(ed, ""); u != "" {
		fv["username"] = u
	}

	if len(ev.Tags) > 0 {
		labels := make([]any, len(ev.Tags))
		for i, t := range ev.Tags {
			labels[i] = t
		}
		fv["tag"] = map[string]any{
			"__container_type__": "event_tag",
			"__type__":           "AttributeContainer",
			"labels":             labels,
		}
	}

	return fv
}

// jsonFieldValue mirrors JSONFieldFormattingHelper.GetFormattedField: the
// display_name/filename/inode/message/values fields use callbacks, everything
// else keeps its raw decoded value.
func (m *JSONModule) jsonFieldValue(name string, raw any, ev *container.Event) any {
	switch name {
	case "display_name":
		return m.fc.displayName(ev)
	case "filename":
		return m.fc.filename(ev)
	case "inode":
		return m.fc.inode(ev)
	case "message":
		return m.fc.message(ev)
	default:
		return raw
	}
}

// serializeDateTime mirrors dfdatetime's CopyToSerializableDict: it keeps the
// class metadata and payload but re-adds is_local_time, time_zone_offset,
// time_zone_hint and is_delta only when non-default (a stored time_zone_offset
// of 0 is dropped, as the serializer drops a falsy offset).
func serializeDateTime(raw map[string]any) map[string]any {
	if raw == nil {
		return nil
	}
	out := make(map[string]any, len(raw))
	for k, v := range raw {
		switch k {
		case "is_local_time", "time_zone_offset", "time_zone_hint", "is_delta":
			continue
		}
		out[k] = v
	}
	if b, ok := raw["is_local_time"].(bool); ok && b {
		out["is_local_time"] = true
	}
	if tzo, ok := raw["time_zone_offset"]; ok && numberNonZero(tzo) {
		out["time_zone_offset"] = tzo
	}
	if h, ok := raw["time_zone_hint"].(string); ok && h != "" {
		out["time_zone_hint"] = h
	}
	if b, ok := raw["is_delta"].(bool); ok && b {
		out["is_delta"] = true
	}
	return out
}

func numberNonZero(v any) bool {
	switch n := v.(type) {
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i != 0
		}
		if f, err := n.Float64(); err == nil {
			return f != 0
		}
	case float64:
		return n != 0
	case int64:
		return n != 0
	case int:
		return n != 0
	}
	return false
}

// pathSpecDict decodes a serialized path specification into the dict form the
// JSON serializer emits. For the flat specs psort reads, the stored form is the
// serialized form.
func pathSpecDict(serialized string) any {
	if serialized == "" {
		return nil
	}
	obj, err := container.DecodeJSONObject([]byte(serialized))
	if err != nil {
		return nil
	}
	return obj
}

// --- json_line sink ----------------------------------------------------------

type jsonLineSink struct {
	bw *bufio.Writer
}

func (s *jsonLineSink) Header() error { return nil }

func (s *jsonLineSink) Single(u *Unit) error {
	if _, err := s.bw.WriteString(u.Line); err != nil {
		return err
	}
	_, err := s.bw.WriteString("\n")
	return err
}

func (s *jsonLineSink) Group(members []*Unit) error {
	for _, u := range members {
		if err := s.Single(u); err != nil {
			return err
		}
	}
	return nil
}

func (s *jsonLineSink) Footer() error { return nil }

// --- json sink ---------------------------------------------------------------

type jsonSink struct {
	bw      *bufio.Writer
	counter int
}

func (s *jsonSink) Header() error {
	_, err := s.bw.WriteString("{")
	return err
}

func (s *jsonSink) Single(u *Unit) error {
	if s.counter != 0 {
		if _, err := s.bw.WriteString(", "); err != nil {
			return err
		}
	}
	if _, err := s.bw.WriteString(`"event_` + strconv.Itoa(s.counter) + `": ` + u.Line + "\n"); err != nil {
		return err
	}
	s.counter++
	return nil
}

func (s *jsonSink) Group(members []*Unit) error {
	for _, u := range members {
		if err := s.Single(u); err != nil {
			return err
		}
	}
	return nil
}

func (s *jsonSink) Footer() error {
	_, err := s.bw.WriteString("}")
	return err
}
