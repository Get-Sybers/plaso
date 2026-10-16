// Package output implements psort output modules. The first slice provides the
// dynamic module's field set, emittable either as its native delimiter-
// separated values (CSV, byte-compatible with Python psort — the differential
// test oracle) or as JSON lines (the port's deliverable: "dynamic as JSONL").
package output

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"

	"github.com/Get-Sybers/plaso/goplaso/internal/container"
	"github.com/Get-Sybers/plaso/goplaso/internal/dfpath"
	"github.com/Get-Sybers/plaso/goplaso/internal/dftime"
	"github.com/Get-Sybers/plaso/goplaso/internal/formatters"
	"github.com/Get-Sybers/plaso/goplaso/internal/mediator"
)

// DynamicFieldNames is the dynamic module's default field order.
var DynamicFieldNames = []string{
	"datetime", "timestamp_desc", "source", "source_long",
	"message", "parser", "display_name", "tag",
}

// dynamicSortKeyFields is the within-timestamp textual sub-sort key.
var dynamicSortKeyFields = []string{"datetime", "display_name", "message"}

// Record holds one event's computed field values, both raw (for JSON output)
// and DSV-sanitized (for CSV output and the textual sub-sort key).
type Record struct {
	Names []string
	Raw   map[string]string
	San   map[string]string
}

// Dynamic computes the dynamic module's fields for events.
type Dynamic struct {
	med *mediator.Mediator
}

// NewDynamic returns a dynamic field computer.
func NewDynamic(med *mediator.Mediator) *Dynamic { return &Dynamic{med: med} }

// FieldNames returns the output field names in order.
func (d *Dynamic) FieldNames() []string { return DynamicFieldNames }

// Compute produces the field record for an event.
func (d *Dynamic) Compute(ev *container.Event) *Record {
	rec := &Record{
		Names: DynamicFieldNames,
		Raw:   make(map[string]string, len(DynamicFieldNames)),
		San:   make(map[string]string, len(DynamicFieldNames)),
	}
	for _, name := range DynamicFieldNames {
		raw := d.field(name, ev)
		if raw == "" && name != "message" {
			// plaso: a None field value becomes the "-" placeholder. Our
			// callbacks already return "-"/"N/A" for the None cases; message
			// is the one field allowed to be legitimately empty.
		}
		rec.Raw[name] = raw
		rec.San[name] = sanitizeDSV(raw, ",")
	}
	return rec
}

// field dispatches a single dynamic field (mirrors DynamicFieldFormattingHelper).
func (d *Dynamic) field(name string, ev *container.Event) string {
	ed := ev.EventData
	switch name {
	case "datetime":
		return d.formatDateTime(ev)
	case "timestamp_desc":
		if ev.TimestampDesc != "" {
			return ev.TimestampDesc
		}
		return "-"
	case "source":
		short, _ := d.med.GetSourceMapping(dataType(ed))
		if short != "" {
			return short
		}
		return "N/A"
	case "source_long":
		_, long := d.med.GetSourceMapping(dataType(ed))
		if long != "" {
			return long
		}
		return "N/A"
	case "message":
		return d.formatMessage(ed)
	case "parser":
		if ed != nil && ed.ParserChain != "" {
			return ed.ParserChain
		}
		return "-"
	case "display_name":
		if ed != nil {
			if dn := ed.StringAttr("display_name"); dn != "" {
				return dn
			}
			if ed.Stream != nil && ed.Stream.PathSpec != "" {
				if ps, err := dfpath.Parse(ed.Stream.PathSpec); err == nil && ps != nil {
					return ps.DisplayName()
				}
			}
		}
		return "-"
	case "tag":
		if len(ev.Tags) > 0 {
			return strings.Join(ev.Tags, " ")
		}
		return "-"
	}
	return "-"
}

func dataType(ed *container.EventData) string {
	if ed == nil || ed.DataType == "" {
		return "-"
	}
	return ed.DataType
}

// formatMessage mirrors FieldFormattingHelper._FormatMessage.
func (d *Dynamic) formatMessage(ed *container.EventData) string {
	if ed == nil {
		return "-"
	}
	values := ed.CopyToDict()
	formatter := d.med.GetMessageFormatter(ed.DataType)
	if formatter == nil {
		return formatters.DefaultMessage(values)
	}
	formatter.FormatEventValues(values, d.med.Formatters.Custom)
	return formatter.GetMessage(values)
}

// formatDateTime mirrors FieldFormattingHelper._FormatDateTime for the
// non-dynamic-time path. For the first slice this renders the stored
// microsecond timestamp in the output time zone as ISO8601.
func (d *Dynamic) formatDateTime(ev *container.Event) string {
	micros := dftime.DisplayMicros(ev.DateTime, ev.Timestamp)
	if micros == 0 {
		return "0000-00-00T00:00:00.000000+00:00"
	}
	loc := d.med.TimeZone
	if loc == nil {
		loc = time.UTC
	}
	t := time.UnixMicro(micros).In(loc)
	return t.Format("2006-01-02T15:04:05.000000-07:00")
}

// SortKey returns the DSV-sanitized within-timestamp sort key.
func (r *Record) SortKey() string {
	parts := make([]string, len(dynamicSortKeyFields))
	for i, f := range dynamicSortKeyFields {
		parts[i] = r.San[f]
	}
	return strings.Join(parts, " ")
}

// CSVLine renders the record as a Python-psort-compatible dynamic CSV line.
func (r *Record) CSVLine() string {
	vals := make([]string, len(r.Names))
	for i, n := range r.Names {
		vals[i] = r.San[n]
	}
	return strings.Join(vals, ",") + "\n"
}

// JSONLine renders the record as one JSON object (raw values, field order
// preserved) followed by a newline — the "dynamic as JSONL" deliverable.
func (r *Record) JSONLine() string {
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range r.Names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(jsonString(n))
		b.WriteByte(':')
		b.WriteString(jsonString(r.Raw[n]))
	}
	b.WriteString("}\n")
	return b.String()
}

// CSVHeader is the dynamic CSV header line.
func CSVHeader() string { return strings.Join(DynamicFieldNames, ",") + "\n" }

// sanitizeDSV mirrors DSVEventFormattingHelper._SanitizeField.
func sanitizeDSV(field, delim string) string {
	if delim != "" {
		field = strings.ReplaceAll(field, delim, " ")
	}
	field = strings.ReplaceAll(field, "\r", " ")
	field = strings.ReplaceAll(field, "\n", " ")
	if len(field) > 1 {
		switch field[0] {
		case '=', '+', '-', '@':
			field = "'" + field
		}
	}
	return field
}

// jsonString encodes s as a JSON string without HTML escaping, preserving UTF-8.
func jsonString(s string) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	return strings.TrimRight(b.String(), "\n")
}
