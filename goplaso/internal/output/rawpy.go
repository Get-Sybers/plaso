package output

import (
	"bufio"
	"sort"
	"strings"

	"github.com/Get-Sybers/plaso/goplaso/internal/container"
	"github.com/Get-Sybers/plaso/goplaso/internal/dfpath"
	"github.com/Get-Sybers/plaso/goplaso/internal/dftime"
	"github.com/Get-Sybers/plaso/goplaso/internal/formatters"
	"github.com/Get-Sybers/plaso/goplaso/internal/mediator"
)

// Rawpy is the native ("raw") Python output module (plaso/output/rawpy.py).
type Rawpy struct {
	fc *FieldComputer
}

// NewRawpy returns the rawpy output module.
func NewRawpy(med *mediator.Mediator) *Rawpy {
	return &Rawpy{fc: NewFieldComputer(med)}
}

// rawpyRowSeparator is the "+-"×40 record separator, built once.
var rawpyRowSeparator = strings.Repeat("+-", 40)

// rawpyReservedFields mirrors NativePythonOutputModule._RESERVED_FIELDS.
var rawpyReservedFields = map[string]bool{
	"body": true, "data_type": true, "date_time": true, "display_name": true,
	"hostname": true, "http_headers": true, "mapped_files": true, "metadata": true,
	"offset": true, "parser": true, "path_spec": true, "query": true,
	"source_long": true, "source_short": true, "tag": true, "timestamp": true,
	"timestamp_desc": true, "timezone": true, "username": true,
}

// NewSink returns a direct (unsorted) sink.
func (m *Rawpy) NewSink(bw *bufio.Writer) Sink { return &directSink{bw: bw} }

// Prepare builds the full native-Python text block for one event.
func (m *Rawpy) Prepare(ev *container.Event) *Unit {
	ed := ev.EventData

	fields := map[string]any{}
	var pathSpec *dfpath.PathSpec

	if ed != nil {
		attrs := eventDataAttributes(ed)
		if ed.Stream != nil {
			attrs = append(attrs, eventDataStreamAttributes(ed.Stream)...)
		}
		for _, kv := range attrs {
			name := kv.name
			if name == "path_spec" {
				if ps, ok := kv.value.(*dfpath.PathSpec); ok {
					pathSpec = ps
				}
				continue
			}
			if strings.HasPrefix(name, "_") && name != "_parser_chain" {
				continue
			}
			if name == "_parser_chain" {
				name = "parser"
			}
			fields[name] = kv.value
		}
	}
	// Generated display_name (use_fallback_path_spec is disabled for psort, so
	// only display_name is derived — not filename/inode).
	if _, ok := fields["display_name"]; !ok {
		fields["display_name"] = m.fc.displayName(ev)
	}

	timestampISO := dftime.ISO8601Micros(ev.Timestamp, m.fc.loc())

	var reserved, additional []string
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fieldString := "  {" + name + "} " + formatters.PyStr(fields[name])
		if rawpyReservedFields[name] {
			reserved = append(reserved, fieldString)
		} else {
			additional = append(additional, fieldString)
		}
	}

	lines := []string{rawpyRowSeparator, "[Timestamp]:", "  " + timestampISO}
	if pathSpec != nil {
		lines = append(lines, "", "[Pathspec]:")
		for _, l := range strings.Split(pathSpec.Comparable(), "\n") {
			lines = append(lines, "  "+l)
		}
		lines = lines[:len(lines)-1] // remove the trailing empty line
	}
	lines = append(lines, "", "[Reserved attributes]:")
	lines = append(lines, reserved...)
	lines = append(lines, "", "[Additional attributes]:")
	lines = append(lines, additional...)

	if len(ev.Tags) > 0 {
		quoted := make([]string, len(ev.Tags))
		for i, t := range ev.Tags {
			quoted[i] = "'" + t + "'"
		}
		lines = append(lines, "", "[Tag]:", "  {labels} ["+strings.Join(quoted, ", ")+"]")
	}
	lines = append(lines, "")

	return &Unit{
		TS:   ev.Timestamp,
		Hash: hashOf(ev),
		Desc: ev.TimestampDesc,
		Line: strings.Join(lines, "\n") + "\n",
	}
}
