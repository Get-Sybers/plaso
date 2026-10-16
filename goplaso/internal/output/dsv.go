package output

import (
	"bufio"
	"strings"

	"github.com/Get-Sybers/plaso/goplaso/internal/container"
	"github.com/Get-Sybers/plaso/goplaso/internal/mediator"
)

// DSV is the delimiter-separated-values output module family: dynamic, l2tcsv,
// tln and l2ttln. Each is this machinery configured with a field list, a
// delimiter, a header and the within-timestamp textual sort fields; l2tcsv
// additionally collapses MACB groups into a single row.
type DSV struct {
	fc       *FieldComputer
	names    []string
	fns      []fieldFn
	delim    string
	header   string
	sortIdx  []int
	collapse bool
	jsonl    bool // dynamic-as-JSONL deliverable (not a plaso module)
}

type fieldDef struct {
	name string
	fn   fieldFn
}

func newDSV(fc *FieldComputer, defs []fieldDef, delim, header string, sortNames []string) *DSV {
	d := &DSV{fc: fc, delim: delim, header: header}
	d.names = make([]string, len(defs))
	d.fns = make([]fieldFn, len(defs))
	for i, def := range defs {
		d.names[i] = def.name
		d.fns[i] = def.fn
	}
	for _, sn := range sortNames {
		idx := -1
		for i, n := range d.names {
			if n == sn {
				idx = i
				break
			}
		}
		d.sortIdx = append(d.sortIdx, idx)
	}
	return d
}

// Prepare computes the field values, the output line and the sort keys.
func (d *DSV) Prepare(ev *container.Event) *Unit {
	raw := make([]string, len(d.fns))
	san := make([]string, len(d.fns))
	for i, fn := range d.fns {
		raw[i] = fn(d.fc, ev)
		san[i] = sanitizeDSV(raw[i], d.delim)
	}

	u := &Unit{
		TS:   ev.Timestamp,
		Hash: hashOf(ev),
		Desc: ev.TimestampDesc,
	}
	if d.jsonl {
		u.Line = jsonlObject(d.names, raw)
	} else {
		u.Line = strings.Join(san, d.delim) + "\n"
	}

	parts := make([]string, len(d.sortIdx))
	for i, idx := range d.sortIdx {
		if idx >= 0 {
			parts[i] = san[idx]
		}
	}
	u.SortKey = strings.Join(parts, " ")
	if len(parts) > 0 {
		u.PrimaryKey = parts[0]
	}
	if d.collapse {
		u.Fields = &DSVFields{names: d.names, raw: raw}
	}
	return u
}

// NewSink returns the serial writer for this DSV module.
func (d *DSV) NewSink(bw *bufio.Writer) Sink {
	s := &sortedSink{bw: bw, header: d.header}
	if d.collapse {
		s.collapse = d.collapseGroup
	}
	return s
}

// collapseGroup renders a MACB group as a single l2tcsv row, mirroring
// L2TCSVEventFormattingHelper.GetFormattedMACBGroup: the MACB field is the ORed
// representation of the members' descriptions, the type field joins them with
// "; ", and every other field is taken from the first member.
func (d *DSV) collapseGroup(members []*Unit) *Unit {
	descs := make([]string, len(members))
	for i, m := range members {
		descs[i] = m.Desc
	}
	first := members[0].Fields.raw
	san := make([]string, len(d.names))
	for i, name := range d.names {
		var raw string
		switch name {
		case "MACB":
			raw = d.fc.med.GetMACBRepresentationFromDescriptions(descs)
		case "type":
			raw = strings.Join(descs, "; ")
		default:
			raw = first[i]
		}
		san[i] = sanitizeDSV(raw, d.delim)
	}
	return &Unit{Line: strings.Join(san, d.delim) + "\n"}
}

// --- module constructors -----------------------------------------------------

// DynamicFieldNames is the dynamic module's default field order.
var DynamicFieldNames = []string{
	"datetime", "timestamp_desc", "source", "source_long",
	"message", "parser", "display_name", "tag",
}

func dynamicDefs(fc *FieldComputer) []fieldDef {
	return []fieldDef{
		{"datetime", (*FieldComputer).datetime},
		{"timestamp_desc", (*FieldComputer).timestampDesc},
		{"source", (*FieldComputer).sourceShort},
		{"source_long", (*FieldComputer).sourceLong},
		{"message", (*FieldComputer).message},
		{"parser", (*FieldComputer).parser},
		{"display_name", (*FieldComputer).displayName},
		{"tag", (*FieldComputer).tag},
	}
}

// NewDynamic returns the dynamic CSV output module.
func NewDynamic(med *mediator.Mediator) *DSV {
	fc := NewFieldComputer(med)
	return newDSV(fc, dynamicDefs(fc), ",", strings.Join(DynamicFieldNames, ","),
		[]string{"datetime", "display_name", "message"})
}

// NewDynamicJSONL returns the dynamic field set rendered as JSON lines.
func NewDynamicJSONL(med *mediator.Mediator) *DSV {
	fc := NewFieldComputer(med)
	d := newDSV(fc, dynamicDefs(fc), ",", "",
		[]string{"datetime", "display_name", "message"})
	d.jsonl = true
	return d
}

// NewL2TCSV returns the l2tcsv output module.
func NewL2TCSV(med *mediator.Mediator) *DSV {
	fc := NewFieldComputer(med)
	defs := []fieldDef{
		{"date", (*FieldComputer).dateL2T},
		{"time", (*FieldComputer).timeField},
		{"timezone", (*FieldComputer).timezone},
		{"MACB", (*FieldComputer).macb},
		{"source", (*FieldComputer).sourceShort},
		{"sourcetype", (*FieldComputer).sourceLong},
		{"type", (*FieldComputer).timestampDesc},
		{"user", (*FieldComputer).username},
		{"host", (*FieldComputer).hostname},
		{"short", (*FieldComputer).messageShort},
		{"desc", (*FieldComputer).message},
		{"version", (*FieldComputer).version},
		{"filename", (*FieldComputer).displayName},
		{"inode", (*FieldComputer).inode},
		{"notes", (*FieldComputer).tag},
		{"format", (*FieldComputer).parser},
		{"extra", (*FieldComputer).extraAttributes},
	}
	names := make([]string, len(defs))
	for i, def := range defs {
		names[i] = def.name
	}
	d := newDSV(fc, defs, ",", strings.Join(names, ","),
		[]string{"time", "filename", "inode"})
	d.collapse = true
	return d
}

// NewTLN returns the tln output module.
func NewTLN(med *mediator.Mediator) *DSV {
	fc := NewFieldComputer(med)
	defs := []fieldDef{
		{"time", (*FieldComputer).timestampSeconds},
		{"source", (*FieldComputer).sourceShort},
		{"host", (*FieldComputer).hostname},
		{"user", (*FieldComputer).username},
		{"description", (*FieldComputer).description},
	}
	return newDSV(fc, defs, "|", "Time|Source|Host|User|Description", []string{"time"})
}

// NewL2TTLN returns the l2ttln output module.
func NewL2TTLN(med *mediator.Mediator) *DSV {
	fc := NewFieldComputer(med)
	defs := []fieldDef{
		{"time", (*FieldComputer).timestampSeconds},
		{"source", (*FieldComputer).sourceShort},
		{"host", (*FieldComputer).hostname},
		{"user", (*FieldComputer).username},
		{"description", (*FieldComputer).description},
		{"tz", (*FieldComputer).timezone},
		{"notes", (*FieldComputer).notes},
	}
	return newDSV(fc, defs, "|", "Time|Source|Host|User|Description|TZ|Notes", []string{"time"})
}
