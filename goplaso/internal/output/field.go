package output

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Get-Sybers/plaso/goplaso/internal/container"
	"github.com/Get-Sybers/plaso/goplaso/internal/dfpath"
	"github.com/Get-Sybers/plaso/goplaso/internal/dftime"
	"github.com/Get-Sybers/plaso/goplaso/internal/formatters"
	"github.com/Get-Sybers/plaso/goplaso/internal/mediator"
)

// macbDescriptions is the exact set of timestamp descriptions that make an event
// eligible for MACB grouping (plaso/multi_process/output_engine.py).
var macbDescriptions = map[string]bool{
	"Last Access Time":           true,
	"Creation Time":              true,
	"Metadata Modification Time": true,
	"Content Modification Time":  true,
}

// IsMACBEligible reports whether a timestamp description makes an event part of
// a MACB group.
func IsMACBEligible(desc string) bool { return macbDescriptions[desc] }

// FieldComputer computes output field values for an event, mirroring plaso's
// FieldFormattingHelper callbacks across the dynamic, l2tcsv and tln field sets.
// It is read-only after construction and safe for concurrent use.
type FieldComputer struct {
	med *mediator.Mediator
}

// NewFieldComputer returns a field computer bound to a mediator.
func NewFieldComputer(med *mediator.Mediator) *FieldComputer {
	return &FieldComputer{med: med}
}

// fieldFn is a field callback bound to a FieldComputer.
type fieldFn func(c *FieldComputer, ev *container.Event) string

func (c *FieldComputer) loc() *time.Location {
	if c.med.TimeZone != nil {
		return c.med.TimeZone
	}
	return time.UTC
}

func dataType(ed *container.EventData) string {
	if ed == nil || ed.DataType == "" {
		return "-"
	}
	return ed.DataType
}

// datetime mirrors _FormatDateTime (non-dynamic-time): the displayed microsecond
// timestamp rendered as ISO 8601 with offset, with the 0-timestamp sentinel.
func (c *FieldComputer) datetime(ev *container.Event) string {
	if ev.Timestamp == 0 {
		return "0000-00-00T00:00:00.000000+00:00"
	}
	micros := dftime.DisplayMicros(ev.DateTime, ev.Timestamp)
	if micros == 0 {
		return "0000-00-00T00:00:00.000000+00:00"
	}
	return dftime.ISO8601Micros(micros, c.loc())
}

// dateISO mirrors DynamicFieldFormattingHelper._FormatDate ("YYYY-MM-DD").
func (c *FieldComputer) dateISO(ev *container.Event) string {
	if ev.Timestamp == 0 {
		return "0000-00-00"
	}
	seconds, ok := dftime.PosixSecondsForDate(ev.DateTime, ev.Timestamp)
	if !ok {
		return "0000-00-00"
	}
	return dftime.FormatDate(seconds)
}

// dateL2T mirrors L2TCSVFieldFormattingHelper._FormatDate ("MM/DD/YYYY").
func (c *FieldComputer) dateL2T(ev *container.Event) string {
	if ev.Timestamp == 0 {
		return "00/00/0000"
	}
	seconds, ok := dftime.PosixSecondsForDate(ev.DateTime, ev.Timestamp)
	if !ok {
		return "00/00/0000"
	}
	return dftime.FormatL2TDate(seconds)
}

// timeField mirrors _FormatTime ("HH:MM:SS").
func (c *FieldComputer) timeField(ev *container.Event) string {
	if ev.Timestamp == 0 {
		return "--:--:--"
	}
	seconds, ok := dftime.PosixSecondsForDate(ev.DateTime, ev.Timestamp)
	if !ok {
		return "--:--:--"
	}
	return dftime.FormatTimeOfDay(seconds)
}

// timezone mirrors _FormatTimeZone.
func (c *FieldComputer) timezone(ev *container.Event) string {
	if ev.Timestamp == 0 {
		return "-"
	}
	loc := c.loc()
	if loc == time.UTC {
		return "UTC"
	}
	seconds, ok := dftime.PosixSecondsForDate(ev.DateTime, ev.Timestamp)
	if !ok {
		return "-"
	}
	name, _ := time.Unix(seconds, 0).In(loc).Zone()
	return name
}

// timestampSeconds mirrors TLNFieldFormattingHelper._FormatTimestamp.
func (c *FieldComputer) timestampSeconds(ev *container.Event) string {
	if ev.DateTime == nil {
		return intToString(floorDivMicros(ev.Timestamp))
	}
	seconds, ok := dftime.CopyToPosixTimestamp(ev.DateTime, ev.Timestamp)
	if !ok {
		seconds = 0
	}
	return intToString(seconds)
}

func (c *FieldComputer) timestampDesc(ev *container.Event) string {
	if ev.TimestampDesc != "" {
		return ev.TimestampDesc
	}
	return "-"
}

func (c *FieldComputer) sourceShort(ev *container.Event) string {
	short, _ := c.med.GetSourceMapping(dataType(ev.EventData))
	if short != "" {
		return short
	}
	return "N/A"
}

func (c *FieldComputer) sourceLong(ev *container.Event) string {
	_, long := c.med.GetSourceMapping(dataType(ev.EventData))
	if long != "" {
		return long
	}
	return "N/A"
}

func (c *FieldComputer) parser(ev *container.Event) string {
	if ed := ev.EventData; ed != nil && ed.ParserChain != "" {
		return ed.ParserChain
	}
	return "-"
}

func (c *FieldComputer) tag(ev *container.Event) string {
	if len(ev.Tags) > 0 {
		return strings.Join(ev.Tags, " ")
	}
	return "-"
}

func (c *FieldComputer) hostname(ev *container.Event) string {
	return c.med.GetHostname(ev.EventData, "-")
}

func (c *FieldComputer) username(ev *container.Event) string {
	return c.med.GetUsername(ev.EventData, "-")
}

func (c *FieldComputer) macb(ev *container.Event) string {
	return c.med.GetMACBRepresentation(ev, ev.EventData)
}

func (c *FieldComputer) version(ev *container.Event) string { return "2" }

func (c *FieldComputer) displayName(ev *container.Event) string {
	ed := ev.EventData
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
}

// filename mirrors _FormatFilename with use_fallback_path_spec disabled.
func (c *FieldComputer) filename(ev *container.Event) string {
	if ed := ev.EventData; ed != nil {
		if fn := ed.StringAttr("filename"); fn != "" {
			return fn
		}
	}
	return "-"
}

func (c *FieldComputer) inode(ev *container.Event) string {
	ed := ev.EventData
	if ed == nil {
		return "-"
	}
	if v, ok := ed.Attr("inode"); ok && v != nil {
		if s, ok := v.(string); ok {
			return s
		}
		return formatters.PyStr(v)
	}
	return "-"
}

func (c *FieldComputer) message(ev *container.Event) string {
	ed := ev.EventData
	if ed == nil {
		return "-"
	}
	values := ed.CopyToDict()
	formatter := c.med.GetMessageFormatter(ed.DataType)
	if formatter == nil {
		return formatters.DefaultMessage(values)
	}
	formatter.FormatEventValues(values, c.med.Formatters.Custom)
	return formatter.GetMessage(values)
}

func (c *FieldComputer) messageShort(ev *container.Event) string {
	ed := ev.EventData
	if ed == nil {
		return "-"
	}
	values := ed.CopyToDict()
	formatter := c.med.GetMessageFormatter(ed.DataType)
	if formatter == nil {
		return formatters.DefaultMessageShort(values)
	}
	formatter.FormatEventValues(values, c.med.Formatters.Custom)
	return formatter.GetMessageShort(values)
}

// description mirrors TLNFieldFormattingHelper._FormatDescription.
func (c *FieldComputer) description(ev *container.Event) string {
	dt := c.datetime(ev)
	desc := ev.TimestampDesc
	if desc == "" {
		desc = "UNKNOWN"
	}
	msg := strings.ReplaceAll(c.message(ev), ";", " ")
	return dt + "; " + desc + "; " + msg
}

// notes mirrors TLNFieldFormattingHelper._FormatNotes.
func (c *FieldComputer) notes(ev *container.Event) string {
	inode := c.inode(ev)
	if ed := ev.EventData; ed != nil {
		if n := ed.StringAttr("notes"); n != "" {
			return n
		}
	}
	notes := "File: " + c.displayName(ev)
	if inode != "-" {
		notes = notes + " inode: " + inode
	}
	return notes
}

// extraAttributes mirrors L2TCSVFieldFormattingHelper._FormatExtraAttributes.
func (c *FieldComputer) extraAttributes(ev *container.Event) string {
	ed := ev.EventData
	if ed == nil {
		return ""
	}
	formatted := map[string]bool{}
	if formatter := c.med.GetMessageFormatter(ed.DataType); formatter != nil {
		for name := range formatter.GetFormatStringAttributeNames() {
			formatted[name] = true
		}
	}
	for name := range l2tReservedVariableNames {
		formatted[name] = true
	}

	var extra []string
	for _, kv := range eventDataAttributes(ed) {
		if formatted[kv.name] {
			continue
		}
		extra = append(extra, kv.name+": "+formatters.PyStr(kv.value))
	}
	if ed.Stream != nil {
		for _, kv := range eventDataStreamAttributes(ed.Stream) {
			if kv.name == "path_spec" {
				continue
			}
			extra = append(extra, kv.name+": "+formatters.PyStr(kv.value))
		}
	}
	sort.Strings(extra)
	joined := strings.Join(extra, "; ")
	joined = strings.ReplaceAll(joined, "\n", "-")
	joined = strings.ReplaceAll(joined, "\r", "")
	return joined
}

func intToString(v int64) string {
	return strconv.FormatInt(v, 10)
}

func floorDivMicros(ts int64) int64 {
	q := ts / 1_000_000
	if (ts%1_000_000 != 0) && (ts < 0) {
		q--
	}
	return q
}

// l2tReservedVariableNames mirrors L2TCSVFieldFormattingHelper._RESERVED_VARIABLE_NAMES.
var l2tReservedVariableNames = map[string]bool{
	"_corrupted": true, "_event_values_hash": true, "_parser_chain": true,
	"_recovered": true, "body": true, "data_type": true, "date_time": true,
	"display_name": true, "filename": true, "hostname": true, "http_headers": true,
	"inode": true, "mapped_files": true, "metadata": true, "offset": true,
	"path_spec": true, "query": true, "source_long": true, "source_short": true,
	"tag": true, "timestamp": true, "timestamp_desc": true, "timezone": true,
	"username": true,
}
