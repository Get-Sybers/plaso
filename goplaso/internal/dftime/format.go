package dftime

import (
	"fmt"
	"strings"
	"time"

	"github.com/Get-Sybers/plaso/goplaso/internal/container"
)

// ISO8601Micros renders a microsecond POSIX timestamp as an ISO 8601 string with
// a numeric offset in loc, e.g. "2012-01-22T07:52:33.000000+00:00". This mirrors
// datetime.isoformat() spliced with a forced 6-digit microsecond field, the form
// FieldFormattingHelper._FormatDateTime emits in non-dynamic-time mode.
func ISO8601Micros(micros int64, loc *time.Location) string {
	if loc == nil {
		loc = time.UTC
	}
	t := time.UnixMicro(micros).In(loc)
	return t.Format("2006-01-02T15:04:05.000000-07:00")
}

// ISO8601Dynamic renders the dynamic-time datetime field, mirroring the
// dynamic-time branch of _FormatDateTime: the date_time's native-precision
// ISO 8601 string, converted to loc by splicing the time-zone-converted
// date/time and offset around the native fractional seconds. Returns "Invalid"
// when the value cannot be rendered, and the semantic "string" when present.
func ISO8601Dynamic(dt *container.DateTimeValues, stored int64, loc *time.Location) string {
	if loc == nil {
		loc = time.UTC
	}
	if dt != nil {
		if s, ok := dt.Raw["string"].(string); ok && s != "" {
			return s
		}
	}

	// Local-time coercion: a local date_time is rendered from the stored POSIX
	// timestamp (as seconds when precision is "1s", otherwise microseconds),
	// mirroring _FormatDateTime.
	if dt != nil && isLocal(dt) {
		sec := floorDiv(stored, 1_000_000)
		var iso string
		if precisionIs1s(dt) {
			iso = isoSeconds(sec, "+00:00")
		} else {
			iso = isoFromMicros(stored, 6, 0)
		}
		if loc != time.UTC {
			frac := iso[19 : len(iso)-6]
			isoTz := time.Unix(sec, 0).In(loc).Format("2006-01-02T15:04:05-07:00")
			iso = isoTz[:19] + frac + isoTz[19:]
		}
		return iso
	}

	iso, ok := copyToISO8601(dt, stored)
	if !ok {
		return "Invalid"
	}
	if strings.HasSuffix(iso, "Z") {
		iso = iso[:len(iso)-1] + "+00:00"
	}

	if loc != time.UTC || timeZoneOffsetMinutes(dt) != 0 {
		if len(iso) < 25 {
			return "Invalid"
		}
		seconds, ok := CopyToPosixTimestamp(dt, stored)
		if !ok {
			return "Invalid"
		}
		frac := iso[19 : len(iso)-6]
		isoTz := time.Unix(seconds, 0).In(loc).Format("2006-01-02T15:04:05-07:00")
		iso = isoTz[:19] + frac + isoTz[19:]
	}
	return iso
}

// copyToISO8601 mirrors dfdatetime CopyToDateTimeStringISO8601 for the classes in
// the psort corpus (TimeElements and the POSIX families) plus a microsecond
// fallback. The returned string is UTC-based (offset "+00:00" unless the value
// carries its own time zone offset).
func copyToISO8601(dt *container.DateTimeValues, stored int64) (string, bool) {
	if dt == nil {
		return isoFromMicros(stored, 6, stored%1_000_000), true
	}
	switch dt.ClassName {
	case "TimeElements":
		if len(dt.TimeElementsTuple) >= 6 {
			te := dt.TimeElementsTuple
			return fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d%s",
				te[0], te[1], te[2], te[3], te[4], te[5], offsetSuffix(dt)), true
		}
	case "PosixTime":
		if ts, ok := numToInt64(dt.Timestamp); ok {
			return isoSeconds(ts, offsetSuffix(dt)), true
		}
	case "PosixTimeInMilliseconds":
		if ts, ok := numToInt64(dt.Timestamp); ok {
			return isoFraction(ts, 1_000, 3, dt), true
		}
	case "PosixTimeInMicroseconds":
		if ts, ok := numToInt64(dt.Timestamp); ok {
			return isoFraction(ts, 1_000_000, 6, dt), true
		}
	case "PosixTimeInNanoseconds":
		if ts, ok := numToInt64(dt.Timestamp); ok {
			return isoFraction(ts, 1_000_000_000, 9, dt), true
		}
	}
	// Fallback: render the stored microsecond timestamp.
	return isoFromMicros(stored, 6, 0), true
}

func isoSeconds(seconds int64, offset string) string {
	t := time.Unix(seconds, 0).UTC()
	return t.Format("2006-01-02T15:04:05") + offset
}

// isoFraction renders a POSIX timestamp expressed in unitsPerSecond into an
// ISO 8601 string with a fraction of the given digit width.
func isoFraction(ts, unitsPerSecond int64, digits int, dt *container.DateTimeValues) string {
	seconds := floorDiv(ts, unitsPerSecond)
	frac := ts - seconds*unitsPerSecond
	t := time.Unix(seconds, 0).UTC()
	return fmt.Sprintf("%s.%0*d%s", t.Format("2006-01-02T15:04:05"), digits, frac, offsetSuffix(dt))
}

func isoFromMicros(micros int64, digits int, _ int64) string {
	seconds := floorDiv(micros, 1_000_000)
	frac := micros - seconds*1_000_000
	t := time.Unix(seconds, 0).UTC()
	return fmt.Sprintf("%s.%0*d+00:00", t.Format("2006-01-02T15:04:05"), digits, frac)
}

// offsetSuffix renders the ±HH:MM offset from a date_time's time_zone_offset
// (minutes); "+00:00" when unset or zero.
func offsetSuffix(dt *container.DateTimeValues) string {
	m := timeZoneOffsetMinutes(dt)
	if m == 0 {
		return "+00:00"
	}
	sign := "+"
	if m < 0 {
		sign = "-"
		m = -m
	}
	return fmt.Sprintf("%s%02d:%02d", sign, m/60, m%60)
}

func precisionIs1s(dt *container.DateTimeValues) bool {
	if dt == nil {
		return false
	}
	p, _ := dt.Raw["precision"].(string)
	return p == "1s"
}

func timeZoneOffsetMinutes(dt *container.DateTimeValues) int {
	if dt == nil || dt.TimeZoneOffset == nil {
		return 0
	}
	return *dt.TimeZoneOffset
}

// PosixSecondsForDate returns the whole POSIX seconds used by the date/time
// field callbacks, mirroring _FormatDate/_FormatTime: when the date_time is
// absent or local the stored microsecond timestamp is used, otherwise the
// date_time's own POSIX timestamp (floored to the second).
func PosixSecondsForDate(dt *container.DateTimeValues, stored int64) (int64, bool) {
	if dt == nil || isLocal(dt) {
		return floorDiv(stored, 1_000_000), true
	}
	return CopyToPosixTimestamp(dt, stored)
}

// CopyToPosixTimestamp returns the date_time value as whole POSIX seconds,
// mirroring dfdatetime CopyToPosixTimestamp (floored). stored is used only when
// the date_time cannot be interpreted. ok is false for semantic/invalid times.
func CopyToPosixTimestamp(dt *container.DateTimeValues, stored int64) (int64, bool) {
	if dt == nil {
		return floorDiv(stored, 1_000_000), true
	}
	switch dt.ClassName {
	case "PosixTime", "PosixTimeInMilliseconds", "PosixTimeInMicroseconds", "PosixTimeInNanoseconds":
		ts, ok := numToInt64(dt.Timestamp)
		if !ok {
			return 0, false
		}
		return floorDiv(ts, posixUnitsPerSecond(dt.ClassName)), true
	case "TimeElements", "TimeElementsInMilliseconds", "TimeElementsInMicroseconds", "TimeElementsInNanoseconds":
		if len(dt.TimeElementsTuple) >= 6 {
			te := dt.TimeElementsTuple
			t := time.Date(te[0], time.Month(te[1]), te[2], te[3], te[4], te[5], 0, time.UTC)
			return t.Unix(), true
		}
		return floorDiv(stored, 1_000_000), true
	default:
		return floorDiv(stored, 1_000_000), true
	}
}

// FormatDate renders a date field as "YYYY-MM-DD" (dynamic) for a POSIX second
// in loc.
func FormatDate(seconds int64, loc *time.Location) string {
	y, m, d, _, _, _ := components(seconds, loc)
	return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
}

// FormatL2TDate renders a date field as "MM/DD/YYYY" (l2tcsv) for a POSIX second
// in loc.
func FormatL2TDate(seconds int64, loc *time.Location) string {
	y, m, d, _, _, _ := components(seconds, loc)
	return fmt.Sprintf("%02d/%02d/%04d", m, d, y)
}

// FormatTimeOfDay renders a time field as "HH:MM:SS" for a POSIX second in loc.
func FormatTimeOfDay(seconds int64, loc *time.Location) string {
	_, _, _, h, mi, s := components(seconds, loc)
	return fmt.Sprintf("%02d:%02d:%02d", h, mi, s)
}

// TimeZoneName returns the time zone abbreviation for a POSIX second in loc,
// mirroring tz.tzname(datetime); UTC is rendered as "UTC".
func TimeZoneName(seconds int64, loc *time.Location) string {
	if loc == nil || loc == time.UTC {
		return "UTC"
	}
	name, _ := time.Unix(seconds, 0).In(loc).Zone()
	return name
}

func components(seconds int64, loc *time.Location) (year, month, day, hour, minute, sec int) {
	if loc == nil {
		loc = time.UTC
	}
	t := time.Unix(seconds, 0).In(loc)
	return t.Year(), int(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second()
}

func posixUnitsPerSecond(className string) int64 {
	switch className {
	case "PosixTimeInNanoseconds":
		return 1_000_000_000
	case "PosixTimeInMicroseconds":
		return 1_000_000
	case "PosixTimeInMilliseconds":
		return 1_000
	default:
		return 1
	}
}

// floorDiv divides flooring toward negative infinity (Python // semantics).
func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}
