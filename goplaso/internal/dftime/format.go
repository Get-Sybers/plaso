package dftime

import (
	"fmt"
	"time"

	"github.com/Get-Sybers/plaso/goplaso/internal/container"
)

// ISO8601Micros renders a microsecond POSIX timestamp as an ISO 8601 string with
// a numeric UTC/offset suffix, e.g. "2012-01-22T07:52:33.000000+00:00". This
// mirrors datetime.isoformat() spliced with a forced 6-digit microsecond field,
// the form FieldFormattingHelper._FormatDateTime emits in non-dynamic-time mode.
func ISO8601Micros(micros int64, loc *time.Location) string {
	if loc == nil {
		loc = time.UTC
	}
	t := time.UnixMicro(micros).In(loc)
	return t.Format("2006-01-02T15:04:05.000000-07:00")
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

// DateComponents returns the UTC calendar components (year, month, day, hour,
// minute, second) of a whole-second POSIX timestamp, as GetDateWithTimeOfDay
// returns for the UTC output time zone.
func DateComponents(seconds int64) (year, month, day, hour, minute, sec int) {
	t := time.Unix(seconds, 0).UTC()
	return t.Year(), int(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second()
}

// FormatDate renders a date field as "YYYY-MM-DD" (dynamic) for a POSIX second.
func FormatDate(seconds int64) string {
	y, m, d, _, _, _ := DateComponents(seconds)
	return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
}

// FormatL2TDate renders a date field as "MM/DD/YYYY" (l2tcsv) for a POSIX second.
func FormatL2TDate(seconds int64) string {
	y, m, d, _, _, _ := DateComponents(seconds)
	return fmt.Sprintf("%02d/%02d/%04d", m, d, y)
}

// FormatTimeOfDay renders a time field as "HH:MM:SS" for a POSIX second.
func FormatTimeOfDay(seconds int64) string {
	_, _, _, h, mi, s := DateComponents(seconds)
	return fmt.Sprintf("%02d:%02d:%02d", h, mi, s)
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
