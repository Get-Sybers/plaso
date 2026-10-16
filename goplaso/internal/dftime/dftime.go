// Package dftime implements the subset of dfdatetime semantics psort's output
// needs. In particular it reproduces plaso's _FormatDateTime rule that the
// displayed microsecond timestamp is recomputed from the event's date_time
// value (rounding the sub-second fraction UP), which can differ by one
// microsecond from the stored, round-half-up `timestamp` column.
package dftime

import (
	"encoding/json"
	"math"

	"github.com/Get-Sybers/plaso/goplaso/internal/container"
)

// DisplayMicros returns the microsecond timestamp used to render the datetime
// field, mirroring FieldFormattingHelper._FormatDateTime (non-dynamic-time).
// stored is the event's `timestamp` column, used when date_time is absent,
// local, or of a class whose stored value already matches.
func DisplayMicros(dt *container.DateTimeValues, stored int64) int64 {
	if dt == nil || isLocal(dt) {
		return stored
	}
	switch dt.ClassName {
	case "PosixTimeInNanoseconds":
		ts, ok := numToInt64(dt.Timestamp)
		if !ok {
			return stored
		}
		return posixFractionMicros(ts, 1_000_000_000)
	case "PosixTimeInMicroseconds":
		ts, ok := numToInt64(dt.Timestamp)
		if !ok {
			return stored
		}
		return posixFractionMicros(ts, 1_000_000)
	default:
		// TimeElements and second-precision classes: the stored microsecond
		// column already equals the recomputed value. (Full dfdatetime class
		// coverage is a later milestone.)
		return stored
	}
}

// posixFractionMicros replicates the Python math: split a POSIX timestamp
// expressed in subSecondUnits per second into whole seconds and a fractional
// part, scale the fraction to microseconds and round it UP (math.ceil).
func posixFractionMicros(ts, unitsPerSecond int64) int64 {
	seconds := ts / unitsPerSecond
	frac := ts - seconds*unitsPerSecond
	// Python floors toward negative infinity; emulate for negative values.
	if frac < 0 {
		seconds--
		frac += unitsPerSecond
	}
	micros := seconds * 1_000_000
	if frac != 0 {
		f := float64(frac)
		for f < 1e6 {
			f *= 10
		}
		for f >= 1e6 {
			f /= 10
		}
		if micros < 0 {
			micros -= int64(math.Ceil(f))
		} else {
			micros += int64(math.Ceil(f))
		}
	}
	return micros
}

func isLocal(dt *container.DateTimeValues) bool {
	if v, ok := dt.Raw["is_local_time"]; ok {
		b, _ := v.(bool)
		return b
	}
	return false
}

func numToInt64(n *json.Number) (int64, bool) {
	if n == nil {
		return 0, false
	}
	if i, err := n.Int64(); err == nil {
		return i, true
	}
	return 0, false
}
