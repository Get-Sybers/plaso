package output

import (
	"bufio"

	"github.com/Get-Sybers/plaso/goplaso/internal/container"
)

// Null is the no-op output module: it consumes the event stream but writes
// nothing (plaso/output/null.py).
type Null struct{}

// NewNull returns the null output module.
func NewNull() *Null { return &Null{} }

// Prepare records only the ordering keys; no formatting is performed.
func (m *Null) Prepare(ev *container.Event) *Unit {
	return &Unit{TS: ev.Timestamp, Hash: hashOf(ev), Desc: ev.TimestampDesc}
}

// NewSink returns a sink that discards everything.
func (m *Null) NewSink(bw *bufio.Writer) Sink { return nullSink{} }
