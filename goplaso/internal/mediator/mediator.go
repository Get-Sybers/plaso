// Package mediator mediates between the output modules and the loaded
// formatters / configuration, mirroring plaso/output/mediator.py for the
// subset the dynamic output module needs (message formatters, source mapping,
// output time zone, dynamic-time flag).
package mediator

import (
	"time"

	"github.com/Get-Sybers/plaso/goplaso/internal/formatters"
)

// Mediator holds the shared formatting state for an export run.
type Mediator struct {
	Formatters  *formatters.Set
	TimeZone    *time.Location
	DynamicTime bool
}

// New creates a mediator with the given formatter set, defaulting to UTC.
func New(f *formatters.Set) *Mediator {
	return &Mediator{Formatters: f, TimeZone: time.UTC}
}

// GetMessageFormatter returns the formatter for a data type, or nil.
func (m *Mediator) GetMessageFormatter(dataType string) *formatters.Formatter {
	return m.Formatters.Get(dataType)
}

// GetSourceMapping returns the (short, long) source for a data type.
func (m *Mediator) GetSourceMapping(dataType string) (string, string) {
	return m.Formatters.SourceMapping(dataType)
}
