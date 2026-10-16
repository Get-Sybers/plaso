// Package mediator mediates between the output modules and the loaded
// formatters / configuration, mirroring plaso/output/mediator.py for the
// subset the output modules need (message formatters, source mapping, hostname,
// username, MACB representation, output time zone, dynamic-time flag).
package mediator

import (
	"strings"
	"time"

	"github.com/Get-Sybers/plaso/goplaso/internal/container"
	"github.com/Get-Sybers/plaso/goplaso/internal/formatters"
)

// Time description constants (plaso/lib/definitions.py).
const (
	descLastAccess           = "Last Access Time"
	descCreation             = "Creation Time"
	descMetadataModification = "Metadata Modification Time"
	descModification         = "Content Modification Time"
	descAccountCreated       = "Account Created"
	descLastVisited          = "Last Visited Time"
	descStart                = "Start Time"
	descLastShutdown         = "Last Shutdown Time"
	descLastLogin            = "Last Login Time"
	descLastPasswordReset    = "Last Password Reset Time"
	descLastConnected        = "Last Connection Time"
	descLastRun              = "Last Time Executed"
	descLastPrinted          = "Last Printed Time"
	descWritten              = descModification
	descDeleted              = "Content Deletion Time"
	descAdded                = "Added Time"
	descFileDownloaded       = "File Downloaded Time"
	descFirstConnected       = "First Connection Time"
)

// Mediator holds the shared formatting state for an export run.
type Mediator struct {
	Formatters  *formatters.Set
	TimeZone    *time.Location
	DynamicTime bool

	// UseFallbackHostname / UseFallbackPathSpec mirror the processing
	// configuration; both default to false for psort.
	UseFallbackHostname bool
	UseFallbackPathSpec bool
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

// GetHostname returns the hostname related to the event data, mirroring
// OutputMediator.GetHostname. The fallback-hostname path (system configuration)
// is not populated for psort's default configuration.
func (m *Mediator) GetHostname(ed *container.EventData, defaultHostname string) string {
	if ed != nil {
		if h := ed.StringAttr("hostname"); h != "" {
			return h
		}
	}
	return defaultHostname
}

// GetUsername returns the username related to the event data, mirroring
// OutputMediator.GetUsername. The user_sid → user_account resolution has no data
// to resolve against in psort's default corpus and falls back to the default.
func (m *Mediator) GetUsername(ed *container.EventData, defaultUsername string) string {
	if ed != nil {
		if u := ed.StringAttr("username"); u != "" && u != "-" {
			return u
		}
	}
	return defaultUsername
}

// GetMACBRepresentation mirrors OutputMediator.GetMACBRepresentation for a
// single event (used when an event is not part of a MACB group).
func (m *Mediator) GetMACBRepresentation(ev *container.Event, ed *container.EventData) string {
	dataType := ""
	if ed != nil {
		dataType = ed.DataType
	}
	if dataType == "" {
		return "...."
	}

	if dataType == "fs:stat" {
		chars := []byte{'.', '.', '.', '.'}
		for _, d := range strings.Split(ev.TimestampDesc, ";") {
			switch d {
			case "mtime", descModification:
				chars[0] = 'M'
			case "atime", descLastAccess:
				chars[1] = 'A'
			case "ctime", descMetadataModification:
				chars[2] = 'C'
			case "crtime", descCreation:
				chars[3] = 'B'
			}
		}
		return string(chars)
	}

	switch ev.TimestampDesc {
	case descLastAccess, descAccountCreated, descLastVisited, descStart,
		descLastShutdown, descLastLogin, descLastPasswordReset, descLastConnected,
		descLastRun, descLastPrinted:
		return ".A.."
	case descModification, descDeleted: // descWritten aliases descModification
		return "M..."
	case descCreation, descAdded, descFileDownloaded, descFirstConnected:
		return "...B"
	case descMetadataModification:
		return "..C."
	}
	return "...."
}

// GetMACBRepresentationFromDescriptions mirrors
// OutputMediator.GetMACBRepresentationFromDescriptions, used for MACB groups.
func (m *Mediator) GetMACBRepresentationFromDescriptions(descriptions []string) string {
	has := func(short, full string) bool {
		for _, d := range descriptions {
			if d == short || d == full {
				return true
			}
		}
		return false
	}
	var b strings.Builder
	if has("mtime", descModification) {
		b.WriteByte('M')
	} else {
		b.WriteByte('.')
	}
	if has("atime", descLastAccess) {
		b.WriteByte('A')
	} else {
		b.WriteByte('.')
	}
	if has("ctime", descMetadataModification) {
		b.WriteByte('C')
	} else {
		b.WriteByte('.')
	}
	if has("crtime", descCreation) {
		b.WriteByte('B')
	} else {
		b.WriteByte('.')
	}
	return b.String()
}
