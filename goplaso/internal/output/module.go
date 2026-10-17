// Package output implements psort output modules: the per-event field
// formatting (parallel-safe Prepare) and the serial write path (Sink), mirroring
// plaso/output/{interface,text_file,shared_dsv,dynamic,l2t_csv,tln,rawpy,null,
// shared_json,json_line,json_out}.py.
package output

import (
	"bufio"
	"sort"
	"strings"

	"github.com/Get-Sybers/plaso/goplaso/internal/container"
)

// Unit is one event reduced to what the serial write stage needs: the ordering
// keys, the rendered output text and (for MACB-collapsing modules) the raw
// field values. It is produced by Module.Prepare in a worker.
type Unit struct {
	TS         int64
	Hash       string
	Desc       string
	Line       string // rendered output text for a single event
	SortKey    string // full textual sort key (sorted modules only)
	PrimaryKey string // primary (first) sort-key field value (sorted modules only)
	Fields     *DSVFields
}

// DSVFields holds the ordered raw (pre-sanitization) field values of one event,
// retained only by l2tcsv so a MACB group can be collapsed into one row.
type DSVFields struct {
	names []string
	raw   []string
}

// Module is an output module: a per-event formatter and a serial sink factory.
type Module interface {
	// Prepare performs the heavy per-event formatting; safe for concurrent use.
	Prepare(ev *container.Event) *Unit
	// NewSink returns the serial writer for one export run.
	NewSink(bw *bufio.Writer) Sink
}

// Sink writes units to the output in dedup/MACB emission order.
type Sink interface {
	Header() error
	Single(u *Unit) error
	Group(members []*Unit) error
	Footer() error
}

// hashOf returns the event_values_hash used for ordering and dedup, mirroring
// plaso's missing-hash fallback.
func hashOf(ev *container.Event) string {
	if ev.EventData != nil && ev.EventData.EventValuesHash != "" {
		return ev.EventData.EventValuesHash
	}
	return "UNKNOWN"
}

// sanitizeDSV mirrors DSVEventFormattingHelper._SanitizeField. The whitespace
// fold (delimiter/CR/LF → space) is done in one pass by a pre-built replacer:
// all three map distinct single bytes to a space, and a space is never itself a
// match target, so one pass is byte-identical to three sequential ReplaceAlls.
func sanitizeDSV(field string, folder *strings.Replacer) string {
	field = folder.Replace(field)
	if len(field) > 1 {
		switch field[0] {
		case '=', '+', '-', '@':
			field = "'" + field
		}
	}
	return field
}

// --- sorted sink (SortedTextFileOutputModule) --------------------------------

const maxHeapStrings = 100000

type heapEntry struct {
	sortKey string
	line    string
}

// sortedSink buffers single-event lines in a heap and flushes them, sorted by
// (sortKey, line), whenever the primary sort key changes or the heap fills —
// mirroring text_file.SortedTextFileOutputModule. MACB-collapsing modules
// (l2tcsv) write group rows immediately via collapse, bypassing the heap, which
// is exactly how plaso's WriteFieldValuesOfMACBGroup override behaves.
type sortedSink struct {
	bw     *bufio.Writer
	header string
	// collapse renders a MACB group into one row; nil expands the group into
	// its members (each written through the heap).
	collapse func(members []*Unit) *Unit

	heap        []heapEntry
	lastPrimary string
	started     bool
}

func (s *sortedSink) Header() error {
	if s.header == "" {
		return nil
	}
	_, err := s.bw.WriteString(s.header + "\n")
	return err
}

func (s *sortedSink) flush() error {
	sort.Slice(s.heap, func(i, j int) bool {
		if s.heap[i].sortKey != s.heap[j].sortKey {
			return s.heap[i].sortKey < s.heap[j].sortKey
		}
		return s.heap[i].line < s.heap[j].line
	})
	for _, e := range s.heap {
		if _, err := s.bw.WriteString(e.line); err != nil {
			return err
		}
	}
	s.heap = s.heap[:0]
	s.started = false
	return nil
}

func (s *sortedSink) Single(u *Unit) error {
	if !s.started {
		s.lastPrimary = u.PrimaryKey
		s.started = true
	}
	if u.PrimaryKey != s.lastPrimary || len(s.heap) >= maxHeapStrings {
		if err := s.flush(); err != nil {
			return err
		}
		s.lastPrimary = u.PrimaryKey
		s.started = true
	}
	s.heap = append(s.heap, heapEntry{sortKey: u.SortKey, line: u.Line})
	return nil
}

func (s *sortedSink) Group(members []*Unit) error {
	if s.collapse != nil {
		u := s.collapse(members)
		_, err := s.bw.WriteString(u.Line)
		return err
	}
	for _, m := range members {
		if err := s.Single(m); err != nil {
			return err
		}
	}
	return nil
}

func (s *sortedSink) Footer() error { return s.flush() }

// --- direct sink (TextFileOutputModule) --------------------------------------

// directSink writes each unit's line immediately in emission order (no textual
// re-sort): rawpy and json_line.
type directSink struct {
	bw     *bufio.Writer
	header string
	footer string
}

func (s *directSink) Header() error {
	if s.header == "" {
		return nil
	}
	_, err := s.bw.WriteString(s.header)
	return err
}

func (s *directSink) Single(u *Unit) error {
	_, err := s.bw.WriteString(u.Line)
	return err
}

func (s *directSink) Group(members []*Unit) error {
	for _, m := range members {
		if err := s.Single(m); err != nil {
			return err
		}
	}
	return nil
}

func (s *directSink) Footer() error {
	if s.footer == "" {
		return nil
	}
	_, err := s.bw.WriteString(s.footer)
	return err
}

// --- null sink ---------------------------------------------------------------

type nullSink struct{}

func (nullSink) Header() error       { return nil }
func (nullSink) Single(*Unit) error  { return nil }
func (nullSink) Group([]*Unit) error { return nil }
func (nullSink) Footer() error       { return nil }
