// Package pipeline implements the psort export loop: it reads events in
// timestamp order, time-orders and deduplicates them within each equal-
// timestamp bucket, applies the dynamic module's within-bucket textual
// sub-sort, and writes each surviving event. Mirrors the serial core of
// plaso/multi_process/output_engine.py (PsortEventHeap, _FlushExportBuffer)
// and the SortedStringHeap in plaso/output/text_file.py.
//
// The expensive per-event work (event_data blob inflate + JSON decode, and
// message formatting) is independent across events, so it can run on a bounded
// worker pool while the ordered reader and the serial dedup/sort/write stage
// preserve byte-identical output.
package pipeline

import (
	"bufio"
	"io"
	"runtime"
	"sort"

	"github.com/Get-Sybers/plaso/goplaso/internal/container"
	"github.com/Get-Sybers/plaso/goplaso/internal/output"
	"github.com/Get-Sybers/plaso/goplaso/internal/plasostore"
)

// Mode selects the serialization of the dynamic field set.
type Mode int

const (
	// ModeCSV emits the native dynamic CSV (byte-compatible with Python psort).
	ModeCSV Mode = iota
	// ModeJSONL emits one JSON object per row ("dynamic as JSONL").
	ModeJSONL
)

// Exporter drives a single export.
type Exporter struct {
	Reader  *plasostore.Reader
	Dynamic *output.Dynamic
	Mode    Mode
	Dedup   bool
	// Workers bounds the decode/format worker pool. 0 selects runtime.NumCPU();
	// 1 runs the fully serial path.
	Workers int
}

// processed is a hydrated event with its computed output record.
type processed struct {
	ev  *container.Event
	rec *output.Record
}

// Export writes all events to w.
func (e *Exporter) Export(w io.Writer) error {
	bw := bufio.NewWriter(w)
	defer bw.Flush()

	if e.Mode == ModeCSV {
		if _, err := bw.WriteString(output.CSVHeader()); err != nil {
			return err
		}
	}

	s := &sink{bw: bw, mode: e.Mode, dedup: e.Dedup, dyn: e.Dynamic}

	workers := e.Workers
	if workers == 0 {
		workers = runtime.NumCPU()
	}

	// Decode every event_data blob up front, in parallel at the chosen width
	// (this is the dominant per-event cost). With workers==1 this is a serial
	// decode, giving a meaningful serial-vs-parallel comparison.
	if err := e.Reader.PreloadEventData(workers); err != nil {
		return err
	}

	var err error
	if workers <= 1 {
		err = e.exportSerial(s)
	} else {
		err = e.exportParallel(s, workers)
	}
	if err != nil {
		return err
	}
	if err := s.finish(); err != nil {
		return err
	}
	return bw.Flush()
}

// exportSerial hydrates and formats each event inline, in order.
func (e *Exporter) exportSerial(s *sink) error {
	return e.Reader.StreamRaw(func(raw plasostore.RawEvent) error {
		ev, err := e.Reader.Hydrate(raw)
		if err != nil {
			return err
		}
		return s.add(processed{ev: ev, rec: e.Dynamic.Compute(ev)})
	})
}

type procResult struct {
	ev  *container.Event
	rec *output.Record
	err error
}

// exportParallel hydrates and formats events on a bounded worker pool while a
// queue of per-event result channels preserves the reader's timestamp order.
func (e *Exporter) exportParallel(s *sink, workers int) error {
	sem := make(chan struct{}, workers)
	futures := make(chan chan procResult, workers*2)

	var produceErr error
	go func() {
		defer close(futures)
		produceErr = e.Reader.StreamRaw(func(raw plasostore.RawEvent) error {
			ch := make(chan procResult, 1)
			futures <- ch
			sem <- struct{}{}
			go func(raw plasostore.RawEvent, ch chan procResult) {
				defer func() { <-sem }()
				ev, err := e.Reader.Hydrate(raw)
				if err != nil {
					ch <- procResult{err: err}
					return
				}
				ch <- procResult{ev: ev, rec: e.Dynamic.Compute(ev)}
			}(raw, ch)
			return nil
		})
	}()

	var firstErr error
	for ch := range futures {
		r := <-ch
		if firstErr != nil {
			continue // keep draining so workers finish and the producer unblocks
		}
		if r.err != nil {
			firstErr = r.err
			continue
		}
		if err := s.add(processed{ev: r.ev, rec: r.rec}); err != nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return firstErr
	}
	return produceErr
}

// sink accumulates processed events into equal-timestamp buckets and flushes
// each bucket (dedup + within-bucket textual sub-sort) to the writer.
type sink struct {
	bw    *bufio.Writer
	mode  Mode
	dedup bool
	dyn   *output.Dynamic

	bucket   []processed
	bucketTS int64
	started  bool
}

func (s *sink) add(p processed) error {
	if s.started && p.ev.Timestamp != s.bucketTS {
		if err := s.flush(); err != nil {
			return err
		}
		s.bucket = s.bucket[:0]
	}
	s.bucketTS = p.ev.Timestamp
	s.started = true
	s.bucket = append(s.bucket, p)
	return nil
}

func (s *sink) finish() error {
	if s.started {
		return s.flush()
	}
	return nil
}

// flush processes one equal-timestamp bucket.
func (s *sink) flush() error {
	bucket := s.bucket

	// Primary within-bucket order: (event_values_hash, timestamp_desc), ties
	// broken by identifier for determinism (plaso breaks them on duplicate
	// content, which is then discarded anyway).
	sort.SliceStable(bucket, func(i, j int) bool {
		a, b := bucket[i].ev, bucket[j].ev
		ha, hb := hashOf(a), hashOf(b)
		if ha != hb {
			return ha < hb
		}
		if a.TimestampDesc != b.TimestampDesc {
			return a.TimestampDesc < b.TimestampDesc
		}
		return a.Identifier < b.Identifier
	})

	// Deduplicate against the immediately-preceding kept event (same
	// timestamp_desc and event_values_hash).
	survivors := bucket[:0:0]
	var lastDesc, lastHash string
	hasPrev := false
	for _, p := range bucket {
		h := hashOf(p.ev)
		if s.dedup && hasPrev && p.ev.TimestampDesc == lastDesc && h == lastHash {
			continue
		}
		survivors = append(survivors, p)
		lastDesc, lastHash = p.ev.TimestampDesc, h
		hasPrev = true
	}

	// Dynamic module textual sub-sort: ascending by (sort key, output line),
	// matching the SortedStringHeap.
	sort.SliceStable(survivors, func(i, j int) bool {
		ki, kj := survivors[i].rec.SortKey(), survivors[j].rec.SortKey()
		if ki != kj {
			return ki < kj
		}
		return survivors[i].rec.CSVLine() < survivors[j].rec.CSVLine()
	})

	for _, p := range survivors {
		var err error
		if s.mode == ModeCSV {
			_, err = s.bw.WriteString(p.rec.CSVLine())
		} else {
			_, err = s.bw.WriteString(p.rec.JSONLine())
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// hashOf returns the event_values_hash used for ordering and dedup, mirroring
// plaso's missing-hash fallback.
func hashOf(ev *container.Event) string {
	if ev.EventData != nil && ev.EventData.EventValuesHash != "" {
		return ev.EventData.EventValuesHash
	}
	return "UNKNOWN"
}
