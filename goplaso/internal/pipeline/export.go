// Package pipeline implements the psort export loop: it streams events in
// timestamp order (each paired with its event_data blob), decodes and formats
// them on a bounded worker pool, then time-orders, deduplicates, and writes
// each surviving event. Mirrors the serial core of
// plaso/multi_process/output_engine.py (PsortEventHeap, _FlushExportBuffer)
// and the SortedStringHeap in plaso/output/text_file.py.
//
// Events and their event_data are streamed and decoded on demand, then
// discarded once formatted, so the Go heap stays bounded regardless of store
// size: only the in-flight worker window and the current equal-timestamp bucket
// are held, never every decoded event.
package pipeline

import (
	"bufio"
	"io"
	"runtime"
	"sort"
	"sync"

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

// processed is one event reduced to just what the ordering/dedup/write stage
// needs: precomputed strings and the dedup/sort keys. The output line and sort
// key are built here (in the worker), so the serial sink only compares and
// writes them; the heavy decoded EventData and Record are not retained.
type processed struct {
	sortKey string // within-bucket textual sub-sort key
	out     string // the exact line to write (CSV or JSON)
	hash    string
	desc    string
	id      int64
	ts      int64
}

func reduce(ev *container.Event, rec *output.Record, mode Mode) processed {
	out := rec.CSVLine()
	if mode == ModeJSONL {
		out = rec.JSONLine()
	}
	return processed{
		sortKey: rec.SortKey(),
		out:     out,
		hash:    hashOf(ev),
		desc:    ev.TimestampDesc,
		id:      ev.Identifier,
		ts:      ev.Timestamp,
	}
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

	s := &sink{bw: bw, dedup: e.Dedup}

	workers := e.Workers
	if workers == 0 {
		workers = runtime.NumCPU()
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
	return e.Reader.StreamRawEvents(func(raw plasostore.RawEvent, blob []byte) error {
		ev, err := e.Reader.Hydrate(raw, blob)
		if err != nil {
			return err
		}
		return s.add(reduce(ev, e.Dynamic.Compute(ev), e.Mode))
	})
}

// exportParallel decodes+formats events on a fixed worker pool. The reader
// streams jobs in timestamp order; workers process them concurrently and tag
// each result with its sequence number; a collector reassembles results into
// timestamp order (a small reorder buffer) before feeding the serial sink. A
// fixed pool with two channels avoids allocating a goroutine and channel per
// event, which otherwise swamps the scheduler on large stores.
func (e *Exporter) exportParallel(s *sink, workers int) error {
	type job struct {
		seq  int64
		raw  plasostore.RawEvent
		blob []byte
	}
	type res struct {
		seq int64
		p   processed
		err error
	}
	jobs := make(chan job, workers*4)
	results := make(chan res, workers*4)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				ev, err := e.Reader.Hydrate(j.raw, j.blob)
				if err != nil {
					results <- res{seq: j.seq, err: err}
					continue
				}
				results <- res{seq: j.seq, p: reduce(ev, e.Dynamic.Compute(ev), e.Mode)}
			}
		}()
	}

	var produceErr error
	go func() {
		defer close(jobs)
		var seq int64
		produceErr = e.Reader.StreamRawEvents(func(raw plasostore.RawEvent, blob []byte) error {
			jobs <- job{seq: seq, raw: raw, blob: blob}
			seq++
			return nil
		})
	}()

	go func() { wg.Wait(); close(results) }()

	// Reassemble into sequence order and feed the sink.
	pending := make(map[int64]processed)
	pendErr := make(map[int64]error)
	var next int64
	var firstErr error
	for r := range results {
		if r.err != nil {
			pendErr[r.seq] = r.err
		} else {
			pending[r.seq] = r.p
		}
		for {
			if err, ok := pendErr[next]; ok {
				delete(pendErr, next)
				if firstErr == nil {
					firstErr = err
				}
				next++
				continue
			}
			p, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			if firstErr == nil {
				if err := s.add(p); err != nil {
					firstErr = err
				}
			}
			next++
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
	dedup bool

	bucket   []processed
	bucketTS int64
	started  bool
}

func (s *sink) add(p processed) error {
	if s.started && p.ts != s.bucketTS {
		if err := s.flush(); err != nil {
			return err
		}
		s.bucket = s.bucket[:0]
	}
	s.bucketTS = p.ts
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
		a, b := bucket[i], bucket[j]
		if a.hash != b.hash {
			return a.hash < b.hash
		}
		if a.desc != b.desc {
			return a.desc < b.desc
		}
		return a.id < b.id
	})

	// Deduplicate against the immediately-preceding kept event (same
	// timestamp_desc and event_values_hash).
	survivors := bucket[:0:0]
	var lastDesc, lastHash string
	hasPrev := false
	for _, p := range bucket {
		if s.dedup && hasPrev && p.desc == lastDesc && p.hash == lastHash {
			continue
		}
		survivors = append(survivors, p)
		lastDesc, lastHash = p.desc, p.hash
		hasPrev = true
	}

	// Dynamic module textual sub-sort: ascending by (sort key, output line),
	// matching the SortedStringHeap. Keys were precomputed by the workers.
	sort.SliceStable(survivors, func(i, j int) bool {
		if survivors[i].sortKey != survivors[j].sortKey {
			return survivors[i].sortKey < survivors[j].sortKey
		}
		return survivors[i].out < survivors[j].out
	})

	for _, p := range survivors {
		if _, err := s.bw.WriteString(p.out); err != nil {
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
