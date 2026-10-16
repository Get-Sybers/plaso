// Package pipeline implements the psort export loop: it streams events in
// timestamp order (each paired with its event_data blob), decodes and formats
// them on a bounded worker pool, then time-orders, deduplicates and MACB-groups
// each surviving event before handing it to an output module's sink. Mirrors the
// serial core of plaso/multi_process/output_engine.py (PsortEventHeap,
// _FlushExportBuffer) and the output modules' WriteFieldValues[OfMACBGroup].
//
// Events and their event_data are streamed and decoded on demand, then
// discarded once formatted, so the Go heap stays bounded regardless of store
// size: only the in-flight worker window and the current equal-timestamp bucket
// are held, never every decoded event. Output is byte-identical at any worker
// count because the ordering/dedup/MACB/write stage is serial.
package pipeline

import (
	"bufio"
	"io"
	"runtime"
	"sort"
	"sync"

	"github.com/Get-Sybers/plaso/goplaso/internal/output"
	"github.com/Get-Sybers/plaso/goplaso/internal/plasostore"
)

// Exporter drives a single export through an output module.
type Exporter struct {
	Reader *plasostore.Reader
	Module output.Module
	Dedup  bool
	// Workers bounds the decode/format worker pool. 0 selects runtime.NumCPU();
	// 1 runs the fully serial path.
	Workers int
}

// Export writes all events to w.
func (e *Exporter) Export(w io.Writer) error {
	bw := bufio.NewWriter(w)
	defer bw.Flush()

	sink := e.Module.NewSink(bw)
	if err := sink.Header(); err != nil {
		return err
	}

	b := &bucketer{sink: sink, dedup: e.Dedup}

	workers := e.Workers
	if workers == 0 {
		workers = runtime.NumCPU()
	}

	var err error
	if workers <= 1 {
		err = e.exportSerial(b)
	} else {
		err = e.exportParallel(b, workers)
	}
	if err != nil {
		return err
	}
	if err := b.finish(); err != nil {
		return err
	}
	if err := sink.Footer(); err != nil {
		return err
	}
	return bw.Flush()
}

// exportSerial hydrates and formats each event inline, in order.
func (e *Exporter) exportSerial(b *bucketer) error {
	return e.Reader.StreamRawEvents(func(raw plasostore.RawEvent, blob []byte) error {
		ev, err := e.Reader.Hydrate(raw, blob)
		if err != nil {
			return err
		}
		return b.add(e.Module.Prepare(ev))
	})
}

// exportParallel decodes+formats events on a fixed worker pool. The reader
// streams jobs in timestamp order; workers process them concurrently and tag
// each result with its sequence number; a collector reassembles results into
// timestamp order (a small reorder buffer) before feeding the serial bucketer.
func (e *Exporter) exportParallel(b *bucketer, workers int) error {
	type job struct {
		seq  int64
		raw  plasostore.RawEvent
		blob []byte
	}
	type res struct {
		seq int64
		u   *output.Unit
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
				results <- res{seq: j.seq, u: e.Module.Prepare(ev)}
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

	pending := make(map[int64]*output.Unit)
	pendErr := make(map[int64]error)
	var next int64
	var firstErr error
	for r := range results {
		if r.err != nil {
			pendErr[r.seq] = r.err
		} else {
			pending[r.seq] = r.u
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
			u, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			if firstErr == nil {
				if err := b.add(u); err != nil {
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

// bucketer accumulates processed units into equal-timestamp buckets and flushes
// each bucket (dedup + MACB grouping) to the output sink, mirroring
// output_engine.py's _ExportEvent/_FlushExportBuffer.
type bucketer struct {
	sink  output.Sink
	dedup bool

	bucket  []*output.Unit
	ts      int64
	started bool
}

func (b *bucketer) add(u *output.Unit) error {
	if b.started && u.TS != b.ts {
		if err := b.flush(); err != nil {
			return err
		}
		b.bucket = b.bucket[:0]
	}
	b.ts = u.TS
	b.started = true
	b.bucket = append(b.bucket, u)
	return nil
}

func (b *bucketer) finish() error {
	if b.started {
		return b.flush()
	}
	return nil
}

// flush processes one equal-timestamp bucket: order by (event_values_hash,
// timestamp_desc) — ties kept in the reader's _identifier order via a stable
// sort — then deduplicate and MACB-group, emitting to the sink.
func (b *bucketer) flush() error {
	bucket := b.bucket
	sort.SliceStable(bucket, func(i, j int) bool {
		a, c := bucket[i], bucket[j]
		if a.Hash != c.Hash {
			return a.Hash < c.Hash
		}
		return a.Desc < c.Desc
	})

	var group []*output.Unit
	var lastHash, lastDesc, lastGroupID string
	lastGroupIsNone := true
	hasKept := false

	for _, u := range bucket {
		if b.dedup && hasKept && u.Desc == lastDesc && u.Hash == lastHash {
			continue
		}

		eligible := output.IsMACBEligible(u.Desc)
		groupID := ""
		if eligible {
			groupID = u.Hash
		}

		if !eligible {
			if len(group) > 0 {
				if err := b.sink.Group(group); err != nil {
					return err
				}
				group = nil
			}
			if err := b.sink.Single(u); err != nil {
				return err
			}
		} else {
			if (!lastGroupIsNone && lastGroupID == groupID) || len(group) == 0 {
				group = append(group, u)
			} else {
				if err := b.sink.Group(group); err != nil {
					return err
				}
				group = []*output.Unit{u}
			}
		}

		lastHash = u.Hash
		lastDesc = u.Desc
		lastGroupID = groupID
		lastGroupIsNone = !eligible
		hasKept = true
	}

	if len(group) > 0 {
		if err := b.sink.Group(group); err != nil {
			return err
		}
	}
	return nil
}
