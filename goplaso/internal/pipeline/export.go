// Package pipeline implements the psort export loop: it reads events in
// timestamp order, time-orders and deduplicates them within each equal-
// timestamp bucket, applies the dynamic module's within-bucket textual
// sub-sort, and writes each surviving event. Mirrors the serial core of
// plaso/multi_process/output_engine.py (PsortEventHeap, _FlushExportBuffer)
// and the SortedStringHeap in plaso/output/text_file.py.
package pipeline

import (
	"bufio"
	"io"
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

	var bucket []*container.Event
	var bucketTS int64
	started := false

	flush := func() error { return e.flushBucket(bw, bucket) }

	err := e.Reader.Each(func(ev *container.Event) error {
		if started && ev.Timestamp != bucketTS {
			if err := flush(); err != nil {
				return err
			}
			bucket = bucket[:0]
		}
		bucketTS = ev.Timestamp
		started = true
		bucket = append(bucket, ev)
		return nil
	})
	if err != nil {
		return err
	}
	if started {
		if err := flush(); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// flushBucket processes one equal-timestamp bucket.
func (e *Exporter) flushBucket(bw *bufio.Writer, bucket []*container.Event) error {
	// Primary within-bucket order: (event_values_hash, timestamp_desc), ties
	// broken by identifier for determinism (plaso breaks them on duplicate
	// content which is then discarded anyway).
	sort.SliceStable(bucket, func(i, j int) bool {
		a, b := bucket[i], bucket[j]
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
	for _, ev := range bucket {
		h := hashOf(ev)
		if e.Dedup && hasPrev && ev.TimestampDesc == lastDesc && h == lastHash {
			continue
		}
		survivors = append(survivors, ev)
		lastDesc, lastHash = ev.TimestampDesc, h
		hasPrev = true
	}

	// Compute field values and apply the dynamic module's textual sub-sort:
	// ascending by (sort key, output line), matching the SortedStringHeap.
	type row struct {
		sortKey string
		csv     string
		rec     *output.Record
	}
	rows := make([]row, 0, len(survivors))
	for _, ev := range survivors {
		rec := e.Dynamic.Compute(ev)
		rows = append(rows, row{sortKey: rec.SortKey(), csv: rec.CSVLine(), rec: rec})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].sortKey != rows[j].sortKey {
			return rows[i].sortKey < rows[j].sortKey
		}
		return rows[i].csv < rows[j].csv
	})

	for _, r := range rows {
		var err error
		if e.Mode == ModeCSV {
			_, err = bw.WriteString(r.csv)
		} else {
			_, err = bw.WriteString(r.rec.JSONLine())
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
