// Package plasostore reads plaso SQLite storage files (.plaso).
//
// A .plaso file is a SQLite database. Container rows are stored either in
// schema-based tables (explicit typed columns: event, event_tag,
// event_data_stream, ...) or in schema-less BLOB tables whose `_data` column
// holds a zlib-compressed JSON attribute container (event_data,
// analysis_report). See docs/PORT_PLAN.md / scratchpad GROUND_TRUTH.
//
// event_data blobs are decoded lazily and cached, so an export can decode them
// concurrently across a worker pool: GetEventData is safe for concurrent use.
package plasostore

import (
	"bytes"
	"compress/zlib"
	"database/sql"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/Get-Sybers/plaso/goplaso/internal/container"

	_ "modernc.org/sqlite" // pure-Go SQLite driver (registers "sqlite")
)

// Supported format expectations. These mirror plaso's sqlite_file.py.
const (
	compatibleFormatVersion = 20260516
	expectCompression       = "zlib"
	expectSerialization     = "json"
)

// Reader reads events from a .plaso storage file.
type Reader struct {
	db   *sql.DB
	meta map[string]string

	// Small, shared, read-only after Open — safe for concurrent reads.
	eventStream map[int64]*container.EventDataStream
	tagsByEvent map[int64][]string

	// event_data is bulk-scanned then decoded in parallel at Open and cached.
	// After preload the cache is immutable, so reads need no lock.
	edMu        sync.Mutex
	edCache     map[int64]*container.EventData
	edPreloaded bool
}

// Open opens a .plaso file read-only and loads the small related-container
// indexes (event_data_stream, event_tag). event_data is decoded lazily.
func Open(path string) (r *Reader, err error) {
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro&immutable=1")
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		if err != nil {
			db.Close()
		}
	}()
	// Allow the ordered streaming query and concurrent per-event point lookups
	// to each hold their own connection (the file is opened read-only).
	db.SetMaxOpenConns(0) // unlimited; actual concurrency is bounded by the exporter
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}

	r = &Reader{db: db, edCache: map[int64]*container.EventData{}}
	if err = r.loadMetadata(); err != nil {
		return nil, err
	}
	if err = r.validateFormat(); err != nil {
		return nil, err
	}
	if err = r.loadEventDataStreams(); err != nil {
		return nil, err
	}
	if err = r.loadEventTags(); err != nil {
		return nil, err
	}
	return r, nil
}

// PreloadEventData bulk-scans every event_data blob (fast serial I/O) and
// decodes them (zlib inflate + JSON) across a worker pool, since the decode is
// the dominant per-event cost and is independent per row. After this the cache
// is complete and immutable, so GetEventData reads it without locking. It is
// idempotent; workers < 1 is treated as 1 (serial decode). When not called,
// GetEventData falls back to decoding lazily per row.
func (r *Reader) PreloadEventData(workers int) error {
	if r.edPreloaded {
		return nil
	}
	rows, err := r.db.Query("SELECT _identifier, _data FROM event_data")
	if err != nil {
		return fmt.Errorf("read event_data: %w", err)
	}
	type rawBlob struct {
		id   int64
		blob []byte
	}
	var all []rawBlob
	for rows.Next() {
		var id int64
		var blob []byte
		if err := rows.Scan(&id, &blob); err != nil {
			rows.Close()
			return fmt.Errorf("scan event_data: %w", err)
		}
		all = append(all, rawBlob{id: id, blob: append([]byte(nil), blob...)})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	if workers < 1 {
		workers = 1
	}
	decoded := make([]*container.EventData, len(all))
	errs := make([]error, len(all))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for i := range all {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			decoded[i], errs[i] = r.buildEventData(all[i].id, all[i].blob)
		}(i)
	}
	wg.Wait()

	for i := range all {
		if errs[i] != nil {
			return errs[i]
		}
		r.edCache[all[i].id] = decoded[i]
	}
	r.edPreloaded = true
	return nil
}

// Close releases the underlying database.
func (r *Reader) Close() error { return r.db.Close() }

// Metadata returns the storage metadata key/value pairs.
func (r *Reader) Metadata() map[string]string { return r.meta }

func (r *Reader) loadMetadata() error {
	rows, err := r.db.Query("SELECT key, value FROM metadata")
	if err != nil {
		return fmt.Errorf("read metadata: %w", err)
	}
	defer rows.Close()
	r.meta = map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return fmt.Errorf("scan metadata: %w", err)
		}
		r.meta[k] = v
	}
	return rows.Err()
}

func (r *Reader) validateFormat() error {
	if got := r.meta["serialization_format"]; got != expectSerialization {
		return fmt.Errorf("unsupported serialization_format: %q (want %q)", got, expectSerialization)
	}
	if got := r.meta["compression_format"]; got != expectCompression {
		return fmt.Errorf("unsupported compression_format: %q (want %q)", got, expectCompression)
	}
	var fv int
	fmt.Sscanf(r.meta["format_version"], "%d", &fv)
	if fv < compatibleFormatVersion {
		return fmt.Errorf("format_version %d older than read-compatible %d", fv, compatibleFormatVersion)
	}
	return nil
}

// CountEvents returns the number of rows in the event table.
func (r *Reader) CountEvents() (int64, error) {
	var n int64
	err := r.db.QueryRow("SELECT COUNT(*) FROM event").Scan(&n)
	return n, err
}

// decodeContainerBlob decompresses (zlib) and JSON-decodes a `_data` blob.
func decodeContainerBlob(blob []byte) (map[string]any, error) {
	zr, err := zlib.NewReader(bytes.NewReader(blob))
	if err != nil {
		return nil, fmt.Errorf("zlib: %w", err)
	}
	defer zr.Close()
	raw, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("zlib read: %w", err)
	}
	return container.DecodeJSONObject(raw)
}

// reservedEventDataKeys are decoded into typed fields rather than Attributes.
var reservedEventDataKeys = map[string]bool{
	"__type__":                      true,
	"__container_type__":            true,
	"_parser_chain":                 true,
	"data_type":                     true,
	"_event_data_stream_identifier": true,
	"_event_values_hash":            true,
}

// buildEventData decodes one event_data blob into a container.EventData and
// resolves its event_data_stream.
func (r *Reader) buildEventData(id int64, blob []byte) (*container.EventData, error) {
	obj, err := decodeContainerBlob(blob)
	if err != nil {
		return nil, fmt.Errorf("event_data %d: %w", id, err)
	}
	// Reuse the decoded map as Attributes instead of copying into a second map
	// (that copy was the single largest cost in the decode hot path): pull the
	// reserved keys into typed fields, then delete them in place.
	ed := &container.EventData{Identifier: id, Attributes: obj}
	if v, ok := obj["data_type"].(string); ok {
		ed.DataType = v
	}
	if v, ok := obj["_parser_chain"].(string); ok {
		ed.ParserChain = v
	}
	if v, ok := obj["_event_data_stream_identifier"].(string); ok {
		ed.EventDataStreamIdentifier = v
	}
	if v, ok := obj["_event_values_hash"].(string); ok {
		ed.EventValuesHash = v
	}
	for k := range reservedEventDataKeys {
		delete(obj, k)
	}
	if idf, ok := container.ParseIdentifier(ed.EventDataStreamIdentifier); ok {
		ed.Stream = r.eventStream[idf.SequenceNumber] // read-only map
	}
	return ed, nil
}

// GetEventData returns the decoded event_data with the given sequence number
// (rowid), decoding and caching on first use. Safe for concurrent use.
func (r *Reader) GetEventData(seq int64) (*container.EventData, error) {
	if r.edPreloaded {
		// Cache is immutable after preload: read without locking.
		return r.edCache[seq], nil
	}

	r.edMu.Lock()
	if ed, ok := r.edCache[seq]; ok {
		r.edMu.Unlock()
		return ed, nil
	}
	r.edMu.Unlock()

	// Decode outside the lock so concurrent decodes of different rows proceed
	// in parallel (a rare duplicate decode of the same row is harmless).
	var blob []byte
	err := r.db.QueryRow("SELECT _data FROM event_data WHERE _identifier = ?", seq).Scan(&blob)
	if err != nil {
		return nil, fmt.Errorf("read event_data %d: %w", seq, err)
	}
	ed, err := r.buildEventData(seq, blob)
	if err != nil {
		return nil, err
	}

	r.edMu.Lock()
	if cached, ok := r.edCache[seq]; ok {
		ed = cached // keep the first stored instance
	} else {
		r.edCache[seq] = ed
	}
	r.edMu.Unlock()
	return ed, nil
}

func (r *Reader) loadEventDataStreams() error {
	rows, err := r.db.Query(
		"SELECT _identifier, path_spec, md5_hash, sha1_hash, sha256_hash, file_entropy, yara_match FROM event_data_stream")
	if err != nil {
		return fmt.Errorf("read event_data_stream: %w", err)
	}
	defer rows.Close()
	r.eventStream = map[int64]*container.EventDataStream{}
	for rows.Next() {
		s := &container.EventDataStream{}
		var pathSpec, md5, sha1, sha256, entropy, yara sql.NullString
		if err := rows.Scan(&s.Identifier, &pathSpec, &md5, &sha1, &sha256, &entropy, &yara); err != nil {
			return fmt.Errorf("scan event_data_stream: %w", err)
		}
		s.PathSpec = pathSpec.String
		s.MD5Hash = md5.String
		s.SHA1Hash = sha1.String
		s.SHA256Hash = sha256.String
		s.FileEntropy = entropy.String
		s.YaraMatch = yara.String
		r.eventStream[s.Identifier] = s
	}
	return rows.Err()
}

func (r *Reader) loadEventTags() error {
	rows, err := r.db.Query("SELECT _event_identifier, labels FROM event_tag")
	if err != nil {
		return fmt.Errorf("read event_tag: %w", err)
	}
	defer rows.Close()
	r.tagsByEvent = map[int64][]string{}
	for rows.Next() {
		var eventRef, labelsJSON string
		if err := rows.Scan(&eventRef, &labelsJSON); err != nil {
			return fmt.Errorf("scan event_tag: %w", err)
		}
		idf, ok := container.ParseIdentifier(eventRef)
		if !ok {
			continue
		}
		labels, err := decodeStringArray(labelsJSON)
		if err != nil {
			return fmt.Errorf("event_tag labels %q: %w", labelsJSON, err)
		}
		r.tagsByEvent[idf.SequenceNumber] = append(r.tagsByEvent[idf.SequenceNumber], labels...)
	}
	return rows.Err()
}

func decodeStringArray(s string) ([]string, error) {
	obj, err := container.DecodeJSONArray([]byte(s))
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(obj))
	for _, v := range obj {
		if str, ok := v.(string); ok {
			out = append(out, str)
		}
	}
	return out, nil
}

// RawEvent is an undecoded event row: the event_data is not resolved and the
// date_time JSON is not parsed, so that work can be deferred to a worker pool.
type RawEvent struct {
	Identifier          int64
	EventDataIdentifier string
	DateTimeJSON        string
	Timestamp           int64
	TimestampDesc       string
}

// StreamRaw yields event rows in timestamp order (ties broken by _identifier)
// without resolving event_data or parsing date_time. Iteration stops on the
// first error fn returns.
//
// The rows are read with an unindexed scan and ordered in Go rather than with
// SQL ORDER BY: plaso's schema has no index on event(timestamp), so SQLite
// would materialize a full temporary sort b-tree every run (~10x the cost of
// the scan itself). Sorting the lightweight rows in Go is far cheaper and keeps
// the on-disk file untouched (read-only/immutable).
func (r *Reader) StreamRaw(fn func(RawEvent) error) error {
	rows, err := r.db.Query(
		"SELECT _identifier, _event_data_identifier, date_time, timestamp, timestamp_desc FROM event")
	if err != nil {
		return fmt.Errorf("read event: %w", err)
	}
	defer rows.Close()

	var all []RawEvent
	for rows.Next() {
		var (
			raw          RawEvent
			dataRef      sql.NullString
			dateTimeJSON sql.NullString
			ts           sql.NullInt64
			desc         sql.NullString
		)
		if err := rows.Scan(&raw.Identifier, &dataRef, &dateTimeJSON, &ts, &desc); err != nil {
			return fmt.Errorf("scan event: %w", err)
		}
		raw.EventDataIdentifier = dataRef.String
		raw.Timestamp = ts.Int64
		raw.TimestampDesc = desc.String
		raw.DateTimeJSON = dateTimeJSON.String
		all = append(all, raw)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].Timestamp != all[j].Timestamp {
			return all[i].Timestamp < all[j].Timestamp
		}
		return all[i].Identifier < all[j].Identifier
	})

	for _, raw := range all {
		if err := fn(raw); err != nil {
			return err
		}
	}
	return nil
}

// Hydrate resolves a raw event into a full Event: parses date_time, resolves
// its (preloaded) event_data, and attaches its tags. Safe for concurrent use.
func (r *Reader) Hydrate(raw RawEvent) (*container.Event, error) {
	ev := &container.Event{
		Identifier:          raw.Identifier,
		EventDataIdentifier: raw.EventDataIdentifier,
		Timestamp:           raw.Timestamp,
		TimestampDesc:       raw.TimestampDesc,
		Tags:                r.tagsByEvent[raw.Identifier], // read-only map
	}
	if raw.DateTimeJSON != "" {
		obj, err := container.DecodeJSONObject([]byte(raw.DateTimeJSON))
		if err != nil {
			return nil, fmt.Errorf("event %d date_time: %w", raw.Identifier, err)
		}
		ev.DateTime = container.ParseDateTime(obj)
	}
	if idf, ok := container.ParseIdentifier(raw.EventDataIdentifier); ok {
		ed, err := r.GetEventData(idf.SequenceNumber)
		if err != nil {
			return nil, err
		}
		ev.EventData = ed
	}
	return ev, nil
}

// Each streams fully-resolved events in timestamp order, invoking fn per event.
// Iteration stops and returns the first error fn returns.
func (r *Reader) Each(fn func(*container.Event) error) error {
	return r.StreamRaw(func(raw RawEvent) error {
		ev, err := r.Hydrate(raw)
		if err != nil {
			return err
		}
		return fn(ev)
	})
}
