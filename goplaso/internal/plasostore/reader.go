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
}

// Open opens a .plaso file read-only and loads the small related-container
// indexes (event_data_stream, event_tag). Events and their event_data blobs are
// streamed and decoded on demand (see StreamRawEvents), not held in memory.
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
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}

	r = &Reader{db: db}
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

// zlibReaderPool recycles zlib readers across the decode worker pool. Each
// zlib.NewReader allocates a fresh ~32 KiB flate dictionary window, which
// dominated allocation (≈67%) and GC pressure on the per-event decode hot path;
// resetting a pooled reader reuses that window. A pooled reader's decompressed
// output is byte-identical, and sync.Pool is safe for concurrent use: a reader
// is held by one goroutine between Get and Put.
var zlibReaderPool sync.Pool

// decodeContainerBlob decompresses (zlib) and JSON-decodes a `_data` blob.
func decodeContainerBlob(blob []byte) (map[string]any, error) {
	br := bytes.NewReader(blob)

	zr, _ := zlibReaderPool.Get().(io.ReadCloser)
	if zr == nil {
		var err error
		if zr, err = zlib.NewReader(br); err != nil {
			return nil, fmt.Errorf("zlib: %w", err)
		}
	} else if err := zr.(zlib.Resetter).Reset(br, nil); err != nil {
		return nil, fmt.Errorf("zlib reset: %w", err)
	}

	// Decode straight from the inflate stream — no io.ReadAll of the whole blob
	// (the JSON decoder keeps its own read buffer, so ReadAll was a redundant
	// second full copy). The reader (and its window) is pooled; its output is
	// not recycled, so decoded values that alias the decoder's buffer stay valid.
	obj, err := container.DecodeJSONObjectFrom(zr)
	zr.Close()
	zlibReaderPool.Put(zr)
	if err != nil {
		return nil, err
	}
	return obj, nil
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

// StreamRawEvents streams event rows in timestamp order (ties broken by
// _identifier), each paired with its still-compressed event_data blob, and
// invokes fn per row. It uses a single JOIN so events and their event_data are
// read in one ordered pass without holding the whole store in memory: the Go
// heap stays bounded (SQLite performs the ordering), and the expensive blob
// decode is left to the caller so it can be parallelized and then discarded.
//
// The _event_data_identifier reference ("event_data.<N>") is joined to
// event_data by its rowid via substr/CAST. A LEFT JOIN keeps events whose
// event_data is missing (blob is then nil). The blob passed to fn is a fresh
// copy, safe to retain across goroutines. Iteration stops on the first error.
func (r *Reader) StreamRawEvents(fn func(RawEvent, []byte) error) error {
	const query = "SELECT e._identifier, e._event_data_identifier, e.date_time, " +
		"e.timestamp, e.timestamp_desc, ed._data " +
		"FROM event e LEFT JOIN event_data ed " +
		"ON ed._identifier = CAST(substr(e._event_data_identifier, 12) AS INTEGER) " +
		"ORDER BY e.timestamp, e._identifier"
	rows, err := r.db.Query(query)
	if err != nil {
		return fmt.Errorf("read event: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			raw          RawEvent
			dataRef      sql.NullString
			dateTimeJSON sql.NullString
			ts           sql.NullInt64
			desc         sql.NullString
			blob         []byte
		)
		if err := rows.Scan(&raw.Identifier, &dataRef, &dateTimeJSON, &ts, &desc, &blob); err != nil {
			return fmt.Errorf("scan event: %w", err)
		}
		raw.EventDataIdentifier = dataRef.String
		raw.Timestamp = ts.Int64
		raw.TimestampDesc = desc.String
		raw.DateTimeJSON = dateTimeJSON.String
		var blobCopy []byte
		if blob != nil {
			blobCopy = append([]byte(nil), blob...) // stable across Next() and goroutines
		}
		if err := fn(raw, blobCopy); err != nil {
			return err
		}
	}
	return rows.Err()
}

// Hydrate resolves a raw event and its compressed event_data blob into a full
// Event: it decodes the blob, parses date_time, and attaches tags. The decoded
// EventData is not retained by the reader, so callers may discard the Event
// after use. Safe for concurrent use.
func (r *Reader) Hydrate(raw RawEvent, blob []byte) (*container.Event, error) {
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
	if blob != nil {
		idf, _ := container.ParseIdentifier(raw.EventDataIdentifier)
		ed, err := r.buildEventData(idf.SequenceNumber, blob)
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
	return r.StreamRawEvents(func(raw RawEvent, blob []byte) error {
		ev, err := r.Hydrate(raw, blob)
		if err != nil {
			return err
		}
		return fn(ev)
	})
}
