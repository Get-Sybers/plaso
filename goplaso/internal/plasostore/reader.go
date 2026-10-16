// Package plasostore reads plaso SQLite storage files (.plaso).
//
// A .plaso file is a SQLite database. Container rows are stored either in
// schema-based tables (explicit typed columns: event, event_tag,
// event_data_stream, ...) or in schema-less BLOB tables whose `_data` column
// holds a zlib-compressed JSON attribute container (event_data,
// analysis_report). See scratchpad/GROUND_TRUTH.md.
package plasostore

import (
	"bytes"
	"compress/zlib"
	"database/sql"
	"fmt"
	"io"

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

	eventData   map[int64]*container.EventData
	eventStream map[int64]*container.EventDataStream
	tagsByEvent map[int64][]string
}

// Open opens a .plaso file read-only and loads the related-container indexes
// (event_data, event_data_stream, event_tag) needed to resolve events.
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
	if err = r.loadEventData(); err != nil {
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
}

func (r *Reader) loadEventData() error {
	rows, err := r.db.Query("SELECT _identifier, _data FROM event_data")
	if err != nil {
		return fmt.Errorf("read event_data: %w", err)
	}
	defer rows.Close()
	r.eventData = map[int64]*container.EventData{}
	for rows.Next() {
		var id int64
		var blob []byte
		if err := rows.Scan(&id, &blob); err != nil {
			return fmt.Errorf("scan event_data: %w", err)
		}
		obj, err := decodeContainerBlob(blob)
		if err != nil {
			return fmt.Errorf("event_data %d: %w", id, err)
		}
		ed := &container.EventData{
			Identifier: id,
			Attributes: map[string]any{},
		}
		for k, v := range obj {
			switch k {
			case "data_type":
				ed.DataType, _ = v.(string)
			case "_parser_chain":
				ed.ParserChain, _ = v.(string)
			case "_event_data_stream_identifier":
				ed.EventDataStreamIdentifier, _ = v.(string)
			default:
				if !reservedEventDataKeys[k] {
					ed.Attributes[k] = v
				}
			}
		}
		if idf, ok := container.ParseIdentifier(ed.EventDataStreamIdentifier); ok {
			ed.Stream = r.eventStream[idf.SequenceNumber]
		}
		r.eventData[id] = ed
	}
	return rows.Err()
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

// Each streams events in timestamp order (ties broken by _identifier),
// resolving each event's EventData (+Stream) and Tags, invoking fn per event.
// Iteration stops and returns the first error fn returns.
func (r *Reader) Each(fn func(*container.Event) error) error {
	rows, err := r.db.Query(
		"SELECT _identifier, _event_data_identifier, date_time, timestamp, timestamp_desc " +
			"FROM event ORDER BY timestamp, _identifier")
	if err != nil {
		return fmt.Errorf("read event: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			ev           container.Event
			dataRef      sql.NullString
			dateTimeJSON sql.NullString
			ts           sql.NullInt64
			desc         sql.NullString
		)
		if err := rows.Scan(&ev.Identifier, &dataRef, &dateTimeJSON, &ts, &desc); err != nil {
			return fmt.Errorf("scan event: %w", err)
		}
		ev.EventDataIdentifier = dataRef.String
		ev.Timestamp = ts.Int64
		ev.TimestampDesc = desc.String
		if dateTimeJSON.Valid && dateTimeJSON.String != "" {
			obj, err := container.DecodeJSONObject([]byte(dateTimeJSON.String))
			if err != nil {
				return fmt.Errorf("event %d date_time: %w", ev.Identifier, err)
			}
			ev.DateTime = container.ParseDateTime(obj)
		}
		if idf, ok := container.ParseIdentifier(ev.EventDataIdentifier); ok {
			ev.EventData = r.eventData[idf.SequenceNumber]
		}
		ev.Tags = r.tagsByEvent[ev.Identifier]
		if err := fn(&ev); err != nil {
			return err
		}
	}
	return rows.Err()
}
