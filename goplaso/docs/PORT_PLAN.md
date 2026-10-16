# plaso `psort` → Go Port Plan

Status: authoritative architecture plan for the implementation team.
Reference source: `Get-Sybers/plaso` @ `__version__ 20260928`, storage `format_version 20260516`.
Existing scaffold: `/home/user/plaso/goplaso` (module `github.com/Get-Sybers/plaso/goplaso`, Go 1.26, `modernc.org/sqlite`), already containing `internal/container/container.go` and `internal/plasostore/reader.go`. **Build on this scaffold — do not restart.**

---

## 1. Executive summary

### What `psort` does, end to end

`psort` is plaso's timeline **output** stage. It takes a finished `.plaso` storage file (a plain SQLite3 DB of forensic "attribute containers") and produces a human/tool-readable timeline. The flow is:

1. **Open & validate** the SQLite file read-only; check the `metadata` table (`format_version`, `serialization_format == 'json'`, `compression_format ∈ {zlib,none}`).
2. **Stream events in time order**: `SELECT _identifier,_event_data_identifier,date_time,timestamp,timestamp_desc FROM event [WHERE timestamp BETWEEN ...] ORDER BY timestamp`. The integer `timestamp` column (POSIX microseconds, signed) is the only sort key SQLite applies.
3. **Join** each event to its `event_data` (a zlib+JSON blob, resolved by rowid), its optional `event_data_stream` (schema columns), and its optional `event_tag` (loaded once, cached by event id).
4. **Secondary order + dedup + MACB-group** within each equal-`timestamp` bucket, using a min-heap keyed by `(event_values_hash, timestamp_desc)`.
5. **Format** each event: reconstruct the human `message`/`message_short` from per-`data_type` YAML formatters, resolve hostname/username/timezone/MACB/display-name via an `OutputMediator`.
6. **Emit** through the chosen output module (default `dynamic`, a configurable CSV), which for text formats applies a *second* textual sub-sort before writing.

Despite living in `multi_process/output_engine.py` and being named `OutputAndFormattingMultiProcessEngine`, **the export runs single-threaded.** The only extra thread is a status ticker. The code literally carries `# TODO: move this to a single process engine.` Multiprocessing here buys nothing.

### Why it is slow in Python, and why Go wins

The dominant costs, in order:

1. **Per-event random-access join + decompress + JSON parse.** For every event: one `SELECT _data FROM event_data WHERE rowid=?` → `zlib.decompress` → `json.loads` → container rebuild. `event_data` is near-unique per event, so the 32768-entry LRU mostly *misses*. This is N point-queries + N inflates + N JSON parses, all in interpreted Python, under the GIL. **This is the single biggest cost.**
2. **Per-event message formatting.** `CopyToDict` → run formatter helpers → Python `str.format(**values)` for `message` *and again* for `message_short`. Pure interpreter overhead, twice per event.
3. **`ORDER BY timestamp` with no index.** SQLite materializes a temporary sort b-tree over the whole `event` table every run. (This is a plaso defect the Go port can simply fix.)
4. **Per-event datetime/timezone conversion** via `datetime`+`pytz` `astimezone`.

The slowness is **not** algorithmic — it is per-event constant-factor overhead paid in an interpreted, GIL-bound language with no real parallelism in the export stage. Go wins because:

- **Real parallelism.** Events are independent until the ordering/dedup stage. Decompress + JSON-decode + message-format can run on a goroutine worker pool across all cores; the single-threaded Python version cannot.
- **Compiled hot path.** `zlib` (`compress/flate`), JSON decode, string formatting and MD5 are native, not bytecode.
- **We can fix the missing index** (or stream-and-merge-sort on the int column) instead of re-paying SQLite's full-table temp sort.
- **No serialization tax between stages.** Python's multiprocess engines pay pickling/IPC; a Go pipeline passes pointers over channels.

Realistic expectation: **5–20× throughput** on large timelines, dominated by parallel blob decode/format, while producing **byte-identical** output to Python `psort`.

---

## 2. Proposed Go module / package layout

Single module, extend the existing `goplaso` module. All packages under `internal/` except the CLI.

| Go package | Responsibility | Python module(s) replaced |
|---|---|---|
| `internal/container` *(exists)* | Core structs: `Event`, `EventData`, `EventDataStream`, `EventTag`, `Identifier`, `DateTimeValues`; attribute iteration semantics | `plaso/containers/events.py`, `acstore/containers/interface.py` |
| `internal/plasostore` *(exists)* | SQLite reader: open RO, metadata validation, `GetSortedEvents`, resolve-by-rowid, blob zlib+JSON decode, LRU cache, schema-vs-blob table dispatch | `acstore/sqlite_store.py`, `acstore/interface.py`, `plaso/storage/sqlite/{sqlite_file,reader}.py`, `plaso/storage/factory.py` |
| `internal/dftime` | dfdatetime subset: class registry by `__class_name__`, normalized-timestamp, ISO8601 render, `is_local_time`/`time_zone_offset`/`precision`, semantic times | `dfdatetime/*` (`posix_time`, `time_elements`, `semantic_time`, `serializer`, `factory`, `interface`) |
| `internal/dfpath` | Minimal dfVFS PathSpec: parent chain, `type_indicator`, `location`, `data_stream`, `.comparable`, display-name/relative-path/inode derivation, JSON (de)serialize | `dfvfs` path specs + factory, `plaso/engine/path_helper.py` |
| `internal/jsonwire` | Python-`json`-exact encode/decode of attribute containers: `__type__` dispatch (bytes=quoted-printable, tuple, `collections.Counter`, DateTimeValues, PathSpec), **and** a Python-compatible encoder (ensure_ascii `\uXXXX`, `', '`/`': '` separators) for JSON output modules | `plaso/serializer/json_serializer.py`, `plaso/storage/serializers.py`, `acstore/helpers/json_serializer.py` |
| `internal/pipeline` | The export engine: time-ordered iteration, equal-timestamp heap, dedup, MACB grouping, worker-pool orchestration, status ticker | `plaso/multi_process/output_engine.py` (`PsortEventHeap`, `_ExportEvents`, `_FlushExportBuffer`) |
| `internal/mediator` | `OutputMediator`: hostname/username/MACB/source/timezone/LCID resolution, lazy caches, winevt resource lookup entry points | `plaso/output/mediator.py`, `plaso/output/winevt_rc.py`, `plaso/helpers/windows/*` |
| `internal/formatters` | YAML formatter loader (multi-doc), `basic`/`conditional` formatters, boolean/enum/flags/custom helpers, Python-`str.format` mini-engine, `DefaultEventFormatter` | `plaso/formatters/*`, `plaso/output/formatting_helper.py` (message/field callbacks) |
| `internal/output` | Output-module interface + implementations; `FieldFormattingHelper` field callbacks; DSV sanitizer; `SortedStringHeap` textual sub-sort; file writer | `plaso/output/{interface,manager,text_file,shared_dsv,dynamic,l2t_csv,tln,shared_json,json_line,json_out,rawpy,null}.py` |
| `internal/filter` *(defer)* | Event filter language (lexer/parser → tree → `Matches`) + `TimeSlice`/`TimeRange` | `plaso/filters/*`, `plaso/cli/time_slices.py`, `plaso/storage/time_range.py` |
| `cmd/goplaso-psort` | CLI: flags (`-o`, `-w`, `--fields`, `--output_time_zone`, `--dynamic_time`, …), wiring | `plaso/cli/psort_tool.py`, `plaso/cli/helpers/*` |
| `internal/testutil` | Golden-file diff harness that shells out to real Python `psort` | — |

**Out of scope (separate future ports if ever needed):** redis store, fake store, task-merge, the writer, extraction/analysis engines, `kml`/`xlsx`/`opensearch` output modules, extraction-time file-entry filters and `--date-filter`.

---

## 3. Data model

### 3.1 Core structs

The scaffold's `container.go` already has the right shape. Keep and extend:

```go
type Identifier struct {           // "<type>.<seq>", seq is 1-based == rowid
    ContainerType  string
    SequenceNumber int64
}

type Event struct {                // schema table `event`
    Identifier          int64      // rowid
    EventDataIdentifier string     // "event_data.N"
    DateTime            *DateTimeValues
    Timestamp           int64      // POSIX microseconds, SIGNED, 0 == not-a-time
    TimestampDesc       string
    EventData           *EventData // joined
    Tags                []string   // from EventTag.labels
}

type EventData struct {            // BLOB table `event_data` (zlib+JSON)
    Identifier                int64
    EventDataStreamIdentifier string   // "event_data_stream.M" (injected in JSON, not a column)
    ParserChain               string   // _parser_chain
    DataType                  string
    EventValuesHash           string   // _event_values_hash (dedup/MACB key); "" -> treat as "UNKNOWN"
    Attributes                map[string]any // dynamic parser attrs; json.Number preserved
    Stream                    *EventDataStream
}

type EventDataStream struct {      // schema table `event_data_stream`
    Identifier  int64
    FileEntropy string
    MD5, SHA1, SHA256 string
    PathSpec    *dfpath.PathSpec
    YaraMatch   []string
}

type EventTag struct {             // schema table `event_tag` (+ index event_tag_per_event)
    Identifier      int64
    EventIdentifier string         // "event.K"
    Labels          []string
}
```

Critical modeling rules (from the maps):

- **`timestamp` is `int64` and can be negative** (pre-1970). Never use unsigned. `0` means "invalid / not-a-time" and still sorts first.
- **`Attributes map[string]any` with `json.Number`** (decode via `json.Decoder.UseNumber()`) — mandatory to keep >2⁵³ nanosecond timestamps lossless and to reproduce formatting exactly.
- **`EventValuesHash` is read, never recomputed** for normal psort. Missing → the literal string `"UNKNOWN"`. Missing `TimestampDesc` → `"Unknown Time"`.
- **Attribute iteration (acstore semantics)** must be centralized: `GetAttributes()` yields public (non-`_`) attributes **plus** the 5 whitelisted protected names (`_corrupted`, `_event_data_stream_identifier`, `_event_values_hash`, `_parser_chain`, `_recovered`), skipping `nil`. The *same* iteration feeds message formatting, l2tcsv `extra`, JSON assembly, and (if ever recomputing) the MD5 hash — so implement it once.

### 3.2 Reading the `.plaso` SQLite file

- **Driver: `modernc.org/sqlite`** (already in `go.mod`), pure-Go, no cgo. Open DSN `file:<abspath>?mode=ro`. On the connection set `PRAGMA journal_mode=MEMORY; PRAGMA synchronous=OFF;` to mirror plaso.
- **Metadata gate on open:** read `metadata(key,value)`; require `serialization_format == 'json'`, `compression_format ∈ {zlib,none}`. Be **lenient on read** about `format_version` (accept the value present); record that the writer emits `20260516`.
- **Two physical table shapes, dispatched by container type:**
  - **Blob** if the type has no schema **OR** type == `event_data`: table is `(_identifier INTEGER PK AUTOINCREMENT, _data BLOB|TEXT)`. `_data` = `zlib.compress(utf8(json))` when zlib, else raw UTF-8 JSON. Decode: inflate (`compress/zlib`) → UTF-8 → JSON object `{"__type__":"AttributeContainer","__container_type__":...}`.
  - **Schema** otherwise: real columns, one per schema key **in sorted order**. The schema is **not stored in the file** — acstore derives it from registered classes. **Hardcode each table's column list in sorted order** in Go (also read actual table columns to stay robust).
- **Identifiers / joins:** reference strings are `"type.seq"`, `seq` 1-based == rowid. Resolve any reference via `... WHERE rowid = seq`. Event → `_event_data_identifier` → EventData → `_event_data_stream_identifier` → EventDataStream; EventTag → `_event_identifier` → Event.
- **`_event_data_stream_identifier` is injected into the event_data JSON blob**, not a column — re-attach it on decode (the scaffold already does this).
- **Typed JSON values** (`internal/jsonwire`): `bytes` = **quoted-printable** (`binascii.a2b_qp`, *not* hex, despite the `"base16"` label) → implement a2b_qp or use `mime/quotedprintable` with validation; `tuple` → slice; `collections.Counter` → map; `DateTimeValues` → `dftime`; `PathSpec` → `dfpath`.
- **Counts via `MAX(_ROWID_)`**, not `COUNT(*)` (acstore never deletes). `parser_count` row `name=='total'` gives the event total for the status bar.
- **Concurrency hazard:** the sorted-event stream runs on a dedicated cursor; point lookups must **not** share that `*sql.Rows`/connection. Run the ordered query on one connection and point lookups on a separate connection/statement pool, or buffer — mixing a streaming query with point queries on one connection can deadlock depending on driver.

---

## 4. The core pipeline

### 4.1 Stage diagram

```
                       ┌─────────────── ordered, serial ───────────────┐
reader ──events──►  decode/join/format (PARALLEL worker pool)  ──►  reorder+dedup+MACB  ──►  output module
  (SQLite              │  resolve event_data (rowid lookup)          │   (per-timestamp     │  (text: 2nd
  ORDER BY             │  zlib inflate + JSON decode                 │    heap, serial)     │   SortedStringHeap
  timestamp)           │  resolve event_data_stream                 │                      │   sub-sort)
                       │  resolve event_tag (cached map)             │                      │
                       │  build field_values + message (formatters)  │                      │
                       └─────────────────────────────────────────────┘
```

### 4.2 Ordering is two-level and mostly SQLite's job

- **Primary:** SQL `ORDER BY timestamp` ASC (int64). The Go port issues the exact column list and the optional `WHERE timestamp >= ? [AND <= ?]`.
- **Secondary (in memory):** within each run of equal `timestamp`, order by `(event_values_hash ASC, timestamp_desc ASC)`. The Python `PsortEventHeap` flushes whenever the next event's `timestamp` differs **or** the buffer exceeds **100000**. Reproduce exactly.
- **Do not use `Event.__lt__` as the comparator** — in Python it is `self.timestamp < other.timestamp or self.timestamp_desc < other.timestamp_desc`, which is *not* a total order; it is inert inside a single-timestamp bucket. Use the explicit tuple key `(hash, desc)`.

Net deterministic output order = **timestamp ASC → event_values_hash ASC → timestamp_desc ASC**. For text/CSV modules a *third* order is applied by the output module's `SortedStringHeap` (per-module sort fields, e.g. `dynamic`=`[datetime,display_name,message]`, `l2tcsv`=`[time,filename,inode]`), flushed on primary-sort-key change or at 100000 strings.

### 4.3 Dedup (exact)

Within one flush, pop in heap order; **drop** an event iff `deduplicate_events && timestamp_desc == prevKept.timestamp_desc && event_values_hash == prevKept.event_values_hash`. Compare **only to the immediately-preceding kept event**, **only within one timestamp bucket**. Events with equal content but different timestamps are never deduped; a 100000-overflow split can let duplicates survive — match that bug-for-bug.

### 4.4 MACB grouping (exact)

An event joins a group only when `timestamp_desc` ∈ **exactly** {`Last Access Time`, `Creation Time`, `Metadata Modification Time`, `Content Modification Time`}. Group key = `event_values_hash`. Consecutive same-key eligible events accumulate and emit via `WriteFieldValuesOfMACBGroup`; everything else emits individually. Default `WriteFieldValuesOfMACBGroup` writes each member as its own row; **only `l2tcsv` collapses a group into one row** (ORed MACB letters, `'; '`-joined descs). Port both.

### 4.5 Event-tag resolution timing (subtle, must preserve)

`resolve_event_tag = (event_filter == nil)`. With **no** filter, tags are resolved **lazily at flush, after dedup**. With a filter present, the tag is resolved **before** filtering (so filters can match on `tag`). The tag cache is a single full scan of `event_tag` into `map["event.<seq>"]EventTag`, `setdefault` semantics (**first tag per event wins**).

### 4.6 Parallelization strategy

- Keep **reader serial** (it must emit in `ORDER BY timestamp` order).
- Fan events out to a **bounded worker pool** that does the expensive, independent work: event_data rowid lookup + inflate + JSON decode, event_data_stream resolve, and `GetFieldValues`/message formatting. Carry an incrementing **sequence number** with each event.
- Fan back **in order** into the serial reorder/dedup/MACB stage (a reorder buffer keyed by sequence number, or a channel-of-channels). The dedup/MACB/heap stage and the output writer stay **single-goroutine** — they are inherently sequential and cheap relative to decode+format.
- One `time.Ticker` goroutine for status, matching the Python daemon thread.
- Share the `OutputMediator` read-only across workers; its lazy caches (hostname, username-by-SID, winevt LRU, system configs) need a mutex since workers format concurrently (Python is single-threaded here and unguarded).

### 4.7 The hot path

**`decode/join/format` per event is the hot path.** Optimizations the port should take that Python cannot:
- Parallel inflate+decode+format across cores.
- `CREATE INDEX IF NOT EXISTS` on `event(timestamp)` at open time on a scratch copy, or read `(_identifier, timestamp)` and sort in Go — avoid the full-table temp sort. (If byte-parity of *ordering ties* is a concern, prefer SQLite's own `ORDER BY` for v1 and optimize later.)
- Build the event_data **value map once per event** and share it between `message` and `message_short` (Python builds it twice).
- Cache `event_data_stream` aggressively (many events share one stream — high hit rate).

---

## 5. Formatter strategy

### 5.1 Reuse the YAML at runtime — do **not** code-generate

The 9 shipped files live at `plaso/data/formatters/*.yaml` (**299 definitions**: 274 `conditional`, 25 `basic`). They are purely declarative and churn upstream frequently. **Parse them at startup** with `gopkg.in/yaml.v3` using a `Decoder` loop for multi-document (`safe_load_all`) semantics. Bundle the YAMLs with the binary via `go:embed` (and allow an override dir/file to match `--custom_formatter_definitions`).

Loading precedence (reproduce `output_engine._ReadMessageFormatters`): directory `data/formatters/*.yaml` wins over a single `formatters.yaml`; the custom file is applied **last** with override semantics; a duplicate `data_type` without override is a hard error. Keys (`data_type`, helper `identifier`) match **case-insensitively** (lowercased). Build `map[dataType]*Formatter` and `map[dataType][2]string` (short_source, source).

Only the **16 custom helpers** are code (`plaso/formatters/{chrome,chrome_preferences,file_system,firefox,msiecf,msie_zones,shell_items,winlnk,winprefetch,winreg,winevt,...}.py`). Port those as Go functions in a registry keyed by lowercased `IDENTIFIER` (mirror `FormattersManager`). 15 are small pure string transforms; `windows_eventlog_message` is the heavy one (see §5.4).

### 5.2 The Python `str.format` mini-engine — the top correctness risk

Go has no `str.format`. Write a small engine supporting what the templates actually use: `{name}`, `{name:spec}`, `{name!conv}`, `{name!conv:spec}`, with specs `:s`, `:d`, `:08x`, `:04x`, `:06d` (string, decimal, zero-padded hex/decimal with width). It **must**:
- Reproduce the **KeyError fallback**: a missing referenced attribute replaces the whole message with every `name: value` joined by spaces.
- Strip **all** `\r` and `\n` from the final string (via replace, not trim).
- Truncate short messages to `first-77 + "..."` only when `len > 80`.

Port the attribute-name regex **verbatim**: `{([a-z][a-zA-Z0-9_]*)[!]?[^:}]*[:]?[^}]*}`. The **lowercase-first-letter** constraint is load-bearing: a piece whose only placeholder starts non-lowercase (e.g. `{Name}`) is treated as pure text and therefore **always included** in conditional output.

### 5.3 Formatter types & helper semantics

- **Basic:** one `message`/`short_message` string.
- **Conditional:** join only pieces whose single attribute `is not None` (value `0`/`""`/`false` **still emit** — do not use Go zero-value truthiness), separator default `" "`; a piece may reference **at most one** attribute.
- **Helpers run before templating, mutating the value map in order:** boolean (truthy→`value_if_true` else `value_if_false`, always runs), enumeration (`values.get(input, default or input)`; skip when input nil; keep original when default nil and no hit), flags (bitwise-AND over an int `values` map, join matches with `', '`; skip when input nil), custom (dispatch to Go helper).
- **Default formatter** (no match for a `data_type`): `"<WARNING DEFAULT FORMATTER> Attributes: {k: v ...}"` / `"<DEFAULT> ..."`, skipping reserved names, identifier values and datetime values; log a warning. Parity matters for diffing.

### 5.4 The `winevt_rc` resource DB

Windows EventLog messages are **not** stored as text; they are reconstructed from a resource DB with two backends selected at construction:
- **(A) storage-embedded:** when the `.plaso` file has `windows_eventlog_provider` containers → read `windows_eventlog_{provider,message_file,message_table,message_string}` (+ `windows_wevt_template_event` for event→message-id mapping); return `message_strings[0].string` **without** PEP3101 conversion.
- **(B) external `winevt-rc.db`:** a separate acstore SQLite store (`format_version 20240929`, `string_format ∈ {'wrc','pep3101'}`) at `data_location/winevt-rc.db` → read `winevtrc_*` containers; return `.text`, applying `FormatMessageStringInPEP3101` **only when** `string_format == 'wrc'`.

Reproduce: the **65536-entry LRU** (`container/list` + map), key format `"{provider}:0x{msgid:08x}[:{ver}]"` and a parallel `log_source` key; provider lookup by GUID then log_source (lowercased); MUI/language-tag message-file path variant; the 5-pass **WRC→PEP3101** conversion (`resource_files.py`); and `%%N` parameter expansion before `.format(*strings)`. Watch the **`.string` (storage) vs `.text` (DB)** attribute-name difference.

**This whole subsystem is deferrable** — it only affects Windows EventLog `data_type`s. Ship a stub that returns the un-resolved message for v1 and golden-test against a `.plaso` without EVTX events first.

---

## 6. Recommended FIRST vertical slice

**Goal:** read `test_data/psort_test.plaso` and emit **byte-identical** output to real Python `psort` for **one** output module, proving the entire read→order→dedup→format→write stack.

**Output module: `dynamic`.** Rationale: it is psort's **default** (`-o` defaults to `dynamic`), so `psort -w out.csv input.plaso` with no flags is the oracle; and it exercises the *entire* output stack — field callbacks, the YAML message-formatter pipeline, the `OutputMediator` (source/hostname/username/tz/MACB/display-name), DSV sanitization, and the `SortedStringHeap` within-timestamp re-sort. Every other text format is a specialization of this machinery.

**Default `dynamic` fields:** `datetime,timestamp_desc,source,source_long,message,parser,display_name,tag`.

**In scope for the slice:**
- `internal/plasostore`: open RO + metadata validate; `GetSortedEvents` (no time range); resolve event_data/event_data_stream by rowid; blob zlib+JSON decode; event_tag full-scan cache. (Scaffold already does most of this — finish/verify it.)
- `internal/container` + `internal/jsonwire`: typed-value decode (bytes=quoted-printable, tuple, Counter, DateTimeValues, PathSpec).
- `internal/dftime`: enough to render `datetime` as microsecond ISO8601 with offset for the classes present in `psort_test.plaso` (expect Posix*, Filetime, TimeElements*, and the semantic family) + the `0`-timestamp sentinel `0000-00-00T00:00:00.000000+00:00`.
- `internal/dfpath`: PathSpec model + `display_name` derivation (TYPE:relative-path, VSS prefix, GZIP/compressed handling).
- `internal/pipeline`: serial path first (defer the worker pool), exact heap + dedup + MACB.
- `internal/formatters`: YAML loader + basic/conditional + the `str.format` mini-engine + the 4 helper kinds + default formatter. Port **only the custom helpers whose `data_type`s actually appear** in `psort_test.plaso`.
- `internal/mediator`: hostname/username/source/MACB/timezone (UTC default).
- `internal/output`: `OutputModule` interface, `dynamic` DSV module, DSV sanitizer, `SortedStringHeap`, file writer (refuse to overwrite existing file → match the `OSError` behavior).
- `cmd/goplaso-psort`: minimal — positional input, `-w output`.

**Explicitly deferred:** event filters, `--slice`/`--slicer` time-slicing, all analysis plugins, multiprocessing/worker-pool (add after correctness), every other output module, `winevt_rc` (stub it), `--output_time_zone`/`--dynamic_time`/`--fields`/`--additional_fields`/`--custom_fields` (hardcode defaults; add after parity), and recomputing `event_values_hash` (read it).

**Differential test (the acceptance gate):**
```
python -m plaso.cli.psort_tool -o dynamic -w /tmp/py.csv  test_data/psort_test.plaso
goplaso-psort                  -o dynamic -w /tmp/go.csv  test_data/psort_test.plaso
diff /tmp/py.csv /tmp/go.csv   # must be empty
```
Put this in `internal/testutil` as a golden harness that shells out to Python. The slice is "done" when `diff` is empty on `psort_test.plaso`.

---

## 7. Milestones

| # | Milestone | Scope | Verifiable deliverable |
|---|---|---|---|
| **M0** | Reader hardening | Finish `plasostore`: metadata gate, schema-vs-blob dispatch, rowid joins, LRU, quoted-printable + typed-value decode; hardcode known schemas | `reader_test.go` dumps every container from `psort_test.plaso`; counts match `pinfo`/Python |
| **M1** | **First slice: `dynamic`, serial** | §6 | `diff` empty vs Python `psort -o dynamic` on `psort_test.plaso` |
| **M2** ✅ | `null` + `rawpy` | Prove the engine contract with the trivial no-op module; `rawpy` needs no message formatting except display_name | **done** — byte-identical `rawpy`, `null` produces nothing |
| **M3** ✅ | DSV family: `l2tcsv`, `tln`, `l2ttln` | Field-set/delimiter/header specialization; l2tcsv MACB-collapse + `extra` | **done** — byte-identical for all three vs Python (dedup + `-a`) |
| **M4** ✅ | JSON family: `json_line`, `json` | Python-exact encoder (`ensure_ascii \uXXXX`, `', '`/`': '`, sort_keys), nested PathSpec/DateTime dicts, no textual sub-sort | **done** — byte-identical JSON output |
| **M5** ✅ | `dftime` + timezone | Corpus classes rendered precisely (TimeElements, PosixTimeIn{Micro,Nano}seconds) with a microsecond fallback for others; `--output_time_zone`, `--dynamic_time`, local-time coercion, fraction-of-second, semantic `.string` | **done** — byte-identical against `America/New_York` + dynamic-time fixtures (DST-correct). Remaining ~28-class breadth tracked under M10 |
| **M6** | CLI config surface | `--fields`, `--additional_fields`, `--custom_fields`, `--output_time_zone`, `--dynamic_time`, fallback hostname/path flags | matches Python for each flag combination |
| **M7** | **Parallel hot path** | Worker pool for decode/join/format with in-order reassembly; mediator cache mutexes; status ticker | identical output to M1–M4; ≥5× speedup on a large `.plaso`, benchmarked |
| **M8** | Event filters + time slicing | `internal/filter` (lexer/parser→tree→`Matches`), `TimeSlice`/`TimeRange`, filter-before-tag ordering, dead `limit` omitted | differential tests driven by `tests/filters/*` as oracle |
| **M9** | `winevt_rc` resource DB | Both backends, LRU, WRC→PEP3101, MUI/language resolution | byte-identical on an EVTX-heavy `.plaso` + external `winevt-rc.db` |
| **M10** | Feature parity sweep | Remaining custom helpers; all `data_type`s; large real-world corpus | zero diffs across a broad corpus; perf report |

---

## 8. Key risks & correctness pitfalls

1. **dfdatetime semantics** — the deepest risk. ISO8601 is **always microsecond precision with explicit ±HH:MM offset** in non-dynamic mode. Port `GetDateWithTimeOfDay` (returns UTC components) + the `astimezone` splice **rather than** a single `time.Time` conversion, or offsets near DST diverge. Honor `is_local_time` (coerce to PosixTime 1s / PosixTimeInMicroseconds), `precision`, `time_zone_offset` (minutes), `time_zone_hint`, `is_delta`. Semantic times (`InvalidTime`/`Never`/`NotSet`/`SemanticTime`) normalize to `nil`/`0` and all land in the `timestamp==0` bucket. Class-conditional key stripping in the deserializer (`is_delta` only for TimeElements*; `time_zone_offset` dropped for `GolangTime`/`RFC2579DateTime`; `string` dropped for Invalid/Never/NotSet) must be replicated.
2. **Timestamp precision / rounding** — stored `timestamp` is `int64` POSIX **microseconds**, **signed**. If ever recomputing from `date_time`, replicate `GetPlasoTimestamp`: `Decimal(seconds) * 1e6` quantized **ROUND_HALF_UP**; float64 mis-rounds. Sub-microsecond precision is lost in the int column but preserved in the blob (so `--dynamic_time` can show more digits than the sort key). For pure psort read we **use the stored column** — do not recompute.
3. **Dedup key exactness** — compare `(timestamp_desc, event_values_hash)` to the **immediately preceding kept** event, **only within one timestamp flush**. Missing hash → `"UNKNOWN"`; missing desc → `"Unknown Time"`. The 100000-overflow flush can split a run — match it.
4. **MACB set is exactly 4 strings** — hardcode the exact `TIME_DESCRIPTION_*` constants; `fs:stat` is special (split `timestamp_desc` on `;`). Any other description emits individually.
5. **Timezone handling** — output mediator `SetTimeZone` accepts **IANA names only** (use `time.LoadLocation`; embed `time/tzdata`). Verify `tzname()` parity with `pytz` for abbreviations. `UTC` → render `"UTC"`; error/0 → `"-"`.
6. **Encoding pitfalls** —
   - Stored bytes attributes are **quoted-printable** (`a2b_qp`), **not** hex, despite the `"base16"` label.
   - **JSON output modules need a custom encoder**: Python `json.dumps` uses `', '`/`': '` separators, `ensure_ascii=True` (`café`→`caf\u00e9`), and does **not** HTML-escape. Go's `encoding/json` does the opposite on all three — a hand-written marshaller (sorted keys, spaces, `\uXXXX` for every rune >0x7F) is mandatory for `json`/`json_line` parity.
   - Text modules write UTF-8 with `errors='backslashreplace'`; replicate for non-UTF-8 byte attributes.
7. **Byte-for-byte output matching** — the `SortedStringHeap` must flush at the **same boundaries** (primary-sort-key change or 100000) or equal-timestamp sub-ordering drifts. DSV sanitizer: replace delimiter/`\r`/`\n` with space; prefix `'` only when `len>1` and first char ∈ `{=,+,-,@}` (a lone `-` placeholder is left intact). Short-message truncation `77+"..."`. CSV "filename" column in l2tcsv is the **display name**, not `event_data.filename`.
8. **Schema not in file** — hardcode each table's columns in **sorted key order** to match acstore's `SELECT` column order and `_MAPPINGS` (bool/int→INTEGER, str→TEXT, timestamp→BIGINT, else TEXT/JSON).
9. **Version/compat note for the "update plaso" goal** — `sqlite_file.py:414` calls module-level `sqlite_store.PythonAST2SQL`, but current upstream acstore (20260709) exposes only the class `PythonAST2SQLHelper`. This fork expects an older/patched acstore. It is only hit when filtering schema-less/`event_data` containers (not psort's hot path) but will `AttributeError` if hit — resolve the acstore pin when bringing plaso up to date, and the Go port should just implement the filter→SQL translation directly (`==`→` = `, `!=`→` <> `, string constants double-quoted).
10. **Regex dialect** (filters, M8) — plaso uses Python `re`; Go `regexp` is RE2 (no backreferences/lookaround). User `regexp`/`iregexp` filters using those will behave differently — document or vendor a PCRE engine.

---

## 9. Recommended Go dependencies

| Module | Justification |
|---|---|
| `modernc.org/sqlite` | Pure-Go, cgo-free SQLite driver; already in `go.mod`. Reads `.plaso` and `winevt-rc.db` via `database/sql`. |
| `database/sql` (stdlib) | SQLite access layer. |
| `gopkg.in/yaml.v3` | Multi-document (`Decoder` loop) parse of `data/formatters/*.yaml`; decode enum/flags `values` int keys (incl. `0x` hex) into `map[int64]string`. |
| `encoding/json` + `json.Number` (stdlib) | Decode attribute-container JSON losslessly (`UseNumber()` for >2⁵³ nanosecond timestamps). Base layer only — a **custom marshaller** sits on top for Python-exact JSON *output*. |
| `compress/zlib` (stdlib) | Inflate `event_data` `_data` blobs (Python `zlib.compress` = standard zlib-wrapped DEFLATE). |
| `mime/quotedprintable` (stdlib) *or* hand-rolled `a2b_qp` | Decode `bytes` attributes (quoted-printable). Validate against `binascii.a2b_qp` edge cases. |
| `crypto/md5` + `encoding/hex` (stdlib) | Only if recomputing `event_values_hash` (missing-hash path / re-hash tooling). |
| `container/heap` (stdlib) | `PsortEventHeap` and the output `SortedStringHeap` (or `sort.Slice` on equal-timestamp runs). |
| `container/list` (stdlib) + map | 65536-entry LRU for winevt message strings (M9). |
| `time` + `time/tzdata` (stdlib, embedded) | IANA timezones, replacing `pytz`; embed tzdata so the binary is self-contained. |
| `regexp` (stdlib, RE2) | Formatter attribute-name regex, WRC→PEP3101 passes, DSV; and (M8) the filter lexer — note RE2 limits for user `regexp` filters. |
| `go:embed` (stdlib) | Bundle `data/formatters/*.yaml` into the binary with a runtime override path. |
| `golang.org/x/text/encoding` (optional) | Only if replicating `backslashreplace` for non-UTF-8 byte attributes needs more than a manual pass. |

Deliberately **avoided for the core:** `github.com/mattn/go-sqlite3` (cgo; only if `modernc` proves too slow), `shopspring/decimal` / `math/big` (only if the port ever recomputes timestamps — not needed for read-only psort), `participle`/`goyacc` (filter parser, M8, optional — a hand-written recursive-descent parser over the ~12-production grammar is simpler), and any OpenSearch/xlsx/redis client (out-of-scope modules).

---

### Immediate next action for the implementation team

Finish **M0** on the existing `goplaso/internal/plasostore/reader.go` + `container/container.go`, then build **M1** (`dynamic`, serial) to a clean `diff` against `python -m plaso.cli.psort_tool -o dynamic -w … test_data/psort_test.plaso`. That single passing diff validates the reader, the typed-JSON decode, dftime rendering, the heap/dedup/MACB semantics, the formatter engine and the DSV writer at once — everything after it is specialization and parallelization.