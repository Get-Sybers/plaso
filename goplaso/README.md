# goplaso — a Go port of plaso, starting with `psort`

This is an in-progress Go reimplementation of [plaso](https://github.com/log2timeline/plaso),
beginning with **`psort`** — the timeline output/sort stage and the slowest part
of the Python pipeline (per-event blob decompress + JSON parse + message
formatting, single-threaded under the GIL).

The goal is **byte-for-byte compatible output** with Python `psort`, verified by
differential tests, while being able to parallelize the hot path in Go.

## Status — first vertical slice complete ✅

`goplaso-psort` reads a real `.plaso` SQLite storage file, time-orders and
deduplicates events, formats them through plaso's own `data/formatters/*.yaml`,
and emits the **`dynamic`** output module's default fields — either as native
`dynamic` CSV (**byte-identical to Python `psort`**, the differential-test
oracle) or as **JSON lines** ("dynamic as JSONL", the chosen deliverable).

Verified on `test_data/psort_test.plaso`: 38 events → 20 after MACB dedup,
byte-identical to Python `psort -o dynamic` (and `-a`), including the nanosecond
→ microsecond ceil rounding of the `datetime` field and the DSV comma
sanitization.

```
go run ./cmd/goplaso-psort -o dynamic       -w out.csv   --data_location ../plaso/data  ../test_data/psort_test.plaso
go run ./cmd/goplaso-psort -o dynamic-jsonl -w out.jsonl --data_location ../plaso/data  ../test_data/psort_test.plaso
go run ./cmd/goplaso-psort -o dynamic --workers 8 -w out.csv --data_location ../plaso/data in.plaso
```

The per-event hot path (event_data zlib inflate + JSON decode, date_time parse,
and message formatting) runs on a bounded worker pool (`--workers`, default
`NumCPU`); the reader and the dedup/sort/write stage stay serial, so output is
byte-identical at any worker count.

### Performance

On a synthetic 114k-event store (4 vCPU), producing byte-identical output to
Python `psort -o dynamic` (best of 4 runs):

| | time | vs Python |
|---|---|---|
| Python `psort` | 10.2 s | 1.0× |
| `goplaso-psort --workers 1` | 7.5 s | 1.4× |
| `goplaso-psort --workers 4` | 4.3 s | 2.4× |
| `goplaso-psort --workers 8` | 3.8 s | 2.7× |

Scaling is sub-linear because the read and the dedup/sort/write stage are
inherently serial (Amdahl's law); the parallel decode+format is what the worker
pool accelerates.

### Why not a different database engine?

Profiling shows the bottleneck is **not** SQLite. On the 114k store, reading
rows from SQLite is cheap — the event scan and the full `event_data` BLOB scan
are each ~130–220 ms via the pure-Go driver. The cost is CPU **decoding the
`event_data` blobs** (zlib inflate ≈ 1.5 s + JSON parse ≈ 1.3 s per 102k,
plus GC pressure from the decoded maps). Any storage engine would hold the same
zlib-compressed JSON (it's the format `log2timeline` writes), so swapping the
engine would not move the needle — and a different *format* would break
compatibility with the plaso toolchain unless a conversion step were added
(only worth it if the same file is queried repeatedly).

The real levers, all within the SQLite format, are: **(1)** parallel
decode+format (done — the worker pool); **(2)** a faster JSON decoder
(`goccy/go-json`, adopted — ~2.4× faster blob JSON, drop-in, preserves
`json.Number` precision); **(3)** ordering in Go instead of SQL `ORDER BY`
(done — plaso's schema has no `event(timestamp)` index, so SQL sort would
build a temp b-tree each run). A future win is a streaming decode-and-discard
design to cut the GC pressure from retaining every decoded event.

## Package layout

| Package | Responsibility | Python origin |
|---|---|---|
| `internal/container` | Event / EventData / EventDataStream model, dfDateTime decode, lossless JSON (`json.Number`) | `plaso/containers`, `acstore` |
| `internal/plasostore` | `.plaso` SQLite reader (metadata gate, zlib+JSON blobs, rowid joins, event-tag cache, timestamp-ordered stream) | `acstore/sqlite_store`, `plaso/storage/sqlite` |
| `internal/dftime` | dfdatetime display-microsecond rule (`_FormatDateTime`), ceil rounding | `dfdatetime` |
| `internal/dfpath` | dfVFS path-spec decode + display name | `dfvfs`, `plaso/engine/path_helper` |
| `internal/formatters` | YAML formatter loader, conditional/basic formatters, helpers, Python `str.format` mini-engine | `plaso/formatters` |
| `internal/mediator` | message-formatter / source-mapping / time-zone resolution | `plaso/output/mediator` |
| `internal/output` | `dynamic` field callbacks, DSV sanitizer + CSV, JSONL encoder, within-bucket sort key | `plaso/output/{formatting_helper,shared_dsv,dynamic,text_file}` |
| `internal/pipeline` | export loop: timestamp buckets, `(hash, desc)` order, dedup, MACB passthrough, textual sub-sort, bounded worker pool with in-order reassembly | `plaso/multi_process/output_engine` |
| `cmd/goplaso-psort` | CLI | `plaso/cli/psort_tool`, `plaso/scripts/psort` |

## Testing

```
go test ./...                                    # hermetic: diffs against committed golden fixtures
GOPLASO_PSORT_ORACLE=1 go test ./internal/pipeline/   # live: diffs against a fresh Python psort run
```

The golden fixtures in `internal/pipeline/testdata/` were produced by Python
`psort` (plaso v20260928); see that directory's README to regenerate.

## Roadmap

See [`docs/PORT_PLAN.md`](docs/PORT_PLAN.md) for the full architecture map and
milestones. Done so far: **M1** (dynamic output, byte-identical) and **M7**
(parallel decode+format hot path). Still open: other output modules (l2tcsv,
tln, json/json_line), full dfdatetime + arbitrary output time zones, the CLI
config surface (`--fields`, time-slicing), event filters, and `winevt_rc`.

Current simplifications (tracked for later milestones): output time zone is UTC
only; `dftime` covers the classes in the test corpus (`TimeElements`,
`PosixTimeInNanoseconds`, `PosixTimeInMicroseconds`); `dfpath` covers flat path
specs; event filters, time-slicing, analysis plugins, and other output modules
are not yet implemented. The parallel path formats every event before dedup
(Python formats only survivors); harmless and faster on low-duplicate data, but
a future tweak can defer formatting to post-dedup.
