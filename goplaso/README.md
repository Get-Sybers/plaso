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

Events and their `event_data` blobs are **streamed** from SQLite in timestamp
order (a single ordered JOIN) and the per-event hot path (zlib inflate + JSON
decode, date_time parse, message formatting, output-line assembly) runs on a
bounded worker pool (`--workers`, default `NumCPU`). Decoded events are
discarded once formatted, so the Go heap is bounded by the in-flight window,
not the store size. The dedup/sort/write stage stays serial, so output is
byte-identical at any worker count.

### Performance

On a synthetic 114k-event store (4 vCPU), producing byte-identical output to
Python `psort -o dynamic` (best of 4 runs):

| | time | vs Python | peak RSS |
|---|---|---|---|
| Python `psort` | 10.2 s | 1.0× | — |
| `goplaso-psort --workers 1` | 3.7 s | 2.8× | ~50 MB |
| `goplaso-psort --workers 4` | 2.2 s | 4.6× | ~90 MB |
| `goplaso-psort --workers 8` | 2.05 s (1.5 s at `--gc_percent 800`) | 5–6.8× | ~200 MB |

Memory is bounded by the worker window (`--gc_percent` × the small live set),
**not** the event count: the previous load-everything design used ~900 MB on
this store; streaming uses ~90 MB and would stay flat on a 10×-larger file.

### Why not a different database engine?

Profiling shows the bottleneck is **not** SQLite. Reading rows is cheap — the
event scan and the full `event_data` BLOB scan are each ~130–450 ms via the
pure-Go driver, even with the ordered JOIN. The cost is CPU **decoding the
`event_data` blobs** (zlib inflate + JSON) and, above all, **GC pressure** from
the decode allocations. Any storage engine would hold the same zlib-compressed
JSON (it is the format `log2timeline` writes), so swapping the engine would not
move the needle — and a different *format* would break compatibility with the
plaso toolchain unless a conversion step were added (only worth it if the same
file is queried repeatedly).

The real levers, all within the SQLite format, applied here:
- **Parallel decode+format** on a worker pool, output lines precomputed in the
  workers so the serial stage only compares and writes them.
- **Streaming decode-and-discard** (ordered JOIN) so decoded events are never
  all held at once — cuts peak memory ~10× and removes the file-size ceiling.
- **GC tuning** (`--gc_percent`, default 400): the tight decode/discard loop
  keeps the live heap tiny, so the default GC (100) fires constantly and its
  assists stall the workers; letting the heap breathe is a ~2.5× throughput win
  at a modest, still-bounded memory cost. This was the single biggest factor in
  the parallel speedup.
- **`goccy/go-json`** for the blob JSON (~2.4× faster than `encoding/json`,
  drop-in, preserves `json.Number` precision for nanosecond timestamps).

## Package layout

| Package | Responsibility | Python origin |
|---|---|---|
| `internal/container` | Event / EventData / EventDataStream model, dfDateTime decode, lossless JSON (`json.Number`) | `plaso/containers`, `acstore` |
| `internal/plasostore` | `.plaso` SQLite reader: metadata gate, event+event_data streamed in timestamp order via one ordered JOIN, zlib+JSON blob decode, event-tag/stream indexes | `acstore/sqlite_store`, `plaso/storage/sqlite` |
| `internal/dftime` | dfdatetime display-microsecond rule (`_FormatDateTime`), ceil rounding | `dfdatetime` |
| `internal/dfpath` | dfVFS path-spec decode + display name | `dfvfs`, `plaso/engine/path_helper` |
| `internal/formatters` | YAML formatter loader, conditional/basic formatters, helpers, Python `str.format` mini-engine | `plaso/formatters` |
| `internal/mediator` | message-formatter / source-mapping / time-zone resolution | `plaso/output/mediator` |
| `internal/output` | `dynamic` field callbacks, DSV sanitizer + CSV, JSONL encoder, within-bucket sort key | `plaso/output/{formatting_helper,shared_dsv,dynamic,text_file}` |
| `internal/pipeline` | streaming export loop: fixed worker pool (decode→format→discard) with sequence-ordered reassembly, timestamp buckets, `(hash, desc)` dedup, MACB passthrough, textual sub-sort | `plaso/multi_process/output_engine` |
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
milestones. Done so far: **M1** (dynamic output, byte-identical), **M7**
(parallel decode+format hot path), and a **streaming, bounded-memory** engine
(constant-ish RSS regardless of store size). Still open: other output modules
(l2tcsv, tln, json/json_line), full dfdatetime + arbitrary output time zones,
the CLI config surface (`--fields`, time-slicing), event filters, and
`winevt_rc`.

Current simplifications (tracked for later milestones): output time zone is UTC
only; `dftime` covers the classes in the test corpus (`TimeElements`,
`PosixTimeInNanoseconds`, `PosixTimeInMicroseconds`); `dfpath` covers flat path
specs; event filters, time-slicing, analysis plugins, and other output modules
are not yet implemented. The pipeline formats every event before dedup (Python
formats only survivors); harmless and faster on low-duplicate data. The
event-tag and event-data-stream indexes are still loaded up front (small
relative to events); the bucket of equal-timestamp events is held transiently.
