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
```

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
| `internal/pipeline` | export loop: timestamp buckets, `(hash, desc)` order, dedup, MACB passthrough, textual sub-sort | `plaso/multi_process/output_engine` |
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
milestones (M0 reader → M1 dynamic *(done)* → other output modules → full
dfdatetime/timezone → CLI surface → parallel hot path → filters → winevt_rc →
parity sweep).

Current simplifications (tracked for later milestones): output time zone is UTC
only; `dftime` covers the classes in the test corpus (`TimeElements`,
`PosixTimeInNanoseconds`); `dfpath` covers flat path specs; event filters,
time-slicing, analysis plugins, the parallel worker pool, and other output
modules are not yet implemented.
