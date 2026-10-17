package pipeline_test

import (
	"io"
	"os"
	"testing"

	"github.com/Get-Sybers/plaso/goplaso/internal/formatters"
	"github.com/Get-Sybers/plaso/goplaso/internal/mediator"
	"github.com/Get-Sybers/plaso/goplaso/internal/output"
	"github.com/Get-Sybers/plaso/goplaso/internal/pipeline"
	"github.com/Get-Sybers/plaso/goplaso/internal/plasostore"
)

// BenchmarkExport runs the dynamic exporter over the store named by GOPLASO_BIG.
// Not a golden test; a throwaway perf harness (skips if the env var is unset).
func BenchmarkExport(b *testing.B) {
	store := os.Getenv("GOPLASO_BIG")
	if store == "" {
		b.Skip("set GOPLASO_BIG to a large .plaso")
	}
	fdir := os.Getenv("GOPLASO_FMT")
	for i := 0; i < b.N; i++ {
		fset := formatters.NewSet()
		if err := fset.LoadDir(fdir); err != nil {
			b.Fatal(err)
		}
		r, err := plasostore.Open(store)
		if err != nil {
			b.Fatal(err)
		}
		exp := &pipeline.Exporter{Reader: r, Module: output.NewDynamic(mediator.New(fset)), Dedup: true, Workers: 1}
		if err := exp.Export(io.Discard); err != nil {
			b.Fatal(err)
		}
		r.Close()
	}
}
