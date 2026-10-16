// Command goplaso-psort is a Go reimplementation of plaso's psort, starting
// with the dynamic output module emittable as native CSV (byte-compatible with
// Python psort, used as the differential-test oracle) or as JSON lines.
//
// Usage:
//
//	goplaso-psort [-o dynamic|dynamic-jsonl] [-w OUTPUT] [-a] [--data_location DIR] STORAGE.plaso
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Get-Sybers/plaso/goplaso/internal/formatters"
	"github.com/Get-Sybers/plaso/goplaso/internal/mediator"
	"github.com/Get-Sybers/plaso/goplaso/internal/output"
	"github.com/Get-Sybers/plaso/goplaso/internal/pipeline"
	"github.com/Get-Sybers/plaso/goplaso/internal/plasostore"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		outputFormat = flag.String("o", "dynamic", "output format: dynamic | dynamic-jsonl")
		writePath    = flag.String("w", "", "output file (default: stdout)")
		dataLocation = flag.String("data_location", "", "plaso data directory (contains formatters/)")
		includeAll   = flag.Bool("a", false, "include all events (disable deduplication)")
	)
	flag.Parse()

	if flag.NArg() < 1 {
		return fmt.Errorf("missing storage file (usage: goplaso-psort [-o FORMAT] [-w OUT] STORAGE.plaso)")
	}
	storagePath := flag.Arg(flag.NArg() - 1)

	var mode pipeline.Mode
	switch *outputFormat {
	case "dynamic":
		mode = pipeline.ModeCSV
	case "dynamic-jsonl", "dynamic_jsonl", "jsonl":
		mode = pipeline.ModeJSONL
	default:
		return fmt.Errorf("unsupported output format: %q (supported: dynamic, dynamic-jsonl)", *outputFormat)
	}

	formattersDir, err := resolveFormattersDir(*dataLocation)
	if err != nil {
		return err
	}
	formatterSet := formatters.NewSet()
	if err := formatterSet.LoadDir(formattersDir); err != nil {
		return fmt.Errorf("loading formatters from %s: %w", formattersDir, err)
	}

	reader, err := plasostore.Open(storagePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	med := mediator.New(formatterSet)
	exporter := &pipeline.Exporter{
		Reader:  reader,
		Dynamic: output.NewDynamic(med),
		Mode:    mode,
		Dedup:   !*includeAll,
	}

	out := os.Stdout
	if *writePath != "" {
		// Mirror plaso: refuse to overwrite an existing output file.
		if _, err := os.Stat(*writePath); err == nil {
			return fmt.Errorf("unable to use an already existing file for output [%s]", *writePath)
		}
		f, err := os.Create(*writePath)
		if err != nil {
			return err
		}
		defer f.Close()
		out = f
	}

	return exporter.Export(out)
}

// resolveFormattersDir returns the directory holding the formatter YAML files.
// With --data_location DIR it uses DIR/formatters; otherwise it searches the
// current directory and its ancestors for plaso/data/formatters.
func resolveFormattersDir(dataLocation string) (string, error) {
	if dataLocation != "" {
		dir := filepath.Join(dataLocation, "formatters")
		if isDir(dir) {
			return dir, nil
		}
		if isDir(dataLocation) {
			return dataLocation, nil // already points at formatters/
		}
		return "", fmt.Errorf("no formatters directory under data location %q", dataLocation)
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for dir := cwd; ; {
		candidate := filepath.Join(dir, "plaso", "data", "formatters")
		if isDir(candidate) {
			return candidate, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("could not locate plaso/data/formatters; pass --data_location")
}

func isDir(p string) bool {
	info, err := os.Stat(p)
	return err == nil && info.IsDir()
}
