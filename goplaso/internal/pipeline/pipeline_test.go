package pipeline_test

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Get-Sybers/plaso/goplaso/internal/formatters"
	"github.com/Get-Sybers/plaso/goplaso/internal/mediator"
	"github.com/Get-Sybers/plaso/goplaso/internal/output"
	"github.com/Get-Sybers/plaso/goplaso/internal/pipeline"
	"github.com/Get-Sybers/plaso/goplaso/internal/plasostore"
)

var (
	repoRoot      = filepath.Join("..", "..", "..")
	testStore     = filepath.Join(repoRoot, "test_data", "psort_test.plaso")
	formattersDir = filepath.Join(repoRoot, "plaso", "data", "formatters")
)

func export(t *testing.T, mode pipeline.Mode, dedup bool) []byte {
	t.Helper()
	fset := formatters.NewSet()
	if err := fset.LoadDir(formattersDir); err != nil {
		t.Fatalf("load formatters: %v", err)
	}
	reader, err := plasostore.Open(testStore)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer reader.Close()

	exp := &pipeline.Exporter{
		Reader:  reader,
		Dynamic: output.NewDynamic(mediator.New(fset)),
		Mode:    mode,
		Dedup:   dedup,
	}
	var buf bytes.Buffer
	if err := exp.Export(&buf); err != nil {
		t.Fatalf("export: %v", err)
	}
	return buf.Bytes()
}

func TestDynamicCSVMatchesGolden(t *testing.T) {
	golden, err := os.ReadFile(filepath.Join("testdata", "dynamic.csv"))
	if err != nil {
		t.Fatal(err)
	}
	got := export(t, pipeline.ModeCSV, true)
	if !bytes.Equal(got, golden) {
		t.Errorf("dynamic CSV (dedup) differs from Python golden\n%s", firstDiff(golden, got))
	}
}

func TestDynamicCSVAllMatchesGolden(t *testing.T) {
	golden, err := os.ReadFile(filepath.Join("testdata", "dynamic_all.csv"))
	if err != nil {
		t.Fatal(err)
	}
	got := export(t, pipeline.ModeCSV, false)
	if !bytes.Equal(got, golden) {
		t.Errorf("dynamic CSV (include-all) differs from Python golden\n%s", firstDiff(golden, got))
	}
}

func TestJSONLDeliverable(t *testing.T) {
	got := export(t, pipeline.ModeJSONL, true)
	lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")
	if len(lines) != 20 {
		t.Fatalf("got %d JSONL rows, want 20", len(lines))
	}

	wantOrder := output.DynamicFieldNames
	for i, line := range lines {
		// Valid JSON object.
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("row %d invalid JSON: %v", i, err)
		}
		if len(obj) != len(wantOrder) {
			t.Errorf("row %d has %d fields, want %d", i, len(obj), len(wantOrder))
		}
		// Field key order preserved (JSONL is the dynamic field order).
		if keys := jsonKeyOrder(line); !equalStrings(keys, wantOrder) {
			t.Errorf("row %d key order = %v, want %v", i, keys, wantOrder)
		}
	}

	// Raw (un-sanitized) message: comma preserved, unlike the CSV form.
	if !strings.Contains(string(got), "[client, pid: 30840]") {
		t.Errorf("expected raw comma-containing message in JSONL output")
	}
}

// TestDifferentialAgainstPython regenerates the oracle from live Python psort
// and diffs, when GOPLASO_PSORT_ORACLE=1 and python3+plaso are available.
func TestDifferentialAgainstPython(t *testing.T) {
	if os.Getenv("GOPLASO_PSORT_ORACLE") != "1" {
		t.Skip("set GOPLASO_PSORT_ORACLE=1 to run the live Python differential test")
	}
	tmp := filepath.Join(t.TempDir(), "py.csv")
	// Paths are relative to cmd.Dir (the repo root).
	cmd := exec.Command("python3", filepath.Join("plaso", "scripts", "psort.py"),
		"-o", "dynamic", "-w", tmp, filepath.Join("test_data", "psort_test.plaso"))
	cmd.Dir = repoRoot
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("python psort unavailable: %v\n%s", err, out)
	}
	pyOut, err := os.ReadFile(tmp)
	if err != nil {
		t.Fatal(err)
	}
	got := export(t, pipeline.ModeCSV, true)
	if !bytes.Equal(got, pyOut) {
		t.Errorf("Go output differs from live Python psort\n%s", firstDiff(pyOut, got))
	}
}

func jsonKeyOrder(line string) []string {
	dec := json.NewDecoder(strings.NewReader(line))
	dec.Token() // opening {
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		if k, ok := tok.(string); ok {
			keys = append(keys, k)
			// skip the value
			var v any
			_ = dec.Decode(&v)
		}
	}
	return keys
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func firstDiff(want, got []byte) string {
	wl := strings.Split(string(want), "\n")
	gl := strings.Split(string(got), "\n")
	for i := 0; i < len(wl) && i < len(gl); i++ {
		if wl[i] != gl[i] {
			return "line " + itoa(i+1) + ":\n  want: " + wl[i] + "\n  got:  " + gl[i]
		}
	}
	if len(wl) != len(gl) {
		return "line counts differ: want " + itoa(len(wl)) + " got " + itoa(len(gl))
	}
	return "(no line-level diff found)"
}

func itoa(i int) string { return strconv.Itoa(i) }
