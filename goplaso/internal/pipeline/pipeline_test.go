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

// moduleCase describes one output module and its golden fixtures.
type moduleCase struct {
	name     string                                 // psort -o NAME
	factory  func(*mediator.Mediator) output.Module // Go module under test
	dedup    string                                 // golden file for deduplicated output ("" = no golden)
	all      string                                 // golden file for -a output ("" = no golden)
	noGolden bool                                   // module produces no output (null)
}

func modules() []moduleCase {
	return []moduleCase{
		{name: "dynamic", factory: func(m *mediator.Mediator) output.Module { return output.NewDynamic(m) }, dedup: "dynamic.csv", all: "dynamic_all.csv"},
		{name: "l2tcsv", factory: func(m *mediator.Mediator) output.Module { return output.NewL2TCSV(m) }, dedup: "l2tcsv.csv", all: "l2tcsv_all.csv"},
		{name: "tln", factory: func(m *mediator.Mediator) output.Module { return output.NewTLN(m) }, dedup: "tln.txt", all: "tln_all.txt"},
		{name: "l2ttln", factory: func(m *mediator.Mediator) output.Module { return output.NewL2TTLN(m) }, dedup: "l2ttln.txt", all: "l2ttln_all.txt"},
		{name: "rawpy", factory: func(m *mediator.Mediator) output.Module { return output.NewRawpy(m) }, dedup: "rawpy.txt", all: "rawpy_all.txt"},
		{name: "null", factory: func(m *mediator.Mediator) output.Module { return output.NewNull() }, noGolden: true},
	}
}

func export(t *testing.T, factory func(*mediator.Mediator) output.Module, dedup bool, workers int) []byte {
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
		Module:  factory(mediator.New(fset)),
		Dedup:   dedup,
		Workers: workers,
	}
	var buf bytes.Buffer
	if err := exp.Export(&buf); err != nil {
		t.Fatalf("export: %v", err)
	}
	return buf.Bytes()
}

// TestGoldens diffs every module against the committed Python psort fixtures,
// for both the deduplicated and the include-all (-a) cases.
func TestGoldens(t *testing.T) {
	for _, mc := range modules() {
		mc := mc
		t.Run(mc.name, func(t *testing.T) {
			if mc.noGolden {
				if got := export(t, mc.factory, true, 0); len(got) != 0 {
					t.Errorf("%s produced %d bytes, want empty", mc.name, len(got))
				}
				if got := export(t, mc.factory, false, 0); len(got) != 0 {
					t.Errorf("%s -a produced %d bytes, want empty", mc.name, len(got))
				}
				return
			}
			for _, v := range []struct {
				golden string
				dedup  bool
			}{{mc.dedup, true}, {mc.all, false}} {
				golden, err := os.ReadFile(filepath.Join("testdata", v.golden))
				if err != nil {
					t.Fatal(err)
				}
				got := export(t, mc.factory, v.dedup, 0)
				if !bytes.Equal(got, golden) {
					t.Errorf("%s (dedup=%v) differs from golden %s\n%s", mc.name, v.dedup, v.golden, firstDiff(golden, got))
				}
			}
		})
	}
}

// TestSerialParallelEquivalence asserts the worker count never changes output.
func TestSerialParallelEquivalence(t *testing.T) {
	for _, mc := range modules() {
		if mc.noGolden {
			continue
		}
		mc := mc
		t.Run(mc.name, func(t *testing.T) {
			base := export(t, mc.factory, true, 1)
			for _, w := range []int{2, 4, 16} {
				got := export(t, mc.factory, true, w)
				if !bytes.Equal(got, base) {
					t.Errorf("%s workers=%d differs from serial\n%s", mc.name, w, firstDiff(base, got))
				}
			}
		})
	}
}

func TestDynamicJSONLDeliverable(t *testing.T) {
	factory := func(m *mediator.Mediator) output.Module { return output.NewDynamicJSONL(m) }
	got := export(t, factory, true, 0)
	lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")
	if len(lines) != 20 {
		t.Fatalf("got %d JSONL rows, want 20", len(lines))
	}
	wantOrder := output.DynamicFieldNames
	for i, line := range lines {
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("row %d invalid JSON: %v", i, err)
		}
		if len(obj) != len(wantOrder) {
			t.Errorf("row %d has %d fields, want %d", i, len(obj), len(wantOrder))
		}
		if keys := jsonKeyOrder(line); !equalStrings(keys, wantOrder) {
			t.Errorf("row %d key order = %v, want %v", i, keys, wantOrder)
		}
	}
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
	for _, mc := range modules() {
		if mc.noGolden {
			continue
		}
		mc := mc
		for _, dedup := range []bool{true, false} {
			t.Run(mc.name+dedupSuffix(dedup), func(t *testing.T) {
				pyOut := runPython(t, mc.name, dedup)
				got := export(t, mc.factory, dedup, 0)
				if !bytes.Equal(got, pyOut) {
					t.Errorf("Go %s (dedup=%v) differs from live Python psort\n%s", mc.name, dedup, firstDiff(pyOut, got))
				}
			})
		}
	}
}

func dedupSuffix(dedup bool) string {
	if dedup {
		return "/dedup"
	}
	return "/all"
}

func runPython(t *testing.T, module string, dedup bool) []byte {
	t.Helper()
	tmp := filepath.Join(t.TempDir(), "py.out")
	args := []string{filepath.Join("plaso", "scripts", "psort.py"), "-q", "-o", module, "-w", tmp}
	if !dedup {
		args = append(args, "-a")
	}
	args = append(args, filepath.Join("test_data", "psort_test.plaso"))
	cmd := exec.Command("python3", args...)
	cmd.Dir = repoRoot
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("python psort unavailable: %v\n%s", err, out)
	}
	data, err := os.ReadFile(tmp)
	if err != nil {
		// null writes no file.
		return nil
	}
	return data
}

func jsonKeyOrder(line string) []string {
	dec := json.NewDecoder(strings.NewReader(line))
	dec.Token()
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		if k, ok := tok.(string); ok {
			keys = append(keys, k)
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
