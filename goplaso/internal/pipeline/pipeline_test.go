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
		{name: "json_line", factory: func(m *mediator.Mediator) output.Module { return output.NewJSONLine(m) }, dedup: "json_line.txt", all: "json_line_all.txt"},
		{name: "json", factory: func(m *mediator.Mediator) output.Module { return output.NewJSON(m) }, dedup: "json.txt", all: "json_all.txt"},
		{name: "null", factory: func(m *mediator.Mediator) output.Module { return output.NewNull() }, noGolden: true},
	}
}

// outputConfig configures the mediator for an export (output time zone and the
// dynamic-time flag).
type outputConfig struct {
	tz          string
	dynamicTime bool
}

func export(t *testing.T, factory func(*mediator.Mediator) output.Module, dedup bool, workers int) []byte {
	return exportCfg(t, factory, dedup, workers, outputConfig{})
}

func exportCfg(t *testing.T, factory func(*mediator.Mediator) output.Module, dedup bool, workers int, cfg outputConfig) []byte {
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

	med := mediator.New(fset)
	if err := med.SetTimeZone(cfg.tz); err != nil {
		t.Fatalf("set time zone: %v", err)
	}
	med.DynamicTime = cfg.dynamicTime

	exp := &pipeline.Exporter{
		Reader:  reader,
		Module:  factory(med),
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

// tzCase describes a --output_time_zone / --dynamic_time golden.
type tzCase struct {
	name    string
	golden  string
	factory func(*mediator.Mediator) output.Module
	dedup   bool
	cfg     outputConfig
	module  string // psort -o NAME for the live oracle
}

func tzCases() []tzCase {
	dyn := func(m *mediator.Mediator) output.Module { return output.NewDynamic(m) }
	l2t := func(m *mediator.Mediator) output.Module { return output.NewL2TCSV(m) }
	tln := func(m *mediator.Mediator) output.Module { return output.NewTLN(m) }
	ny := outputConfig{tz: "America/New_York"}
	dt := outputConfig{dynamicTime: true}
	dtny := outputConfig{tz: "America/New_York", dynamicTime: true}
	return []tzCase{
		{"dynamic_nyc", "dynamic_nyc.csv", dyn, true, ny, "dynamic"},
		{"dynamic_nyc_all", "dynamic_nyc_all.csv", dyn, false, ny, "dynamic"},
		{"dynamic_dt", "dynamic_dt.csv", dyn, true, dt, "dynamic"},
		{"dynamic_dt_nyc", "dynamic_dt_nyc.csv", dyn, true, dtny, "dynamic"},
		{"l2tcsv_nyc", "l2tcsv_nyc.csv", l2t, true, ny, "l2tcsv"},
		{"tln_dt", "tln_dt.txt", tln, true, dt, "tln"},
	}
}

// TestTimeZoneAndDynamicTime diffs the --output_time_zone and --dynamic_time
// combinations against committed Python psort fixtures.
func TestTimeZoneAndDynamicTime(t *testing.T) {
	for _, tc := range tzCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			golden, err := os.ReadFile(filepath.Join("testdata", tc.golden))
			if err != nil {
				t.Fatal(err)
			}
			got := exportCfg(t, tc.factory, tc.dedup, 0, tc.cfg)
			if !bytes.Equal(got, golden) {
				t.Errorf("%s differs from golden %s\n%s", tc.name, tc.golden, firstDiff(golden, got))
			}
			// Worker count must not change output.
			if par := exportCfg(t, tc.factory, tc.dedup, 8, tc.cfg); !bytes.Equal(par, golden) {
				t.Errorf("%s workers=8 differs from golden", tc.name)
			}
		})
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
				pyOut := runPython(t, mc.name, dedup, nil)
				got := export(t, mc.factory, dedup, 0)
				if !bytes.Equal(got, pyOut) {
					t.Errorf("Go %s (dedup=%v) differs from live Python psort\n%s", mc.name, dedup, firstDiff(pyOut, got))
				}
			})
		}
	}
	for _, tc := range tzCases() {
		tc := tc
		t.Run("tz/"+tc.name, func(t *testing.T) {
			var flags []string
			if tc.cfg.tz != "" {
				flags = append(flags, "--output_time_zone", tc.cfg.tz)
			}
			if tc.cfg.dynamicTime {
				flags = append(flags, "--dynamic_time")
			}
			pyOut := runPython(t, tc.module, tc.dedup, flags)
			got := exportCfg(t, tc.factory, tc.dedup, 0, tc.cfg)
			if !bytes.Equal(got, pyOut) {
				t.Errorf("Go %s differs from live Python psort\n%s", tc.name, firstDiff(pyOut, got))
			}
		})
	}
}

func dedupSuffix(dedup bool) string {
	if dedup {
		return "/dedup"
	}
	return "/all"
}

func runPython(t *testing.T, module string, dedup bool, extraFlags []string) []byte {
	t.Helper()
	tmp := filepath.Join(t.TempDir(), "py.out")
	args := []string{filepath.Join("plaso", "scripts", "psort.py"), "-q", "-o", module, "-w", tmp}
	if !dedup {
		args = append(args, "-a")
	}
	args = append(args, extraFlags...)
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
