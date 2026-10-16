package plasostore

import (
	"encoding/json"
	"path/filepath"
	"sort"
	"testing"

	"github.com/Get-Sybers/plaso/goplaso/internal/container"
)

// testStore is the repo's canonical psort test fixture.
var testStore = filepath.Join("..", "..", "..", "test_data", "psort_test.plaso")

func openTest(t *testing.T) *Reader {
	t.Helper()
	r, err := Open(testStore)
	if err != nil {
		t.Fatalf("Open(%s): %v", testStore, err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func TestMetadata(t *testing.T) {
	r := openTest(t)
	meta := r.Metadata()
	if meta["compression_format"] != "zlib" {
		t.Errorf("compression_format = %q, want zlib", meta["compression_format"])
	}
	if meta["serialization_format"] != "json" {
		t.Errorf("serialization_format = %q, want json", meta["serialization_format"])
	}
	if meta["format_version"] != "20260516" {
		t.Errorf("format_version = %q, want 20260516", meta["format_version"])
	}
}

func TestCountEvents(t *testing.T) {
	r := openTest(t)
	n, err := r.CountEvents()
	if err != nil {
		t.Fatal(err)
	}
	if n != 38 {
		t.Errorf("CountEvents = %d, want 38", n)
	}
}

func TestEachResolvesAndOrders(t *testing.T) {
	r := openTest(t)

	var (
		count     int
		lastTS    int64 = -1
		dataTypes       = map[string]int{}
		tagged    int
	)
	err := r.Each(func(ev *container.Event) error {
		count++
		if ev.Timestamp < lastTS {
			t.Errorf("events out of timestamp order: %d after %d", ev.Timestamp, lastTS)
		}
		lastTS = ev.Timestamp
		if ev.EventData == nil {
			t.Errorf("event %d has nil EventData (ref %q)", ev.Identifier, ev.EventDataIdentifier)
			return nil
		}
		dataTypes[ev.EventData.DataType]++
		if len(ev.Tags) > 0 {
			tagged++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 38 {
		t.Errorf("iterated %d events, want 38", count)
	}

	wantTypes := []string{"fs:stat", "syslog:cron:task_run", "syslog:line"}
	var gotTypes []string
	for dt := range dataTypes {
		gotTypes = append(gotTypes, dt)
	}
	sort.Strings(gotTypes)
	if len(gotTypes) != len(wantTypes) {
		t.Errorf("data types = %v, want %v", gotTypes, wantTypes)
	} else {
		for i := range wantTypes {
			if gotTypes[i] != wantTypes[i] {
				t.Errorf("data types = %v, want %v", gotTypes, wantTypes)
				break
			}
		}
	}

	// 4 event_tag rows reference 4 distinct events.
	if tagged != 4 {
		t.Errorf("tagged events = %d, want 4", tagged)
	}
}

// TestNanosecondPrecision ensures large nanosecond timestamps survive JSON
// decoding without float64 precision loss.
func TestNanosecondPrecision(t *testing.T) {
	r := openTest(t)
	const wantNS = "1783014477955519754" // from a fs:stat modification_time
	found := false
	err := r.Each(func(ev *container.Event) error {
		if ev.EventData == nil || ev.EventData.DataType != "fs:stat" {
			return nil
		}
		for _, name := range []string{"modification_time", "change_time", "access_time"} {
			v, ok := ev.EventData.Attr(name)
			if !ok {
				continue
			}
			m, ok := v.(map[string]any)
			if !ok {
				continue
			}
			if ts, ok := m["timestamp"].(json.Number); ok && ts.String() == wantNS {
				found = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("did not find exact nanosecond timestamp %s (precision loss?)", wantNS)
	}
}
