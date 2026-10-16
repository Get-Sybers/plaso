package formatters

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

// formattersDir is plaso's shipped formatter YAML directory.
var formattersDir = filepath.Join("..", "..", "..", "plaso", "data", "formatters")

func num(s string) json.Number { return json.Number(s) }

func loadSet(t *testing.T) *Set {
	t.Helper()
	s := NewSet()
	if err := s.LoadDir(formattersDir); err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	return s
}

func TestLoadAndSourceMapping(t *testing.T) {
	s := loadSet(t)
	cases := []struct{ dt, short, long string }{
		{"fs:stat", "FILE", "File stat"},
		{"syslog:line", "LOG", "Log File"},
		{"syslog:cron:task_run", "LOG", "Cron log"},
	}
	for _, c := range cases {
		short, long := s.SourceMapping(c.dt)
		if short != c.short || long != c.long {
			t.Errorf("SourceMapping(%s) = (%q,%q), want (%q,%q)", c.dt, short, long, c.short, c.long)
		}
		if s.Get(c.dt) == nil {
			t.Errorf("no formatter for %s", c.dt)
		}
	}
}

func TestMessages(t *testing.T) {
	s := loadSet(t)
	cases := []struct {
		name   string
		dt     string
		values map[string]any
		want   string
	}{
		{
			name: "fs:stat allocated (unallocated piece dropped, octal mode)",
			dt:   "fs:stat",
			values: map[string]any{
				"display_name":     "OS:/tmp/test/test_data/syslog/syslog",
				"file_entry_type":  "file",
				"owner_identifier": num("1000"),
				"group_identifier": num("1000"),
				"mode":             num("420"),
				"number_of_links":  num("1"),
				"is_allocated":     true,
			},
			want: "OS:/tmp/test/test_data/syslog/syslog Type: file Owner identifier: 1000 " +
				"Group identifier: 1000 Mode: 0o644 Number of links: 1",
		},
		{
			name: "syslog:cron:task_run",
			dt:   "syslog:cron:task_run",
			values: map[string]any{
				"command":  "touch /var/run/crond.somecheck",
				"username": "root",
				"pid":      num("31051"),
			},
			want: "Cron ran: touch /var/run/crond.somecheck for user: root pid: 31051",
		},
		{
			name: "syslog:line no severity (piece dropped, empty separator, raw comma)",
			dt:   "syslog:line",
			values: map[string]any{
				"reporter":     "client",
				"pid":          num("30840"),
				"message_body": "INFO No change in [/etc/netgroup]. Done",
			},
			want: "[client, pid: 30840] INFO No change in [/etc/netgroup]. Done",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := s.Get(c.dt)
			if f == nil {
				t.Fatalf("no formatter for %s", c.dt)
			}
			f.FormatEventValues(c.values, s.Custom)
			got := f.GetMessage(c.values)
			if got != c.want {
				t.Errorf("GetMessage =\n  %q\nwant\n  %q", got, c.want)
			}
		})
	}
}
