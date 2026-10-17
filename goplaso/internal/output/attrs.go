package output

import (
	"github.com/Get-Sybers/plaso/goplaso/internal/container"
	"github.com/Get-Sybers/plaso/goplaso/internal/dfpath"
	"github.com/Get-Sybers/plaso/goplaso/internal/formatters"
)

// kv is a single attribute name/value pair.
type kv struct {
	name  string
	value any
}

// streamPathSpec returns the stream's path specification, parsed once by the
// reader and cached on the stream (nil when absent or unparsable).
func streamPathSpec(s *container.EventDataStream) *dfpath.PathSpec {
	ps, _ := s.ParsedPathSpec.(*dfpath.PathSpec)
	return ps
}

// eventDataAttributes yields the public event-data attributes plus the
// _parser_chain protected attribute, skipping date/time values, mirroring the
// acstore GetAttributes iteration the rawpy, l2tcsv-extra and JSON modules
// consume (the other protected/reserved keys are decoded into typed fields and
// are never emitted). Order is unspecified; callers sort as needed.
func eventDataAttributes(ed *container.EventData) []kv {
	out := make([]kv, 0, len(ed.Attributes)+2)
	for name, value := range ed.Attributes {
		if formatters.IsDateTimeValue(value) {
			continue
		}
		out = append(out, kv{name: name, value: value})
	}
	// data_type is a public attribute in plaso; it is decoded into a typed field
	// here, so re-surface it for the GetAttributes iteration.
	if ed.DataType != "" {
		out = append(out, kv{name: "data_type", value: ed.DataType})
	}
	if ed.ParserChain != "" {
		out = append(out, kv{name: "_parser_chain", value: ed.ParserChain})
	}
	return out
}

// eventDataStreamAttributes yields the set event-data-stream attributes. The
// path_spec value is the parsed path specification; callers that do not need it
// skip it by name.
func eventDataStreamAttributes(s *container.EventDataStream) []kv {
	var out []kv
	if s.FileEntropy != "" {
		out = append(out, kv{name: "file_entropy", value: s.FileEntropy})
	}
	if s.MD5Hash != "" {
		out = append(out, kv{name: "md5_hash", value: s.MD5Hash})
	}
	if s.SHA1Hash != "" {
		out = append(out, kv{name: "sha1_hash", value: s.SHA1Hash})
	}
	if s.SHA256Hash != "" {
		out = append(out, kv{name: "sha256_hash", value: s.SHA256Hash})
	}
	if s.PathSpec != "" {
		out = append(out, kv{name: "path_spec", value: streamPathSpec(s)})
	}
	if s.YaraMatch != "" {
		out = append(out, kv{name: "yara_match", value: s.YaraMatch})
	}
	return out
}
