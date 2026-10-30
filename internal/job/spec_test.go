package job

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"testing/fstest"
)

func ptr(f float64) *float64 { return &f }

func validSpec() Spec {
	return Spec{
		Name: "orders",
		Sources: []SourceSpec{
			{Name: "orders-csv", Type: "csv", Path: "orders.csv"},
			{Name: "orders-api", Type: "http", URL: "https://example.com/orders", RecordsPath: "results"},
		},
		Schema: Schema{Fields: []Field{
			{Name: "order_id", Type: "int", Required: true, Min: ptr(1)},
			{Name: "country", Type: "string", Pattern: "^[A-Z]{2}$"},
			{Name: "amount", Type: "float", Min: ptr(0), Max: ptr(10000)},
			{Name: "order_date", Type: "date"},
		}},
		Transforms: []Transform{
			{Op: "trim", Fields: []string{"country"}},
			{Op: "multiply", Fields: []string{"amount", "order_id"}, Target: "total"},
		},
		Aggregations: []Aggregation{
			{Name: "by_country", GroupBy: []string{"country"}, Metrics: []Metric{{Op: "count"}, {Op: "sum", Field: "amount"}}},
		},
		Exports: []Export{{Type: "json", Content: "records"}, {Type: "csv", Content: "aggregates"}},
	}
}

var testFiles = fstest.MapFS{
	"orders.csv": {Data: []byte("order_id\n1\n")},
	"dir":        {Mode: os.ModeDir},
}

func TestApplyDefaults(t *testing.T) {
	s := validSpec()
	s.ApplyDefaults()

	o := s.Options
	if o.ValidateWorkers != DefaultWorkers || o.TransformWorkers != DefaultWorkers || o.BufferSize != DefaultBufferSize ||
		o.TimeoutSeconds != DefaultTimeoutSeconds || o.MaxStoredErrors != DefaultMaxStoredErrors {
		t.Errorf("options defaults not applied: %+v", o)
	}
	if s.Sources[1].TimeoutMS != DefaultHTTPTimeoutMS {
		t.Errorf("http timeout = %d", s.Sources[1].TimeoutMS)
	}
	if s.Sources[0].TimeoutMS != 0 {
		t.Errorf("csv source got a timeout: %d", s.Sources[0].TimeoutMS)
	}
	if s.Schema.UnknownFields != UnknownFieldsDrop || s.Schema.Fields[3].Format != DefaultDateFormat {
		t.Errorf("schema defaults not applied: %+v", s.Schema)
	}
}

func TestApplyDefaultsKeepsExplicitValues(t *testing.T) {
	s := validSpec()
	s.Options.ValidateWorkers = 8
	s.ApplyDefaults()

	if s.Options.ValidateWorkers != 8 {
		t.Errorf("validate_workers = %d, want 8", s.Options.ValidateWorkers)
	}
}

func TestValidSpecPasses(t *testing.T) {
	s := validSpec()
	s.ApplyDefaults()

	if err := s.Validate(testFiles); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateProblems(t *testing.T) {
	tests := []struct {
		name      string
		change    func(s *Spec)
		wantField string
	}{
		{"missing name", func(s *Spec) { s.Name = " " }, "name"},
		{"name too long", func(s *Spec) { s.Name = strings.Repeat("a", 101) }, "name"},
		{"no sources", func(s *Spec) { s.Sources = nil }, "sources"},
		{"bad source name", func(s *Spec) { s.Sources[0].Name = "orders csv" }, "sources[0].name"},
		{"duplicate source name", func(s *Spec) { s.Sources[1].Name = "orders-csv" }, "sources[1].name"},
		{"unknown source type", func(s *Spec) { s.Sources[0].Type = "xml" }, "sources[0].type"},
		{"csv without path", func(s *Spec) { s.Sources[0].Path = "" }, "sources[0].path"},
		{"path escapes data dir", func(s *Spec) { s.Sources[0].Path = "../secret.csv" }, "sources[0].path"},
		{"absolute path", func(s *Spec) { s.Sources[0].Path = "/etc/passwd" }, "sources[0].path"},
		{"file not found", func(s *Spec) { s.Sources[0].Path = "missing.csv" }, "sources[0].path"},
		{"path is a directory", func(s *Spec) { s.Sources[0].Path = "dir" }, "sources[0].path"},
		{"csv with url", func(s *Spec) { s.Sources[0].URL = "https://example.com" }, "sources[0].url"},
		{"csv with records_path", func(s *Spec) { s.Sources[0].RecordsPath = "data" }, "sources[0].records_path"},
		{"http without url", func(s *Spec) { s.Sources[1].URL = "" }, "sources[1].url"},
		{"http with ftp url", func(s *Spec) { s.Sources[1].URL = "ftp://example.com/x" }, "sources[1].url"},
		{"http url without host", func(s *Spec) { s.Sources[1].URL = "https:///orders" }, "sources[1].url"},
		{"http url with password", func(s *Spec) { s.Sources[1].URL = "https://u:p@example.com" }, "sources[1].url"},
		{"http with path", func(s *Spec) { s.Sources[1].Path = "orders.csv" }, "sources[1].path"},
		{"http timeout too long", func(s *Spec) { s.Sources[1].TimeoutMS = 600000 }, "sources[1].timeout_ms"},
		{"no schema fields", func(s *Spec) { s.Schema.Fields = nil }, "schema.fields"},
		{"field without name", func(s *Spec) { s.Schema.Fields[0].Name = "" }, "schema.fields[0].name"},
		{"duplicate field", func(s *Spec) { s.Schema.Fields[1].Name = "order_id" }, "schema.fields[1].name"},
		{"unknown field type", func(s *Spec) { s.Schema.Fields[0].Type = "money" }, "schema.fields[0].type"},
		{"min above max", func(s *Spec) { s.Schema.Fields[2].Min = ptr(50000) }, "schema.fields[2].min"},
		{"min on date", func(s *Spec) { s.Schema.Fields[3].Min = ptr(1) }, "schema.fields[3]"},
		{"bad regex", func(s *Spec) { s.Schema.Fields[1].Pattern = "([A-Z" }, "schema.fields[1].pattern"},
		{"pattern on int", func(s *Spec) { s.Schema.Fields[0].Pattern = "^1$" }, "schema.fields[0].pattern"},
		{"allowed on float", func(s *Spec) { s.Schema.Fields[2].Allowed = []string{"1"} }, "schema.fields[2].allowed"},
		{"format on string", func(s *Spec) { s.Schema.Fields[1].Format = "2006" }, "schema.fields[1].format"},
		{"date format without date parts", func(s *Spec) { s.Schema.Fields[3].Format = "yyyy-mm-dd" }, "schema.fields[3].format"},
		{"bad unknown_fields", func(s *Spec) { s.Schema.UnknownFields = "error" }, "schema.unknown_fields"},
		{"unknown transform", func(s *Spec) { s.Transforms[0].Op = "reverse" }, "transforms[0].op"},
		{"trim without fields", func(s *Spec) { s.Transforms[0].Fields = nil }, "transforms[0].fields"},
		{"multiply with one field", func(s *Spec) { s.Transforms[1].Fields = []string{"amount"} }, "transforms[1].fields"},
		{"multiply without target", func(s *Spec) { s.Transforms[1].Target = "" }, "transforms[1].target"},
		{"rename without to", func(s *Spec) { s.Transforms[0] = Transform{Op: "rename", From: "a"} }, "transforms[0].to"},
		{"default without value", func(s *Spec) { s.Transforms[0] = Transform{Op: "default", Field: "a"} }, "transforms[0].value"},
		{"lookup without table", func(s *Spec) { s.Transforms[0] = Transform{Op: "lookup", Field: "a", Target: "b"} }, "transforms[0].table"},
		{"aggregation bad name", func(s *Spec) { s.Aggregations[0].Name = "" }, "aggregations[0].name"},
		{"aggregation without metrics", func(s *Spec) { s.Aggregations[0].Metrics = nil }, "aggregations[0].metrics"},
		{"unknown metric", func(s *Spec) { s.Aggregations[0].Metrics[0].Op = "median" }, "aggregations[0].metrics[0].op"},
		{"sum without field", func(s *Spec) { s.Aggregations[0].Metrics[1].Field = "" }, "aggregations[0].metrics[1].field"},
		{"empty group_by entry", func(s *Spec) { s.Aggregations[0].GroupBy = []string{""} }, "aggregations[0].group_by[0]"},
		{"no exports", func(s *Spec) { s.Exports = nil }, "exports"},
		{"unknown export type", func(s *Spec) { s.Exports[0].Type = "xml" }, "exports[0].type"},
		{"aggregates without aggregations", func(s *Spec) { s.Aggregations = nil }, "exports[1].content"},
		{"duplicate export", func(s *Spec) { s.Exports[1] = s.Exports[0] }, "exports[1]"},
		{"too many workers", func(s *Spec) { s.Options.ValidateWorkers = 33 }, "options.validate_workers"},
		{"negative workers", func(s *Spec) { s.Options.TransformWorkers = -1 }, "options.transform_workers"},
		{"buffer too big", func(s *Spec) { s.Options.BufferSize = 5001 }, "options.buffer_size"},
		{"timeout too long", func(s *Spec) { s.Options.TimeoutSeconds = 3601 }, "options.timeout_seconds"},
		{"too many stored errors", func(s *Spec) { s.Options.MaxStoredErrors = 100001 }, "options.max_stored_errors"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := validSpec()
			s.ApplyDefaults()
			tc.change(&s)

			err := s.Validate(testFiles)
			var vErr *ValidationError
			if !errors.As(err, &vErr) {
				t.Fatalf("err = %v, want *ValidationError", err)
			}
			for _, p := range vErr.Problems {
				if p.Field == tc.wantField {
					return
				}
			}
			t.Errorf("no problem for %q in %v", tc.wantField, vErr.Problems)
		})
	}
}

func TestValidateReportsAllProblems(t *testing.T) {
	s := Spec{}
	s.ApplyDefaults()

	var vErr *ValidationError
	if !errors.As(s.Validate(nil), &vErr) {
		t.Fatal("want a *ValidationError")
	}
	if len(vErr.Problems) < 4 {
		t.Errorf("got %d problems, want name, sources, schema.fields and exports: %v", len(vErr.Problems), vErr.Problems)
	}
	if !strings.HasPrefix(vErr.Error(), "invalid job spec: name: is required") {
		t.Errorf("message = %q", vErr.Error())
	}
}

func TestValidateWithoutFilesSkipsExistenceCheck(t *testing.T) {
	s := validSpec()
	s.Sources[0].Path = "not/on/disk.csv"
	s.ApplyDefaults()

	if err := s.Validate(nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExampleSpecFile(t *testing.T) {
	data, err := os.ReadFile("../../examples/orders-job.json")
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	var s Spec
	if err := dec.Decode(&s); err != nil {
		t.Fatalf("decode: %v", err)
	}
	s.ApplyDefaults()

	if err := s.Validate(os.DirFS("../..")); err != nil {
		t.Fatalf("example spec is invalid: %v", err)
	}
	if len(s.Sources) != 2 || len(s.Transforms) != 6 || len(s.Aggregations) != 3 || len(s.Exports) != 3 {
		t.Errorf("unexpected shape: %d sources, %d transforms, %d aggregations, %d exports",
			len(s.Sources), len(s.Transforms), len(s.Aggregations), len(s.Exports))
	}
}
