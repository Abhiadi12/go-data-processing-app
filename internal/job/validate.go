package job

import (
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
)

// Problem is one invalid value in a spec, addressed by its JSON path.
type Problem struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError lists every problem found in a spec, not only the first.
type ValidationError struct {
	Problems []Problem
}

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Problems))
	for i, p := range e.Problems {
		parts[i] = p.Field + ": " + p.Message
	}
	return "invalid job spec: " + strings.Join(parts, "; ")
}

var (
	fieldTypes    = []string{"string", "int", "float", "bool", "date"}
	stringOps     = []string{"trim", "lowercase", "uppercase", "title"}
	metricOps     = []string{"count", "sum", "avg", "min", "max"}
	exportTypes   = []string{"json", "csv"}
	exportContent = []string{"records", "aggregates"}
	identifier    = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

// Validate checks the whole spec and returns a *ValidationError listing every problem.
// When files is not nil, csv and json source paths must exist in it.
// Call ApplyDefaults first, so zero values have been replaced.
func (s *Spec) Validate(files fs.FS) error {
	var v validator

	v.required("name", s.Name)
	if len(s.Name) > MaxNameLength {
		v.add("name", "must be at most %d characters", MaxNameLength)
	}
	v.sources(s.Sources, files)
	v.schema(s.Schema)
	v.transforms(s.Transforms)
	v.aggregations(s.Aggregations)
	v.exports(s.Exports, len(s.Aggregations))
	v.options(s.Options)

	if len(v.problems) == 0 {
		return nil
	}
	return &ValidationError{Problems: v.problems}
}

type validator struct {
	problems []Problem
}

func (v *validator) add(field, format string, args ...any) {
	v.problems = append(v.problems, Problem{Field: field, Message: fmt.Sprintf(format, args...)})
}

func (v *validator) required(field, value string) bool {
	if strings.TrimSpace(value) == "" {
		v.add(field, "is required")
		return false
	}
	return true
}

func (v *validator) oneOf(field, value string, allowed []string) bool {
	if !slices.Contains(allowed, value) {
		v.add(field, "must be one of %s", strings.Join(allowed, ", "))
		return false
	}
	return true
}

func (v *validator) upTo(field string, value, hi int) {
	if value < 1 || value > hi {
		v.add(field, "must be between 1 and %d", hi)
	}
}

func (v *validator) sources(srcs []SourceSpec, files fs.FS) {
	if len(srcs) == 0 {
		v.add("sources", "at least one source is required")
		return
	}
	if len(srcs) > MaxSources {
		v.add("sources", "at most %d sources are allowed", MaxSources)
	}

	seen := make(map[string]bool, len(srcs))
	for i, src := range srcs {
		p := fmt.Sprintf("sources[%d]", i)

		if !identifier.MatchString(src.Name) {
			v.add(p+".name", "must be 1-64 letters, digits, _ or -")
		} else if seen[src.Name] {
			v.add(p+".name", "duplicate source name %q", src.Name)
		}
		seen[src.Name] = true

		switch src.Type {
		case "csv", "json":
			if src.URL != "" {
				v.add(p+".url", "is only allowed for http sources")
			}
			if src.Type == "csv" && src.RecordsPath != "" {
				v.add(p+".records_path", "is not supported for csv sources")
			}
			v.filePath(p+".path", src.Path, files)
		case "http":
			if src.Path != "" {
				v.add(p+".path", "is not allowed for http sources, use url")
			}
			v.httpURL(p+".url", src.URL)
			v.upTo(p+".timeout_ms", src.TimeoutMS, MaxHTTPTimeoutMS)
		default:
			v.add(p+".type", "must be one of csv, json, http")
		}
	}
}

func (v *validator) filePath(field, path string, files fs.FS) {
	if !v.required(field, path) {
		return
	}
	if !fs.ValidPath(path) {
		v.add(field, "must be a clean relative path inside the data directory, e.g. testdata/orders.csv")
		return
	}
	if files == nil {
		return
	}
	info, err := fs.Stat(files, path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		v.add(field, "file not found in the data directory")
	case err != nil:
		v.add(field, "cannot be read")
	case info.IsDir():
		v.add(field, "is a directory, not a file")
	}
}

func (v *validator) httpURL(field, raw string) {
	if !v.required(field, raw) {
		return
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		v.add(field, "must be an absolute http or https URL")
		return
	}
	if u.User != nil {
		v.add(field, "must not contain credentials")
	}
}

func (v *validator) schema(s Schema) {
	if len(s.Fields) == 0 {
		v.add("schema.fields", "at least one field is required")
	}

	seen := make(map[string]bool, len(s.Fields))
	for i, f := range s.Fields {
		p := fmt.Sprintf("schema.fields[%d]", i)

		if v.required(p+".name", f.Name) {
			if seen[f.Name] {
				v.add(p+".name", "duplicate field %q", f.Name)
			}
			seen[f.Name] = true
		}
		if !v.oneOf(p+".type", f.Type, fieldTypes) {
			continue
		}

		if (f.Min != nil || f.Max != nil) && f.Type != "int" && f.Type != "float" && f.Type != "string" {
			v.add(p, "min and max only apply to int, float and string fields")
		}
		if f.Min != nil && f.Max != nil && *f.Min > *f.Max {
			v.add(p+".min", "must not be greater than max")
		}
		if f.Pattern != "" {
			if f.Type != "string" {
				v.add(p+".pattern", "only applies to string fields")
			} else if _, err := regexp.Compile(f.Pattern); err != nil {
				v.add(p+".pattern", "is not a valid regular expression: %v", err)
			}
		}
		if len(f.Allowed) > 0 && f.Type != "string" {
			v.add(p+".allowed", "only applies to string fields")
		}
		if f.Format != "" {
			if f.Type != "date" {
				v.add(p+".format", "only applies to date fields")
			} else if time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC).Format(f.Format) == f.Format {
				v.add(p+".format", "must be a Go time layout such as 2006-01-02")
			}
		}
	}

	v.oneOf("schema.unknown_fields", s.UnknownFields, []string{UnknownFieldsDrop, UnknownFieldsKeep})
}

func (v *validator) transforms(ts []Transform) {
	for i, t := range ts {
		p := fmt.Sprintf("transforms[%d]", i)
		switch {
		case slices.Contains(stringOps, t.Op):
			v.fieldList(p+".fields", t.Fields, 1)
		case t.Op == "rename":
			v.required(p+".from", t.From)
			v.required(p+".to", t.To)
		case t.Op == "default":
			v.required(p+".field", t.Field)
			if t.Value == nil {
				v.add(p+".value", "is required")
			}
		case t.Op == "multiply":
			v.fieldList(p+".fields", t.Fields, 2)
			v.required(p+".target", t.Target)
		case t.Op == "lookup":
			v.required(p+".field", t.Field)
			v.required(p+".target", t.Target)
			if len(t.Table) == 0 {
				v.add(p+".table", "must have at least one entry")
			}
		default:
			v.add(p+".op", "must be one of trim, lowercase, uppercase, title, rename, default, multiply, lookup")
		}
	}
}

func (v *validator) fieldList(field string, names []string, minLen int) {
	if len(names) < minLen {
		v.add(field, "needs at least %d field name(s)", minLen)
	}
	for i, n := range names {
		v.required(fmt.Sprintf("%s[%d]", field, i), n)
	}
}

func (v *validator) aggregations(aggs []Aggregation) {
	seen := make(map[string]bool, len(aggs))
	for i, a := range aggs {
		p := fmt.Sprintf("aggregations[%d]", i)

		if !identifier.MatchString(a.Name) {
			v.add(p+".name", "must be 1-64 letters, digits, _ or -")
		} else if seen[a.Name] {
			v.add(p+".name", "duplicate aggregation name %q", a.Name)
		}
		seen[a.Name] = true

		for j, g := range a.GroupBy {
			v.required(fmt.Sprintf("%s.group_by[%d]", p, j), g)
		}
		if len(a.Metrics) == 0 {
			v.add(p+".metrics", "at least one metric is required")
		}
		for j, m := range a.Metrics {
			mp := fmt.Sprintf("%s.metrics[%d]", p, j)
			if v.oneOf(mp+".op", m.Op, metricOps) && m.Op != "count" {
				v.required(mp+".field", m.Field)
			}
		}
	}
}

func (v *validator) exports(exps []Export, numAggregations int) {
	if len(exps) == 0 {
		v.add("exports", "at least one export is required")
		return
	}
	seen := make(map[Export]bool, len(exps))
	for i, e := range exps {
		p := fmt.Sprintf("exports[%d]", i)
		okType := v.oneOf(p+".type", e.Type, exportTypes)
		okContent := v.oneOf(p+".content", e.Content, exportContent)
		if okContent && e.Content == "aggregates" && numAggregations == 0 {
			v.add(p+".content", "aggregates export needs at least one aggregation")
		}
		if okType && okContent && seen[e] {
			v.add(p, "duplicate export %s/%s", e.Type, e.Content)
		}
		seen[e] = true
	}
}

func (v *validator) options(o Options) {
	v.upTo("options.validate_workers", o.ValidateWorkers, MaxWorkers)
	v.upTo("options.transform_workers", o.TransformWorkers, MaxWorkers)
	v.upTo("options.buffer_size", o.BufferSize, MaxBufferSize)
	v.upTo("options.timeout_seconds", o.TimeoutSeconds, MaxTimeoutSeconds)
	v.upTo("options.max_stored_errors", o.MaxStoredErrors, MaxStoredErrors)
}
