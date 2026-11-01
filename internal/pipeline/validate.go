package pipeline

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/record"
)

// Validator checks records against a schema and converts each value to its
// declared type. It is read-only after NewValidator, so many workers can share it.
type Validator struct {
	fields      []rule
	keepUnknown bool
}

type rule struct {
	job.Field
	pattern *regexp.Regexp
	allowed map[string]bool
}

// NewValidator prepares a validated schema: regexes are compiled once here,
// not once per record.
func NewValidator(s job.Schema) (*Validator, error) {
	v := &Validator{keepUnknown: s.UnknownFields == job.UnknownFieldsKeep}
	for _, f := range s.Fields {
		r := rule{Field: f}
		if f.Pattern != "" {
			re, err := regexp.Compile(f.Pattern)
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", f.Name, err)
			}
			r.pattern = re
		}
		if len(f.Allowed) > 0 {
			r.allowed = make(map[string]bool, len(f.Allowed))
			for _, a := range f.Allowed {
				r.allowed[strings.ToLower(a)] = true
			}
		}
		v.fields = append(v.fields, r)
	}
	return v, nil
}

// Validate returns a new record with typed values, or every reason the record
// is invalid. The input record is not modified.
func (v *Validator) Validate(in record.Record) (record.Record, []string) {
	out := record.New(in.Source, in.Index)
	if v.keepUnknown {
		out = in.Clone()
	}

	var reasons []string
	for _, r := range v.fields {
		raw, present := in.Get(r.Name)
		if !present || isBlank(raw) {
			if r.Required {
				reasons = append(reasons, r.Name+": is required")
			}
			delete(out.Fields, r.Name)
			continue
		}

		val, problem := r.check(raw)
		if problem != "" {
			reasons = append(reasons, r.Name+": "+problem)
			continue
		}
		out.Fields[r.Name] = val
	}

	if len(reasons) > 0 {
		return record.Record{}, reasons
	}
	return out, nil
}

func isBlank(v any) bool {
	if v == nil {
		return true
	}
	s, ok := v.(string)
	return ok && strings.TrimSpace(s) == ""
}

func (r rule) check(raw any) (any, string) {
	switch r.Type {
	case "int":
		n, err := record.ToInt(raw)
		if err != nil {
			return nil, "must be a whole number"
		}
		return n, r.checkRange(float64(n), "")
	case "float":
		f, err := record.ToFloat(raw)
		if err != nil {
			return nil, "must be a number"
		}
		return f, r.checkRange(f, "")
	case "bool":
		b, err := record.ToBool(raw)
		if err != nil {
			return nil, "must be true or false"
		}
		return b, ""
	case "date":
		s, ok := raw.(string)
		if !ok {
			return nil, "must be a date string"
		}
		t, err := time.Parse(r.Format, strings.TrimSpace(s))
		if err != nil {
			return nil, "must be a date in format " + r.Format
		}
		return t, ""
	default:
		return r.checkString(raw)
	}
}

// checkString applies string rules to the trimmed value but returns the value
// unchanged: trimming is the transform stage's job.
func (r rule) checkString(raw any) (any, string) {
	var s string
	switch x := raw.(type) {
	case string:
		s = x
	case float64:
		s = strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		s = strconv.FormatBool(x)
	default:
		return nil, "must be a string"
	}

	t := strings.TrimSpace(s)
	if p := r.checkRange(float64(utf8.RuneCountInString(t)), " characters"); p != "" {
		return nil, p
	}
	if r.pattern != nil && !r.pattern.MatchString(t) {
		return nil, "does not match pattern " + r.Pattern
	}
	if r.allowed != nil && !r.allowed[strings.ToLower(t)] {
		return nil, "must be one of " + strings.Join(r.Allowed, ", ")
	}
	return s, ""
}

func (r rule) checkRange(x float64, unit string) string {
	if r.Min != nil && x < *r.Min {
		return fmt.Sprintf("must be at least %v%s", *r.Min, unit)
	}
	if r.Max != nil && x > *r.Max {
		return fmt.Sprintf("must be at most %v%s", *r.Max, unit)
	}
	return ""
}
