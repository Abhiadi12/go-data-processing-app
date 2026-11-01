package pipeline

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/record"
)

func ptr(f float64) *float64 { return &f }

func testSchema() job.Schema {
	return job.Schema{
		Fields: []job.Field{
			{Name: "id", Type: "int", Required: true, Min: ptr(1)},
			{Name: "name", Type: "string", Required: true, Max: ptr(10)},
			{Name: "country", Type: "string", Pattern: "^[A-Za-z]{2}$"},
			{Name: "category", Type: "string", Allowed: []string{"books", "toys"}},
			{Name: "amount", Type: "float", Min: ptr(0), Max: ptr(1000)},
			{Name: "vip", Type: "bool"},
			{Name: "day", Type: "date", Format: "2006-01-02"},
		},
		UnknownFields: job.UnknownFieldsDrop,
	}
}

func rec(fields map[string]any) record.Record {
	r := record.New("test", 1)
	for k, v := range fields {
		r.Fields[k] = v
	}
	return r
}

func TestValidateConvertsTypes(t *testing.T) {
	v, err := NewValidator(testSchema())
	if err != nil {
		t.Fatal(err)
	}

	out, reasons := v.Validate(rec(map[string]any{
		"id": " 7 ", "name": " Asha ", "country": "in", "category": "Books",
		"amount": "12.50", "vip": "yes", "day": "2026-10-01", "extra": "dropped",
	}))
	if reasons != nil {
		t.Fatalf("unexpected reasons: %v", reasons)
	}

	want := map[string]any{
		"id": int64(7), "name": " Asha ", "country": "in", "category": "Books",
		"amount": 12.5, "vip": true, "day": time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
	}
	if !reflect.DeepEqual(out.Fields, want) {
		t.Errorf("fields = %#v\nwant     %#v", out.Fields, want)
	}
	if out.Source != "test" || out.Index != 1 {
		t.Errorf("source/index lost: %q %d", out.Source, out.Index)
	}
}

func TestValidateJSONValues(t *testing.T) {
	v, _ := NewValidator(testSchema())

	out, reasons := v.Validate(rec(map[string]any{"id": float64(2001), "name": "Kenji", "amount": 320.0, "vip": false}))
	if reasons != nil {
		t.Fatalf("unexpected reasons: %v", reasons)
	}
	if out.Fields["id"] != int64(2001) || out.Fields["amount"] != 320.0 || out.Fields["vip"] != false {
		t.Errorf("fields = %v", out.Fields)
	}
}

func TestValidateOptionalMissingIsFine(t *testing.T) {
	v, _ := NewValidator(testSchema())

	out, reasons := v.Validate(rec(map[string]any{"id": "1", "name": "x", "amount": "", "country": nil}))
	if reasons != nil {
		t.Fatalf("unexpected reasons: %v", reasons)
	}
	if _, ok := out.Fields["amount"]; ok {
		t.Error("blank optional field should be absent")
	}
}

func TestValidateRejects(t *testing.T) {
	tests := []struct {
		name   string
		fields map[string]any
		want   string
	}{
		{"missing required", map[string]any{"name": "x"}, "id: is required"},
		{"blank required", map[string]any{"id": "1", "name": "   "}, "name: is required"},
		{"null required", map[string]any{"id": nil, "name": "x"}, "id: is required"},
		{"not a number", map[string]any{"id": "1", "name": "x", "amount": "abc"}, "amount: must be a number"},
		{"fraction for int", map[string]any{"id": 2.5, "name": "x"}, "id: must be a whole number"},
		{"below min", map[string]any{"id": "-1", "name": "x"}, "id: must be at least 1"},
		{"above max", map[string]any{"id": "1", "name": "x", "amount": "5000"}, "amount: must be at most 1000"},
		{"string too long", map[string]any{"id": "1", "name": "abcdefghijk"}, "name: must be at most 10 characters"},
		{"pattern", map[string]any{"id": "1", "name": "x", "country": "IND"}, "country: does not match pattern"},
		{"not allowed", map[string]any{"id": "1", "name": "x", "category": "garden"}, "category: must be one of books, toys"},
		{"bad bool", map[string]any{"id": "1", "name": "x", "vip": "maybe"}, "vip: must be true or false"},
		{"bad date", map[string]any{"id": "1", "name": "x", "day": "2026-13-45"}, "day: must be a date in format 2006-01-02"},
		{"date not string", map[string]any{"id": "1", "name": "x", "day": 20261001.0}, "day: must be a date string"},
		{"object for string", map[string]any{"id": "1", "name": map[string]any{}}, "name: must be a string"},
	}
	v, _ := NewValidator(testSchema())
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, reasons := v.Validate(rec(tc.fields))
			if !strings.Contains(strings.Join(reasons, "; "), tc.want) {
				t.Errorf("reasons %v do not contain %q", reasons, tc.want)
			}
		})
	}
}

func TestValidateReportsEveryReason(t *testing.T) {
	v, _ := NewValidator(testSchema())

	_, reasons := v.Validate(rec(map[string]any{"amount": "abc", "day": "nope"}))
	if len(reasons) != 4 {
		t.Errorf("got %d reasons, want 4 (id, name, amount, day): %v", len(reasons), reasons)
	}
}

func TestValidateKeepUnknown(t *testing.T) {
	s := testSchema()
	s.UnknownFields = job.UnknownFieldsKeep
	v, _ := NewValidator(s)

	in := rec(map[string]any{"id": "1", "name": "x", "coupon": "DIWALI"})
	out, _ := v.Validate(in)
	if out.Fields["coupon"] != "DIWALI" || out.Fields["id"] != int64(1) {
		t.Errorf("fields = %v", out.Fields)
	}
	if in.Fields["id"] != "1" {
		t.Error("input record was modified")
	}
}

func TestNewValidatorBadPattern(t *testing.T) {
	s := testSchema()
	s.Fields[2].Pattern = "([a-z"
	if _, err := NewValidator(s); err == nil {
		t.Error("want an error for a bad pattern")
	}
}

func TestErrorsKeepsFirstN(t *testing.T) {
	e := NewErrors(2)
	for i := range 5 {
		e.Add(job.RecordError{Index: i})
	}

	items, total := e.Snapshot()
	if total != 5 || len(items) != 2 || items[0].Index != 0 || items[1].Index != 1 {
		t.Errorf("total %d items %+v", total, items)
	}
}
