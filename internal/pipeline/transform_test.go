package pipeline

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
)

func TestTransformOps(t *testing.T) {
	tests := []struct {
		name string
		step job.Transform
		in   map[string]any
		want map[string]any
	}{
		{"trim", job.Transform{Op: "trim", Fields: []string{"name"}},
			map[string]any{"name": "  Asha  "}, map[string]any{"name": "Asha"}},
		{"lowercase", job.Transform{Op: "lowercase", Fields: []string{"cat"}},
			map[string]any{"cat": "Books"}, map[string]any{"cat": "books"}},
		{"uppercase", job.Transform{Op: "uppercase", Fields: []string{"cc"}},
			map[string]any{"cc": "in"}, map[string]any{"cc": "IN"}},
		{"title", job.Transform{Op: "title", Fields: []string{"name"}},
			map[string]any{"name": "  john   SMITH "}, map[string]any{"name": "John Smith"}},
		{"title unicode", job.Transform{Op: "title", Fields: []string{"name"}},
			map[string]any{"name": "élodie durand"}, map[string]any{"name": "Élodie Durand"}},
		{"string op skips missing field", job.Transform{Op: "trim", Fields: []string{"nope"}},
			map[string]any{"a": " x "}, map[string]any{"a": " x "}},
		{"string op leaves numbers alone", job.Transform{Op: "uppercase", Fields: []string{"n"}},
			map[string]any{"n": int64(5)}, map[string]any{"n": int64(5)}},
		{"rename", job.Transform{Op: "rename", From: "amt", To: "amount"},
			map[string]any{"amt": 5.0}, map[string]any{"amount": 5.0}},
		{"rename missing source", job.Transform{Op: "rename", From: "amt", To: "amount"},
			map[string]any{"x": 1}, map[string]any{"x": 1}},
		{"default fills missing", job.Transform{Op: "default", Field: "country", Value: "IN"},
			map[string]any{}, map[string]any{"country": "IN"}},
		{"default fills nil", job.Transform{Op: "default", Field: "country", Value: "IN"},
			map[string]any{"country": nil}, map[string]any{"country": "IN"}},
		{"default keeps value", job.Transform{Op: "default", Field: "country", Value: "IN"},
			map[string]any{"country": "US"}, map[string]any{"country": "US"}},
		{"multiply ints and floats", job.Transform{Op: "multiply", Fields: []string{"amount", "qty"}, Target: "total"},
			map[string]any{"amount": 2.5, "qty": int64(4)}, map[string]any{"amount": 2.5, "qty": int64(4), "total": 10.0}},
		{"lookup hit", job.Transform{Op: "lookup", Field: "cc", Target: "region", Table: map[string]string{"IN": "APAC"}, Default: "OTHER"},
			map[string]any{"cc": "IN"}, map[string]any{"cc": "IN", "region": "APAC"}},
		{"lookup miss uses default", job.Transform{Op: "lookup", Field: "cc", Target: "region", Table: map[string]string{"IN": "APAC"}, Default: "OTHER"},
			map[string]any{"cc": "ZZ"}, map[string]any{"cc": "ZZ", "region": "OTHER"}},
		{"lookup miss without default", job.Transform{Op: "lookup", Field: "cc", Target: "region", Table: map[string]string{"IN": "APAC"}},
			map[string]any{"cc": "ZZ"}, map[string]any{"cc": "ZZ"}},
		{"lookup on a number", job.Transform{Op: "lookup", Field: "code", Target: "name", Table: map[string]string{"7": "seven"}},
			map[string]any{"code": int64(7)}, map[string]any{"code": int64(7), "name": "seven"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, reasons := NewTransformer([]job.Transform{tc.step}).Transform(rec(tc.in))
			if reasons != nil {
				t.Fatalf("unexpected reasons: %v", reasons)
			}
			if !reflect.DeepEqual(out.Fields, tc.want) {
				t.Errorf("fields = %#v, want %#v", out.Fields, tc.want)
			}
		})
	}
}

func TestTransformMultiplyFails(t *testing.T) {
	tr := NewTransformer([]job.Transform{{Op: "multiply", Fields: []string{"amount", "qty"}, Target: "total"}})

	_, reasons := tr.Transform(rec(map[string]any{"amount": true}))
	got := strings.Join(reasons, "; ")
	if !strings.Contains(got, "multiply: amount is not a number") || !strings.Contains(got, "multiply: qty is missing") {
		t.Errorf("reasons = %v", reasons)
	}
}

func TestTransformStepsRunInOrder(t *testing.T) {
	tr := NewTransformer([]job.Transform{
		{Op: "trim", Fields: []string{"cc"}},
		{Op: "uppercase", Fields: []string{"cc"}},
		{Op: "lookup", Field: "cc", Target: "region", Table: map[string]string{"IN": "APAC"}, Default: "OTHER"},
		{Op: "rename", From: "cc", To: "country"},
	})

	out, _ := tr.Transform(rec(map[string]any{"cc": " in "}))
	want := map[string]any{"country": "IN", "region": "APAC"}
	if !reflect.DeepEqual(out.Fields, want) {
		t.Errorf("fields = %v, want %v", out.Fields, want)
	}
}

func TestTransformDoesNotModifyInput(t *testing.T) {
	in := rec(map[string]any{"name": " asha "})
	NewTransformer([]job.Transform{{Op: "title", Fields: []string{"name"}}}).Transform(in)

	if in.Fields["name"] != " asha " {
		t.Errorf("input changed to %q", in.Fields["name"])
	}
}

func TestTransformNoSteps(t *testing.T) {
	out, reasons := NewTransformer(nil).Transform(rec(map[string]any{"a": 1}))
	if reasons != nil || out.Fields["a"] != 1 || out.Source != "test" {
		t.Errorf("got %+v, %v", out, reasons)
	}
}
