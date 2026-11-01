package pipeline

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/record"
)

func benchSpec(b *testing.B) job.Spec {
	b.Helper()
	data, err := os.ReadFile("../../examples/orders-job.json")
	if err != nil {
		b.Fatal(err)
	}
	var spec job.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		b.Fatal(err)
	}
	spec.ApplyDefaults()
	return spec
}

func rawOrder() record.Record {
	r := record.New("orders-csv", 1)
	for k, v := range map[string]any{
		"order_id": "1002", "customer": "  john smith ", "country": "us", "category": "Electronics",
		"amount": "1200", "quantity": "1", "order_date": "2026-10-01",
	} {
		r.Fields[k] = v
	}
	return r
}

func BenchmarkValidate(b *testing.B) {
	v, err := NewValidator(benchSpec(b).Schema)
	if err != nil {
		b.Fatal(err)
	}
	in := rawOrder()

	for b.Loop() {
		if _, reasons := v.Validate(in); reasons != nil {
			b.Fatal(reasons)
		}
	}
}

func BenchmarkTransform(b *testing.B) {
	spec := benchSpec(b)
	v, _ := NewValidator(spec.Schema)
	valid, _ := v.Validate(rawOrder())
	t := NewTransformer(spec.Transforms)

	for b.Loop() {
		if _, reasons := t.Transform(valid); reasons != nil {
			b.Fatal(reasons)
		}
	}
}

func BenchmarkAggregate(b *testing.B) {
	spec := benchSpec(b)
	v, _ := NewValidator(spec.Schema)
	valid, _ := v.Validate(rawOrder())
	out, _ := NewTransformer(spec.Transforms).Transform(valid)
	agg := NewAggregator(spec.Aggregations)

	for b.Loop() {
		agg.Add(out)
	}
}
