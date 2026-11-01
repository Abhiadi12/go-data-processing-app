package pipeline

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
)

func aggregate(specs []job.Aggregation, rows ...map[string]any) []job.AggregateResult {
	a := NewAggregator(specs)
	for _, r := range rows {
		a.Add(rec(r))
	}
	return a.Results()
}

func TestAggregateMetrics(t *testing.T) {
	spec := []job.Aggregation{{
		Name:    "by_cat",
		GroupBy: []string{"cat"},
		Metrics: []job.Metric{
			{Op: "count"}, {Op: "sum", Field: "total"}, {Op: "avg", Field: "total"},
			{Op: "min", Field: "total"}, {Op: "max", Field: "total"}, {Op: "count", Field: "total"},
		},
	}}

	got := aggregate(spec,
		map[string]any{"cat": "toys", "total": 10.0},
		map[string]any{"cat": "books", "total": int64(5)},
		map[string]any{"cat": "books", "total": 0.1},
		map[string]any{"cat": "books", "total": 0.2},
		map[string]any{"cat": "books"},
	)

	want := []job.GroupResult{
		{Key: map[string]any{"cat": "books"}, Metrics: map[string]float64{
			"count": 4, "sum_total": 5.3, "avg_total": 1.77, "min_total": 0.1, "max_total": 5, "count_total": 3,
		}},
		{Key: map[string]any{"cat": "toys"}, Metrics: map[string]float64{
			"count": 1, "sum_total": 10, "avg_total": 10, "min_total": 10, "max_total": 10, "count_total": 1,
		}},
	}
	if got[0].Name != "by_cat" || !reflect.DeepEqual(got[0].Groups, want) {
		t.Errorf("groups = %+v\nwant     %+v", got[0].Groups, want)
	}
}

func TestAggregateWholeJob(t *testing.T) {
	got := aggregate([]job.Aggregation{{Name: "overall", Metrics: []job.Metric{{Op: "count"}, {Op: "sum", Field: "x"}}}},
		map[string]any{"x": 1.0}, map[string]any{"x": 2.0})

	if len(got[0].Groups) != 1 || got[0].Groups[0].Metrics["count"] != 2 || got[0].Groups[0].Metrics["sum_x"] != 3 {
		t.Errorf("got %+v", got[0])
	}
	if got[0].GroupBy == nil || len(got[0].Groups[0].Key) != 0 {
		t.Errorf("group_by should be an empty list and the key empty: %+v", got[0])
	}
}

func TestAggregateTwoGroupFieldsSorted(t *testing.T) {
	got := aggregate([]job.Aggregation{{Name: "a", GroupBy: []string{"region", "year"}, Metrics: []job.Metric{{Op: "count"}}}},
		map[string]any{"region": "EMEA", "year": int64(2026)},
		map[string]any{"region": "APAC", "year": int64(10)},
		map[string]any{"region": "APAC", "year": int64(9)},
		map[string]any{"region": "EMEA", "year": int64(2026)},
		map[string]any{"year": int64(1)},
	)

	var keys []string
	for _, g := range got[0].Groups {
		b, _ := json.Marshal(g.Key)
		keys = append(keys, string(b))
	}
	want := []string{
		`{"region":null,"year":1}`,
		`{"region":"APAC","year":9}`,
		`{"region":"APAC","year":10}`,
		`{"region":"EMEA","year":2026}`,
	}
	if !reflect.DeepEqual(keys, want) {
		t.Errorf("keys = %v\nwant %v", keys, want)
	}
	if got[0].Groups[3].Metrics["count"] != 2 {
		t.Errorf("EMEA/2026 count = %v", got[0].Groups[3].Metrics["count"])
	}
}

func TestAggregateSkipsNonNumericValues(t *testing.T) {
	got := aggregate([]job.Aggregation{{Name: "a", Metrics: []job.Metric{{Op: "count"}, {Op: "avg", Field: "x"}, {Op: "sum", Field: "y"}}}},
		map[string]any{"x": 4.0, "y": "n/a"},
		map[string]any{"x": "oops"},
		map[string]any{"x": 2.0},
	)

	m := got[0].Groups[0].Metrics
	if m["count"] != 3 || m["avg_x"] != 3 {
		t.Errorf("metrics = %v", m)
	}
	if _, ok := m["sum_y"]; ok {
		t.Error("a metric with no numeric values should be left out, not reported as 0")
	}
}

func TestAggregateNoRecords(t *testing.T) {
	got := aggregate([]job.Aggregation{{Name: "a", GroupBy: []string{"cat"}, Metrics: []job.Metric{{Op: "count"}}}})

	if len(got) != 1 || got[0].Groups == nil || len(got[0].Groups) != 0 {
		t.Errorf("want one aggregation with an empty group list, got %+v", got)
	}
}

func TestRound2(t *testing.T) {
	for in, want := range map[float64]float64{0.1 + 0.2: 0.3, 804.7475: 804.75, 1599.98: 1599.98, -1.005: -1} {
		if got := round2(in); got != want {
			t.Errorf("round2(%v) = %v, want %v", in, got, want)
		}
	}
}
