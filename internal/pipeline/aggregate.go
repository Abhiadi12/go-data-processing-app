package pipeline

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/record"
)

// Aggregator computes the spec's group-by aggregations over a stream of records.
// It is not safe for concurrent use: the pipeline feeds it from one goroutine,
// which is why it needs no locks.
type Aggregator struct {
	specs  []job.Aggregation
	tables []map[string]*group
}

type group struct {
	key     []any
	count   int64
	metrics []metricState
}

type metricState struct {
	n             int64
	sum, min, max float64
}

func NewAggregator(specs []job.Aggregation) *Aggregator {
	a := &Aggregator{specs: specs, tables: make([]map[string]*group, len(specs))}
	for i := range specs {
		a.tables[i] = make(map[string]*group)
	}
	return a
}

// Add counts one record into every aggregation.
func (a *Aggregator) Add(rec record.Record) {
	for i, spec := range a.specs {
		key, id := groupKey(rec, spec.GroupBy)
		g, ok := a.tables[i][id]
		if !ok {
			g = &group{key: key, metrics: make([]metricState, len(spec.Metrics))}
			a.tables[i][id] = g
		}
		g.count++

		for j, m := range spec.Metrics {
			if m.Field == "" {
				continue
			}
			v, ok := rec.Fields[m.Field]
			if !ok {
				continue
			}
			x, err := record.ToFloat(v)
			if err != nil {
				continue
			}
			s := &g.metrics[j]
			if s.n == 0 || x < s.min {
				s.min = x
			}
			if s.n == 0 || x > s.max {
				s.max = x
			}
			s.sum += x
			s.n++
		}
	}
}

// Results returns every aggregation with its groups sorted by key. Values are
// rounded to 2 decimals here, once, so sums never collect rounding errors.
func (a *Aggregator) Results() []job.AggregateResult {
	out := make([]job.AggregateResult, len(a.specs))
	for i, spec := range a.specs {
		groups := make([]*group, 0, len(a.tables[i]))
		for _, g := range a.tables[i] {
			groups = append(groups, g)
		}
		slices.SortFunc(groups, func(x, y *group) int { return compareKeys(x.key, y.key) })

		res := job.AggregateResult{Name: spec.Name, GroupBy: spec.GroupBy, Groups: make([]job.GroupResult, len(groups))}
		if res.GroupBy == nil {
			res.GroupBy = []string{}
		}
		for gi, g := range groups {
			key := make(map[string]any, len(spec.GroupBy))
			for k, field := range spec.GroupBy {
				key[field] = g.key[k]
			}
			res.Groups[gi] = job.GroupResult{Key: key, Metrics: metricValues(spec.Metrics, g)}
		}
		out[i] = res
	}
	return out
}

func metricValues(metrics []job.Metric, g *group) map[string]float64 {
	vals := make(map[string]float64, len(metrics))
	for j, m := range metrics {
		name := m.Op
		if m.Field != "" {
			name += "_" + m.Field
		}
		s := g.metrics[j]

		switch {
		case m.Op == "count" && m.Field == "":
			vals[name] = float64(g.count)
		case m.Op == "count":
			vals[name] = float64(s.n)
		case s.n == 0:
			// No numeric values in this group: the metric is left out, not reported as 0.
		case m.Op == "sum":
			vals[name] = round2(s.sum)
		case m.Op == "avg":
			vals[name] = round2(s.sum / float64(s.n))
		case m.Op == "min":
			vals[name] = round2(s.min)
		case m.Op == "max":
			vals[name] = round2(s.max)
		}
	}
	return vals
}

func round2(x float64) float64 {
	return math.Round(x*100) / 100
}

// groupKey returns the group-by values and a string that identifies them.
// A missing field becomes nil, so records without a category form their own group.
func groupKey(rec record.Record, fields []string) ([]any, string) {
	key := make([]any, len(fields))
	parts := make([]string, len(fields))
	for i, f := range fields {
		key[i] = rec.Fields[f]
		parts[i] = fmt.Sprintf("%T:%v", key[i], key[i])
	}
	return key, strings.Join(parts, "\x1f")
}

// compareKeys orders numbers numerically and everything else as text;
// nil (missing) sorts first.
func compareKeys(a, b []any) int {
	for i := range a {
		if c := compareValues(a[i], b[i]); c != 0 {
			return c
		}
	}
	return 0
}

func compareValues(a, b any) int {
	if a == nil || b == nil {
		switch {
		case a == nil && b == nil:
			return 0
		case a == nil:
			return -1
		default:
			return 1
		}
	}
	x, errA := record.ToFloat(a)
	y, errB := record.ToFloat(b)
	if errA == nil && errB == nil {
		if c := cmp.Compare(x, y); c != 0 {
			return c
		}
	}
	return cmp.Compare(fmt.Sprint(a), fmt.Sprint(b))
}
