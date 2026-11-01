package pipeline

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/record"
)

type Transformer struct {
	steps []job.Transform
}

func NewTransformer(steps []job.Transform) *Transformer {
	return &Transformer{steps: steps}
}

func (t *Transformer) Transform(in record.Record) (record.Record, []string) {
	out := in.Clone()
	var reasons []string

	for _, s := range t.steps {
		switch s.Op {
		case "trim":
			mapStrings(out, s.Fields, strings.TrimSpace)
		case "lowercase":
			mapStrings(out, s.Fields, strings.ToLower)
		case "uppercase":
			mapStrings(out, s.Fields, strings.ToUpper)
		case "title":
			mapStrings(out, s.Fields, titleCase)
		case "rename":
			if v, ok := out.Fields[s.From]; ok {
				delete(out.Fields, s.From)
				out.Fields[s.To] = v
			}
		case "default":
			if v, ok := out.Fields[s.Field]; !ok || v == nil {
				out.Fields[s.Field] = s.Value
			}
		case "multiply":
			product, problems := multiply(out, s.Fields)
			if len(problems) > 0 {
				reasons = append(reasons, problems...)
				continue
			}
			out.Fields[s.Target] = product
		case "lookup":
			if v, ok := out.Fields[s.Field]; ok {
				if mapped, found := s.Table[fmt.Sprint(v)]; found {
					out.Fields[s.Target] = mapped
					continue
				}
			}
			if s.Default != "" {
				out.Fields[s.Target] = s.Default
			}
		}
	}

	if len(reasons) > 0 {
		return record.Record{}, reasons
	}
	return out, nil
}

func mapStrings(r record.Record, fields []string, fn func(string) string) {
	for _, f := range fields {
		if s, ok := r.Fields[f].(string); ok {
			r.Fields[f] = fn(s)
		}
	}
}

func multiply(r record.Record, fields []string) (float64, []string) {
	product := 1.0
	var problems []string
	for _, f := range fields {
		v, ok := r.Fields[f]
		if !ok || v == nil {
			problems = append(problems, "multiply: "+f+" is missing")
			continue
		}
		n, err := record.ToFloat(v)
		if err != nil {
			problems = append(problems, "multiply: "+f+" is not a number")
			continue
		}
		product *= n
	}
	return product, problems
}

func titleCase(s string) string {
	words := strings.Fields(s)
	for i, w := range words {
		first, size := utf8.DecodeRuneInString(w)
		words[i] = string(unicode.ToUpper(first)) + strings.ToLower(w[size:])
	}
	return strings.Join(words, " ")
}
