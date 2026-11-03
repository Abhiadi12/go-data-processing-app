package export

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/record"
)

// atomicFile is written under a hidden temp name in the final directory and
// renamed into place only when complete. A rename within one directory is
// atomic, so readers see either no file or the whole file.
type atomicFile struct {
	f     *os.File
	buf   *bufio.Writer
	final string
	done  bool
}

func createAtomic(dir, name string) (*atomicFile, error) {
	f, err := os.CreateTemp(dir, "."+name+".tmp-*")
	if err != nil {
		return nil, err
	}
	return &atomicFile{f: f, buf: bufio.NewWriter(f), final: filepath.Join(dir, name)}, nil
}

// finish flushes, syncs to disk and closes the temp file, ready to rename.
func (a *atomicFile) finish() error {
	if err := a.buf.Flush(); err != nil {
		return err
	}
	if err := a.f.Sync(); err != nil {
		return err
	}
	return a.f.Close()
}

func (a *atomicFile) rename() error {
	if err := os.Rename(a.f.Name(), a.final); err != nil {
		return err
	}
	a.done = true
	return nil
}

func (a *atomicFile) discard() {
	if a == nil {
		return
	}
	_ = a.f.Close()
	if a.done {
		_ = os.Remove(a.final)
		return
	}
	_ = os.Remove(a.f.Name())
}

func fileOutput(typ, content string, a *atomicFile, rows int64) job.Output {
	return job.Output{Type: typ, Content: content, Location: a.final, Rows: rows}
}

// jsonRecords streams records as a JSON array, one object per line.
type jsonRecords struct {
	file   *atomicFile
	format formatter
	rows   int64
}

func newJSONRecords(dir string, f formatter) (*jsonRecords, error) {
	file, err := createAtomic(dir, "records.json")
	if err != nil {
		return nil, err
	}
	if _, err := file.buf.WriteString("["); err != nil {
		file.discard()
		return nil, err
	}
	return &jsonRecords{file: file, format: f}, nil
}

func (w *jsonRecords) write(rec record.Record) error {
	obj := w.format.fields(rec)
	obj["_source"], obj["_index"] = rec.Source, rec.Index
	b, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("records.json: %w", err)
	}
	sep := ",\n  "
	if w.rows == 0 {
		sep = "\n  "
	}
	if _, err := w.file.buf.WriteString(sep); err != nil {
		return err
	}
	if _, err := w.file.buf.Write(b); err != nil {
		return err
	}
	w.rows++
	return nil
}

func (w *jsonRecords) prepare(context.Context, []job.AggregateResult) error {
	if _, err := w.file.buf.WriteString("\n]\n"); err != nil {
		return err
	}
	return w.file.finish()
}

func (w *jsonRecords) publish() (job.Output, error) {
	return fileOutput("json", "records", w.file, w.rows), w.file.rename()
}

func (w *jsonRecords) abort() { w.file.discard() }

// csvRecords streams records as CSV. Columns are fixed up front from the
// spec (source, index, then the output fields); a missing value is empty.
type csvRecords struct {
	file    *atomicFile
	csv     *csv.Writer
	columns []string
	format  formatter
	rows    int64
}

func newCSVRecords(dir string, columns []string, f formatter) (*csvRecords, error) {
	file, err := createAtomic(dir, "records.csv")
	if err != nil {
		return nil, err
	}
	w := &csvRecords{file: file, csv: csv.NewWriter(file.buf), columns: columns, format: f}
	if err := w.csv.Write(append([]string{"_source", "_index"}, columns...)); err != nil {
		file.discard()
		return nil, err
	}
	return w, nil
}

func (w *csvRecords) write(rec record.Record) error {
	row := make([]string, 0, len(w.columns)+2)
	row = append(row, rec.Source, fmt.Sprint(rec.Index))
	for _, c := range w.columns {
		row = append(row, w.format.text(c, rec.Fields[c]))
	}
	if err := w.csv.Write(row); err != nil {
		return fmt.Errorf("records.csv: %w", err)
	}
	w.rows++
	return nil
}

func (w *csvRecords) prepare(context.Context, []job.AggregateResult) error {
	w.csv.Flush()
	if err := w.csv.Error(); err != nil {
		return err
	}
	return w.file.finish()
}

func (w *csvRecords) publish() (job.Output, error) {
	return fileOutput("csv", "records", w.file, w.rows), w.file.rename()
}

func (w *csvRecords) abort() { w.file.discard() }

// jsonAggregates writes the aggregation results as one JSON document.
type jsonAggregates struct {
	dir  string
	file *atomicFile
	rows int64
}

func (w *jsonAggregates) write(record.Record) error { return nil }

func (w *jsonAggregates) prepare(_ context.Context, aggs []job.AggregateResult) error {
	file, err := createAtomic(w.dir, "aggregates.json")
	if err != nil {
		return err
	}
	w.file = file
	b, err := json.MarshalIndent(map[string]any{"aggregates": aggs}, "", "  ")
	if err != nil {
		return err
	}
	if _, err := file.buf.Write(append(b, '\n')); err != nil {
		return err
	}
	w.rows = countGroups(aggs)
	return file.finish()
}

func (w *jsonAggregates) publish() (job.Output, error) {
	return fileOutput("json", "aggregates", w.file, w.rows), w.file.rename()
}

func (w *jsonAggregates) abort() { w.file.discard() }

// csvAggregates writes one row per metric value ("long" format), because each
// aggregation can have different group fields and metrics:
// aggregation,group,metric,value  e.g.  by_category,category=books,sum_total,561
type csvAggregates struct {
	dir  string
	file *atomicFile
	rows int64
}

func (w *csvAggregates) write(record.Record) error { return nil }

func (w *csvAggregates) prepare(_ context.Context, aggs []job.AggregateResult) error {
	file, err := createAtomic(w.dir, "aggregates.csv")
	if err != nil {
		return err
	}
	w.file = file
	cw := csv.NewWriter(file.buf)
	if err := cw.Write([]string{"aggregation", "group", "metric", "value"}); err != nil {
		return err
	}
	for _, a := range aggs {
		for _, g := range a.Groups {
			group := groupLabel(a.GroupBy, g.Key)
			for _, name := range slices.Sorted(maps.Keys(g.Metrics)) {
				value := formatter{}.text("", g.Metrics[name])
				if err := cw.Write([]string{a.Name, group, name, value}); err != nil {
					return err
				}
				w.rows++
			}
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return err
	}
	return file.finish()
}

func (w *csvAggregates) publish() (job.Output, error) {
	return fileOutput("csv", "aggregates", w.file, w.rows), w.file.rename()
}

func (w *csvAggregates) abort() { w.file.discard() }

// groupLabel renders a group key as "category=books;region=APAC" in group_by order.
func groupLabel(groupBy []string, key map[string]any) string {
	parts := make([]string, len(groupBy))
	for i, f := range groupBy {
		parts[i] = f + "=" + formatter{}.text(f, key[f])
	}
	return strings.Join(parts, ";")
}

func countGroups(aggs []job.AggregateResult) int64 {
	var n int64
	for _, a := range aggs {
		n += int64(len(a.Groups))
	}
	return n
}
