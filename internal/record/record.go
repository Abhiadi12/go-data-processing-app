package record

// INFO: recoreds represent a single row of data in the pipeline. Each record has a source, an index, and a set of fields that can be accessed by name. The fields are stored as a map of string to any type, allowing for flexible data representation.
type Record struct {
	Source string
	Index  int
	Fields map[string]any
}

func New(source string, index int) Record {
	return Record{Source: source, Index: index, Fields: make(map[string]any)}
}

func (r Record) Get(name string) (any, bool) {
	v, ok := r.Fields[name]
	return v, ok
}

func (r Record) Clone() Record {
	c := r
	c.Fields = make(map[string]any, len(r.Fields))
	for k, v := range r.Fields {
		c.Fields[k] = v
	}
	return c
}
