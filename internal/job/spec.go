// Package job holds the job specification, the job model and the job store.
package job

// Spec is the body of POST /api/v1/pipelines. See docs/job-spec.md.
type Spec struct {
	Name         string        `json:"name"`
	Sources      []SourceSpec  `json:"sources"`
	Schema       Schema        `json:"schema"`
	Transforms   []Transform   `json:"transforms,omitempty"`
	Aggregations []Aggregation `json:"aggregations,omitempty"`
	Exports      []Export      `json:"exports"`
	Options      Options       `json:"options"`
}

type SourceSpec struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Path        string `json:"path,omitempty"`
	URL         string `json:"url,omitempty"`
	TimeoutMS   int    `json:"timeout_ms,omitempty"`
	RecordsPath string `json:"records_path,omitempty"`
}

type Schema struct {
	Fields        []Field `json:"fields"`
	UnknownFields string  `json:"unknown_fields"`
}

type Field struct {
	Name     string   `json:"name"`
	Type     string   `json:"type"`
	Required bool     `json:"required,omitempty"`
	Min      *float64 `json:"min,omitempty"`
	Max      *float64 `json:"max,omitempty"`
	Pattern  string   `json:"pattern,omitempty"`
	Allowed  []string `json:"allowed,omitempty"`
	Format   string   `json:"format,omitempty"`
}

type Transform struct {
	Op      string            `json:"op"`
	Fields  []string          `json:"fields,omitempty"`
	Field   string            `json:"field,omitempty"`
	Target  string            `json:"target,omitempty"`
	From    string            `json:"from,omitempty"`
	To      string            `json:"to,omitempty"`
	Value   any               `json:"value,omitempty"`
	Table   map[string]string `json:"table,omitempty"`
	Default string            `json:"default,omitempty"`
}

type Aggregation struct {
	Name    string   `json:"name"`
	GroupBy []string `json:"group_by"`
	Metrics []Metric `json:"metrics"`
}

type Metric struct {
	Op    string `json:"op"`
	Field string `json:"field,omitempty"`
}

type Export struct {
	Type    string `json:"type"`
	Content string `json:"content"`
}

type Options struct {
	ValidateWorkers       int  `json:"validate_workers"`
	TransformWorkers      int  `json:"transform_workers"`
	BufferSize            int  `json:"buffer_size"`
	TimeoutSeconds        int  `json:"timeout_seconds"`
	MaxStoredErrors       int  `json:"max_stored_errors"`
	ContinueOnSourceError bool `json:"continue_on_source_error"`
}

const (
	DefaultWorkers         = 4
	DefaultBufferSize      = 100
	DefaultTimeoutSeconds  = 60
	DefaultMaxStoredErrors = 1000
	DefaultHTTPTimeoutMS   = 10000
	DefaultDateFormat      = "2006-01-02"

	MaxWorkers        = 32
	MaxBufferSize     = 5000
	MaxTimeoutSeconds = 3600
	MaxStoredErrors   = 100000
	MaxHTTPTimeoutMS  = 60000
	MaxSources        = 20
	MaxNameLength     = 100

	UnknownFieldsDrop = "drop"
	UnknownFieldsKeep = "keep"
)

// ApplyDefaults fills in every optional value left at zero. Call it before Validate.
func (s *Spec) ApplyDefaults() {
	for i := range s.Sources {
		if s.Sources[i].Type == "http" && s.Sources[i].TimeoutMS == 0 {
			s.Sources[i].TimeoutMS = DefaultHTTPTimeoutMS
		}
	}
	if s.Schema.UnknownFields == "" {
		s.Schema.UnknownFields = UnknownFieldsDrop
	}
	for i := range s.Schema.Fields {
		if s.Schema.Fields[i].Type == "date" && s.Schema.Fields[i].Format == "" {
			s.Schema.Fields[i].Format = DefaultDateFormat
		}
	}

	o := &s.Options
	setDefault(&o.ValidateWorkers, DefaultWorkers)
	setDefault(&o.TransformWorkers, DefaultWorkers)
	setDefault(&o.BufferSize, DefaultBufferSize)
	setDefault(&o.TimeoutSeconds, DefaultTimeoutSeconds)
	setDefault(&o.MaxStoredErrors, DefaultMaxStoredErrors)
}

func setDefault(v *int, def int) {
	if *v == 0 {
		*v = def
	}
}
