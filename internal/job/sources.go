package job

import (
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/source"
)

// SourceDeps are the shared resources sources need: the data directory for
// files, and the guarded HTTP client for http sources.
type SourceDeps struct {
	Files        fs.FS
	Client       *http.Client
	HTTPMaxBytes int64
}

// BuildSources turns validated source specs into readers.
func BuildSources(specs []SourceSpec, deps SourceDeps) ([]source.Source, error) {
	out := make([]source.Source, 0, len(specs))
	for _, s := range specs {
		switch s.Type {
		case "csv":
			out = append(out, source.NewCSV(s.Name, deps.Files, s.Path))
		case "json":
			out = append(out, source.NewJSON(s.Name, deps.Files, s.Path, s.RecordsPath))
		case "http":
			out = append(out, source.NewHTTP(source.HTTPConfig{
				Name:        s.Name,
				URL:         s.URL,
				RecordsPath: s.RecordsPath,
				Timeout:     time.Duration(s.TimeoutMS) * time.Millisecond,
				MaxBytes:    deps.HTTPMaxBytes,
			}, deps.Client))
		default:
			return nil, fmt.Errorf("source %s: unknown type %q", s.Name, s.Type)
		}
	}
	return out, nil
}
