package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
)

type openAPIDoc struct {
	OpenAPI    string                                `json:"openapi"`
	Paths      map[string]map[string]json.RawMessage `json:"paths"`
	Components map[string]map[string]json.RawMessage `json:"components"`
}

func loadOpenAPI(t *testing.T) openAPIDoc {
	t.Helper()
	if len(openAPISpec) == 0 {
		t.Fatal("openAPISpec is empty: the //go:embed openapi.json line above it in docs.go is missing")
	}
	var doc openAPIDoc
	if err := json.Unmarshal(openAPISpec, &doc); err != nil {
		t.Fatalf("openapi.json is not valid json: %v", err)
	}
	return doc
}

func TestOpenAPICoversEveryRoute(t *testing.T) {
	doc := loadOpenAPI(t)

	var inCode []string
	for _, rt := range (&server{}).routes() {
		method, path, _ := strings.Cut(rt.pattern, " ")
		inCode = append(inCode, strings.ToLower(method)+" "+path)
	}
	var inSpec []string
	for path, ops := range doc.Paths {
		for method := range ops {
			inSpec = append(inSpec, method+" "+path)
		}
	}

	for _, r := range inCode {
		if !slices.Contains(inSpec, r) {
			t.Errorf("route %q is missing from openapi.json", r)
		}
	}
	for _, r := range inSpec {
		if !slices.Contains(inCode, r) {
			t.Errorf("openapi.json documents %q, which the router does not serve", r)
		}
	}
}

var regexpRefs = regexp.MustCompile(`"\$ref":\s*"#/components/(\w+)/(\w+)"`)

func TestOpenAPIRefsResolve(t *testing.T) {
	doc := loadOpenAPI(t)

	refs := regexpRefs.FindAllSubmatch(openAPISpec, -1)
	if len(refs) == 0 {
		t.Fatal("found no $refs; is the pattern still right?")
	}
	for _, m := range refs {
		kind, name := string(m[1]), string(m[2])
		if _, ok := doc.Components[kind][name]; !ok {
			t.Errorf("$ref #/components/%s/%s does not exist", kind, name)
		}
	}
}

func TestOpenAPIExamplesAreValidSpecs(t *testing.T) {
	var doc struct {
		Paths map[string]map[string]struct {
			RequestBody struct {
				Content map[string]struct {
					Examples map[string]struct {
						Value json.RawMessage `json:"value"`
					} `json:"examples"`
				} `json:"content"`
			} `json:"requestBody"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(openAPISpec, &doc); err != nil {
		t.Fatal(err)
	}
	examples := doc.Paths["/api/v1/pipelines"]["post"].RequestBody.Content["application/json"].Examples
	if len(examples) == 0 {
		t.Fatal("no request examples for POST /api/v1/pipelines")
	}

	// The examples point at testdata/ in the repository root.
	files := os.DirFS("../..")
	for name, ex := range examples {
		t.Run(name, func(t *testing.T) {
			var spec job.Spec
			dec := json.NewDecoder(bytes.NewReader(ex.Value))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&spec); err != nil {
				t.Fatalf("example does not decode: %v", err)
			}
			spec.ApplyDefaults()
			err := spec.Validate(files)
			if name == "invalid" {
				if err == nil {
					t.Error("the invalid example passed validation")
				}
				return
			}
			if err != nil {
				t.Errorf("example is not a valid spec: %v", err)
			}
		})
	}

	t.Run("orders matches examples/orders-job.json", func(t *testing.T) {
		raw, err := os.ReadFile("../../examples/orders-job.json")
		if err != nil {
			t.Fatal(err)
		}
		var fromFile, fromDocs job.Spec
		if err := json.Unmarshal(raw, &fromFile); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(examples["orders"].Value, &fromDocs); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(fromFile, fromDocs) {
			t.Error("the orders example in openapi.json differs from examples/orders-job.json")
		}
	})
}

func TestOpenAPIEndpoint(t *testing.T) {
	rec := do(t, newTestHandler(t, nil), http.MethodGet, "/openapi.json", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("content-type = %q", got)
	}
	if !bytes.Equal(rec.Body.Bytes(), openAPISpec) {
		t.Error("body is not the embedded spec")
	}
}

func TestDocsPage(t *testing.T) {
	rec := do(t, newTestHandler(t, nil), http.MethodGet, "/docs", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Errorf("content-type = %q", got)
	}
	csp := rec.Header().Get("Content-Security-Policy")
	if csp != docsCSP {
		t.Errorf("csp = %q, want the docs policy", csp)
	}
	body := rec.Body.String()
	for _, want := range []string{`integrity="` + swaggerUIScriptSRI, `integrity="` + swaggerUIStyleSRI, `url: "/openapi.json"`} {
		if !strings.Contains(body, want) {
			t.Errorf("page does not contain %s", want)
		}
	}
	if !strings.Contains(csp, scriptHash(docsInitScript)) {
		t.Error("csp does not allow the inline script")
	}
	if rec.Header().Get("X-Frame-Options") != "DENY" {
		t.Error("security headers missing on /docs")
	}
}
