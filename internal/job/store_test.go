package job

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/source"
)

func TestNewJob(t *testing.T) {
	j := New(validSpec(), "req-1")

	if j.ID == "" || j.Name != "orders" || j.Status != StatusPending || j.RequestID != "req-1" {
		t.Errorf("got %+v", j)
	}
	if j.CreatedAt.IsZero() || j.CreatedAt.Location() != time.UTC {
		t.Errorf("created_at = %v, want a UTC time", j.CreatedAt)
	}
	if New(validSpec(), "").ID == j.ID {
		t.Error("two jobs got the same id")
	}
}

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	first := New(validSpec(), "")
	second := New(validSpec(), "")
	second.CreatedAt = first.CreatedAt.Add(time.Second)

	for _, j := range []Job{first, second} {
		if err := s.Create(ctx, j); err != nil {
			t.Fatalf("create: %v", err)
		}
	}
	if err := s.Create(ctx, first); !errors.Is(err, ErrExists) {
		t.Errorf("duplicate create: err = %v, want ErrExists", err)
	}

	got, err := s.Get(ctx, first.ID)
	if err != nil || got.ID != first.ID {
		t.Errorf("get: %+v, %v", got, err)
	}
	if _, err := s.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("get missing: err = %v, want ErrNotFound", err)
	}

	list, err := s.List(ctx)
	if err != nil || len(list) != 2 {
		t.Fatalf("list: %d jobs, %v", len(list), err)
	}
	if list[0].ID != second.ID {
		t.Error("list is not newest first")
	}

	seen := map[string]bool{}
	if err := s.Walk(ctx, func(j Job) { seen[j.ID] = true }); err != nil || len(seen) != 2 || !seen[first.ID] || !seen[second.ID] {
		t.Errorf("walk saw %v, %v; want both jobs", seen, err)
	}
}

func TestMemoryStoreConcurrentUse(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = s.Create(ctx, New(validSpec(), ""))
		}()
		go func() {
			defer wg.Done()
			_, _ = s.List(ctx)
			_ = s.Walk(ctx, func(Job) {})
		}()
	}
	wg.Wait()

	list, _ := s.List(ctx)
	if len(list) != 50 {
		t.Errorf("got %d jobs, want 50", len(list))
	}
}

func TestBuildSources(t *testing.T) {
	spec := validSpec()
	spec.Sources = append(spec.Sources, SourceSpec{Name: "orders-json", Type: "json", Path: "orders.json"})
	spec.ApplyDefaults()

	srcs, err := BuildSources(spec.Sources, SourceDeps{Files: fstest.MapFS{}, Client: http.DefaultClient})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(srcs) != 3 {
		t.Fatalf("got %d sources, want 3", len(srcs))
	}
	if _, ok := srcs[0].(*source.CSVSource); !ok {
		t.Errorf("source 0 is %T", srcs[0])
	}
	if _, ok := srcs[1].(*source.HTTPSource); !ok {
		t.Errorf("source 1 is %T", srcs[1])
	}
	if _, ok := srcs[2].(*source.JSONSource); !ok || srcs[2].Name() != "orders-json" {
		t.Errorf("source 2 is %T named %q", srcs[2], srcs[2].Name())
	}

	if _, err := BuildSources([]SourceSpec{{Name: "x", Type: "xml"}}, SourceDeps{}); err == nil {
		t.Error("unknown type: want an error")
	}
}

func TestMemoryStoreUpdate(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	j := New(validSpec(), "")
	if err := s.Create(ctx, j); err != nil {
		t.Fatal(err)
	}

	got, err := s.Update(ctx, j.ID, func(j *Job) error {
		j.Status = StatusRunning
		return nil
	})
	if err != nil || got.Status != StatusRunning {
		t.Fatalf("update: %+v, %v", got, err)
	}

	errNo := errors.New("no")
	if _, err := s.Update(ctx, j.ID, func(j *Job) error {
		j.Status = StatusFailed
		return errNo
	}); !errors.Is(err, errNo) {
		t.Fatalf("err = %v, want errNo", err)
	}
	if stored, _ := s.Get(ctx, j.ID); stored.Status != StatusRunning {
		t.Errorf("failed update was saved: status %s", stored.Status)
	}

	if _, err := s.Update(ctx, "missing", func(*Job) error { return nil }); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestMemoryStoreDelete(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	j := New(validSpec(), "")
	if err := s.Create(ctx, j); err != nil {
		t.Fatal(err)
	}

	if err := s.Delete(ctx, j.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.Get(ctx, j.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("get after delete: err = %v, want ErrNotFound", err)
	}
	if err := s.Delete(ctx, j.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("second delete: err = %v, want ErrNotFound", err)
	}
}

func TestJobFinished(t *testing.T) {
	for status, want := range map[Status]bool{
		StatusPending: false, StatusRunning: false,
		StatusCompleted: true, StatusFailed: true, StatusCancelled: true,
	} {
		if got := (Job{Status: status}).Finished(); got != want {
			t.Errorf("%s: Finished() = %v, want %v", status, got, want)
		}
	}
}
