package job

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"
)

var (
	ErrNotFound = errors.New("job not found")
	ErrExists   = errors.New("job already exists")
)

// Store keeps jobs. Handlers and the runner only use this interface, so a
// database-backed store can replace MemoryStore without changing them.
type Store interface {
	Create(ctx context.Context, j Job) error
	Get(ctx context.Context, id string) (Job, error)
	List(ctx context.Context) ([]Job, error)
}

var _ Store = (*MemoryStore)(nil)

// MemoryStore is a Store held in memory. Jobs are lost when the process stops.
// It is safe for concurrent use.
type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]Job
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{jobs: make(map[string]Job)}
}

func (s *MemoryStore) Create(_ context.Context, j Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.jobs[j.ID]; ok {
		return ErrExists
	}
	s.jobs[j.ID] = j
	return nil
}

func (s *MemoryStore) Get(_ context.Context, id string) (Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	j, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	return j, nil
}

// List returns all jobs, newest first.
func (s *MemoryStore) List(_ context.Context) ([]Job, error) {
	s.mu.RLock()
	jobs := make([]Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	s.mu.RUnlock()

	slices.SortFunc(jobs, func(a, b Job) int {
		if c := b.CreatedAt.Compare(a.CreatedAt); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return jobs, nil
}
