package storage

import (
	"context"
	"sync"
)

// MemoryArchive is a simple map-backed archive used for testing.
type MemoryArchive struct {
	mu   sync.RWMutex
	data map[string]map[string][]byte // repo -> hash -> payload
}

// NewMemoryArchive constructs an in-memory archive.
func NewMemoryArchive() *MemoryArchive {
	return &MemoryArchive{data: make(map[string]map[string][]byte)}
}

func (m *MemoryArchive) Store(ctx context.Context, repo, hash string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[repo]; !ok {
		m.data[repo] = make(map[string][]byte)
	}
	m.data[repo][hash] = append([]byte{}, data...)
	return nil
}

func (m *MemoryArchive) Fetch(ctx context.Context, repo, hash string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	repoData, ok := m.data[repo]
	if !ok {
		return nil, &NotFoundError{Resource: "archive", Key: hash}
	}
	payload, ok := repoData[hash]
	if !ok {
		return nil, &NotFoundError{Resource: "archive", Key: hash}
	}
	return append([]byte{}, payload...), nil
}

func (m *MemoryArchive) Remove(ctx context.Context, repo, hash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if repoData, ok := m.data[repo]; ok {
		delete(repoData, hash)
	}
	return nil
}

func (m *MemoryArchive) Close() error { return nil }
