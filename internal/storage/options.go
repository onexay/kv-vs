package storage

import (
	"context"
	"time"
)

// Archive persists blob payloads outside of the in-memory/KeyDB cache.
type Archive interface {
	Store(ctx context.Context, repo, hash string, data []byte) error
	Fetch(ctx context.Context, repo, hash string) ([]byte, error)
	Remove(ctx context.Context, repo, hash string) error
	Close() error
}

// RetentionPolicy describes the hot-cache limits for a repository.
type RetentionPolicy struct {
	Repo           string
	HotCommitLimit int
	HotDuration    time.Duration
	Locked         bool
}

// RetentionDefaults provides fallback retention when no policy is configured.
type RetentionDefaults struct {
	HotCommitLimit int
	HotDuration    time.Duration
}

// Options control storage behaviour across backends.
type Options struct {
	Archive   Archive
	Retention RetentionDefaults
}

// WithRepo returns a copy of the policy bound to the provided repo name.
func (p RetentionPolicy) WithRepo(repo string) RetentionPolicy {
	p.Repo = repo
	return p
}

// Copy returns a shallow copy of the policy.
func (p RetentionPolicy) Copy() RetentionPolicy {
	return RetentionPolicy{
		Repo:           p.Repo,
		HotCommitLimit: p.HotCommitLimit,
		HotDuration:    p.HotDuration,
		Locked:         p.Locked,
	}
}
