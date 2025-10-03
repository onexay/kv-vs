package storage

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/onexay/kv-vs/internal/types"
)

// Store defines required persistence operations for versioned blobs.
type Store interface {
	PutBlobAndCommit(ctx context.Context, req BlobWriteRequest) (BlobCommitResult, error)
	ListCommits(ctx context.Context, opts ListCommitsOptions) []types.Commit
	GetCommit(ctx context.Context, repo, hash string) (types.Commit, string, error)
	UpsertBranch(ctx context.Context, req BranchRequest) (types.Branch, error)
	ListBranches(ctx context.Context, repo string) []types.Branch
	GetBranch(ctx context.Context, repo, name string) (types.Branch, error)
	CreateTag(ctx context.Context, req TagRequest) (types.Tag, error)
	ListTags(ctx context.Context, repo string) []types.Tag
	GetTag(ctx context.Context, repo, name string) (types.Tag, error)
	SetPolicy(ctx context.Context, policy RetentionPolicy) (RetentionPolicy, error)
	GetPolicy(ctx context.Context, repo string) (RetentionPolicy, error)
}

// NotFoundError signals missing records.
type NotFoundError struct {
	Resource string
	Key      string
}

func (e *NotFoundError) Error() string {
	return e.Resource + " " + e.Key + " not found"
}

// ConflictError signals concurrent modification or duplicate creation attempts.
type ConflictError struct {
	Resource string
	Key      string
}

func (e *ConflictError) Error() string {
	return e.Resource + " " + e.Key + " conflicts with existing state"
}

// ValidationError represents invalid input supplied by clients.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

// memoryStore provides an in-memory fallback for development and testing.
type memoryStore struct {
	mu            sync.RWMutex
	clock         func() time.Time
	commits       map[string]types.Commit
	contents      map[string]string
	repoCommits   map[string][]string
	branches      map[string]map[string]types.Branch // repo -> branch -> branch metadata
	tags          map[string]map[string]types.Tag    // repo -> tag -> tag metadata
	authors       map[string]map[string]string       // repo -> authorID -> authorName
	policies      map[string]RetentionPolicy
	defaultPolicy RetentionPolicy
	archive       Archive
}

// NewMemoryStore initializes an empty in-memory store.
func NewMemoryStore(opts Options) Store {
	return &memoryStore{
		clock:         time.Now,
		commits:       make(map[string]types.Commit),
		contents:      make(map[string]string),
		repoCommits:   make(map[string][]string),
		branches:      make(map[string]map[string]types.Branch),
		tags:          make(map[string]map[string]types.Tag),
		authors:       make(map[string]map[string]string),
		policies:      make(map[string]RetentionPolicy),
		defaultPolicy: RetentionPolicy{HotCommitLimit: opts.Retention.HotCommitLimit, HotDuration: opts.Retention.HotDuration},
		archive:       opts.Archive,
	}
}

func (m *memoryStore) PutBlobAndCommit(ctx context.Context, req BlobWriteRequest) (BlobCommitResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if req.Name == "" {
		return BlobCommitResult{}, &ValidationError{Message: "name is required"}
	}
	if req.Content == "" {
		return BlobCommitResult{}, &ValidationError{Message: "content is required"}
	}
	if req.AuthorName == "" || req.AuthorID == "" {
		return BlobCommitResult{}, &ValidationError{Message: "author name and id are required"}
	}

	branch := req.Branch
	if branch == "" {
		branch = defaultBranch
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	repoBranches, ok := m.branches[req.Name]
	if !ok {
		repoBranches = make(map[string]types.Branch)
		m.branches[req.Name] = repoBranches
	}

	repoAuthors, ok := m.authors[req.Name]
	if !ok {
		repoAuthors = make(map[string]string)
		m.authors[req.Name] = repoAuthors
	}
	if existingName, ok := repoAuthors[req.AuthorID]; ok && existingName != req.AuthorName {
		return BlobCommitResult{}, &ConflictError{Resource: "author", Key: req.AuthorID}
	}
	repoAuthors[req.AuthorID] = req.AuthorName

	parent := ""
	if existing, ok := repoBranches[branch]; ok {
		parent = existing.Commit
	}
	previousContent := ""
	if parent != "" {
		if content, ok := m.contents[parent]; ok {
			previousContent = content
		} else {
			return BlobCommitResult{}, &NotFoundError{Resource: "commit", Key: parent}
		}
	}

	diff := computeDiff(previousContent, req.Content)
	contentHash := computeContentHash(req.Content)
	now := m.clock().UTC()
	commitHash := computeCommitHash(req.Name, branch, req.Content, parent, now)

	if _, exists := m.commits[commitHash]; exists {
		return BlobCommitResult{}, &ConflictError{Resource: "commit", Key: commitHash}
	}

	commit := types.Commit{
		Repo:        req.Name,
		Branch:      branch,
		Hash:        commitHash,
		Parent:      parent,
		AuthorName:  req.AuthorName,
		AuthorID:    req.AuthorID,
		Message:     "auto commit",
		ContentHash: contentHash,
		Timestamp:   now,
		Archived:    false,
	}

	m.commits[commitHash] = commit
	m.contents[commitHash] = req.Content
	repoBranches[branch] = types.Branch{
		Repo:      req.Name,
		Name:      branch,
		Commit:    commitHash,
		UpdatedAt: now,
	}
	m.repoCommits[req.Name] = append(m.repoCommits[req.Name], commitHash)

	m.applyRetentionLocked(ctx, req.Name)

	return BlobCommitResult{
		CommitHash: commitHash,
		Branch:     branch,
		CreatedAt:  now,
		Diff:       diff,
	}, nil
}

func (m *memoryStore) ListCommits(ctx context.Context, opts ListCommitsOptions) []types.Commit {
	m.mu.RLock()
	defer m.mu.RUnlock()

	commitHashes, ok := m.repoCommits[opts.Repo]
	if !ok {
		return []types.Commit{}
	}

	result := make([]types.Commit, 0, len(commitHashes))
	limit := opts.Limit
	appendCommit := func(hash string) {
		if commit, ok := m.commits[hash]; ok {
			result = append(result, commit)
		}
	}

	if opts.Descending {
		for i := len(commitHashes) - 1; i >= 0; i-- {
			appendCommit(commitHashes[i])
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	} else {
		for _, hash := range commitHashes {
			appendCommit(hash)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}

	return result
}

func (m *memoryStore) GetCommit(ctx context.Context, repo, hash string) (types.Commit, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	commit, ok := m.commits[hash]
	if !ok || commit.Repo != repo {
		return types.Commit{}, "", &NotFoundError{Resource: "commit", Key: hash}
	}

	content, ok := m.contents[hash]
	if !ok {
		if m.archive == nil {
			return types.Commit{}, "", &NotFoundError{Resource: "content", Key: hash}
		}
		data, err := m.archive.Fetch(ctx, repo, hash)
		if err != nil {
			return types.Commit{}, "", err
		}
		content = string(data)
	}

	return commit, content, nil
}

func (m *memoryStore) SetPolicy(ctx context.Context, policy RetentionPolicy) (RetentionPolicy, error) {
	if policy.Repo == "" {
		return RetentionPolicy{}, &ValidationError{Message: "repository name is required"}
	}
	if policy.HotCommitLimit < 0 {
		return RetentionPolicy{}, &ValidationError{Message: "hotCommitLimit must be >= 0"}
	}
	if policy.HotDuration < 0 {
		return RetentionPolicy{}, &ValidationError{Message: "hotDuration must be >= 0"}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.policies[policy.Repo]
	if ok && existing.Locked && (existing.HotCommitLimit != policy.HotCommitLimit || existing.HotDuration != policy.HotDuration) {
		return existing.Copy(), &ConflictError{Resource: "policy", Key: policy.Repo}
	}

	policy.Locked = true
	m.policies[policy.Repo] = policy
	m.applyRetentionLocked(ctx, policy.Repo)
	return policy.Copy(), nil
}

func (m *memoryStore) GetPolicy(ctx context.Context, repo string) (RetentionPolicy, error) {
	if repo == "" {
		return RetentionPolicy{}, &ValidationError{Message: "name query parameter required"}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	policy := m.getPolicyLocked(repo)
	return policy.Copy(), nil
}

func (m *memoryStore) getPolicyLocked(repo string) RetentionPolicy {
	if policy, ok := m.policies[repo]; ok {
		return policy.Copy()
	}
	return m.defaultPolicy.WithRepo(repo)
}

func (m *memoryStore) applyRetentionLocked(ctx context.Context, repo string) {
	if m.archive == nil {
		return
	}
	policy := m.getPolicyLocked(repo)
	if policy.HotCommitLimit <= 0 && policy.HotDuration <= 0 {
		return
	}
	hashes := m.repoCommits[repo]
	if len(hashes) == 0 {
		return
	}

	active := make([]types.Commit, 0, len(hashes))
	for _, hash := range hashes {
		commit := m.commits[hash]
		if commit.Archived {
			continue
		}
		active = append(active, commit)
	}

	toArchive := make(map[string]struct{})
	if policy.HotDuration > 0 {
		cutoff := m.clock().Add(-policy.HotDuration)
		for _, commit := range active {
			if commit.Timestamp.Before(cutoff) {
				toArchive[commit.Hash] = struct{}{}
			}
		}
	}

	if policy.HotCommitLimit > 0 {
		remaining := make([]types.Commit, 0, len(active))
		for _, commit := range active {
			if _, ok := toArchive[commit.Hash]; !ok {
				remaining = append(remaining, commit)
			}
		}
		if excess := len(remaining) - policy.HotCommitLimit; excess > 0 {
			for i := 0; i < excess; i++ {
				toArchive[remaining[i].Hash] = struct{}{}
			}
		}
	}

	for hash := range toArchive {
		m.flushCommitLocked(ctx, repo, hash)
	}
}

func (m *memoryStore) flushCommitLocked(ctx context.Context, repo, hash string) {
	if m.archive == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	commit, ok := m.commits[hash]
	if !ok || commit.Archived {
		return
	}
	content, ok := m.contents[hash]
	if !ok {
		commit.Archived = true
		m.commits[hash] = commit
		return
	}
	if err := m.archive.Store(ctx, repo, hash, []byte(content)); err != nil {
		return
	}
	delete(m.contents, hash)
	commit.Archived = true
	m.commits[hash] = commit
}

func (m *memoryStore) UpsertBranch(ctx context.Context, req BranchRequest) (types.Branch, error) {
	if req.Repo == "" || req.Name == "" || req.Commit == "" {
		return types.Branch{}, &ValidationError{Message: "repo, name, and commit are required"}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	commit, ok := m.commits[req.Commit]
	if !ok || commit.Repo != req.Repo {
		return types.Branch{}, &NotFoundError{Resource: "commit", Key: req.Commit}
	}

	repoBranches, ok := m.branches[req.Repo]
	if !ok {
		repoBranches = make(map[string]types.Branch)
		m.branches[req.Repo] = repoBranches
	}

	branch := types.Branch{
		Repo:      req.Repo,
		Name:      req.Name,
		Commit:    req.Commit,
		UpdatedAt: m.clock().UTC(),
	}

	repoBranches[req.Name] = branch
	return branch, nil
}

func (m *memoryStore) ListBranches(ctx context.Context, repo string) []types.Branch {
	m.mu.RLock()
	defer m.mu.RUnlock()

	repoBranches, ok := m.branches[repo]
	if !ok {
		return []types.Branch{}
	}

	names := make([]string, 0, len(repoBranches))
	for name := range repoBranches {
		names = append(names, name)
	}
	slices.Sort(names)
	result := make([]types.Branch, 0, len(names))
	for _, name := range names {
		result = append(result, repoBranches[name])
	}
	return result
}

func (m *memoryStore) GetBranch(ctx context.Context, repo, name string) (types.Branch, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	repoBranches, ok := m.branches[repo]
	if !ok {
		return types.Branch{}, &NotFoundError{Resource: "branch", Key: name}
	}

	branch, ok := repoBranches[name]
	if !ok {
		return types.Branch{}, &NotFoundError{Resource: "branch", Key: name}
	}

	return branch, nil
}

func (m *memoryStore) CreateTag(ctx context.Context, req TagRequest) (types.Tag, error) {
	if req.Repo == "" || req.Name == "" || req.Commit == "" {
		return types.Tag{}, &ValidationError{Message: "repo, name, and commit are required"}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	commit, ok := m.commits[req.Commit]
	if !ok || commit.Repo != req.Repo {
		return types.Tag{}, &NotFoundError{Resource: "commit", Key: req.Commit}
	}

	repoTags, ok := m.tags[req.Repo]
	if !ok {
		repoTags = make(map[string]types.Tag)
		m.tags[req.Repo] = repoTags
	}

	if _, exists := repoTags[req.Name]; exists {
		return types.Tag{}, &ConflictError{Resource: "tag", Key: req.Name}
	}

	tag := types.Tag{
		Repo:      req.Repo,
		Name:      req.Name,
		Commit:    req.Commit,
		Note:      req.Note,
		CreatedAt: m.clock().UTC(),
	}

	repoTags[req.Name] = tag
	return tag, nil
}

func (m *memoryStore) ListTags(ctx context.Context, repo string) []types.Tag {
	m.mu.RLock()
	defer m.mu.RUnlock()

	repoTags, ok := m.tags[repo]
	if !ok {
		return []types.Tag{}
	}

	names := make([]string, 0, len(repoTags))
	for name := range repoTags {
		names = append(names, name)
	}
	slices.Sort(names)
	result := make([]types.Tag, 0, len(names))
	for _, name := range names {
		result = append(result, repoTags[name])
	}
	return result
}

func (m *memoryStore) GetTag(ctx context.Context, repo, name string) (types.Tag, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	repoTags, ok := m.tags[repo]
	if !ok {
		return types.Tag{}, &NotFoundError{Resource: "tag", Key: name}
	}

	tag, ok := repoTags[name]
	if !ok {
		return types.Tag{}, &NotFoundError{Resource: "tag", Key: name}
	}

	return tag, nil
}
