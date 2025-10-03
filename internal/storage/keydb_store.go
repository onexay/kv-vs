package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	redis "github.com/redis/go-redis/v9"

	"github.com/onexay/kv-vs/internal/types"
)

const (
	repoCommitsKeyPrefix = "repo:commits"
)

type keydbStore struct {
	client        *redis.Client
	clock         func() time.Time
	archive       Archive
	defaultPolicy RetentionPolicy
}

type retentionRecord struct {
	HotCommitLimit     int   `json:"hotCommitLimit,omitempty"`
	HotDurationSeconds int64 `json:"hotDurationSeconds,omitempty"`
	Locked             bool  `json:"locked"`
}

func (r retentionRecord) toPolicy(repo string) RetentionPolicy {
	return RetentionPolicy{
		Repo:           repo,
		HotCommitLimit: r.HotCommitLimit,
		HotDuration:    time.Duration(r.HotDurationSeconds) * time.Second,
		Locked:         r.Locked,
	}
}

// Config defines KeyDB connection settings.
type Config struct {
	Addr     string
	Username string
	Password string
	Database int
}

// NewKeyDBStore initializes a Store backed by KeyDB.
func NewKeyDBStore(cfg Config, opts Options) (Store, error) {
	addr := cfg.Addr
	if addr == "" {
		addr = "localhost:6379"
	}

	redisOpts := &redis.Options{
		Addr:     addr,
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.Database,
	}

	client := redis.NewClient(redisOpts)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connect to keydb: %w", err)
	}

	return &keydbStore{
		client:        client,
		clock:         time.Now,
		archive:       opts.Archive,
		defaultPolicy: RetentionPolicy{HotCommitLimit: opts.Retention.HotCommitLimit, HotDuration: opts.Retention.HotDuration},
	}, nil
}

func (s *keydbStore) PutBlobAndCommit(ctx context.Context, req BlobWriteRequest) (BlobCommitResult, error) {
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

	policy := s.getPolicy(ctx, req.Name)

	branchKey := branchKey(req.Name, branch)
	repoCommitsKey := repoCommitsKey(req.Name)
	branchSet := branchSetKey(req.Name)
	authorKeyName := authorKey(req.Name, req.AuthorID)

	var result BlobCommitResult

	for {
		err := s.client.Watch(ctx, func(tx *redis.Tx) error {
			parent := ""
			branchBytes, err := tx.Get(ctx, branchKey).Bytes()
			if errors.Is(err, redis.Nil) {
				// no parent
			} else if err != nil {
				return err
			} else {
				var branchMeta types.Branch
				if err := json.Unmarshal(branchBytes, &branchMeta); err != nil {
					return err
				}
				parent = branchMeta.Commit
			}

			previousContent := ""
			if parent != "" {
				previousContent, err = tx.Get(ctx, contentKey(req.Name, parent)).Result()
				if errors.Is(err, redis.Nil) {
					return &NotFoundError{Resource: "content", Key: parent}
				}
				if err != nil {
					return err
				}
			}

			existingAuthorName, err := tx.Get(ctx, authorKeyName).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}
			if err == nil && existingAuthorName != req.AuthorName {
				return &ConflictError{Resource: "author", Key: req.AuthorID}
			}

			diff := computeDiff(previousContent, req.Content)
			contentHash := computeContentHash(req.Content)
			now := s.clock().UTC()
			commitHash := computeCommitHash(req.Name, branch, req.Content, parent, now)

			exists, err := tx.Exists(ctx, commitKey(req.Name, commitHash)).Result()
			if err != nil {
				return err
			}
			if exists == 1 {
				return &ConflictError{Resource: "commit", Key: commitHash}
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

			payload, err := json.Marshal(commit)
			if err != nil {
				return err
			}

			pipe := tx.TxPipeline()
			pipe.Set(ctx, commitKey(req.Name, commitHash), payload, 0)
			pipe.Set(ctx, contentKey(req.Name, commitHash), req.Content, 0)
			branchPayload, err := json.Marshal(types.Branch{
				Repo:      req.Name,
				Name:      branch,
				Commit:    commitHash,
				UpdatedAt: now,
			})
			if err != nil {
				return err
			}
			pipe.Set(ctx, branchKey, branchPayload, 0)
			pipe.SAdd(ctx, branchSet, branch)
			pipe.ZAdd(ctx, repoCommitsKey, redis.Z{Score: float64(now.UnixNano()), Member: commitHash})
			pipe.Set(ctx, authorKeyName, req.AuthorName, 0)

			if _, err := pipe.Exec(ctx); err != nil {
				return err
			}

			result = BlobCommitResult{
				CommitHash: commitHash,
				Branch:     branch,
				CreatedAt:  now,
				Diff:       diff,
			}
			return nil
		}, branchKey, repoCommitsKey)

		if err == nil {
			s.enforceRetention(ctx, req.Name, policy)
			return result, nil
		}

		if errors.Is(err, redis.TxFailedErr) {
			continue
		}

		return BlobCommitResult{}, err
	}
}

func (s *keydbStore) ListCommits(ctx context.Context, opts ListCommitsOptions) []types.Commit {
	if opts.Repo == "" {
		return []types.Commit{}
	}

	key := repoCommitsKey(opts.Repo)
	var (
		hashes []string
		err    error
	)
	limit := opts.Limit
	end := int64(-1)
	if limit > 0 {
		end = int64(limit) - 1
	}

	if opts.Descending {
		hashes, err = s.client.ZRevRange(ctx, key, 0, end).Result()
	} else {
		hashes, err = s.client.ZRange(ctx, key, 0, end).Result()
	}
	if err != nil {
		return []types.Commit{}
	}

	result := make([]types.Commit, 0, len(hashes))
	for _, hash := range hashes {
		commitBytes, err := s.client.Get(ctx, commitKey(opts.Repo, hash)).Bytes()
		if err != nil {
			continue
		}
		var commit types.Commit
		if err := json.Unmarshal(commitBytes, &commit); err != nil {
			continue
		}
		result = append(result, commit)
	}
	return result
}

func (s *keydbStore) GetCommit(ctx context.Context, repo, hash string) (types.Commit, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	commitBytes, err := s.client.Get(ctx, commitKey(repo, hash)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return types.Commit{}, "", &NotFoundError{Resource: "commit", Key: hash}
		}
		return types.Commit{}, "", err
	}

	var commit types.Commit
	if err := json.Unmarshal(commitBytes, &commit); err != nil {
		return types.Commit{}, "", err
	}

	content, err := s.client.Get(ctx, contentKey(repo, hash)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			if s.archive == nil {
				return commit, "", &NotFoundError{Resource: "content", Key: hash}
			}
			data, err := s.archive.Fetch(ctx, repo, hash)
			if err != nil {
				return commit, "", err
			}
			return commit, string(data), nil
		}
		return commit, "", err
	}

	return commit, content, nil
}

func (s *keydbStore) UpsertBranch(ctx context.Context, req BranchRequest) (types.Branch, error) {
	if req.Repo == "" || req.Name == "" || req.Commit == "" {
		return types.Branch{}, &ValidationError{Message: "repo, name, and commit are required"}
	}

	commit, _, err := s.GetCommit(ctx, req.Repo, req.Commit)
	if err != nil {
		return types.Branch{}, err
	}
	if commit.Repo != req.Repo {
		return types.Branch{}, &ValidationError{Message: "commit does not belong to repository"}
	}

	branch := types.Branch{
		Repo:      req.Repo,
		Name:      req.Name,
		Commit:    req.Commit,
		UpdatedAt: s.clock().UTC(),
	}

	payload, err := json.Marshal(branch)
	if err != nil {
		return types.Branch{}, err
	}

	pipe := s.client.TxPipeline()
	pipe.Set(ctx, branchKey(req.Repo, req.Name), payload, 0)
	pipe.SAdd(ctx, branchSetKey(req.Repo), req.Name)
	if _, err := pipe.Exec(ctx); err != nil {
		return types.Branch{}, err
	}

	return branch, nil
}

func (s *keydbStore) ListBranches(ctx context.Context, repo string) []types.Branch {
	if repo == "" {
		return []types.Branch{}
	}
	set := branchSetKey(repo)
	names, err := s.client.SMembers(ctx, set).Result()
	if err != nil {
		return []types.Branch{}
	}
	slices.Sort(names)
	result := make([]types.Branch, 0, len(names))
	for _, name := range names {
		branch, err := s.GetBranch(ctx, repo, name)
		if err == nil {
			result = append(result, branch)
		}
	}
	return result
}

func (s *keydbStore) GetBranch(ctx context.Context, repo, name string) (types.Branch, error) {
	if repo == "" || name == "" {
		return types.Branch{}, &ValidationError{Message: "repo and name are required"}
	}

	bytes, err := s.client.Get(ctx, branchKey(repo, name)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return types.Branch{}, &NotFoundError{Resource: "branch", Key: name}
		}
		return types.Branch{}, err
	}

	var branch types.Branch
	if err := json.Unmarshal(bytes, &branch); err != nil {
		return types.Branch{}, err
	}
	return branch, nil
}

func (s *keydbStore) CreateTag(ctx context.Context, req TagRequest) (types.Tag, error) {
	if req.Repo == "" || req.Name == "" || req.Commit == "" {
		return types.Tag{}, &ValidationError{Message: "repo, name, and commit are required"}
	}

	commit, _, err := s.GetCommit(ctx, req.Repo, req.Commit)
	if err != nil {
		return types.Tag{}, err
	}
	if commit.Repo != req.Repo {
		return types.Tag{}, &ValidationError{Message: "commit does not belong to repository"}
	}

	exists, err := s.client.Exists(ctx, tagKey(req.Repo, req.Name)).Result()
	if err != nil {
		return types.Tag{}, err
	}
	if exists == 1 {
		return types.Tag{}, &ConflictError{Resource: "tag", Key: req.Name}
	}

	tag := types.Tag{
		Repo:      req.Repo,
		Name:      req.Name,
		Commit:    req.Commit,
		Note:      req.Note,
		CreatedAt: s.clock().UTC(),
	}

	payload, err := json.Marshal(tag)
	if err != nil {
		return types.Tag{}, err
	}

	pipe := s.client.TxPipeline()
	pipe.Set(ctx, tagKey(req.Repo, req.Name), payload, 0)
	pipe.SAdd(ctx, tagSetKey(req.Repo), req.Name)
	if _, err := pipe.Exec(ctx); err != nil {
		return types.Tag{}, err
	}

	return tag, nil
}

func (s *keydbStore) ListTags(ctx context.Context, repo string) []types.Tag {
	if repo == "" {
		return []types.Tag{}
	}
	names, err := s.client.SMembers(ctx, tagSetKey(repo)).Result()
	if err != nil {
		return []types.Tag{}
	}
	slices.Sort(names)
	result := make([]types.Tag, 0, len(names))
	for _, name := range names {
		tag, err := s.GetTag(ctx, repo, name)
		if err == nil {
			result = append(result, tag)
		}
	}
	return result
}

func (s *keydbStore) GetTag(ctx context.Context, repo, name string) (types.Tag, error) {
	if repo == "" || name == "" {
		return types.Tag{}, &ValidationError{Message: "repo and name are required"}
	}

	bytes, err := s.client.Get(ctx, tagKey(repo, name)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return types.Tag{}, &NotFoundError{Resource: "tag", Key: name}
		}
		return types.Tag{}, err
	}

	var tag types.Tag
	if err := json.Unmarshal(bytes, &tag); err != nil {
		return types.Tag{}, err
	}
	return tag, nil
}

func (s *keydbStore) SetPolicy(ctx context.Context, policy RetentionPolicy) (RetentionPolicy, error) {
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

	key := policyKey(policy.Repo)
	seconds := int64(policy.HotDuration / time.Second)

	existing, err := s.client.Get(ctx, key).Bytes()
	if err == nil {
		var rec retentionRecord
		if err := json.Unmarshal(existing, &rec); err == nil {
			if rec.Locked && (rec.HotCommitLimit != policy.HotCommitLimit || rec.HotDurationSeconds != seconds) {
				return rec.toPolicy(policy.Repo), &ConflictError{Resource: "policy", Key: policy.Repo}
			}
		}
	} else if err != nil && !errors.Is(err, redis.Nil) {
		return RetentionPolicy{}, err
	}

	rec := retentionRecord{
		HotCommitLimit:     policy.HotCommitLimit,
		HotDurationSeconds: seconds,
		Locked:             true,
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return RetentionPolicy{}, err
	}

	if err := s.client.Set(ctx, key, payload, 0).Err(); err != nil {
		return RetentionPolicy{}, err
	}

	policy.Locked = true
	s.enforceRetention(ctx, policy.Repo, policy)
	return policy, nil
}

func (s *keydbStore) GetPolicy(ctx context.Context, repo string) (RetentionPolicy, error) {
	if repo == "" {
		return RetentionPolicy{}, &ValidationError{Message: "name query parameter required"}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	key := policyKey(repo)
	bytes, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return s.defaultPolicy.WithRepo(repo), nil
		}
		return RetentionPolicy{}, err
	}
	var rec retentionRecord
	if err := json.Unmarshal(bytes, &rec); err != nil {
		return RetentionPolicy{}, err
	}
	return rec.toPolicy(repo), nil
}

func (s *keydbStore) getPolicy(ctx context.Context, repo string) RetentionPolicy {
	policy, err := s.GetPolicy(ctx, repo)
	if err != nil {
		return s.defaultPolicy.WithRepo(repo)
	}
	return policy
}

func (s *keydbStore) enforceRetention(ctx context.Context, repo string, policy RetentionPolicy) {
	if s.archive == nil {
		return
	}
	if policy.HotCommitLimit <= 0 && policy.HotDuration <= 0 {
		return
	}
	hashes, err := s.client.ZRange(ctx, repoCommitsKey(repo), 0, -1).Result()
	if err != nil {
		return
	}
	type entry struct {
		hash      string
		timestamp time.Time
		archived  bool
	}
	entries := make([]entry, 0, len(hashes))
	for _, hash := range hashes {
		commit, err := s.getCommitMetadata(ctx, repo, hash)
		if err != nil {
			continue
		}
		entries = append(entries, entry{hash: commit.Hash, timestamp: commit.Timestamp, archived: commit.Archived})
	}
	toArchive := make(map[string]struct{})
	if policy.HotDuration > 0 {
		cutoff := s.clock().Add(-policy.HotDuration)
		for _, e := range entries {
			if e.archived {
				continue
			}
			if e.timestamp.Before(cutoff) {
				toArchive[e.hash] = struct{}{}
			}
		}
	}
	if policy.HotCommitLimit > 0 {
		remaining := make([]entry, 0, len(entries))
		for _, e := range entries {
			if e.archived {
				continue
			}
			if _, ok := toArchive[e.hash]; ok {
				continue
			}
			remaining = append(remaining, e)
		}
		if excess := len(remaining) - policy.HotCommitLimit; excess > 0 {
			for i := 0; i < excess; i++ {
				toArchive[remaining[i].hash] = struct{}{}
			}
		}
	}
	for hash := range toArchive {
		_ = s.archiveCommit(ctx, repo, hash)
	}
}

func (s *keydbStore) archiveCommit(ctx context.Context, repo, hash string) error {
	if s.archive == nil {
		return nil
	}
	commit, content, err := s.GetCommit(ctx, repo, hash)
	if err != nil {
		return err
	}
	if commit.Archived {
		return nil
	}
	if err := s.archive.Store(ctx, repo, hash, []byte(content)); err != nil {
		return err
	}
	commit.Archived = true
	payload, err := json.Marshal(commit)
	if err != nil {
		return err
	}
	pipe := s.client.TxPipeline()
	pipe.Set(ctx, commitKey(repo, hash), payload, 0)
	pipe.Del(ctx, contentKey(repo, hash))
	_, err = pipe.Exec(ctx)
	return err
}

func (s *keydbStore) getCommitMetadata(ctx context.Context, repo, hash string) (types.Commit, error) {
	bytes, err := s.client.Get(ctx, commitKey(repo, hash)).Bytes()
	if err != nil {
		return types.Commit{}, err
	}
	var commit types.Commit
	if err := json.Unmarshal(bytes, &commit); err != nil {
		return types.Commit{}, err
	}
	return commit, nil
}

func commitKey(repo, hash string) string {
	return fmt.Sprintf("commit:%s:%s", repo, hash)
}

func contentKey(repo, hash string) string {
	return fmt.Sprintf("content:%s:%s", repo, hash)
}

func branchKey(repo, branch string) string {
	return fmt.Sprintf("branch:%s:%s", repo, branch)
}

func repoCommitsKey(repo string) string {
	return fmt.Sprintf("%s:%s", repoCommitsKeyPrefix, repo)
}

func branchSetKey(repo string) string {
	return fmt.Sprintf("branchset:%s", repo)
}

func tagKey(repo, name string) string {
	return fmt.Sprintf("tag:%s:%s", repo, name)
}

func tagSetKey(repo string) string {
	return fmt.Sprintf("tagset:%s", repo)
}

func authorKey(repo, authorID string) string {
	return fmt.Sprintf("author:%s:%s", repo, authorID)
}

func policyKey(repo string) string {
	return fmt.Sprintf("policy:%s", repo)
}
