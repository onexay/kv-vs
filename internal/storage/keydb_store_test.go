package storage

import (
	"context"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
)

func TestKeyDBStorePutBlobAndCommit(t *testing.T) {
	addr := os.Getenv("TEST_KEYDB_ADDR")
	var cleanup func()
	var err error
	var store Store
	options := Options{Archive: NewMemoryArchive()}

	if addr == "" {
		mini, merr := miniredis.Run()
		if merr != nil {
			t.Fatalf("start miniredis: %v", merr)
		}
		t.Cleanup(mini.Close)
		store, err = NewKeyDBStore(Config{Addr: mini.Addr()}, options)
	} else {
		// Use the externally provided KeyDB instance.
		cleanup = func() {
			client := redis.NewClient(&redis.Options{Addr: addr})
			_ = client.FlushDB(context.Background()).Err()
			_ = client.Close()
		}
		t.Cleanup(cleanup)
		store, err = NewKeyDBStore(Config{Addr: addr}, options)
		if err == nil {
			// Clear any previous test data using the store itself.
			if ks, ok := store.(*keydbStore); ok {
				_ = ks.client.FlushDB(context.Background()).Err()
			}
		}
	}
	if err != nil {
		t.Fatalf("create store: %v", err)
	}

	req := BlobWriteRequest{
		Name:       "analytics",
		Branch:     "",
		Content:    "line one\nline two\n",
		AuthorName: "Alice",
		AuthorID:   "alice@id",
	}

	ctx := context.Background()
	result, err := store.PutBlobAndCommit(ctx, req)
	if err != nil {
		t.Fatalf("PutBlobAndCommit: %v", err)
	}

	if result.Branch != defaultBranch {
		t.Fatalf("expected branch %s, got %s", defaultBranch, result.Branch)
	}
	if result.CommitHash == "" {
		t.Fatalf("expected commit hash")
	}

	commit, content, err := store.GetCommit(ctx, "analytics", result.CommitHash)
	if err != nil {
		t.Fatalf("GetCommit: %v", err)
	}

	if content != req.Content {
		t.Fatalf("unexpected content: %s", content)
	}
	if commit.Parent != "" {
		t.Fatalf("expected empty parent for first commit")
	}
	if commit.AuthorName != "Alice" || commit.AuthorID != "alice@id" {
		t.Fatalf("unexpected author metadata")
	}

	// second commit on same branch
	second, err := store.PutBlobAndCommit(ctx, BlobWriteRequest{
		Name:       "analytics",
		Branch:     "",
		Content:    "line one\nline two updated\n",
		AuthorName: "Alice",
		AuthorID:   "alice@id",
	})
	if err != nil {
		t.Fatalf("second PutBlobAndCommit: %v", err)
	}

	if second.CommitHash == result.CommitHash {
		t.Fatalf("expected new commit hash")
	}
	if second.Diff == "" {
		t.Fatalf("expected diff for updated content")
	}

	commits := store.ListCommits(ctx, ListCommitsOptions{Repo: "analytics", Descending: true})
	if len(commits) != 2 {
		t.Fatalf("expected 2 commits, got %d", len(commits))
	}
	if commits[0].Hash != second.CommitHash {
		t.Fatalf("expected newest commit first")
	}

	branch, err := store.UpsertBranch(ctx, BranchRequest{Repo: "analytics", Name: "feature", Commit: result.CommitHash})
	if err != nil {
		t.Fatalf("UpsertBranch: %v", err)
	}
	if branch.Commit != result.CommitHash {
		t.Fatalf("unexpected branch commit")
	}

	branches := store.ListBranches(ctx, "analytics")
	if len(branches) != 2 {
		t.Fatalf("expected 2 branches, got %d", len(branches))
	}

	tag, err := store.CreateTag(ctx, TagRequest{Repo: "analytics", Name: "v1", Commit: second.CommitHash})
	if err != nil {
		t.Fatalf("CreateTag: %v", err)
	}
	if tag.Commit != second.CommitHash {
		t.Fatalf("unexpected tag commit")
	}

	tags := store.ListTags(ctx, "analytics")
	if len(tags) != 1 {
		t.Fatalf("expected 1 tag, got %d", len(tags))
	}

	if _, err := store.PutBlobAndCommit(ctx, BlobWriteRequest{Name: "analytics", Content: "third", AuthorName: "Bob", AuthorID: "alice@id"}); err == nil {
		t.Fatalf("expected author conflict")
	}

	policy, err := store.SetPolicy(ctx, RetentionPolicy{Repo: "analytics", HotCommitLimit: 1})
	if err != nil {
		t.Fatalf("SetPolicy: %v", err)
	}
	if !policy.Locked || policy.HotCommitLimit != 1 {
		t.Fatalf("unexpected policy response")
	}

	policyGet, err := store.GetPolicy(ctx, "analytics")
	if err != nil {
		t.Fatalf("GetPolicy: %v", err)
	}
	if policyGet.HotCommitLimit != 1 {
		t.Fatalf("unexpected policy limit: %d", policyGet.HotCommitLimit)
	}
}
