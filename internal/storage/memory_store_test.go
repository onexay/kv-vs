package storage

import (
	"context"
	"testing"
)

func TestMemoryStorePutBlobAndCommit(t *testing.T) {
	store := NewMemoryStore(Options{Archive: NewMemoryArchive()})
	ctx := context.Background()

	res, err := store.PutBlobAndCommit(ctx, BlobWriteRequest{Name: "repo", Content: "hello", AuthorName: "Alice", AuthorID: "alice@id"})
	if err != nil {
		t.Fatalf("PutBlobAndCommit: %v", err)
	}

	if res.CommitHash == "" {
		t.Fatalf("expected commit hash")
	}
	if res.Diff == "" {
		t.Fatalf("expected diff for first commit")
	}

	res2, err := store.PutBlobAndCommit(ctx, BlobWriteRequest{Name: "repo", Content: "hello world", AuthorName: "Alice", AuthorID: "alice@id"})
	if err != nil {
		t.Fatalf("second PutBlobAndCommit: %v", err)
	}

	commits := store.ListCommits(ctx, ListCommitsOptions{Repo: "repo", Descending: true, Limit: 1})
	if len(commits) != 1 {
		t.Fatalf("expected 1 commit, got %d", len(commits))
	}
	if commits[0].Hash != res2.CommitHash {
		t.Fatalf("expected newest commit returned")
	}
	if commits[0].AuthorName != "Alice" || commits[0].AuthorID != "alice@id" {
		t.Fatalf("unexpected author metadata")
	}

	branch, err := store.UpsertBranch(ctx, BranchRequest{Repo: "repo", Name: "dev", Commit: res.CommitHash})
	if err != nil {
		t.Fatalf("UpsertBranch: %v", err)
	}
	if branch.Commit != res.CommitHash {
		t.Fatalf("unexpected branch commit")
	}

	branches := store.ListBranches(ctx, "repo")
	if len(branches) != 2 {
		t.Fatalf("expected 2 branches, got %d", len(branches))
	}

	tag, err := store.CreateTag(ctx, TagRequest{Repo: "repo", Name: "v1", Commit: res2.CommitHash})
	if err != nil {
		t.Fatalf("CreateTag: %v", err)
	}
	if tag.Name != "v1" {
		t.Fatalf("unexpected tag name")
	}

	tags := store.ListTags(ctx, "repo")
	if len(tags) != 1 {
		t.Fatalf("expected 1 tag, got %d", len(tags))
	}

	if _, err := store.PutBlobAndCommit(ctx, BlobWriteRequest{Name: "repo", Content: "new", AuthorName: "Bob", AuthorID: "alice@id"}); err == nil {
		t.Fatalf("expected author conflict when reusing id with different name")
	}

	defaultPolicy, err := store.GetPolicy(ctx, "unconfigured")
	if err != nil {
		t.Fatalf("GetPolicy (default): %v", err)
	}
	if defaultPolicy.Repo != "unconfigured" {
		t.Fatalf("unexpected repo in default policy")
	}

	policy, err := store.SetPolicy(ctx, RetentionPolicy{Repo: "repo", HotCommitLimit: 1})
	if err != nil {
		t.Fatalf("SetPolicy: %v", err)
	}
	if !policy.Locked || policy.HotCommitLimit != 1 {
		t.Fatalf("unexpected policy response")
	}

	policyGet, err := store.GetPolicy(ctx, "repo")
	if err != nil {
		t.Fatalf("GetPolicy: %v", err)
	}
	if policyGet.HotCommitLimit != 1 {
		t.Fatalf("unexpected policy limit: %d", policyGet.HotCommitLimit)
	}
}
