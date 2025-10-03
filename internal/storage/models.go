package storage

import "time"

// BlobWriteRequest describes a versioned blob submission.
type BlobWriteRequest struct {
	Name       string
	Branch     string
	Content    string
	AuthorName string
	AuthorID   string
}

// BlobCommitResult summarises the commit created by a blob upload.
type BlobCommitResult struct {
	CommitHash string
	Branch     string
	CreatedAt  time.Time
	Diff       string
}

const defaultBranch = "main"

// ListCommitsOptions controls history retrieval.
type ListCommitsOptions struct {
	Repo       string
	Descending bool
	Limit      int
}

// BranchRequest is used to create or update a branch pointer.
type BranchRequest struct {
	Repo   string
	Name   string
	Commit string
}

// TagRequest is used to create a tag.
type TagRequest struct {
	Repo   string
	Name   string
	Commit string
	Note   string
}
