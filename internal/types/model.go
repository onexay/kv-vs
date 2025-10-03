package types

import "time"

// Commit captures a repository version entry.
type Commit struct {
	Repo        string    `json:"repo"`
	Branch      string    `json:"branch"`
	Hash        string    `json:"hash"`
	Parent      string    `json:"parent,omitempty"`
	AuthorName  string    `json:"author"`
	AuthorID    string    `json:"authorId"`
	Message     string    `json:"message,omitempty"`
	ContentHash string    `json:"contentHash"`
	Timestamp   time.Time `json:"timestamp"`
	Archived    bool      `json:"archived"`
}

// Branch points to the latest commit for a repository branch.
type Branch struct {
	Repo      string    `json:"repo"`
	Name      string    `json:"name"`
	Commit    string    `json:"commit"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// Tag anchors a commit to a friendly label within a repository.
type Tag struct {
	Repo      string    `json:"repo"`
	Name      string    `json:"name"`
	Commit    string    `json:"commit"`
	Note      string    `json:"note,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}
