package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

func computeContentHash(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func computeCommitHash(repo, branch, content, parent string, ts time.Time) string {
	payload := strings.Join([]string{
		repo,
		branch,
		parent,
		content,
		ts.Format(time.RFC3339Nano),
	}, "\n")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}
