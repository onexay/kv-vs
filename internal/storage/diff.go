package storage

import (
	"strings"

	"github.com/pmezard/go-difflib/difflib"
)

func computeDiff(previous, current string) string {
	if previous == current {
		return ""
	}

	d := difflib.UnifiedDiff{
		A:        difflib.SplitLines(previous),
		B:        difflib.SplitLines(current),
		FromFile: "previous",
		ToFile:   "current",
		Context:  3,
	}

	res, err := difflib.GetUnifiedDiffString(d)
	if err != nil {
		return strings.TrimSpace(current)
	}

	return strings.TrimSpace(res)
}
