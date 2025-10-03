package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/onexay/kv-vs/internal/config"
	"github.com/onexay/kv-vs/internal/storage"
)

// Service holds business logic and storage dependencies.
type Service struct {
	store   storage.Store
	archive storage.Archive
}

const defaultBranchName = "main"
const (
	headerAuthorName = "X-Author-Name"
	headerAuthorID   = "X-Author-ID"
)

// New constructs the service wiring.
func New(ctx context.Context, cfg config.Config) (*Service, error) {
	var archive storage.Archive
	if cfg.Retention.ArchivePath != "" {
		arc, err := storage.NewBoltArchive(cfg.Retention.ArchivePath)
		if err != nil {
			return nil, err
		}
		archive = arc
	}

	options := storage.Options{
		Archive: archive,
		Retention: storage.RetentionDefaults{
			HotCommitLimit: cfg.Retention.HotCommitLimit,
			HotDuration:    cfg.Retention.HotDuration,
		},
	}

	var (
		store storage.Store
		err   error
	)

	switch cfg.Storage.Backend {
	case config.StorageBackendKeyDB:
		store, err = storage.NewKeyDBStore(cfg.Storage.KeyDB, options)
		if err != nil {
			if archive != nil {
				_ = archive.Close()
			}
			return nil, err
		}
	default:
		store = storage.NewMemoryStore(options)
	}

	return &Service{store: store, archive: archive}, nil
}

// Handler builds the REST routes for the service.
func Handler(svc *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/swagger") {
			svc.handleSwagger(w, r, strings.TrimPrefix(r.URL.Path, "/swagger"))
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/api/v1")
		if path == "" || path == "/" {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown endpoint"})
			return
		}

		if _, _, err := authorFromHeaders(r); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		switch {
		case strings.HasPrefix(path, "/blob/repo/"):
			svc.handleBlobRepo(w, r, strings.TrimPrefix(path, "/blob/repo/"))
		case path == "/blob" || strings.HasPrefix(path, "/blob"):
			svc.handleBlob(w, r)
		case strings.HasPrefix(path, "/commits"):
			svc.handleCommits(w, r, strings.TrimPrefix(path, "/commits"))
		case strings.HasPrefix(path, "/branches"):
			svc.handleBranches(w, r, strings.TrimPrefix(path, "/branches"))
		case strings.HasPrefix(path, "/tags"):
			svc.handleTags(w, r, strings.TrimPrefix(path, "/tags"))
		case strings.HasPrefix(path, "/policies"):
			svc.handlePolicies(w, r, strings.TrimPrefix(path, "/policies"))
		default:
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown resource"})
		}
	})
}

func (s *Service) handleBlob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		switch r.Method {
		case http.MethodGet:
			s.handleBlobGet(w, r)
		default:
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		}
		return
	}

	authorName, authorID, err := authorFromHeaders(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	type request struct {
		Name       string `json:"name"`
		BranchName string `json:"branch_name,omitempty"`
		Content    string `json:"content"`
	}

	var req request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid payload"})
		return
	}

	result, err := s.store.PutBlobAndCommit(r.Context(), storage.BlobWriteRequest{
		Name:       req.Name,
		Branch:     req.BranchName,
		Content:    req.Content,
		AuthorName: authorName,
		AuthorID:   authorID,
	})
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"commit":     result.CommitHash,
		"branch":     result.Branch,
		"created_at": result.CreatedAt,
		"diff":       result.Diff,
	})
}

func (s *Service) handleBlobRepo(w http.ResponseWriter, r *http.Request, tail string) {
	repo := strings.Trim(tail, "/")
	if repo == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "repository name required"})
		return
	}

	switch r.Method {
	case http.MethodPut:
		name, id, err := authorFromHeaders(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		contentBytes, err := io.ReadAll(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unable to read request body"})
			return
		}

		branch := r.URL.Query().Get("branch")

		result, err := s.store.PutBlobAndCommit(r.Context(), storage.BlobWriteRequest{
			Name:       repo,
			Branch:     branch,
			Content:    string(contentBytes),
			AuthorName: name,
			AuthorID:   id,
		})
		if err != nil {
			writeError(w, err)
			return
		}

		writeJSON(w, http.StatusCreated, map[string]any{
			"commit":     result.CommitHash,
			"branch":     result.Branch,
			"created_at": result.CreatedAt,
			"diff":       result.Diff,
		})
	case http.MethodGet:
		query := r.URL.Query()
		branch := query.Get("branch")
		if branch == "" {
			branch = defaultBranchName
		}
		commitHash := query.Get("commit")
		if commitHash == "" {
			branchMeta, err := s.store.GetBranch(r.Context(), repo, branch)
			if err != nil {
				writeError(w, err)
				return
			}
			if branchMeta.Commit == "" {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "branch has no commits"})
				return
			}
			commitHash = branchMeta.Commit
		}

		commit, content, err := s.store.GetCommit(r.Context(), repo, commitHash)
		if err != nil {
			writeError(w, err)
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"commit":  commit,
			"content": content,
		})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (s *Service) handleBlobGet(w http.ResponseWriter, r *http.Request) {
	repo := r.URL.Query().Get("name")
	if repo == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name query parameter required"})
		return
	}

	commitHash := r.URL.Query().Get("commit")
	if commitHash == "" {
		branch := r.URL.Query().Get("branch")
		if branch == "" {
			branch = defaultBranchName
		}
		branchMeta, err := s.store.GetBranch(r.Context(), repo, branch)
		if err != nil {
			writeError(w, err)
			return
		}
		if branchMeta.Commit == "" {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "branch has no commits"})
			return
		}
		commitHash = branchMeta.Commit
	}

	commit, content, err := s.store.GetCommit(r.Context(), repo, commitHash)
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"commit":  commit,
		"content": content,
	})
}

func (s *Service) handleCommits(w http.ResponseWriter, r *http.Request, tail string) {
	repo := r.URL.Query().Get("name")
	if repo == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name query parameter required"})
		return
	}

	order := strings.ToLower(r.URL.Query().Get("order"))
	desc := true
	if order == "asc" || order == "ascending" {
		desc = false
	}

	var limit int
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil || limit < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be a non-negative integer"})
			return
		}
	}

	switch {
	case tail == "" && r.Method == http.MethodGet:
		commits := s.store.ListCommits(r.Context(), storage.ListCommitsOptions{
			Repo:       repo,
			Descending: desc,
			Limit:      limit,
		})
		writeJSON(w, http.StatusOK, commits)
	case tail != "" && r.Method == http.MethodGet:
		hash := strings.TrimPrefix(tail, "/")
		commit, content, err := s.store.GetCommit(r.Context(), repo, hash)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"commit":  commit,
			"content": content,
		})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (s *Service) handleBranches(w http.ResponseWriter, r *http.Request, tail string) {
	repo := r.URL.Query().Get("name")
	if repo == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name query parameter required"})
		return
	}

	tail = strings.TrimPrefix(tail, "/")
	switch {
	case tail == "" && r.Method == http.MethodGet:
		branches := s.store.ListBranches(r.Context(), repo)
		writeJSON(w, http.StatusOK, branches)
	case tail == "" && r.Method == http.MethodPost:
		var req storage.BranchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid payload"})
			return
		}
		req.Repo = repo
		branch, err := s.store.UpsertBranch(r.Context(), req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, branch)
	case r.Method == http.MethodGet:
		branch, err := s.store.GetBranch(r.Context(), repo, tail)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, branch)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (s *Service) handleTags(w http.ResponseWriter, r *http.Request, tail string) {
	repo := r.URL.Query().Get("name")
	if repo == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name query parameter required"})
		return
	}

	tail = strings.TrimPrefix(tail, "/")
	switch {
	case tail == "" && r.Method == http.MethodGet:
		tags := s.store.ListTags(r.Context(), repo)
		writeJSON(w, http.StatusOK, tags)
	case tail == "" && r.Method == http.MethodPost:
		var req storage.TagRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid payload"})
			return
		}
		req.Repo = repo
		tag, err := s.store.CreateTag(r.Context(), req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, tag)
	case r.Method == http.MethodGet:
		tag, err := s.store.GetTag(r.Context(), repo, tail)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, tag)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (s *Service) handlePolicies(w http.ResponseWriter, r *http.Request, tail string) {
	tail = strings.TrimPrefix(tail, "/")
	switch {
	case tail == "" && r.Method == http.MethodPost:
		var req policyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid payload"})
			return
		}
		if req.Name == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name is required"})
			return
		}
		policy := storage.RetentionPolicy{Repo: req.Name}
		if req.HotCommitLimit != nil {
			policy.HotCommitLimit = *req.HotCommitLimit
		}
		if req.HotDuration != "" {
			d, err := time.ParseDuration(req.HotDuration)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid hotDuration"})
				return
			}
			policy.HotDuration = d
		}
		policy, err := s.store.SetPolicy(r.Context(), policy)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, makePolicyResponse(policy))
	case tail == "" && r.Method == http.MethodGet:
		repo := r.URL.Query().Get("name")
		if repo == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name query parameter required"})
			return
		}
		policy, err := s.store.GetPolicy(r.Context(), repo)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, makePolicyResponse(policy))
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func authorFromHeaders(r *http.Request) (string, string, error) {
	name := strings.TrimSpace(r.Header.Get(headerAuthorName))
	id := strings.TrimSpace(r.Header.Get(headerAuthorID))
	if name == "" || id == "" {
		return "", "", fmt.Errorf("%s and %s headers are required", headerAuthorName, headerAuthorID)
	}
	return name, id, nil
}

type policyRequest struct {
	Name           string `json:"name"`
	HotCommitLimit *int   `json:"hotCommitLimit,omitempty"`
	HotDuration    string `json:"hotDuration,omitempty"`
}

type policyResponse struct {
	Name           string `json:"name"`
	HotCommitLimit int    `json:"hotCommitLimit,omitempty"`
	HotDuration    string `json:"hotDuration,omitempty"`
	Locked         bool   `json:"locked"`
}

func makePolicyResponse(policy storage.RetentionPolicy) policyResponse {
	resp := policyResponse{
		Name:           policy.Repo,
		HotCommitLimit: policy.HotCommitLimit,
		Locked:         policy.Locked,
	}
	if policy.HotDuration > 0 {
		resp.HotDuration = policy.HotDuration.String()
	}
	return resp
}

func writeError(w http.ResponseWriter, err error) {
	var notFound *storage.NotFoundError
	if errors.As(err, &notFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": notFound.Error()})
		return
	}

	var conflict *storage.ConflictError
	if errors.As(err, &conflict) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": conflict.Error()})
		return
	}

	var validation *storage.ValidationError
	if errors.As(err, &validation) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": validation.Error()})
		return
	}

	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
