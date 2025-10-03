package httpserver

import (
	"context"
	"net/http"

	"github.com/onexay/kv-vs/internal/config"
	"github.com/onexay/kv-vs/internal/service"
)

// Server wraps the HTTP server configuration and dependencies.
type Server struct {
	addr    string
	handler http.Handler
}

// NewServer creates an HTTP server with routes and middleware.
func NewServer(ctx context.Context) (*Server, error) {
	cfg := config.Load()

	svc, err := service.New(ctx, cfg)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/api/v1/", service.Handler(svc))

	return &Server{addr: cfg.APIAddr, handler: mux}, nil
}

// Run starts the HTTP server and blocks until shutdown.
func (s *Server) Run() error {
	return http.ListenAndServe(s.addr, s.handler)
}
