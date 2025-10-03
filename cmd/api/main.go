package main

import (
	"context"
	"log"

	"github.com/onexay/kv-vs/internal/httpserver"
)

func main() {
	ctx := context.Background()
	srv, err := httpserver.NewServer(ctx)
	if err != nil {
		log.Fatalf("failed to initialize server: %v", err)
	}

	if err := srv.Run(); err != nil {
		log.Fatalf("server terminated: %v", err)
	}
}
