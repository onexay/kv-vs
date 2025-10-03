package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/onexay/kv-vs/internal/storage"
)

// StorageBackend enumerates supported persistence layers.
type StorageBackend string

const (
	// StorageBackendMemory keeps data in-process.
	StorageBackendMemory StorageBackend = "memory"
	// StorageBackendKeyDB persists data to KeyDB/Redis.
	StorageBackendKeyDB StorageBackend = "keydb"
)

// Config aggregates runtime configuration.
type Config struct {
	APIAddr   string
	Storage   StorageConfig
	Retention RetentionConfig
}

// StorageConfig contains backend selection and nested settings.
type StorageConfig struct {
	Backend StorageBackend
	KeyDB   storage.Config
}

// RetentionConfig holds defaults for blob archival.
type RetentionConfig struct {
	ArchivePath    string
	HotCommitLimit int
	HotDuration    time.Duration
}

// Load reads configuration from environment variables.
func Load() Config {
	backend := StorageBackend(strings.ToLower(envDefault("STORAGE_BACKEND", string(StorageBackendMemory))))

	return Config{
		APIAddr: envDefault("API_ADDR", ":8080"),
		Storage: StorageConfig{
			Backend: backend,
			KeyDB: storage.Config{
				Addr:     os.Getenv("KEYDB_ADDR"),
				Username: os.Getenv("KEYDB_USERNAME"),
				Password: os.Getenv("KEYDB_PASSWORD"),
				Database: envInt("KEYDB_DB", 0),
			},
		},
		Retention: RetentionConfig{
			ArchivePath:    envDefault("RETENTION_ARCHIVE_PATH", "data/archive.db"),
			HotCommitLimit: envInt("RETENTION_HOT_COMMIT_LIMIT", 0),
			HotDuration:    envDuration("RETENTION_HOT_DURATION", 0),
		},
	}
}

func envDefault(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}

func envInt(key string, def int) int {
	if val := os.Getenv(key); val != "" {
		if n, err := strconv.Atoi(val); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return def
}
