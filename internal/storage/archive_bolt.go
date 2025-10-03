package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"

	bolt "go.etcd.io/bbolt"
)

const (
	boltRootBucket = "repos"
)

// BoltArchive stores blob payloads inside a BoltDB file.
type BoltArchive struct {
	db   *bolt.DB
	once sync.Once
}

// NewBoltArchive opens (or creates) a BoltDB archive at the provided path.
func NewBoltArchive(path string) (*BoltArchive, error) {
	if path == "" {
		return nil, errors.New("archive path is required")
	}

	cleaned := filepath.Clean(path)
	if dir := filepath.Dir(cleaned); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	db, err := bolt.Open(cleaned, 0o600, nil)
	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(boltRootBucket))
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &BoltArchive{db: db}, nil
}

// Store writes payload data under repo/hash.
func (a *BoltArchive) Store(ctx context.Context, repo, hash string, data []byte) error {
	return a.db.Update(func(tx *bolt.Tx) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		root := tx.Bucket([]byte(boltRootBucket))
		if root == nil {
			return errors.New("archive root bucket missing")
		}

		repoBucket, err := root.CreateBucketIfNotExists([]byte(repo))
		if err != nil {
			return err
		}

		return repoBucket.Put([]byte(hash), data)
	})
}

// Fetch retrieves payload data for repo/hash.
func (a *BoltArchive) Fetch(ctx context.Context, repo, hash string) ([]byte, error) {
	var result []byte
	err := a.db.View(func(tx *bolt.Tx) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		root := tx.Bucket([]byte(boltRootBucket))
		if root == nil {
			return &NotFoundError{Resource: "archive", Key: hash}
		}

		repoBucket := root.Bucket([]byte(repo))
		if repoBucket == nil {
			return &NotFoundError{Resource: "archive", Key: hash}
		}

		data := repoBucket.Get([]byte(hash))
		if data == nil {
			return &NotFoundError{Resource: "archive", Key: hash}
		}

		result = append([]byte{}, data...)
		return nil
	})
	return result, err
}

// Remove deletes payload data (best-effort).
func (a *BoltArchive) Remove(ctx context.Context, repo, hash string) error {
	return a.db.Update(func(tx *bolt.Tx) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		root := tx.Bucket([]byte(boltRootBucket))
		if root == nil {
			return nil
		}
		repoBucket := root.Bucket([]byte(repo))
		if repoBucket == nil {
			return nil
		}
		return repoBucket.Delete([]byte(hash))
	})
}

// Close shuts down the Bolt DB.
func (a *BoltArchive) Close() error {
	a.once.Do(func() {
		_ = a.db.Close()
	})
	return nil
}
