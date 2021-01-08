package store

import (
	"context"
	"fmt"

	"github.com/prologic/bitcask"
	log "github.com/sirupsen/logrus"
)

type bitcaskStore struct {
	db *bitcask.Bitcask
}

func NewBitcaskStore(dbPath string) (Store, error) {
	db, err := bitcask.Open(
		dbPath,
		bitcask.WithMaxKeySize(0),
		bitcask.WithMaxValueSize(0),
	)
	if err != nil {
		log.WithError(err).Error("error opening bitcask database")
		return nil, fmt.Errorf("error opening bitcask database: %w", err)
	}

	return &bitcaskStore{db: db}, nil
}

func (s *bitcaskStore) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	var keys []string

	err := s.db.Scan([]byte(prefix), func(key []byte) error {
		keys = append(keys, string(key))
		return nil
	})
	if err != nil {
		log.WithError(err).Error("error scanning keys")
		return nil, fmt.Errorf("error scanning keys: %w", err)
	}

	return keys, nil
}

func (s *bitcaskStore) GetValue(ctx context.Context, key string) ([]byte, error) {
	value, err := s.db.Get([]byte(key))
	if err != nil {
		if err == bitcask.ErrKeyNotFound {
			return nil, nil
		}
		log.WithError(err).Error("error getting key")
		return nil, fmt.Errorf("error getting key: %w", err)
	}

	return value, nil
}

func (s *bitcaskStore) PutValue(ctx context.Context, key string, value []byte) error {
	return s.db.Put([]byte(key), value)
}

func (s *bitcaskStore) DeleteKey(ctx context.Context, key string) error {
	return s.db.Delete([]byte(key))
}

func (s *bitcaskStore) Close() error {
	return s.db.Close()
}
