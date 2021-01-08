package store

import "context"

type Store interface {
	ListKeys(ctx context.Context, prefix string) ([]string, error)
	GetValue(ctx context.Context, key string) ([]byte, error)
	PutValue(ctx context.Context, key string, value []byte) error
	DeleteKey(ctx context.Context, key string) error
	Close() error
}
