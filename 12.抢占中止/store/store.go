package adkstore

import (
	"context"

	"github.com/cloudwego/eino/compose"
)

func NewInMemoryStore() compose.CheckPointStore {
	return &InMemoryStore{
		mem: map[string][]byte{},
	}
}

type InMemoryStore struct {
	mem map[string][]byte
}

func (i *InMemoryStore) Set(ctx context.Context, key string, value []byte) error {
	i.mem[key] = value
	return nil
}

func (i *InMemoryStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	v, ok := i.mem[key]
	return v, ok, nil
}

// Delete implements the CheckPointDeleter interface,
// explicitly removing a checkpoint by key.
func (i *InMemoryStore) Delete(ctx context.Context, key string) error {
	delete(i.mem, key)
	return nil
}
