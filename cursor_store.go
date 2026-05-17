package event

import (
	"context"
	"sync"
)

type MemoryCursorStore struct {
	mu      sync.RWMutex
	cursors map[string][]byte
}

func NewMemoryCursorStore() *MemoryCursorStore {
	return &MemoryCursorStore{cursors: make(map[string][]byte)}
}

func (s *MemoryCursorStore) LoadCursor(ctx context.Context, stream string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	cursor := s.cursors[stream]
	if cursor == nil {
		return nil, nil
	}
	return append([]byte(nil), cursor...), nil
}

func (s *MemoryCursorStore) SaveCursor(ctx context.Context, stream string, cursor []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursors[stream] = append([]byte(nil), cursor...)
	return nil
}
