package dataCoord

import (
	"context"
	"sync"
)

type Meta struct {
	sync.RWMutex
	ctx   context.Context
	files map[string]*fileInfo
}

const (
	Status = "status"
)

const (
	CreatePending = "create_pending"
	Created       = "created"
	Creating      = "creating"
	UpdatePending = "update_pending"
	Updated       = "updated"
	updating      = "updating"
	DeletePending = "delete_pending"
	Deleted       = "Deleted"
	Deleting      = "Deleting"
)

type fileInfo struct {
	ID         int64
	Properties map[string]string
	CreatedAt  uint64
}

func NewMeta(ctx context.Context) *Meta {
	mt := &Meta{
		ctx:   ctx,
		files: make(map[string]*fileInfo),
	}
	return mt
}

func (m *Meta) Create(id string, fi *fileInfo) bool {
	m.Lock()
	defer m.Unlock()

	if _, exists := m.files[id]; exists {
		return false // ID already exists
	}
	m.files[id] = fi
	return true
}

func (m *Meta) Read(id string) (*fileInfo, bool) {
	m.RLock()
	defer m.RUnlock()

	fi, exists := m.files[id]
	return fi, exists
}

func (m *Meta) Update(id string, newFi *fileInfo) bool {
	m.Lock()
	defer m.Unlock()

	if _, exists := m.files[id]; !exists {
		return false // Cannot update non-existing ID
	}
	m.files[id] = newFi
	return true
}

type TsMeta struct {
	sync.RWMutex
	ctx   context.Context
	files map[string]uint64
}

func NewTsMeta(ctx context.Context) *TsMeta {
	return &TsMeta{
		ctx:   ctx,
		files: make(map[string]uint64),
	}
}

func (t *TsMeta) Upsert(key string, ts uint64) {
	t.Lock()
	defer t.Unlock()

	t.files[key] = ts
}

func (t *TsMeta) Read(key string) uint64 {
	t.RLock()
	defer t.RUnlock()

	ts, _ := t.files[key]
	return ts
}
