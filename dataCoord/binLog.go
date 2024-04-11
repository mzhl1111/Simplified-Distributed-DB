package dataCoord

import (
	"sync"
	"time"
)

const (
	DataStore = "data_store"
)

type BinLog struct {
	sync.RWMutex
	// jsonfied data
	data   map[string]map[string][]byte
	dataTs map[string]map[string]uint64
}

func NewBinLog() *BinLog {
	return &BinLog{
		data:   make(map[string]map[string][]byte),
		dataTs: make(map[string]map[string]uint64),
	}
}

func (b *BinLog) Create(key, subkey string, value []byte) {

	if _, ok := b.data[key]; !ok {
		b.data[key] = make(map[string][]byte)
		b.dataTs[key] = make(map[string]uint64)
	}
	b.data[key][subkey] = value
	b.dataTs[key][subkey] = uint64(time.Now().UnixNano())
}

func (b *BinLog) Read(key, subkey string) ([]byte, uint64, bool) {

	if subdata, ok := b.data[key]; ok {
		if value, ok := subdata[subkey]; ok {
			return value, b.dataTs[key][subkey], true
		}
	}
	return nil, 0, false
}

func (b *BinLog) Update(key, subkey string, value []byte) bool {

	if _, ok := b.data[key]; ok {
		if _, ok := b.data[key][subkey]; ok {
			b.data[key][subkey] = value
			b.dataTs[key][subkey] = uint64(time.Now().UnixNano())
			return true
		}
	}
	return false
}

func (b *BinLog) Delete(key, subkey string) bool {

	if _, ok := b.data[key]; ok {
		if _, ok := b.data[key][subkey]; ok {
			delete(b.data[key], subkey)
			if len(b.data[key]) == 0 {
				delete(b.data, key)
				delete(b.dataTs, key)
			}
			return true
		}
	}
	return false
}
