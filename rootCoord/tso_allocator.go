package rootCord

import (
	"errors"
	"sync/atomic"
	"time"
)

type Allocator interface {
	Initialize() error

	UpdateTso() error

	GenerateTso(count int32) (uint64, error)
}

type TsAllocator struct {
	tso *timeStampNode
}

func NewTsAllocator(key string, kv *EtcdNode) *TsAllocator {
	return &TsAllocator{
		tso: &timeStampNode{
			key: key, kv: kv,
		},
	}
}

func (ta *TsAllocator) Initialize() error {
	return ta.tso.InitTimeStamp()
}

func (ta *TsAllocator) UpdateTso() error {
	return ta.tso.UpdateTimestamp()
}

func (ta *TsAllocator) GenerateTso(count int32) (uint64, error) {
	var physical, logical int64

	current := (*atomicTsObject)(atomic.LoadPointer(&ta.tso.TSO))

	maxRetryCount := 10

	for i := 0; i < maxRetryCount; i++ {
		physical = current.physical.UnixMilli()
		logical = atomic.AddInt64(&current.logical, int64(count))
		if logical >= 1e6 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return Ts2Uint64(physical, logical), nil
	}

	return 0, errors.New("fail to get an Ts")
}
