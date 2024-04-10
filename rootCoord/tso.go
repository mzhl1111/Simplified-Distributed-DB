package rootCord

import (
	"github.com/pkg/errors"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"
)

type atomicTsObject struct {
	physical time.Time
	logical  int64
}

type timeStampNode struct {
	key           string
	kv            *EtcdNode
	TSO           unsafe.Pointer
	lastSavedTime atomic.Value
}

func (t *timeStampNode) loadTimeStamp() (time.Time, error) {
	strData, err := t.kv.getFromETCD(t.key)
	if err != nil {
		return ZeroTime, nil
	}

	return ParseTimestamp(string(strData))
}

func (t *timeStampNode) saveTimestamp(ts time.Time) error {
	data := strconv.FormatInt(ts.UnixNano(), 10)
	err := t.kv.saveToETCD(t.key, data)
	if err != nil {
		return errors.WithStack(err)
	}

	t.lastSavedTime.Store(ts)
	return nil
}

func (t *timeStampNode) InitTimeStamp() error {
	last, err := t.loadTimeStamp()
	if err != nil {
		return err
	}

	next := time.Now()

	if TsMinus(next, last) < time.Millisecond {
		next = last.Add(time.Millisecond)
	}

	if err := t.saveTimestamp(next); err != nil {
		return err
	}

	current := &atomicTsObject{
		physical: next,
	}

	atomic.StorePointer(&t.TSO, unsafe.Pointer(current))

	return nil
}

func (t *timeStampNode) UpdateTimestamp() error {
	prev := (*atomicTsObject)(atomic.LoadPointer(&t.TSO))
	now := time.Now()

	lag := TsMinus(now, prev.physical)
	if lag > 100*time.Millisecond {

	}

	var next time.Time

	prevLogical := atomic.LoadInt64(&prev.logical)

	if lag > time.Millisecond {
		next = now
	} else if prevLogical > 5e5 { // assume there are enough ts to allocate
		next = prev.physical.Add(time.Millisecond)
	} else {
		// do nothing
		return nil
	}

	// not update too frequent
	if TsMinus(t.lastSavedTime.Load().(time.Time), next) <= time.Millisecond {
		if err := t.saveTimestamp(next); err != nil {
			return nil
		}
	}

	current := &atomicTsObject{
		physical: next,
		logical:  0,
	}

	atomic.StorePointer(&t.TSO, unsafe.Pointer(current))

	return nil
}
