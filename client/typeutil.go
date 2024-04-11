package client

import (
	"strconv"
	"time"
)

var ZeroTime = time.Time{}

const (
	ts_queue      = "ts_Allocate_queue"
	ts_resp_queue = "resp" + ts_queue
)

func ParseTimestamp(strData string) (time.Time, error) {

	nano, err := strconv.ParseUint(strData, 10, 64)
	if err != nil {
		return ZeroTime, nil
	}

	return time.Unix(0, int64(nano)), nil
}

func TsMinus(after, before time.Time) time.Duration {
	return time.Duration(after.UnixNano() - before.UnixNano())
}

const (
	logicalBits     = 6
	logicalBitsMask = (1 << logicalBits) - 1
)

func Ts2Uint64(physical, logical int64) uint64 {
	return uint64((physical << logicalBits) + logical)
}

func Uint64ToTs(ts uint64) (time.Time, uint64) {
	logical := ts & logicalBitsMask
	physical := ts >> logicalBits
	physicalTime := time.Unix(int64(physical/1000), int64(physical)%1000*time.Millisecond.Nanoseconds())
	return physicalTime, logical
}
