package dataCoord

import "log"

type BaseMsg struct {
	timeStamp uint64
	forceFail string
}

// message to create a file on s3 with the timestamp as content, and fileID as file name
type CreateMsg struct {
	BaseMsg
	fileID string
	data   []byte
}

// message to update a file on S3, overwrite the content with new timestamp
type UpdateMsg struct {
	BaseMsg
	fileID string
	data   []byte
}

type GetMsg struct {
	BaseMsg
	fileID string
}

// message to delete a file on S3
type DeleteMsg struct {
	BaseMsg
	fileID string
}

type ForceFailRequestBody struct {
	forceFail string
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

type etcdFileTS struct {
	fileTsMap map[string]int64
}

const (
	tsAllocateQueue         = "ts_allocate"
	tsAllocateResponseQueue = "resp_ts_allocate"
)
