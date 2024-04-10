package client

type BaseMsg struct {
	timeStamp int64
	forceFail string
}

// message to create a file on s3 with the timestamp as content, and fileID as file name
type CreateMsg struct {
	BaseMsg
	fileID string
}

// message to update a file on S3, overwrite the content with new timestamp
type UpdateMsg struct {
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
