package s3

import (
	"strconv"
)

type objectPageStatus struct {
	delimiter string
	maxKeys   int64
	prefix    string

	// Only used for object
	continuationToken string

	// Only used for part object
	keyMarker      string
	uploadIdMarker string
}

func (i *objectPageStatus) ContinuationToken() string {
	if i.uploadIdMarker != "" {
		return i.continuationToken + "/" + i.uploadIdMarker
	}
	return i.continuationToken
}

type storagePageStatus struct {
	limit    int
	offset   int
	location string
}

func (i *storagePageStatus) ContinuationToken() string {
	return strconv.FormatInt(int64(i.offset), 10)
}

type partPageStatus struct {
	key              string
	maxParts         int64
	partNumberMarker int64
	uploadId         string
}

func (i *partPageStatus) ContinuationToken() string {
	return strconv.FormatInt(int64(i.partNumberMarker), 10)
}
