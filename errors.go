package s3

import "errors"

var (
	// ErrServerSideEncryptionCustomerKey will be returned while server-side encryption customer key is invalid.
	ErrServerSideEncryptionCustomerKey = errors.New("invalid server-side encryption customer key")
)
