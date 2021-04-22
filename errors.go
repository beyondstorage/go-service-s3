package s3

import "errors"

var (
	// ErrInvalidEncryptionCustomerKey will be returned while encryption customer key is invalid.
	ErrInvalidEncryptionCustomerKey = errors.New("invalid encryption customer key")
)
