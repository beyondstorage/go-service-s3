package s3

import (
	"github.com/aos-dev/go-storage/v3/services"
)

var (
	// ErrServerSideEncryptionCustomerKeyInvalid will be returned while server-side encryption customer key is invalid.
	ErrServerSideEncryptionCustomerKeyInvalid = services.NewErrorCode("invalid server-side encryption customer key")
)
