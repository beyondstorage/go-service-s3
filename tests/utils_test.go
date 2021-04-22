package tests

import (
	"os"
	"testing"

	ps "github.com/aos-dev/go-storage/v3/pairs"
	"github.com/aos-dev/go-storage/v3/types"
	"github.com/google/uuid"

	s3 "github.com/aos-dev/go-service-s3"
)

func setupTest(t *testing.T) types.Storager {
	t.Log("Setup test for qingstor")

	store, err := s3.NewStorager(
		ps.WithCredential(os.Getenv("STORAGE_S3_CREDENTIAL")),
		ps.WithName(os.Getenv("STORAGE_S3_NAME")),
		ps.WithLocation(os.Getenv("STORAGE_S3_LOCATION")),
		ps.WithWorkDir("/"+uuid.New().String()+"/"),
	)
	if err != nil {
		t.Errorf("new storager: %v", err)
	}
	return store
}
