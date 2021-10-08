package tests

import (
	"os"
	"testing"

	"github.com/google/uuid"

	s3 "github.com/beyondstorage/go-service-s3/v2"
	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/types"
)

func setupTest(t *testing.T) types.Storager {
	t.Log("Setup test for s3")

	store, err := s3.NewStorager(
		ps.WithCredential(os.Getenv("STORAGE_S3_CREDENTIAL")),
		ps.WithName(os.Getenv("STORAGE_S3_NAME")),
		ps.WithLocation(os.Getenv("STORAGE_S3_LOCATION")),
		ps.WithWorkDir("/"+uuid.New().String()+"/"),
		s3.WithStorageFeatures(s3.StorageFeatures{
			VirtualDir:  true,
			VirtualLink: true,
		}),
	)
	if err != nil {
		t.Errorf("new storager: %v", err)
	}
	return store
}
