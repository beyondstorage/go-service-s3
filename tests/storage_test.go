package tests

import (
	"os"
	"testing"

	tests "github.com/aos-dev/go-integration-test/v3"
)

func TestStorage(t *testing.T) {
	if os.Getenv("STORAGE_S3_INTEGRATION_TEST") != "on" {
		t.Skipf("STORAGE_S3_INTEGRATION_TEST is not 'on', skipped")
	}
	tests.TestStorager(t, setupTest(t))
}

func TestMultiparter(t *testing.T) {
	if os.Getenv("STORAGE_S3_INTEGRATION_TEST") != "on" {
		t.Skipf("STORAGE_S3_INTEGRATION_TEST is not 'on', skipped")
	}
	tests.TestMultiparter(t, setupTest(t))
}
