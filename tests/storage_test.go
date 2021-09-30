package tests

import (
	"bytes"
	"os"
	"testing"

	tests "github.com/beyondstorage/go-integration-test/v4"
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

func TestDirer(t *testing.T) {
	if os.Getenv("STORAGE_S3_INTEGRATION_TEST") != "on" {
		t.Skipf("STORAGE_S3_INTEGRATION_TEST is not 'on', skipped")
	}
	tests.TestDirer(t, setupTest(t))
}

func TestLinker(t *testing.T) {
	if os.Getenv("STORAGE_S3_INTEGRATION_TEST") != "on" {
		t.Skipf("STORAGE_S3_INTEGRATION_TEST is not 'on', skipped")
	}
	tests.TestLinker(t, setupTest(t))
}

func TestHTTPSigner(t *testing.T) {
	if os.Getenv("STORAGE_S3_INTEGRATION_TEST") != "on" {
		t.Skipf("STORAGE_S3_INTEGRATION_TEST is not 'on', skipped")
	}
	tests.TestStorageHTTPSignerWrite(t, setupTest(t))
	tests.TestStorageHTTPSignerRead(t, setupTest(t))
	tests.TestStorageHTTPSignerDelete(t, setupTest(t))
	tests.TestMultipartHTTPSigner(t, setupTest(t))
}

// https://github.com/beyondstorage/go-storage/issues/741
func TestIssue741(t *testing.T) {
	if os.Getenv("STORAGE_S3_INTEGRATION_TEST") != "on" {
		t.Skipf("STORAGE_S3_INTEGRATION_TEST is not 'on', skipped")
	}
	store := setupTest(t)

	content := []byte("Hello, World!")
	r := bytes.NewReader(content)

	_, err := store.Write("IMG@@@¥&_0960.jpg", r, int64(len(content)))
	if err != nil {
		t.Errorf("write: %v", err)
		return
	}
	return
}
