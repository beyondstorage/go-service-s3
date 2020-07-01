package example

import (
	"github.com/aos-dev/go-storage/v2"
	"github.com/aos-dev/go-storage/v2/types"
)

// Storage is the example client.
type Storage struct{}

// String implements Storager.String
func (s *Storage) String() string {
	panic("implement me")
}

// NewStorager will create Storager only.
func NewStorager(pairs ...*types.Pair) (storage.Storager, error) {
	panic("implement me")
}

func (s *Storage) formatError(op string, err error, path ...string) error {
	panic("implement me")
}
