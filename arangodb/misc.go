package arangodb

import (
	driver "github.com/arangodb/go-driver"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/index"
)

const (
	miscCollectionName = "misc"
)

func newMiscStorage(db driver.Database) (miscStorage, error) {
	return miscStorage{}, nil
}

type miscStorage struct{}

func (s *miscStorage) SetShallow(hashes []plumbing.Hash) error {
	return nil
}

func (s *miscStorage) Shallow() ([]plumbing.Hash, error) {
	return nil, nil
}

func (s *miscStorage) SetIndex(idx *index.Index) error {
	return nil
}

func (s *miscStorage) Index() (*index.Index, error) {
	return nil, nil
}

func (s *miscStorage) SetConfig(cfg *config.Config) error {
	return nil
}

func (s *miscStorage) Config() (*config.Config, error) {
	return nil, nil
}
