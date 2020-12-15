package arangodb

import (
	driver "github.com/arangodb/go-driver"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/storer"
)

const (
	referenceCollectionName = "refs"
)

func newReferenceStorage(db driver.Database) (referenceStorage, error) {
	coll, err := getOrCreateCollection(db, referenceCollectionName)
	if err != nil {
		return referenceStorage{}, err
	}

	return referenceStorage{
		db:   db,
		coll: coll,
	}, nil
}

type referenceStorage struct {
	db   driver.Database
	coll driver.Collection
}

type referenceDocument struct {
	Name   string `json:"name,omitempty"`
	Target string `json:"target,omitempty"`
}

func (s *referenceStorage) SetReference(ref *plumbing.Reference) error {
	return nil
}

// CheckAndSetReference sets the reference `new`, but if `old` is
// not `nil`, it first checks that the current stored value for
// `old.Name()` matches the given reference value in `old`.  If
// not, it returns an error and doesn't update `new`.
func (s *referenceStorage) CheckAndSetReference(new, old *plumbing.Reference) error {
	return nil
}

func (s *referenceStorage) Reference(refName plumbing.ReferenceName) (*plumbing.Reference, error) {
	return nil, nil
}

func (s *referenceStorage) IterReferences() (storer.ReferenceIter, error) {
	return nil, nil
}

func (s *referenceStorage) RemoveReference(refName plumbing.ReferenceName) error {
	return nil
}

func (s *referenceStorage) CountLooseRefs() (int, error) {
	return 0, nil
}

func (s *referenceStorage) PackRefs() error {
	return nil
}
