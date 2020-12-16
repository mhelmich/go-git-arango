package arangodb

import (
	"context"
	"io"

	driver "github.com/arangodb/go-driver"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage"
)

const (
	referenceCollectionName = "refs"

	queryReference               = "FOR r IN " + referenceCollectionName + " FILTER r.name == @name RETURN r"
	queryUpsertReference         = "UPSERT { name: @name } INSERT { name: @name, target: @target } UPDATE { target: @target } IN " + referenceCollectionName
	queryIterReferenceDocsByType = "FOR r IN " + referenceCollectionName + " RETURN r"
	queryRemoveReference         = "FOR r IN " + referenceCollectionName + " FILTER r.name == @name REMOVE r"
	queryAllReferences           = "FOR r IN " + referenceCollectionName + " RETURN r"
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
	parts := ref.Strings()
	_, err := s.db.Query(driver.WithWaitForSync(context.Background()), queryUpsertReference, map[string]interface{}{
		"name":   parts[0],
		"target": parts[1],
	})
	return err
}

// CheckAndSetReference sets the reference `new`, but if `old` is
// not `nil`, it first checks that the current stored value for
// `old.Name()` matches the given reference value in `old`.  If
// not, it returns an error and doesn't update `new`.
func (s *referenceStorage) CheckAndSetReference(new, old *plumbing.Reference) error {
	if new == nil {
		return nil
	}

	if old != nil {
		oldRef, err := s.Reference(old.Name())
		if err != nil {
			return err
		}

		if oldRef.Hash() != old.Hash() {
			return storage.ErrReferenceHasChanged
		}
	}

	return s.SetReference(new)
}

func (s *referenceStorage) Reference(refName plumbing.ReferenceName) (*plumbing.Reference, error) {
	doc, err := s.readOneDoc(queryReference, map[string]interface{}{
		"name": refName,
	})
	if err != nil {
		return nil, err
	}

	return plumbing.NewReferenceFromStrings(
		doc.Name,
		doc.Target,
	), nil
}

func (s *referenceStorage) IterReferences() (storer.ReferenceIter, error) {
	cursor, err := s.db.Query(context.Background(), queryIterReferenceDocsByType, nil)
	if err != nil {
		return nil, err
	}

	return &referenceIter{
		cursor: cursor,
	}, nil
}

func (s *referenceStorage) RemoveReference(refName plumbing.ReferenceName) error {
	_, err := s.db.Query(driver.WithWaitForSync(context.Background()), queryRemoveReference, map[string]interface{}{
		"name": refName,
	})
	return err
}

func (s *referenceStorage) CountLooseRefs() (int, error) {
	cursor, err := s.db.Query(driver.WithQueryCount(context.Background()), queryAllReferences, nil)
	if err != nil {
		return 0, err
	}

	defer closeSilently(cursor)
	return int(cursor.Count()), nil
}

func (s *referenceStorage) PackRefs() error {
	return nil
}

func (s *referenceStorage) readOneDoc(query string, bindVars map[string]interface{}) (*referenceDocument, error) {
	cursor, err := s.db.Query(driver.WithQueryCount(context.Background()), query, bindVars)
	if driver.IsNotFound(err) || cursor.Count() == 0 {
		return nil, plumbing.ErrReferenceNotFound
	} else if err != nil {
		return nil, err
	} else if cursor.Count() > 1 {
		return nil, errTooManyResults
	}

	defer closeSilently(cursor)
	var doc *referenceDocument
	_, err = cursor.ReadDocument(context.Background(), &doc)
	return doc, nil
}

type referenceIter struct {
	cursor driver.Cursor
}

func (iter *referenceIter) Next() (*plumbing.Reference, error) {
	ctx := context.Background()
	var doc referenceDocument
	_, err := iter.cursor.ReadDocument(ctx, &doc)
	if driver.IsNoMoreDocuments(err) {
		return nil, io.EOF
	} else if err != nil {
		return nil, err
	}

	return plumbing.NewReferenceFromStrings(doc.Name, doc.Target), nil
}

func (iter *referenceIter) ForEach(f func(*plumbing.Reference) error) error {
	for {
		o, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if err := f(o); err != nil {
			return err
		}
	}
}

func (iter *referenceIter) Close() {
	closeSilently(iter.cursor)
}
