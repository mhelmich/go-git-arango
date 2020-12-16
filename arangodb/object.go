package arangodb

import (
	"context"
	"errors"
	"io"

	driver "github.com/arangodb/go-driver"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/storer"
)

const (
	objectCollectionName = "objects"

	queryReadObjectDocByHash        = "FOR d IN " + objectCollectionName + " FILTER d.hash == @hash RETURN d"
	queryReadObjectDocByHashAndType = "FOR d IN " + objectCollectionName + " FILTER d.hash == @hash && d.type == @type RETURN d"
	queryIterObjectDocsByType       = "FOR d IN " + objectCollectionName + " FILTER d.type == @type RETURN d"
	queryUpsertObject               = "UPSERT { hash: @hash, type: @type } INSERT { hash: @hash, type: @type, object: @object } UPDATE { object: @object } IN " + objectCollectionName
)

var (
	errTooManyResults = errors.New("too many results")
)

func newObjectStorage(db driver.Database) (objectStorage, error) {
	coll, err := getOrCreateCollection(db, objectCollectionName)
	if err != nil {
		return objectStorage{}, err
	}

	return objectStorage{
		db:   db,
		coll: coll,
	}, nil
}

type objectStorage struct {
	db   driver.Database
	coll driver.Collection
}

type objectDocument struct {
	Hash   string              `json:"hash,omitempty"`
	Type   plumbing.ObjectType `json:"type,omitempty"`
	Object []byte              `json:"object,omitempty"`
}

// NewEncodedObject returns a new plumbing.EncodedObject, the real type
// of the object can be a custom implementation or the default one,
// plumbing.MemoryObject.
func (s *objectStorage) NewEncodedObject() plumbing.EncodedObject {
	return newEncodedObject()
}

func (s *objectStorage) SetEncodedObject(o plumbing.EncodedObject) (plumbing.Hash, error) {
	if o.Type() == plumbing.OFSDeltaObject || o.Type() == plumbing.REFDeltaObject {
		return plumbing.ZeroHash, plumbing.ErrInvalidType
	}

	buf, err := readIntoBuffer(o.Reader)
	if err != nil {
		return plumbing.ZeroHash, err
	}

	h := o.Hash()
	_, err = s.db.Query(driver.WithWaitForSync(context.Background()), queryUpsertObject, map[string]interface{}{
		"hash":   h.String(),
		"type":   o.Type(),
		"object": buf.Bytes(),
	})
	if err != nil {
		return plumbing.ZeroHash, err
	}

	return h, nil
}

// EncodedObject gets an object by hash with the given
// plumbing.ObjectType. Implementors should return
// (nil, plumbing.ErrObjectNotFound) if an object doesn't exist with
// both the given hash and object type.
//
// Valid plumbing.ObjectType values are CommitObject, BlobObject, TagObject,
// TreeObject and AnyObject. If plumbing.AnyObject is given, the object must
// be looked up regardless of its type.
func (s *objectStorage) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	doc, err := s.readOneDocByHashAndType(h, t)
	if err != nil {
		return nil, err
	}

	o := s.NewEncodedObject()
	o.SetType(doc.Type)
	o.SetSize(int64(len(doc.Object)))
	err = readIntoWriter(o.Writer, doc.Object)
	return o, err
}

// IterObjects returns a custom EncodedObjectStorer over all the object
// on the storage.
//
// Valid plumbing.ObjectType values are CommitObject, BlobObject, TagObject,
func (s *objectStorage) IterEncodedObjects(t plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	if t != plumbing.CommitObject && t != plumbing.BlobObject && t != plumbing.TagObject {
		return nil, nil
	}

	cursor, err := s.db.Query(context.Background(), queryIterObjectDocsByType, map[string]interface{}{
		"type": t,
	})
	if err != nil {
		return nil, err
	}

	return &objectIter{
		cursor: cursor,
	}, nil
}

// HasEncodedObject returns ErrObjNotFound if the object doesn't
// exist.  If the object does exist, it returns nil.
func (s *objectStorage) HasEncodedObject(h plumbing.Hash) error {
	_, err := s.readOneDocByHash(h)
	if err != nil {
		return err
	}
	return nil
}

// EncodedObjectSize returns the plaintext size of the encoded object.
func (s *objectStorage) EncodedObjectSize(h plumbing.Hash) (int64, error) {
	doc, err := s.readOneDocByHash(h)
	if err == plumbing.ErrObjectNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	return int64(len(doc.Object)), nil
}

func (s *objectStorage) readOneDocByHashAndType(h plumbing.Hash, t plumbing.ObjectType) (*objectDocument, error) {
	return s.readOneDoc(queryReadObjectDocByHashAndType, map[string]interface{}{
		"hash": h.String(),
		"type": t,
	})
}

func (s *objectStorage) readOneDocByHash(h plumbing.Hash) (*objectDocument, error) {
	return s.readOneDoc(queryReadObjectDocByHash, map[string]interface{}{
		"hash": h.String(),
	})
}

func (s *objectStorage) readOneDoc(query string, bindVars map[string]interface{}) (*objectDocument, error) {
	cursor, err := s.db.Query(driver.WithQueryCount(context.Background()), query, bindVars)
	if driver.IsNotFound(err) {
		return nil, plumbing.ErrObjectNotFound
	} else if err != nil {
		return nil, err
	} else if cursor.Count() > 1 {
		return nil, errTooManyResults
	}

	defer closeSilently(cursor)
	var doc *objectDocument
	_, err = cursor.ReadDocument(context.Background(), &doc)
	return doc, nil
}

type objectIter struct {
	cursor driver.Cursor
}

func (iter *objectIter) Next() (plumbing.EncodedObject, error) {
	var doc objectDocument
	_, err := iter.cursor.ReadDocument(context.Background(), &doc)
	if driver.IsNoMoreDocuments(err) {
		return nil, io.EOF
	} else if err != nil {
		return nil, err
	}

	o := newEncodedObject()
	err = readIntoWriter(o.Writer, doc.Object)
	return o, err
}

func (iter *objectIter) ForEach(f func(plumbing.EncodedObject) error) error {
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

func (iter *objectIter) Close() {
	closeSilently(iter.cursor)
}

func newEncodedObject() *plumbing.MemoryObject {
	return &plumbing.MemoryObject{}
}
