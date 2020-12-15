package arangodb

import (
	driver "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/go-git/go-git/v5/storage"
)

// ArangoStore -
type ArangoStore interface {
	storage.Storer
}

type arangoStore struct {
	conn   driver.Connection
	client driver.Client
	db     driver.Database

	objectStorage
	referenceStorage
	miscStorage
}

// NewStore creates a new arango git store and sets it up for use by go-git
func NewStore(connURL string, dbName string) (ArangoStore, bool, error) {
	conn, c, err := newConnectionAndClient(connURL)
	if err != nil {
		return nil, false, err
	}

	db, created, err := getOrCreateDatabase(c, dbName)
	if err != nil {
		return nil, false, err
	}

	os, err := newObjectStorage(db)
	if err != nil {
		return nil, false, err
	}

	rs, err := newReferenceStorage(db)
	if err != nil {
		return nil, false, err
	}

	ms, err := newMiscStorage(db)
	if err != nil {
		return nil, false, err
	}

	return &arangoStore{
		conn:             conn,
		client:           c,
		db:               db,
		objectStorage:    os,
		referenceStorage: rs,
		miscStorage:      ms,
	}, created, nil
}

func newConnectionAndClient(url string) (driver.Connection, driver.Client, error) {
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{url},
		ConnLimit: 32,
	})
	if err != nil {
		return nil, nil, err
	}

	c, err := driver.NewClient(driver.ClientConfig{
		Connection: conn,
	})
	if err != nil {
		return nil, nil, err
	}

	return conn, c, nil
}

func (s *arangoStore) Module(name string) (storage.Storer, error) {
	return nil, nil
}

// func (s *arangoStore) Init() error {
// 	return s.SetReference(plumbing.NewReferenceFromStrings(plumbing.HEAD.String(), ""))
// }
