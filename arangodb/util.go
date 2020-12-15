package arangodb

import (
	"bytes"
	"context"
	"io"

	driver "github.com/arangodb/go-driver"
)

func getOrCreateDatabase(client driver.Client, dbName string) (driver.Database, bool, error) {
	ctx := context.Background()
	exists, err := client.DatabaseExists(ctx, dbName)
	if err != nil {
		return nil, false, err
	}

	var db driver.Database
	var created bool
	if exists {
		db, err = client.Database(ctx, dbName)
	} else {
		db, err = client.CreateDatabase(ctx, dbName, &driver.CreateDatabaseOptions{})
		created = true
	}
	if err != nil {
		return nil, false, err
	}

	return db, created, nil
}

func getOrCreateCollection(db driver.Database, collectionName string) (driver.Collection, error) {
	ctx := context.Background()
	exists, err := db.CollectionExists(ctx, collectionName)
	if err != nil {
		return nil, err
	}

	var coll driver.Collection
	if exists {
		coll, err = db.Collection(ctx, collectionName)
	} else {
		coll, err = db.CreateCollection(ctx, collectionName, &driver.CreateCollectionOptions{})
	}

	return coll, nil
}

func closeSilently(c io.Closer) {
	_ = c.Close()
}

func readIntoBuffer(f func() (r io.ReadCloser, err error)) (*bytes.Buffer, error) {
	r, err := f()
	if err != nil {
		return nil, err
	}

	defer closeSilently(r)
	buf := &bytes.Buffer{}
	_, err = io.Copy(buf, r)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func readIntoWriter(f func() (w io.WriteCloser, err error), bites []byte) error {
	w, err := f()
	if err != nil {
		return err
	}

	defer closeSilently(w)
	_, err = w.Write(bites)
	return err
}
