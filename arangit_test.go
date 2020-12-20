package arangit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/stretchr/testify/assert"
)

const (
	json1 = `{"guid":"3cab3480-04df-4242-a840-35220c156c31","isActive":true,"balance":"$2,620.36","picture":"http://placehold.it/32x32","age":30,"eyeColor":"green","name":"Vang Pace","gender":"male","company":"HINWAY","email":"vangpace@hinway.com","phone":"+1 (858) 431-3710","address":"697 Jaffray Street, Allison, North Dakota, 1638"}`
	json2 = `{"guid":"d1a462f8-8317-4d1e-a0d2-e0e134442799","isActive":true,"balance":"$1,657.31","picture":"http://placehold.it/32x32","age":26,"eyeColor":"green","name":"Jackie Ortega","gender":"female","company":"ARCHITAX","email":"jackieortega@architax.com","phone":"+1 (935) 589-2688","address":"908 Schenck Avenue, Sedley, California, 8089"}`
	json3 = `{"guid":"aaf05a3c-8754-49bb-a4d6-13609c33514a","isActive":true,"balance":"$1,652.24","picture":"http://placehold.it/32x32","age":22,"eyeColor":"brown","name":"Carol Downs","gender":"female","company":"ROBOID","email":"caroldowns@roboid.com","phone":"+1 (987) 503-2401","address":"580 Nevins Street, Gardiner, Alabama, 1775"}`
)

func _TestRepoOpen(t *testing.T) {
	_, err := OpenRepo("arangit")
	assert.Nil(t, err)

	_, err = OpenRepo("arangit")
	assert.Nil(t, err)
}

func TestRepoCommitAndTag(t *testing.T) {
	repo, err := OpenRepo("arangit")
	assert.Nil(t, err)

	err = repo.PrintStatus()
	assert.Nil(t, err)
	err = repo.DeleteTag("my-first-tag")
	assert.Nil(t, err)

	buf := &bytes.Buffer{}
	buf.WriteString("Hello World!")
	err = repo.CommitFile("testing_test.txt", buf)
	assert.Nil(t, err)

	err = repo.PrintStatus()
	assert.Nil(t, err)

	err = repo.TagHead("my-first-tag")
	assert.Nil(t, err)

	bites, err := repo.ReadFileFromHead("testing_test.txt")
	assert.Nil(t, err)
	assert.Equal(t, "Hello World!", string(bites))
	fmt.Printf("%s\n", string(bites))

	err = repo.PrintStatus()
	assert.Nil(t, err)

	buf = &bytes.Buffer{}
	buf.WriteString("second commit")
	err = repo.CommitFile("testing_test.txt", buf)
	assert.Nil(t, err)

	bites, err = repo.ReadFileFromHead("testing_test.txt")
	assert.Nil(t, err)
	assert.Equal(t, "second commit", string(bites))
	fmt.Printf("%s\n", string(bites))

	bites, err = repo.ReadFileFromTag("my-first-tag", "testing_test.txt")
	assert.Nil(t, err)
	assert.Equal(t, "Hello World!", string(bites))
	fmt.Printf("%s\n", string(bites))

	err = repo.PrintStatus()
	assert.Nil(t, err)
}

func TestSearch(t *testing.T) {
	repo, err := OpenRepo("arangit")
	assert.Nil(t, err)

	buf := &bytes.Buffer{}
	_, err = buf.WriteString(json1)
	assert.Nil(t, err)
	err = repo.CommitFile("1.json", buf)
	assert.Nil(t, err)

	buf = &bytes.Buffer{}
	_, err = buf.WriteString(json2)
	assert.Nil(t, err)
	err = repo.CommitFile("2.json", buf)
	assert.Nil(t, err)

	buf = &bytes.Buffer{}
	_, err = buf.WriteString(json3)
	assert.Nil(t, err)
	err = repo.CommitFile("3.json", buf)
	assert.Nil(t, err)

	iter, err := repo.FileIterForHead()
	assert.Nil(t, err)

	_, client, err := newConnectionAndClient("http://localhost:8529")
	assert.Nil(t, err)

	db, _, err := getOrCreateDatabase(client, "arangit")
	assert.Nil(t, err)

	_, err = getOrCreateCollection(db, "searchIdxHead")
	assert.Nil(t, err)

	err = iter.ForEach(func(r io.Reader, info os.FileInfo) error {
		fmt.Printf("%s\n", info.Name())
		buf := &bytes.Buffer{}
		_, err := io.Copy(buf, r)
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", buf.String())
		err = upsertSearchDoc(db, fileDoc{
			Name:    info.Name(),
			Content: buf.Bytes(),
		})
		return err
	})
	assert.Nil(t, err)

	tru := true
	_, err = db.CreateArangoSearchView(context.Background(), "SearchViewHead", &driver.ArangoSearchViewProperties{
		Links: map[string]driver.ArangoSearchElementProperties{
			"searchIdxHead": {
				IncludeAllFields: &tru,
				// Analyzers:        []string{"text"},
			},
		},
	})
	assert.Nil(t, err)
	// sv.SetProperties(context.Background(), driver.ArangoSearchViewProperties{
	// 	Links: map[string]driver.ArangoSearchElementProperties{
	// 		"searchIdxHead": {
	// 			IncludeAllFields: &tru,
	// 			// Analyzers:        []string{"text"},
	// 		},
	// 	},
	// })

	query := "FOR doc IN SearchViewHead SEARCH @term SORT TFIDF(doc) DESC RETURN doc"
	cursor, err := db.Query(context.Background(), query, map[string]interface{}{
		"term": "guid",
	})
	assert.Nil(t, err)
	defer cursor.Close()
	for cursor.HasMore() {
		var doc fileDoc
		_, err = cursor.ReadDocument(context.Background(), doc)
		fmt.Printf("serach hit: %s\n", doc.Content)
	}
}

type fileDoc struct {
	Name    string
	Content json.RawMessage
}

func newConnectionAndClient(url string) (driver.Connection, driver.Client, error) {
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{url},
		ConnLimit: 2,
	})
	if err != nil {
		return nil, nil, err
	}

	c, err := driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication("root", ""),
	})
	if err != nil {
		return nil, nil, err
	}

	return conn, c, nil
}

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

const queryUpsertSearchDoc = "UPSERT { name: @name } INSERT { name: @name, content: @content } UPDATE { content: @content } IN searchIdxHead"

func upsertSearchDoc(db driver.Database, doc fileDoc) error {
	_, err := db.Query(driver.WithWaitForSync(context.Background()), queryUpsertSearchDoc, map[string]interface{}{
		"name":    doc.Name,
		"content": doc.Content,
	})
	return err
}
