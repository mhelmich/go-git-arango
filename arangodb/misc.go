package arangodb

import (
	"context"
	"encoding/json"

	driver "github.com/arangodb/go-driver"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/index"
)

const (
	miscCollectionName = "misc"

	shallowKey = "shallow-key"
	indexKey   = "index-key"
	configKey  = "config-key"

	queryUpsertShallow = "UPSERT { _key: @key } INSERT { _key: @key, shallow: @shallow } UPDATE { shallow: @shallow } IN " + miscCollectionName
	queryUpsertIndex   = "UPSERT { _key: @key } INSERT { _key: @key, idx: @idx } UPDATE { idx: @idx } IN " + miscCollectionName
	queryUpsertConfig  = "UPSERT { _key: @key } INSERT { _key: @key, config: @config } UPDATE { config: @config } IN " + miscCollectionName
)

func newMiscStorage(db driver.Database) (miscStorage, error) {
	coll, err := getOrCreateCollection(db, miscCollectionName)
	if err != nil {
		return miscStorage{}, err
	}

	return miscStorage{
		db:   db,
		coll: coll,
	}, nil
}

type miscStorage struct {
	db   driver.Database
	coll driver.Collection
}

type shallowDocument struct {
	Key     string `json:"_key,omitempty"`
	Shallow string `json:"shallow,omitempty"`
}

func (s *miscStorage) SetShallow(hashes []plumbing.Hash) error {
	json, err := json.Marshal(hashes)
	if err != nil {
		return err
	}

	_, err = s.db.Query(driver.WithWaitForSync(context.Background()), queryUpsertShallow, map[string]interface{}{
		"key":     indexKey,
		"shallow": string(json),
	})
	return err
}

func (s *miscStorage) Shallow() ([]plumbing.Hash, error) {
	var doc shallowDocument
	_, err := s.coll.ReadDocument(context.Background(), shallowKey, &doc)
	if err != nil {
		return nil, err
	}

	var shallow []plumbing.Hash
	err = json.Unmarshal([]byte(doc.Shallow), shallow)
	return shallow, err
}

type indexDocument struct {
	Key string `json:"_key,omitempty"`
	Idx string `json:"idx,omitempty"`
}

func (s *miscStorage) SetIndex(idx *index.Index) error {
	json, err := json.Marshal(idx)
	if err != nil {
		return err
	}

	_, err = s.db.Query(driver.WithWaitForSync(context.Background()), queryUpsertIndex, map[string]interface{}{
		"key": indexKey,
		"idx": string(json),
	})
	return err
}

func (s *miscStorage) Index() (*index.Index, error) {
	var doc indexDocument
	_, err := s.coll.ReadDocument(context.Background(), indexKey, &doc)
	if driver.IsNotFound(err) {
		return &index.Index{}, nil
	} else if err != nil {
		return nil, err
	}

	idx := &index.Index{}
	err = json.Unmarshal([]byte(doc.Idx), idx)
	return idx, err
}

type configDocument struct {
	Key    string `json:"_key,omitempty"`
	Config string `json:"config,omitempty"`
}

func (s *miscStorage) SetConfig(cfg *config.Config) error {
	json, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	_, err = s.db.Query(driver.WithWaitForSync(context.Background()), queryUpsertConfig, map[string]interface{}{
		"key":    configKey,
		"config": string(json),
	})
	return err
}

func (s *miscStorage) Config() (*config.Config, error) {
	var doc configDocument
	_, err := s.coll.ReadDocument(context.Background(), configKey, &doc)
	if driver.IsNotFound(err) {
		return config.NewConfig(), nil
	} else if err != nil {
		return nil, err
	}

	cfg := &config.Config{}
	err = json.Unmarshal([]byte(doc.Config), cfg)
	return cfg, err
}
