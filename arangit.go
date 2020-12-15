package arangit

import (
	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	git "github.com/go-git/go-git/v5"
	"github.com/mhelmich/arangit/arangodb"
)

// Repository -
type Repository interface{}

type repository struct {
	fs   billy.Filesystem
	repo *git.Repository
}

// OpenRepo -
func OpenRepo(name string) (Repository, error) {
	arangoStorage, created, err := arangodb.NewStore("http://localhost:8529", name)
	if err != nil {
		return nil, err
	}

	if created {
		_, err = git.Init(arangoStorage, nil)
		if err != nil {
			return nil, err
		}
	}

	fs := memfs.New()
	repo, err := git.Open(arangoStorage, fs)
	if err != nil {
		return nil, err
	}

	return &repository{
		fs:   fs,
		repo: repo,
	}, nil
}
