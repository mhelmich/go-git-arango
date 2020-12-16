package arangit

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/mhelmich/arangit/arangodb"
)

// Repository -
type Repository interface {
	CommitFile(string, io.Reader) error
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

type repository struct {
	fs   billy.Filesystem
	repo *git.Repository
}

func (r *repository) CommitFile(path string, rdr io.Reader) error {
	err := r.writeFile(path, rdr)
	if err != nil {
		return err
	}

	wt, err := r.repo.Worktree()
	if err != nil {
		return err
	}

	h, err := wt.Add(path)
	if err != nil {
		return err
	}

	fmt.Printf("HASH: %s\n", h.String())

	commit, err := wt.Commit("testing_test.txt commit", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "John Doe",
			Email: "john@doe.org",
			When:  time.Now(),
		},
	})
	if err != nil {
		return err
	}

	fmt.Printf("COMMIT: %s\n", commit.String())
	return err
}

func (r *repository) writeFile(path string, rdr io.Reader) error {
	var f billy.File
	var err error
	var exists bool
	exists, err = r.fileExists(path)
	if err != nil {
		return err
	}

	if exists {
		f, err = r.fs.Open(path)
	} else {
		f, err = r.fs.Create(path)
	}
	if err != nil {
		return err
	}

	defer func() { _ = f.Close() }()
	_, err = io.Copy(f, rdr)
	return err
}

func (r *repository) fileExists(path string) (bool, error) {
	_, err := r.fs.Stat(path)
	if err != nil {
		if err == os.ErrNotExist {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
