package arangit

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/mhelmich/arangit/arangodb"
)

var (
	// ErrTagExists -
	ErrTagExists = errors.New("tag already exists")
)

// Repository -
type Repository interface {
	CommitFile(string, io.Reader) error
	TagHead(tagName string) error
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

func (r *repository) TagHead(tagName string) error {
	exists, err := r.tagExists(tagName)
	if err != nil {
		return err
	} else if exists {
		return ErrTagExists
	}

	h, err := r.repo.Head()
	if err != nil {
		return err
	}

	fmt.Printf("HEAD: %s\n", h.Hash().String())
	tag, err := r.repo.CreateTag(tagName, h.Hash(), &git.CreateTagOptions{
		Message: tagName,
		Tagger: &object.Signature{
			Name:  "John Doe",
			Email: "john@doe.org",
			When:  time.Now(),
		},
	})

	fmt.Printf("TAG: %s\n", tag.Hash().String())
	return err
}

func (r *repository) tagExists(tagName string) (bool, error) {
	tags, err := r.repo.TagObjects()
	if err != nil {
		return false, err
	}

	var exists bool
	err = tags.ForEach(func(t *object.Tag) error {
		if t.Name == tagName {
			exists = true
			return fmt.Errorf("found tag with same name")
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	return exists, nil
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
	// counter to ones intuition, Create truncates the file if it already exists
	// From the doc:
	// Create creates the named file with mode 0666 (before umask), truncating it if it already exists.
	f, err := r.fs.Create(path)
	if err != nil {
		return err
	}

	defer func() { _ = f.Close() }()
	_, err = io.Copy(f, rdr)
	return err
}
