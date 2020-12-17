package arangit

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/mhelmich/arangit/arangodb"
)

var (
	// ErrTagExists -
	ErrTagExists = errors.New("tag already exists")
	// ErrTagDoesntExist -
	ErrTagDoesntExist = errors.New("tag doesn't exist")
)

// Repository -
type Repository interface {
	CommitFile(string, io.Reader) error
	TagHead(tagName string) error
	CheckoutTag(name string) error
	ReadFileFromHead(path string) ([]byte, error)
	ReadFileFromTag(tagName string, path string) ([]byte, error)
}

// OpenRepo -
func OpenRepo(name string) (Repository, error) {
	arangoStorage, created, err := arangodb.NewStore("http://localhost:8529", name)
	if err != nil {
		return nil, err
	}

	fs := memfs.New()
	if created {
		_, err = git.Init(arangoStorage, fs)
		if err != nil {
			return nil, err
		}
	}

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

func (r *repository) ReadFileFromTag(tagName string, path string) ([]byte, error) {
	err := r.CheckoutTag(tagName)
	if err != nil {
		return nil, err
	}

	f, err := r.fs.Open(path)
	if err != nil {
		return nil, err
	}

	buf := &bytes.Buffer{}
	_, err = io.Copy(buf, f)
	return buf.Bytes(), err
}

func (r *repository) ReadFileFromHead(path string) ([]byte, error) {
	ref, err := r.repo.Head()
	if err != nil {
		return nil, err
	}

	w, err := r.repo.Worktree()
	if err != nil {
		return nil, err
	}

	err = w.Checkout(&git.CheckoutOptions{
		Hash: ref.Hash(),
	})
	if err != nil {
		return nil, err
	}

	f, err := r.fs.Open(path)
	if err != nil {
		return nil, err
	}

	buf := &bytes.Buffer{}
	_, err = io.Copy(buf, f)
	return buf.Bytes(), err
}

func (r *repository) CheckoutTag(name string) error {
	tags, err := r.repo.Tags()
	if err != nil {
		return err
	}

	tagRefName := plumbing.NewTagReferenceName(name)
	var tagRef *plumbing.Reference
	err = tags.ForEach(func(ref *plumbing.Reference) error {
		if tagRefName == ref.Name() {
			tagRef = ref
			return storer.ErrStop
		}
		return nil
	})
	if err != nil {
		return err
	} else if tagRef == nil {
		return ErrTagDoesntExist
	}

	wt, err := r.repo.Worktree()
	if err != nil {
		return err
	}

	err = wt.Checkout(&git.CheckoutOptions{
		Hash: tagRef.Hash(),
	})
	if err != nil {
		return err
	}

	infos, err := r.fs.ReadDir(r.fs.Root())
	if err != nil {
		return err
	}

	fmt.Printf("num files: %d\n", len(infos))
	return nil
}

func (r *repository) TagHead(name string) error {
	exists, err := r.tagExists(name)
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
	tag, err := r.repo.CreateTag(name, h.Hash(), &git.CreateTagOptions{
		Message: name,
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

	err = tags.ForEach(func(t *object.Tag) error {
		if t.Name == tagName {
			return storer.ErrStop
		}
		return nil
	})
	if err == storer.ErrStop {
		return true, nil
	} else if err != nil {
		return false, err
	}

	return false, nil
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
