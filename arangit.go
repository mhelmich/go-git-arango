package arangit

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
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
	// ErrTagDoesntExist -
	ErrTagDoesntExist = errors.New("tag doesn't exist")
	// ErrIteratorExhausted -
	ErrIteratorExhausted = errors.New("iterator is exhausted")
	// ErrStop -
	ErrStop = errors.New("stop")
)

// Repository -
type Repository interface {
	CommitFile(string, io.Reader) error
	TagHead(name string) error
	CheckoutTag(name string) error
	ReadFileFromHead(path string) ([]byte, error)
	ReadFileFromTag(tagName string, path string) ([]byte, error)
	PrintStatus() error
	DeleteTag(name string) error
	FileIterForTag(name string) (FileIterator, error)
	FileIterForHead() (FileIterator, error)
}

// FileIterator -
type FileIterator interface {
	Next() (io.Reader, error)
	ForEach(FileIteratorFunc) error
}

// FileIteratorFunc -
type FileIteratorFunc func(io.Reader, os.FileInfo) error

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

func (r *repository) PrintStatus() error {
	wt, err := r.repo.Worktree()
	if err != nil {
		return err
	}

	st, err := wt.Status()
	if err != nil {
		return err
	}

	fmt.Printf("Status:\n%s\n", st.String())
	return nil
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

	// r.PrintStatus()
	err = w.Checkout(&git.CheckoutOptions{
		Hash: ref.Hash(),
	})
	if err != nil {
		return nil, err
	}

	// r.PrintStatus()
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

	// err = r.PrintStatus()
	// if err != nil {
	// 	return err
	// }

	err = wt.Checkout(&git.CheckoutOptions{
		Branch: tagRefName,
	})
	if err != nil {
		return err
	}

	// err = r.PrintStatus()
	// if err != nil {
	// 	return err
	// }

	infos, err := r.fs.ReadDir(r.fs.Root())
	if err != nil {
		return err
	}

	// for _, info := range infos {
	// 	fmt.Printf("FILE: %s\n", info.Name())
	// }
	fmt.Printf("num files: %d\n", len(infos))
	return nil
}

func (r *repository) DeleteTag(name string) error {
	return r.repo.DeleteTag(name)
}

func (r *repository) TagHead(name string) error {
	// exists, err := r.tagExists(name)
	// if err != nil {
	// 	return err
	// } else if exists {
	// 	return ErrTagExists
	// }

	h, err := r.repo.Head()
	if err != nil {
		return err
	}

	fmt.Printf("HEAD: %s\n", h.Hash().String())
	tag, err := r.repo.CreateTag(name, h.Hash(), &git.CreateTagOptions{
		Message: "creating tag " + name,
		Tagger: &object.Signature{
			Name:  "John Doe",
			Email: "john@doe.org",
			When:  time.Now(),
		},
	})

	if err == nil {
		fmt.Printf("TAG: %s %d\n", tag.Hash().String(), tag.Type())
	}
	return err
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

	// r.PrintStatus()
	h, err := wt.Add(path)
	if err != nil {
		return err
	}

	// r.PrintStatus()
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
	// r.PrintStatus()
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

func (r *repository) FileIterForHead() (FileIterator, error) {
	infos, err := r.fs.ReadDir(r.fs.Root())
	if err != nil {
		return nil, err
	}
	return &fileIter{
		infos: infos,
		fs:    r.fs,
	}, nil
}

func (r *repository) FileIterForTag(name string) (FileIterator, error) {
	err := r.CheckoutTag(name)
	if err != nil {
		return nil, err
	}

	infos, err := r.fs.ReadDir(r.fs.Root())
	if err != nil {
		return nil, err
	}

	return &fileIter{
		infos: infos,
		fs:    r.fs,
	}, nil
}

type fileIter struct {
	infos []os.FileInfo
	fs    billy.Filesystem
	idx   int
}

// TODO: fix this signature ... can't return reader without closing it
func (i *fileIter) Next() (io.Reader, error) {
	if i.idx >= len(i.infos) {
		return nil, ErrIteratorExhausted
	}

	file, err := i.fs.Open(i.infos[i.idx].Name())
	i.idx++
	return file, err
}

func (i *fileIter) ForEach(f FileIteratorFunc) error {
	for _, info := range i.infos {
		file, err := i.fs.Open(info.Name())
		if err != nil {
			return err
		}

		defer func() { _ = file.Close() }()
		err = f(file, info)
		if err != nil {
			if err == ErrStop {
				return nil
			}
			return err
		}
	}
	return nil
}
