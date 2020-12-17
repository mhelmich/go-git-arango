package arangit

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRepoOpen(t *testing.T) {
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

	buf := &bytes.Buffer{}
	buf.WriteString("Hello World!")
	err = repo.CommitFile("testing_test.txt", buf)
	assert.Nil(t, err)

	err = repo.PrintStatus()
	assert.Nil(t, err)

	err = repo.TagHead("my-first-tag")
	assert.Nil(t, err)
	defer func() { _ = repo.DeleteTag("my-first-tag") }()

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
