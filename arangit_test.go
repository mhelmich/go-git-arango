package arangit

import (
	"bytes"
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

	buf := &bytes.Buffer{}
	buf.WriteString("Hello World!")
	err = repo.CommitFile("testing_test.txt", buf)
	assert.Nil(t, err)

	err = repo.TagHead("my-first-tag")
	assert.Nil(t, err)
}
