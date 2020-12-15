package arangit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRepoOpen(t *testing.T) {
	_, err := OpenRepo("arangit")
	assert.Nil(t, err)

	_, err = OpenRepo("arangit")
	assert.Nil(t, err)
}
