package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTopic(t *testing.T) {
	input := [][]byte{
		[]byte("/"),
		[]byte("a/b"),
		[]byte("a/b/"),
		[]byte("/a/b"),
		[]byte("a/b/c"),
		[]byte("/a/b/c"),
		[]byte("/asdf/bse/dewer"),
		[]byte("/a"),
		[]byte("/a//b"),
		[]byte("/+/b/c"),
		[]byte("/a/+/c"),
	}
	out := [][]uint32{
		nil,
		nil,
		nil,
		nil,
		nil,
		[]uint32{3238259379, 500706888, 1027807523},
		[]uint32{1753631938, 324405670, 3531030695},
		nil,
		nil,
		nil,
		nil,
	}
	for i, v := range input {
		ids, _ := parseTopic(v, true)
		assert.Equal(t, out[i], ids)
	}
}
