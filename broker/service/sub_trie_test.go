package service

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/mesh"
)

type SubData struct {
	Topic []byte
	Group []byte
	Cid   uint64
	Addr  mesh.PeerName
}

// a1:354238002 , b1: 4114052237,c1:1943813973, d1: 3929575225
// a2: 2033241478 b2 : 3684110692 c2: 2262372255
func TestTrieSubAndLookup(t *testing.T) {
	st := NewSubTrie()
	inputs := []SubData{
		SubData{[]byte("/a1/b1/c1"), []byte("test1"), 1, mesh.PeerName(1)},
		SubData{[]byte("/a1/b1/c1"), []byte("test1"), 2, mesh.PeerName(2)},
		SubData{[]byte("/a1/b1/c1"), []byte("test2"), 3, mesh.PeerName(1)},
		SubData{[]byte("/a1/b1/c1"), []byte("test2"), 4, mesh.PeerName(2)},
		SubData{[]byte("/a1/b1/c1/d1"), []byte("test1"), 5, mesh.PeerName(1)},
		SubData{[]byte("/a1/b2/c1"), []byte("test2"), 6, mesh.PeerName(2)},
		SubData{[]byte("/a1/b2/c2"), []byte("test1"), 7, mesh.PeerName(2)},
		SubData{[]byte("/a2/b1/c1"), []byte("test1"), 8, mesh.PeerName(1)},
	}
	outputs := []Sess{Sess{mesh.PeerName(2), 2}, Sess{mesh.PeerName(2), 4}, Sess{mesh.PeerName(1), 5}, Sess{mesh.PeerName(2), 6}}
	for _, input := range inputs {
		st.Subscribe(input.Topic, input.Group, input.Cid, input.Addr)
	}

	v, _ := st.Lookup([]byte("/a1/+/c1"))
	assert.Equal(t, outputs, v)
}

func TestTrieSubAndLookupExactly(t *testing.T) {
	st := NewSubTrie()
	inputs := []SubData{
		SubData{[]byte("/a1/b1/c1"), []byte("test1"), 1, mesh.PeerName(1)},
		SubData{[]byte("/a1/b1/c1"), []byte("test1"), 2, mesh.PeerName(2)},
		SubData{[]byte("/a1/b1/c1"), []byte("test2"), 3, mesh.PeerName(1)},
		SubData{[]byte("/a1/b1/c1"), []byte("test2"), 4, mesh.PeerName(2)},
		SubData{[]byte("/a1/b1/c1/d1"), []byte("test1"), 5, mesh.PeerName(1)},
		SubData{[]byte("/a1/b2/c2"), []byte("test1"), 6, mesh.PeerName(2)},
		SubData{[]byte("/a2/b1/c1"), []byte("test1"), 7, mesh.PeerName(1)},
	}
	outputs := []Sess{Sess{mesh.PeerName(2), 2}, Sess{mesh.PeerName(2), 4}}
	for _, input := range inputs {
		st.Subscribe(input.Topic, input.Group, input.Cid, input.Addr)
	}

	v, _ := st.LookupExactly([]byte("/a1/b1/c1"))
	assert.Equal(t, outputs, v)
}

func BenchmarkTrieSubscribe(b *testing.B) {
	st := NewSubTrie()
	populateSubs(st)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		n := 1
		if i%2 == 0 {
			n = 2
		}
		topic := []byte(fmt.Sprintf("/%d/%d/%d/%d", 100, n, n, n))
		queue := []byte("test1")
		addr := 1
		if i%2 == 0 {
			queue = []byte("test2")
			addr = 2
		}
		st.Subscribe(topic, queue, uint64(i), mesh.PeerName(addr))
	}
}

func BenchmarkTrieLookup(b *testing.B) {
	st := NewSubTrie()
	populateSubs(st)

	b.ReportAllocs()
	b.ResetTimer()

	t := []byte("/50/3")
	for i := 0; i < b.N; i++ {
		v, err := st.Lookup(t)
		if err != nil {
			b.Fatal(err, len(v))
		}
	}
}

func BenchmarkTrieLookupExactly(b *testing.B) {
	st := NewSubTrie()
	populateSubs(st)

	b.ReportAllocs()
	b.ResetTimer()

	t := []byte("/50/3/6/5000")
	for i := 0; i < b.N; i++ {
		st.LookupExactly(t)
	}
}

func populateSubs(st *SubTrie) {
	for i := 1; i <= 10; i++ {
		for j := 1; j <= 4; j++ {
			for k := 1; k <= 10; k++ {
				for l := 1; l <= 10000; l++ {
					topic := []byte(fmt.Sprintf("/%d/%d/%d/%d", i, j, k, l))
					queue := []byte("test1")
					addr := 1
					if i%2 == 0 {
						queue = []byte("test2")
						addr = 2
					}
					st.Subscribe(topic, queue, uint64(i*j*k*l), mesh.PeerName(addr))
				}
			}
		}
	}
}
