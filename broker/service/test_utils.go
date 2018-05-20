package service

import (
	"fmt"

	"github.com/meqio/meq/proto"
	"github.com/weaveworks/mesh"
)

var mockMsgs = []*proto.PubMsg{
	&proto.PubMsg{[]byte("001"), []byte("/test/g1/+/b1"), []byte("hello world1"), false, 0, 1},
	&proto.PubMsg{[]byte("002"), []byte("/test/g1/+/b1"), []byte("hello world2"), false, 0, 1},
	&proto.PubMsg{[]byte("003"), []byte("/test/g1/+/b1"), []byte("hello world3"), false, 0, 1},
	&proto.PubMsg{[]byte("004"), []byte("/test/g1/+/b1"), []byte("hello world4"), false, 0, 1},
	&proto.PubMsg{[]byte("005"), []byte("/test/g1/+/b1"), []byte("hello world5"), false, 0, 1},
	&proto.PubMsg{[]byte("006"), []byte("/test/g1/+/b1"), []byte("hello world6"), false, 0, 1},
	&proto.PubMsg{[]byte("007"), []byte("/test/g1/+/b1"), []byte("hello world7"), false, 0, 1},
	&proto.PubMsg{[]byte("008"), []byte("/test/g1/+/b1"), []byte("hello world8"), false, 0, 1},
	&proto.PubMsg{[]byte("009"), []byte("/test/g1/+/b1"), []byte("hello world9"), false, 0, 1},
	&proto.PubMsg{[]byte("010"), []byte("/test/g1/+/b1"), []byte("hello world10"), false, 0, 1},
	&proto.PubMsg{[]byte("011"), []byte("/test/g1/+/b1"), []byte("hello world11"), false, 0, 1},
}

func populateSubs(st *SubTrie) {
	n := 0
	for i := 1; i <= 10; i++ {
		for j := 1; j <= 4; j++ {
			for k := 1; k <= 10; k++ {
				for l := 1; l <= 1000; l++ {
					n++
					topic := []byte(fmt.Sprintf("/test/g1/%d/b1/%d/%d/%d", i, j, k, l))
					queue := []byte("test1")
					addr := 1
					if i%2 == 0 {
						queue = []byte("test2")
						addr = 2
					}
					st.Subscribe(topic, queue, uint64(n), mesh.PeerName(addr))
				}
			}
		}
	}
}
