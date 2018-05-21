package service

import (
	"bytes"
	"encoding/gob"
	"errors"
	"math/rand"
	"sync"

	"github.com/chaingod/talent"

	"github.com/meqio/meq/proto"
	"github.com/weaveworks/mesh"
)

type Node struct {
	ID       uint32
	Topic    []byte
	Subs     []*SubGroup
	Children map[uint32]*Node
}

type SubTrie struct {
	Roots map[uint32]*Node
}

type SubGroup struct {
	ID     []byte
	Sesses []Sess
}
type Sess struct {
	Addr mesh.PeerName
	Cid  uint64
}

type TopicSess struct {
	Topic []byte
	Sess  Sess
}

const subCacheLen = 1000

var subCache = make(map[string]int)

var (
	wildcard = talent.MurMurHash([]byte{proto.TopicWildcard})
	sublock  = &sync.RWMutex{}
)

func NewSubTrie() *SubTrie {
	return &SubTrie{
		Roots: make(map[uint32]*Node),
	}
}

func (st *SubTrie) Subscribe(topic []byte, queue []byte, cid uint64, addr mesh.PeerName) error {
	tids, err := proto.ParseTopic(topic, true)
	if err != nil {
		return err
	}
	rootid := tids[0]
	last := tids[len(tids)-1]

	sublock.RLock()
	root, ok := st.Roots[rootid]
	sublock.RUnlock()

	if !ok {
		root = &Node{
			ID:       rootid,
			Children: make(map[uint32]*Node),
			// Subs:     make(map[string][]*SubGroup),
		}
		sublock.Lock()
		st.Roots[rootid] = root
		sublock.Unlock()
	}

	curr := root
	for _, tid := range tids[1:] {
		sublock.RLock()
		child, ok := curr.Children[tid]
		sublock.RUnlock()
		if !ok {
			child = &Node{
				ID:       tid,
				Children: make(map[uint32]*Node),
				// Subs:     make(map[string][]*SubGroup),
			}
			sublock.Lock()
			curr.Children[tid] = child
			sublock.Unlock()
		}

		curr = child
		// if encounters the last node in the tree branch, we should add topic to the subs of this node
		if tid == last {
			curr.Topic = topic
			if !ok {
				// []group
				g := &SubGroup{
					ID: queue,
					Sesses: []Sess{
						Sess{
							Addr: addr,
							Cid:  cid,
						},
					},
				}
				curr.Subs = []*SubGroup{g}
			} else {
				for _, g := range curr.Subs {
					// group already exist,add to group
					if bytes.Compare(g.ID, queue) == 0 {
						g.Sesses = append(g.Sesses, Sess{
							Addr: addr,
							Cid:  cid,
						})
						return nil
					}
				}
				// create group
				g := &SubGroup{
					ID: queue,
					Sesses: []Sess{
						Sess{
							Addr: addr,
							Cid:  cid,
						},
					},
				}
				curr.Subs = append(curr.Subs, g)
			}
		}
	}

	return nil
}

func (st *SubTrie) UnSubscribe(topic []byte, group []byte, cid uint64, addr mesh.PeerName) error {
	tids, err := proto.ParseTopic(topic, true)
	if err != nil {
		return err
	}
	rootid := tids[0]
	last := tids[len(tids)-1]

	sublock.RLock()
	root, ok := st.Roots[rootid]
	sublock.RUnlock()

	if !ok {
		return errors.New("no subscribe info")
	}

	curr := root
	for _, tid := range tids[1:] {
		sublock.RLock()
		child, ok := curr.Children[tid]
		sublock.RUnlock()
		if !ok {
			return errors.New("no subscribe info")
		}

		curr = child
		// if encounters the last node in the tree branch, we should remove topic in this node
		if tid == last {
			for j, g := range curr.Subs {
				if bytes.Compare(g.ID, group) == 0 {
					// group exist
					for i, c := range g.Sesses {
						if c.Cid == cid && addr == c.Addr {
							// delete sess
							g.Sesses = append(g.Sesses[:i], g.Sesses[i+1:]...)
							if len(g.Sesses) == 0 {
								//delete group
								curr.Subs = append(curr.Subs[:j], curr.Subs[j+1:]...)
							}
							break
						}
					}
				}
			}
		}
	}

	return nil
}

//@todo
// add query cache for heavy lookup
func (st *SubTrie) Lookup(topic []byte) ([]TopicSess, error) {
	t := string(topic)

	tids, err := proto.ParseTopic(topic, false)
	if err != nil {
		return nil, err
	}

	var sesses []TopicSess
	sublock.RLock()
	cl, ok := subCache[t]
	sublock.RUnlock()
	if ok {
		sesses = make([]TopicSess, 0, cl+100)
	} else {
		sesses = make([]TopicSess, 0, 10)
	}

	rootid := tids[0]

	sublock.RLock()
	root, ok := st.Roots[rootid]
	sublock.RUnlock()
	if !ok {
		return nil, nil
	}

	// 所有比target长的都应该收到
	// target中的通配符'+'可以匹配任何tid
	// 找到所有路线的最后一个node节点

	var lastNodes []*Node
	if len(tids) == 1 {
		lastNodes = append(lastNodes, root)
	} else {
		st.findLastNodes(root, tids[1:], &lastNodes)
	}

	// 找到lastNode的所有子节点
	//@performance 这段代码耗时92毫秒

	sublock.RLock()
	for _, last := range lastNodes {
		st.findSesses(last, &sesses)
	}
	sublock.RUnlock()

	//@todo
	//Remove duplicate elements from the list.
	if len(sesses) >= subCacheLen {
		sublock.Lock()
		subCache[string(topic)] = len(sesses)
		sublock.Unlock()
	}
	return sesses, nil
}

func (st *SubTrie) LookupExactly(topic []byte) ([]TopicSess, error) {
	tids, err := proto.ParseTopic(topic, true)
	if err != nil {
		return nil, err
	}

	var sesses []TopicSess
	rootid := tids[0]

	sublock.RLock()
	defer sublock.RUnlock()
	root, ok := st.Roots[rootid]
	if !ok {
		return nil, nil
	}

	// 所有比target长的都应该收到
	// target中的通配符'+'可以匹配任何tid
	// 找到所有路线的最后一个node节点
	lastNode := root
	for _, tid := range tids[1:] {
		// 任何一个node匹配不到，则认为完全无法匹配
		node, ok := lastNode.Children[tid]
		if !ok {
			return nil, nil
		}

		lastNode = node
	}

	for _, g := range lastNode.Subs {
		s := g.Sesses[rand.Intn(len(g.Sesses))]
		sesses = append(sesses, TopicSess{lastNode.Topic, s})
	}

	return sesses, nil
}

func (st *SubTrie) findSesses(n *Node, sesses *[]TopicSess) {
	//@performance 50% time used here
	for _, g := range n.Subs {
		//@performance 随机数消耗30毫秒
		var s Sess
		if len(g.Sesses) == 1 {
			s = g.Sesses[0]
		} else {
			s = g.Sesses[rand.Intn(len(g.Sesses))]
		}
		if cap(*sesses) == len(*sesses) {
			temp := make([]TopicSess, len(*sesses), cap(*sesses)*6)
			copy(temp, *sesses)
			*sesses = temp
		}
		*sesses = append(*sesses, TopicSess{n.Topic, s})
	}

	if len(n.Children) == 0 {
		return
	}
	for _, child := range n.Children {
		st.findSesses(child, sesses)
	}
}

func (st *SubTrie) findLastNodes(n *Node, tids []uint32, nodes *[]*Node) {
	if len(tids) == 1 {
		// 如果只剩一个节点，那就直接查找，不管能否找到，都返回
		node, ok := n.Children[tids[0]]
		if ok {
			*nodes = append(*nodes, node)
		}
		return
	}

	tid := tids[0]
	if tid != wildcard {
		node, ok := n.Children[tid]
		if !ok {
			return
		}
		st.findLastNodes(node, tids[1:], nodes)
	} else {
		for _, node := range n.Children {
			st.findLastNodes(node, tids[1:], nodes)
		}
	}
}

// cluster interface

var _ mesh.GossipData = &SubTrie{}

// Encode serializes our complete state to a slice of byte-slices.
// In this simple example, we use a single gob-encoded
// buffer: see https://golang.org/pkg/encoding/gob/
func (st *SubTrie) Encode() [][]byte {
	sublock.RLock()
	defer sublock.RUnlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(st); err != nil {
		panic(err)
	}
	msg := make([]byte, len(buf.Bytes())+5)
	msg[4] = CLUSTER_SUBS_SYNC_RESP
	copy(msg[5:], buf.Bytes())
	return [][]byte{msg}
}

// Merge merges the other GossipData into this one,
// and returns our resulting, complete state.
func (st *SubTrie) Merge(osubs mesh.GossipData) (complete mesh.GossipData) {
	return
}

type SubMessage struct {
	TP    int
	Topic []byte
	Group []byte
	Cid   uint64
}

func (st SubMessage) Encode() [][]byte {
	// sync to other nodes
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(st); err != nil {
		panic(err)
	}

	return [][]byte{buf.Bytes()}
}

func (st SubMessage) Merge(new mesh.GossipData) (complete mesh.GossipData) {
	return
}
