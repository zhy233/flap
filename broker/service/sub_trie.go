package service

import (
	"bytes"
	"math/rand"
	"sync"

	"github.com/chaingod/talent"

	"github.com/meqio/proto"
	"github.com/weaveworks/mesh"
)

type Node struct {
	ID       uint32
	Subs     map[string][]*SubGroup
	Parent   *Node
	Children map[uint32]*Node
}

type SubTrie struct {
	sync.RWMutex
	Roots map[uint32]*Node
}

var (
	wildcard = talent.MurMurHash([]byte{proto.TopicWildcard})
)

func NewSubTrie() *SubTrie {
	return &SubTrie{
		Roots: make(map[uint32]*Node),
	}
}

func (st *SubTrie) Subscribe(topic []byte, queue []byte, cid uint64, addr mesh.PeerName) error {
	t := string(topic)
	tids, err := parseTopic(topic, true)
	if err != nil {
		return err
	}
	rootid := tids[0]
	last := tids[len(tids)-1]

	st.RLock()
	root, ok := st.Roots[rootid]
	st.RUnlock()

	if !ok {
		root = &Node{
			ID:       rootid,
			Children: make(map[uint32]*Node),
			Subs:     make(map[string][]*SubGroup),
		}
		st.Lock()
		st.Roots[rootid] = root
		st.Unlock()
	}

	curr := root
	for _, tid := range tids[1:] {
		st.RLock()
		child, ok := curr.Children[tid]
		st.RUnlock()
		if !ok {
			child = &Node{
				ID:       tid,
				Parent:   curr,
				Children: make(map[uint32]*Node),
				Subs:     make(map[string][]*SubGroup),
			}
			st.Lock()
			curr.Children[tid] = child
			st.Unlock()
		}

		curr = child
		// if encounters the last node in the tree branch, we should add topic to the subs of this node
		if tid == last {
			st.RLock()
			t1, ok := curr.Subs[t]
			st.RUnlock()
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
				st.Lock()
				curr.Subs[t] = []*SubGroup{g}
				st.Unlock()
			} else {
				for _, g := range t1 {
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
				st.Lock()
				curr.Subs[t] = append(curr.Subs[t], g)
				st.Unlock()
			}
		}
	}

	return nil
}

func (st *SubTrie) Lookup(topic []byte) ([]Sess, error) {
	tids, err := parseTopic(topic, false)
	if err != nil {
		return nil, err
	}

	var sesses []Sess
	rootid := tids[0]

	st.RLock()
	root, ok := st.Roots[rootid]
	st.RUnlock()
	if !ok {
		return nil, nil
	}

	// 所有比target长的都应该收到
	// target中的通配符'+'可以匹配任何tid
	// 找到所有路线的最后一个node节点
	//@todo '+' has not been implemented
	lastNode := root
	for _, tid := range tids[1:] {
		// 任何一个node匹配不到，则认为完全无法匹配
		st.RLock()
		node, ok := lastNode.Children[tid]
		st.RUnlock()
		if !ok {
			return nil, nil
		}

		lastNode = node
	}

	// 找到lastNode的所有子节点
	st.RLock()
	st.findSesses(lastNode, &sesses)
	st.RUnlock()

	return sesses, nil
}

func (st *SubTrie) LookupExactly(topic []byte) ([]Sess, error) {
	tids, err := parseTopic(topic, false)
	if err != nil {
		return nil, err
	}

	var sesses []Sess
	rootid := tids[0]

	st.RLock()
	defer st.RUnlock()
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

	for _, gs := range lastNode.Subs {
		for _, g := range gs {
			s := g.Sesses[rand.Intn(len(g.Sesses))]
			sesses = append(sesses, s)
		}
	}

	return sesses, nil
}

func (st *SubTrie) findSesses(n *Node, sesses *[]Sess) {
	for _, gs := range n.Subs {
		for _, g := range gs {
			s := g.Sesses[rand.Intn(len(g.Sesses))]
			*sesses = append(*sesses, s)
		}
	}

	if len(n.Children) == 0 {
		return
	}
	for _, child := range n.Children {
		st.findSesses(child, sesses)
	}
}
