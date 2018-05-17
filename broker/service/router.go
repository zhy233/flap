package service

import (
	"math/rand"
	"sync"

	"github.com/meqio/proto"
	"go.uber.org/zap"
)

type Router struct {
	bk *Broker
	sync.RWMutex
}

const (
	ROUTER_MSG_ADD = 'a'
)

type RouterTarget struct {
	Addr string
	Cid  uint64
}

func (r *Router) Init() {

}

func (r *Router) Close() {

}

func (r *Router) recvRoute(buf []byte) {
	if buf[4] != ROUTER_MSG_ADD {
		return
	}
	msgs, cid, err := proto.UnpackRouteMsgs(buf[5:])
	if err != nil {
		L.Warn("route process error", zap.Error(err))
		return
	}

	r.RLock()
	c, ok := r.bk.clients[cid]
	r.RUnlock()
	if ok {
		c.msgSender <- msgs
	}
}
func (r *Router) route(outer map[Sess][]*proto.PubMsg) {
	//@todo
	// async + batch,current implementation will block the client's read loop
	for s, ms := range outer {
		m := proto.PackRouteMsgs(ms, ROUTER_MSG_ADD, s.Cid)
		r.bk.cluster.peer.send.GossipUnicast(s.Addr, m)
	}
}

func (r *Router) FindRoutes(msgs []*proto.PubMsg) (map[Sess][]*proto.PubMsg, map[Sess][]*proto.PubMsg) {
	local := make(map[Sess][]*proto.PubMsg)
	outer := make(map[Sess][]*proto.PubMsg)

	for _, msg := range msgs {
		t := string(msg.Topic)
		groups, ok := r.bk.subs[t]
		if !ok || len(groups) == 0 {
			continue
		}

		for _, g := range groups {
			s := g.Sesses[rand.Intn(len(g.Sesses))]
			if s.Addr == r.bk.cluster.peer.name {
				local[s] = append(local[s], msg)
			} else {
				outer[s] = append(outer[s], msg)
			}
		}
	}

	return local, outer
}
