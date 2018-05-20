package service

import (
	"sync"

	"github.com/meqio/proto"
	"github.com/weaveworks/mesh"
	"go.uber.org/zap"
)

type Router struct {
	bk *Broker
	sync.RWMutex
}

const (
	ROUTER_MSG_ADD       = 'a'
	ROUTER_SUBS_SYNC     = 'b'
	ROUTER_RUNNING_TIME  = 'c'
	ROUTER_SUBS_SYNC_REQ = 'd'
)

type RouterTarget struct {
	Addr string
	Cid  uint64
}

func (r *Router) Init() {

}

func (r *Router) Close() {

}

func (r *Router) recvRoute(src mesh.PeerName, buf []byte) {
	switch buf[4] {
	case ROUTER_MSG_ADD:
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

	topics := make(map[string][]*proto.PubMsg)
	for _, msg := range msgs {
		t := string(msg.Topic)
		topics[t] = append(topics[t], msg)
	}

	for t, msgs := range topics {
		sesses, err := r.bk.subtrie.Lookup([]byte(t))
		if err != nil {
			L.Info("sub trie lookup error", zap.Error(err), zap.String("topic", t))
		}

		for _, s := range sesses {
			if s.Addr == r.bk.cluster.peer.name {
				local[s] = append(local[s], msgs...)
			} else {
				outer[s] = append(outer[s], msgs...)
			}
		}
	}
	return local, outer
}
