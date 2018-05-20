package service

import (
	"encoding/binary"
	"sync"

	"github.com/golang/snappy"
	"github.com/meqio/proto"
	"github.com/weaveworks/mesh"
	"go.uber.org/zap"
)

type Router struct {
	bk *Broker
	sync.RWMutex
}

type RouterTarget struct {
	Addr string
	Cid  uint64
}

func (r *Router) Init() {

}

func (r *Router) Close() {

}

func (r *Router) recvRoute(src mesh.PeerName, buf []byte) {
	msgs, cid, err := unpackRouteMsgs(buf[5:])
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
		m := packRouteMsgs(ms, CLUSTER_MSG_ROUTE, s.Cid)
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

func packRouteMsgs(ms []*proto.PubMsg, cmd byte, cid uint64) []byte {
	bl := 11 * len(ms)
	for _, m := range ms {
		bl += (len(m.ID) + len(m.Topic) + len(m.Payload))
	}
	body := make([]byte, bl)

	last := 0
	for _, m := range ms {
		ml, tl, pl := len(m.ID), len(m.Topic), len(m.Payload)
		//msgid
		binary.PutUvarint(body[last:last+2], uint64(ml))
		copy(body[last+2:last+2+ml], m.ID)
		//topic
		binary.PutUvarint(body[last+2+ml:last+4+ml], uint64(tl))
		copy(body[last+4+ml:last+4+ml+tl], m.Topic)
		//payload
		binary.PutUvarint(body[last+4+ml+tl:last+8+ml+tl], uint64(pl))
		copy(body[last+8+ml+tl:last+8+ml+tl+pl], m.Payload)
		//Acked
		if m.Acked {
			body[last+8+ml+tl+pl] = '1'
		} else {
			body[last+8+ml+tl+pl] = '0'
		}
		//type
		binary.PutUvarint(body[last+9+ml+tl+pl:last+10+ml+tl+pl], uint64(m.Type))
		//qos
		binary.PutUvarint(body[last+10+ml+tl+pl:last+11+ml+tl+pl], uint64(m.QoS))
		last = last + 11 + ml + tl + pl
	}

	// 压缩body
	cbody := snappy.Encode(nil, body)

	msg := make([]byte, len(cbody)+11)
	//header
	binary.PutUvarint(msg[:4], uint64(len(cbody)+7))
	//command
	msg[4] = cmd
	//msg count
	binary.PutUvarint(msg[5:7], uint64(len(ms)))
	//cid
	binary.PutUvarint(msg[7:11], cid)
	//body
	copy(msg[11:], cbody)
	return msg
}

func unpackRouteMsgs(m []byte) ([]*proto.PubMsg, uint64, error) {
	// msg count
	msl, _ := binary.Uvarint(m[:2])
	msgs := make([]*proto.PubMsg, msl)
	// cid
	cid, _ := binary.Uvarint(m[2:6])

	// decompress
	b, err := snappy.Decode(nil, m[6:])
	if err != nil {
		return nil, 0, err
	}

	var last uint64
	bl := uint64(len(b))
	index := 0
	for {
		if last >= bl {
			break
		}
		//msgid
		ml, _ := binary.Uvarint(b[last : last+2])
		msgid := b[last+2 : last+2+ml]
		//topic
		tl, _ := binary.Uvarint(b[last+2+ml : last+4+ml])
		topic := b[last+4+ml : last+4+ml+tl]
		//payload
		pl, _ := binary.Uvarint(b[last+4+ml+tl : last+8+ml+tl])
		payload := b[last+8+ml+tl : last+8+ml+tl+pl]
		//acked
		var acked bool
		if b[last+8+ml+tl+pl] == '1' {
			acked = true
		}
		//type
		tp, _ := binary.Uvarint(b[last+9+ml+tl+pl : last+10+ml+tl+pl])
		//qos
		qos, _ := binary.Uvarint(b[last+10+ml+tl+pl : last+11+ml+tl+pl])
		msgs[index] = &proto.PubMsg{msgid, topic, payload, acked, int8(tp), int8(qos)}

		index++
		last = last + 11 + ml + tl + pl
	}

	return msgs, cid, nil
}
