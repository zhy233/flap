package service

import (
	"net"
	"time"

	"github.com/meqio/meq/proto"
)

type pushPacket struct {
	msgs []*proto.PubMsg
	from uint64
}

func pushOnline(from uint64, bk *Broker, msgs []*proto.PubMsg) {
	local, outer := bk.router.FindRoutes(msgs)
	for s, ms := range local {
		cid := s.Cid
		if from == cid {
			continue
		}

		bk.Lock()
		c, ok := bk.clients[cid]
		bk.Unlock()
		if !ok { // clients offline,delete it
			bk.Lock()
			delete(bk.clients, cid)
			bk.Unlock()
		} else { // push to clients sender
			c.msgSender <- ms
		}
	}
	bk.router.route(outer)
}

func pushOne(conn net.Conn, m []*proto.PubMsg) error {
	msg := proto.PackPubMsgs(m, proto.MSG_PUB)
	conn.SetWriteDeadline(time.Now().Add(MAX_IDLE_TIME * time.Second))
	_, err := conn.Write(msg)
	return err
}
