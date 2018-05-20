package service

import (
	"net"
	"time"

	"github.com/meqio/meq/proto"
	"go.uber.org/zap"
)

type pushPacket struct {
	msgs []*proto.PubMsg
	from uint64
}

func pushOnline(from uint64, bk *Broker, msgs []*proto.PubMsg) {
	topics := make(map[string][]*proto.PubMsg)
	for _, msg := range msgs {
		t := string(msg.Topic)
		topics[t] = append(topics[t], msg)
	}

	for t, msgs := range topics {
		sesses, err := bk.subtrie.Lookup([]byte(t))
		if err != nil {
			L.Info("sub trie lookup error", zap.Error(err), zap.String("topic", t))
			continue
		}
		for _, sess := range sesses {
			if sess.Addr == bk.cluster.peer.name {
				if sess.Cid == from {
					continue
				}
				bk.RLock()
				c, ok := bk.clients[sess.Cid]
				bk.RUnlock()
				if !ok {
					bk.Lock()
					delete(bk.clients, sess.Cid)
					bk.Unlock()
				} else {
					c.msgSender <- msgs
				}
			} else {
				bk.router.route(sess, msgs)
			}
		}
	}
}

func pushOne(conn net.Conn, m []*proto.PubMsg) error {
	msg := proto.PackPubMsgs(m, proto.MSG_PUB)
	conn.SetWriteDeadline(time.Now().Add(MAX_IDLE_TIME * time.Second))
	_, err := conn.Write(msg)
	return err
}
