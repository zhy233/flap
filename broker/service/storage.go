package service

import (
	"github.com/meqio/proto"
	"github.com/weaveworks/mesh"
)

type Storage interface {
	Init()
	Close()

	Put([]*proto.PubMsg)
	ACK([][]byte)

	Get([]byte, int, []byte) []*proto.PubMsg
	GetCount([]byte) int

	Flush()

	Sub([]byte, []byte, uint64, mesh.PeerName)
	Unsub([]byte, []byte, uint64, mesh.PeerName)

	PutTimerMsg(*proto.TimerMsg)
	GetTimerMsg() []*proto.PubMsg
}
