package service

import (
	"github.com/meqio/meq/proto"
)

type Storage interface {
	Init()
	Close()

	Put([]*proto.PubMsg)
	ACK([]proto.Ack)

	Get([]byte, int, []byte, proto.TopicProp) []*proto.PubMsg
	GetCount([]byte) int

	PutTimerMsg(*proto.TimerMsg)
	GetTimerMsg() []*proto.PubMsg

	SetTopicProp([]byte, proto.TopicProp) error
	GetTopicProp([]byte) (proto.TopicProp, bool)
}
