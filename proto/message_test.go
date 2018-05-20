package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var mockMsgs = []*PubMsg{
	&PubMsg{[]byte("001"), []byte("test"), []byte("hello world1"), false, 0, 1},
	&PubMsg{[]byte("002"), []byte("test"), []byte("hello world2"), false, 0, 1},
	&PubMsg{[]byte("003"), []byte("test"), []byte("hello world3"), false, 0, 1},
	&PubMsg{[]byte("004"), []byte("test"), []byte("hello world4"), false, 0, 1},
	&PubMsg{[]byte("005"), []byte("test"), []byte("hello world5"), false, 0, 1},
	&PubMsg{[]byte("006"), []byte("test"), []byte("hello world6"), false, 0, 1},
	&PubMsg{[]byte("007"), []byte("test"), []byte("hello world7"), false, 0, 1},
	&PubMsg{[]byte("008"), []byte("test"), []byte("hello world8"), false, 0, 1},
	&PubMsg{[]byte("009"), []byte("test"), []byte("hello world9"), false, 0, 1},
	&PubMsg{[]byte("010"), []byte("test"), []byte("hello world10"), false, 0, 1},
	&PubMsg{[]byte("011"), []byte("test"), []byte("hello world11"), false, 0, 1},
}

func TestPubMsgsPackUnpack(t *testing.T) {
	packed := PackPubMsgs(mockMsgs, MSG_PUB)
	unpacked, err := UnpackPubMsgs(packed[5:])

	assert.NoError(t, err)
	assert.Equal(t, mockMsgs, unpacked)
}

func TestSubPackUnpack(t *testing.T) {
	topic := []byte("test")
	group := []byte("group")

	packed := PackSub(topic, group)
	ntopic, ngroup := UnpackSub(packed[5:])

	assert.Equal(t, topic, ntopic)
	assert.Equal(t, group, ngroup)
}

func TestAckPackUnpack(t *testing.T) {
	var msgids [][]byte
	for _, m := range mockMsgs {
		msgids = append(msgids, m.ID)
	}

	packed := PackAck(msgids, MSG_PUBACK)
	unpacked := UnpackAck(packed[5:])

	assert.Equal(t, msgids, unpacked)
}

func TestMsgCountPackUnpack(t *testing.T) {
	topic := []byte("test")
	count := 10

	packed := PackMsgCount(topic, count)
	ntopic, ncount := UnpackMsgCount(packed[5:])

	assert.Equal(t, topic, ntopic)
	assert.Equal(t, count, ncount)
}

func TestPullPackUnpack(t *testing.T) {
	msgid := []byte("00001")
	topic := []byte("test")
	count := 10

	packed := PackPullMsg(topic, count, msgid)
	ntopic, ncount, nmsgid := UnPackPullMsg(packed[5:])

	assert.Equal(t, topic, ntopic)
	assert.Equal(t, count, ncount)
	assert.Equal(t, msgid, nmsgid)
}

func TestTimerMsgPackUnpack(t *testing.T) {
	tmsg := &TimerMsg{[]byte("0001"), []byte("test"), []byte("timer msg emit!"), time.Now().Unix(), 10}
	packed := PackTimerMsg(tmsg, MSG_PUB_TIMER)
	unpacked := UnpackTimerMsg(packed[5:])

	assert.Equal(t, tmsg, unpacked)
}

func BenchmarkPubMsgPack(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		PackPubMsgs(mockMsgs, MSG_PUB)
	}
}

func BenchmarkPubMsgUnpack(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	packed := PackPubMsgs(mockMsgs, MSG_PUB)
	for i := 0; i < b.N; i++ {
		UnpackPubMsgs(packed[5:])
	}
}

func BenchmarkAckPack(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	var msgids [][]byte
	for _, m := range mockMsgs {
		msgids = append(msgids, m.ID)
	}

	for i := 0; i < b.N; i++ {
		PackAck(msgids, MSG_PUBACK)
	}
}

func BenchmarkAckUnpack(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	var msgids [][]byte
	for _, m := range mockMsgs {
		msgids = append(msgids, m.ID)
	}
	packed := PackAck(msgids, MSG_PUBACK)
	for i := 0; i < b.N; i++ {
		UnpackAck(packed[5:])
	}
}
