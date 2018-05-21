package proto

type TopicProp struct {
	Topic                    []byte
	PushMsgWhenSub           bool
	GetMsgFromOldestToNewest bool
	AckStrategy              int8
	GetMsgStrategy           int8
}

const (
	TopicSep      = '/'
	TopicWildcard = '+'
)

func GetTopicPrefix(topic []byte) []byte {
	n := 0
	for i, b := range topic {
		if b == '/' {
			n++
		}
		if n == 3 {
			return topic[:i]
		}
	}

	return nil
}
