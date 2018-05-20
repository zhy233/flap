package proto

type TopicProp struct {
	PushMsgWhenSub           bool
	GetMsgFromOldestToNewest bool
	AckStrategy              int8
	GetMsgStrategy           int8
}

const (
	TopicSep      = '/'
	TopicWildcard = '+'
)
