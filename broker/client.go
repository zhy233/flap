package broker

import (
	"bufio"
	"errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/imdevlab/flap/pkg/message"
	"github.com/imdevlab/flap/pkg/network/mqtt"
	"github.com/imdevlab/g"
	"go.uber.org/zap"
)

// For controlling dynamic buffer sizes.
const (
	headerSize  = 4
	maxBodySize = 65536 * 16 // 1MB
)

type client struct {
	id   uint64 // do not exceeds max(int32)
	conn net.Conn
	bk   *Broker

	sender chan []*message.PubMsg

	subs map[string]struct{}

	username []byte // username

	closed  bool
	closech chan struct{}
}

func newClient(conn net.Conn, b *Broker) *client {
	id := atomic.AddUint64(&uid, 1)
	c := &client{
		id:      id,
		conn:    conn,
		bk:      b,
		sender:  make(chan []*message.PubMsg, MAX_CHANNEL_LEN),
		subs:    make(map[string]struct{}),
		closech: make(chan struct{}),
	}
	b.Lock()
	b.clients[id] = c
	b.Unlock()

	return c
}

func (c *client) process() {
	defer func() {
		c.bk.Lock()
		delete(c.bk.clients, c.id)
		c.bk.Unlock()

		c.closed = true
		c.conn.Close()
		g.L.Debug("client closed", zap.Uint64("conn_id", c.id))
	}()

	// must waiting for connect first
	err := c.waitForConnect()
	if err != nil {
		g.L.Debug("cant receive connect packet from client", zap.Uint64("cid", c.id), zap.Error(err))
		return
	}

	g.L.Debug("new user online", zap.Uint64("cid", c.id), zap.String("username", string(c.username)), zap.String("ip", c.conn.RemoteAddr().String()))

	// start a goroutine for other clients sending msg to this client
	go c.sendLoop()

	// waiting for client's message
	reader := bufio.NewReaderSize(c.conn, 65536)
	for !c.closed {
		c.conn.SetDeadline(time.Now().Add(time.Second * message.MAX_IDLE_TIME))
		msg, err := mqtt.DecodePacket(reader)
		if err != nil {
			g.L.Info("Decode packet error", zap.Uint64("cid", c.id), zap.Error(err))
			return
		}

		// Handle the receive
		if err := c.onReceive(msg); err != nil {
			g.L.Info("handle receive error", zap.Uint64("cid", c.id), zap.Error(err))
			return
		}
	}
}

func (c *client) onReceive(msg mqtt.Message) error {
	switch msg.Type() {
	case mqtt.TypeOfSubscribe:
		packet := msg.(*mqtt.Subscribe)

		ack := mqtt.Suback{
			MessageID: packet.MessageID,
			Qos:       make([]uint8, 0, len(packet.Subscriptions)),
		}

		// Subscribe for each subscription
		for _, sub := range packet.Subscriptions {
			if err := c.onSubscribe(sub.Topic); err != nil {
				ack.Qos = append(ack.Qos, 0x80) // 0x80 indicate subscription failure
				// c.notifyError(err, packet.MessageID)
				continue
			}

			// Append the QoS
			ack.Qos = append(ack.Qos, sub.Qos)
		}

		if _, err := ack.EncodeTo(c.conn); err != nil {
			return err
		}
	case mqtt.TypeOfUnsubscribe:
	case mqtt.TypeOfPingreq:
		ack := mqtt.Pingresp{}
		if _, err := ack.EncodeTo(c.conn); err != nil {
			return err
		}
	case mqtt.TypeOfDisconnect:
	case mqtt.TypeOfPublish:

	}

	return nil
}

// func (c *client) onReceive() error {
// 	defer func() {
// 		c.closed = true
// 		c.closech <- struct{}{}
// unsub topics
// for topic := range c.subs {
// 	t := []byte(topic)
// 	tp := message.GetTopicType(t)
// 	if tp == message.TopicTypeChat {
// 		// because when we send a message to a chat topic,we will add one unread count for every user,
// 		// so when unconnected,we need to se chat topic's unread count to 0
// 		c.bk.store.UpdateUnreadCount(t, c.username, false, 0)

// 		// notify to all the user in topic ,that someone has been offline
// 		if tp == message.TopicTypeChat {
// 			notifyOnline(c.bk, t, message.PackOfflineNotify(t, c.username))
// 		}
// 	}
// 	g.L.Info("user offline,unsub topic", zap.Uint64("conn_id", c.cid), zap.String("topic", topic))
// 	err := c.bk.subtrie.UnSubscribe(t, c.cid, c.bk.cluster.peer.name)
// 	if err != nil {
// 		g.L.Info("user offline,unsub error", zap.Uint64("conn_id", c.cid), zap.Error(err))
// 	}

// 	//@todo
// 	// aync + batch
// 	submsg := SubMessage{CLUSTER_UNSUB, []byte(topic), c.cid, []byte("")}
// 	c.bk.cluster.peer.send.GossipBroadcast(submsg)
// }
// 	if err := recover(); err != nil {
// 		g.L.Info("read loop panic:", zap.Error(err.(error)), zap.Stack("stack"))
// 		return
// 	}
// }()

// reader := bufio.NewReaderSize(c.conn, 65536)
// for !c.closed {
// 	c.conn.SetDeadline(time.Now().Add(time.Second * message.MAX_IDLE_TIME))
// 	msg, err := mqtt.DecodePacket(reader)
// 	if err != nil {
// 		return err
// 	}
// 	switch msg.Type() {
// 	case mqtt.TypeOfSubscribe:
// 		packet := msg.(*mqtt.Subscribe)
// 		for _, sub := range packet.Subscriptions {
// 			t := sub.Topic
// 			err := c.bk.subtrie.Subscribe(t, c.cid, c.bk.cluster.peer.name, c.username)
// 			if err != nil {
// 				g.L.Info("sub error", zap.Uint64("conn_id", c.cid), zap.ByteString("topic", t))
// 				return err
// 			}
// 			submsg := SubMessage{CLUSTER_SUB, t, c.cid, c.username}
// 			c.bk.cluster.peer.send.GossipBroadcast(submsg)

// 			c.subs[string(t)] = struct{}{}
// 			//@todo
// 			count := c.bk.store.UnreadCount(t, c.username)
// 			// push out the count of the unread messages
// 			msg := mqtt.Publish{
// 				Header: &mqtt.StaticHeader{
// 					QOS: 0,
// 				},
// 				Topic:   t,
// 				Payload: message.PackMsgCount(count),
// 			}
// 			msg.EncodeTo(c.conn)

// 			// check the topic is TopicTypeChat
// 			// notify to all the user in topic ,that someone has been online
// 			tp := message.GetTopicType(t)

// 			if tp == message.TopicTypeChat {
// 				notifyOnline(c.bk, t, message.PackOnlineNotify(t, c.username))
// 			}
// 		}

// 		ack := mqtt.Suback{
// 			MessageID: packet.MessageID,
// 		}
// 		ack.EncodeTo(c.conn)
// 	case mqtt.TypeOfUnsubscribe:

// 	case mqtt.TypeOfPingreq:
// 		pong := mqtt.Pingresp{}
// 		pong.EncodeTo(c.conn)
// 	case mqtt.TypeOfDisconnect:
// 		return nil

// 	case mqtt.TypeOfPublish:
// 		packet := msg.(*mqtt.Publish)
// 		// if len(packet.Payload) > 0 {
// 		// 	cmd := packet.Payload[0]
// 		// 	switch cmd {
// 		// 	case message.MSG_PULL:
// 		// 		count, offset := message.UnPackPullMsg(packet.Payload[1:])
// 		// 		// pulling out the all messages is not allowed
// 		// 		if count > MAX_MESSAGE_PULL_COUNT || count < 0 {
// 		// 			return fmt.Errorf("the pull count %d is larger than :%d or smaller than 0", count, MAX_MESSAGE_PULL_COUNT)
// 		// 		}

// 		// 		// check the topic is already subed
// 		// 		_, ok := c.subs[string(packet.Topic)]
// 		// 		if !ok {
// 		// 			return errors.New("pull messages without subscribe the topic:" + string(packet.Topic))
// 		// 		}

// 		// 		msgs := c.bk.store.Query(packet.Topic, count, offset, true)
// 		// 		if len(msgs) > 0 {
// 		// 			c.msgSender <- msgs
// 		// 		}
// 		// 	case message.MSG_PUB_BATCH: //batch pub
// 		// 		// clients publish  messages to a concrete topic
// 		// 		// single publish will store the messages according to the message qos
// 		// 		ms, err := message.UnpackPubBatch(packet.Payload[1:])
// 		// 		if err != nil {
// 		// 			return err
// 		// 		}

// 		// 		now := time.Now().Unix()
// 		// 		for _, m := range ms {
// 		// 			// gen msg id
// 		// 			m.ID = c.bk.idgen.Generate().Bytes()
// 		// 			m.Sender = c.username
// 		// 			// validate msg
// 		// 			_, _, err := message.AppidAndTopicType(m.Topic)
// 		// 			if err != nil {
// 		// 				g.L.Info("pub msg topic invalid", zap.Error(err), zap.ByteString("topic", m.Topic))
// 		// 				continue
// 		// 			}
// 		// 			// update the ttl to a unix time
// 		// 			if m.TTL != message.NeverExpires {
// 		// 				m.TTL = now + m.TTL
// 		// 			}
// 		// 		}

// 		// 		// save the messages
// 		// 		c.bk.store.Store(ms)
// 		// 		// push to online clients in all nodes
// 		// 		publishOnline(c.cid, c.bk, ms, false)

// 		// 		// ack to mqtt client
// 		// 		if packet.Header.QOS == 1 {
// 		// 			msg := mqtt.Puback{packet.MessageID}
// 		// 			msg.EncodeTo(c.conn)
// 		// 		}
// 		// 	case message.MSG_PUB_ONE: // single pub
// 		// 		// clients publish  messages to a concrete topic
// 		// 		// single publish will store the messages according to the message qos
// 		// 		m, err := message.UnpackMsg(packet.Payload[1:])
// 		// 		if err != nil {
// 		// 			return err
// 		// 		}
// 		// 		// validate msg
// 		// 		_, _, err = message.AppidAndTopicType(m.Topic)
// 		// 		if err != nil {
// 		// 			g.L.Info("pub msg topic invalid", zap.Error(err), zap.ByteString("topic", m.Topic))
// 		// 			continue
// 		// 		}

// 		// 		now := time.Now().Unix()
// 		// 		// generate messageID
// 		// 		id := c.bk.idgen.Generate()
// 		// 		m.ID = id.Bytes()
// 		// 		c.bk.idgen.Generate().Time()
// 		// 		// set sender user
// 		// 		m.Sender = c.username
// 		// 		// update the ttl to a unix time
// 		// 		if m.TTL != message.NeverExpires {
// 		// 			m.TTL = now + m.TTL
// 		// 		}

// 		// 		m.Timestamp = talent.String2Bytes(strconv.FormatInt(id.Time(), 10))
// 		// 		// save the messages
// 		// 		ms := []*message.PubMsg{m}
// 		// 		c.bk.store.Store(ms)

// 		// 		// push to online clients in all nodes
// 		// 		publishOnline(c.cid, c.bk, ms, false)

// 		// 		// ack to mqtt client
// 		// 		if packet.Header.QOS == 1 {
// 		// 			msg := mqtt.Puback{packet.MessageID}
// 		// 			msg.EncodeTo(c.conn)
// 		// 		}
// 		// 	case message.MSG_BROADCAST:
// 		// 		// clients publish messges to a broadcast topic
// 		// 		// broadcast will not store the messages
// 		// 		m, err := message.UnpackMsg(packet.Payload[1:])
// 		// 		if err != nil {
// 		// 			return err
// 		// 		}
// 		// 		// gen msg id
// 		// 		m.ID = c.bk.idgen.Generate().Bytes()

// 		// 		ms := []*message.PubMsg{m}
// 		// 		publishOnline(c.cid, c.bk, ms, true)

// 		// 	case message.MSG_REDUCE_COUNT:
// 		// 		topic := packet.Topic
// 		// 		count := message.UnpackReduceCount(packet.Payload[1:])
// 		// 		if count < 0 {
// 		// 			return fmt.Errorf("malice ack count, topic:%s,count:%d", string(topic), count)
// 		// 		}

// 		// 		c.bk.store.UpdateUnreadCount(topic, c.username, false, count)
// 		// 	case message.MSG_MARK_READ: // clients receive the publish message
// 		// 		topic, msgids := message.UnpackMarkRead(packet.Payload[1:])
// 		// 		if len(msgids) > 0 {
// 		// 			// ack the messages
// 		// 			c.bk.store.MarkRead(topic, msgids)
// 		// 		}

// 		// 	case message.MSG_PRESENCE_ALL:
// 		// 		topic := message.UnpackPresence(packet.Payload[1:])
// 		// 		users := c.bk.subtrie.GetPrensence(topic)
// 		// 		msg := mqtt.Publish{
// 		// 			Header: &mqtt.StaticHeader{
// 		// 				QOS: 0,
// 		// 			},
// 		// 			Topic:   topic,
// 		// 			Payload: message.PackPresenceUsers(users, message.MSG_PRESENCE_ALL),
// 		// 		}
// 		// 		msg.EncodeTo(c.conn)

// 		// 	case message.MSG_ALL_CHAT_USERS:
// 		// 		topic := message.UnpackAllChatUsers(packet.Payload[1:])
// 		// 		users := c.bk.store.GetChatUsers(topic)
// 		// 		msg := mqtt.Publish{
// 		// 			Header: &mqtt.StaticHeader{
// 		// 				QOS: 0,
// 		// 			},
// 		// 			Topic:   topic,
// 		// 			Payload: message.PackPresenceUsers(users, message.MSG_ALL_CHAT_USERS),
// 		// 		}
// 		// 		msg.EncodeTo(c.conn)

// 		// 	case message.MSG_JOIN_CHAT:
// 		// 		topic := message.UnpackJoinChat(packet.Payload[1:])
// 		// 		// validate msg
// 		// 		_, _, err := message.AppidAndTopicType(topic)
// 		// 		if err != nil {
// 		// 			g.L.Info("leave chat topic invalid", zap.Error(err), zap.ByteString("topic", topic))
// 		// 			return err
// 		// 		}

// 		// 		// check the topic is TopicTypeChat
// 		// 		tp := message.GetTopicType(topic)

// 		// 		if tp == message.TopicTypeChat {
// 		// 			c.bk.store.JoinChat(topic, c.username)
// 		// 			notifyOnline(c.bk, topic, message.PackJoinChatNotify(topic, c.username))
// 		// 		}

// 		// 	case message.MSG_LEAVE_CHAT:
// 		// 		topic := message.UnpackLeaveChat(packet.Payload[1:])
// 		// 		// validate msg
// 		// 		_, _, err := message.AppidAndTopicType(topic)
// 		// 		if err != nil {
// 		// 			g.L.Info("leave chat topic invalid", zap.Error(err), zap.ByteString("topic", topic))
// 		// 			return err
// 		// 		}

// 		// 		tp := message.GetTopicType(topic)

// 		// 		if tp == message.TopicTypeChat {
// 		// 			c.bk.store.LeaveChat(topic, c.username)
// 		// 			notifyOnline(c.bk, topic, message.PackLeaveChatNotify(topic, c.username))
// 		// 		}

// 		// 	case message.MSG_RETRIEVE:
// 		// 		topic, msgid := message.UnpackRetrieve(packet.Payload[1:])
// 		// 		err := c.bk.store.Del(topic, msgid)
// 		// 		if err == nil {
// 		// 			notifyOnline(c.bk, topic, packet.Payload)
// 		// 		}
// 		// 	}
// 		// }
// 	}
// }

// Start read buffer.
// header := make([]byte, headerSize)
// for !c.closed {
// 	// read header
// 	var bl uint64
// 	if _, err := talent.ReadFull(c.conn, header, message.MAX_IDLE_TIME); err != nil {
// 		return err
// 	}
// 	if bl, _ = binary.Uvarint(header); bl <= 0 || bl >= maxBodySize {
// 		return fmt.Errorf("packet not valid,header:%v,bl:%v", header, bl)
// 	}

// 	// read body
// 	buf := make([]byte, bl)
// 	if _, err := talent.ReadFull(c.conn, buf, message.MAX_IDLE_TIME); err != nil {
// 		return err
// 	}
// 	switch buf[0] {

// 	case message.MSG_UNSUB: // clients unsubscribe the specify topic
// 		topic, group := message.UnpackSub(buf[1:])
// 		if topic == nil {
// 			return errors.New("the unsub topic is null")
// 		}

// 		c.bk.subtrie.UnSubscribe(topic, group, c.cid, c.bk.cluster.peer.name)
// 		//@todo
// 		// aync + batch
// 		submsg := SubMessage{CLUSTER_UNSUB, topic, group, c.cid}
// 		c.bk.cluster.peer.send.GossipBroadcast(submsg)

// 		delete(c.subs, string(topic))

// 	case message.MSG_PUB_TIMER, message.MSG_PUB_RESTORE:
// 		m := message.UnpackTimerMsg(buf[1:])
// 		now := time.Now().Unix()
// 		// trigger first
// 		if m.Trigger == 0 {
// 			if m.Delay != 0 {
// 				m.Trigger = now + int64(m.Delay)
// 			}
// 		}
// 		if m.Trigger > now {
// 			c.bk.store.PutTimerMsg(m)
// 		}

// 		// ack the timer msg
// 		c.ackSender <- []message.Ack{message.Ack{m.Topic, m.ID}}
// 	}
// }

// 	return nil
// }

func (c *client) onSubscribe(topic []byte) error {
	// err := c.bk.subtrie.Subscribe(topic, c.cid, c.bk.cluster.peer.name, c.username)
	// if err != nil {
	// 	g.L.Info("sub error", zap.Uint64("conn_id", c.cid), zap.ByteString("topic", t))
	// 	return err
	// }
	// submsg := SubMessage{CLUSTER_SUB, t, c.cid, c.username}
	// c.bk.cluster.peer.send.GossipBroadcast(submsg)

	// c.subs[string(t)] = struct{}{}
	// //@todo
	// count := c.bk.store.UnreadCount(t, c.username)
	// // push out the count of the unread messages
	// msg := mqtt.Publish{
	// 	Header: &mqtt.StaticHeader{
	// 		QOS: 0,
	// 	},
	// 	Topic:   t,
	// 	Payload: message.PackMsgCount(count),
	// }
	// msg.EncodeTo(c.conn)

	// // check the topic is TopicTypeChat
	// // notify to all the user in topic ,that someone has been online
	// tp := message.GetTopicType(t)

	// if tp == message.TopicTypeChat {
	// 	notifyOnline(c.bk, t, message.PackOnlineNotify(t, c.username))
	// }

	return nil
}
func (c *client) sendLoop() {
	defer func() {
		c.closed = true
		c.conn.Close()
		if err := recover(); err != nil {
			g.L.Warn("panic happend in write loop", zap.Error(err.(error)), zap.Stack("stack"), zap.Uint64("cid", c.id))
			return
		}
	}()

	for {
		select {
		case _ = <-c.sender:
		// err := publishOne(c.conn, msgs)
		// if err != nil {
		// 	g.L.Info("push one error", zap.Error(err))
		// 	return
		// }
		case <-c.closech:
			return
		}
	}
}

func (c *client) waitForConnect() error {
	reader := bufio.NewReaderSize(c.conn, 65536)
	c.conn.SetDeadline(time.Now().Add(time.Second * message.MAX_IDLE_TIME))

	msg, err := mqtt.DecodePacket(reader)
	if err != nil {
		return err
	}

	if msg.Type() == mqtt.TypeOfConnect {
		packet := msg.(*mqtt.Connect)
		if len(packet.Username) <= 0 {
			return errors.New("no username exist")
		}
		c.username = packet.Username

		// reply the connect ack
		ack := mqtt.Connack{ReturnCode: 0x00}
		if _, err := ack.EncodeTo(c.conn); err != nil {
			return err
		}
		return nil
	}

	return errors.New("first packet is not MSG_CONNECT")
}
