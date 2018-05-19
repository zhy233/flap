package service

import (
	"strconv"

	"github.com/labstack/echo"
	"github.com/meqio/proto"
)

type admin struct {
	bk *Broker
}

func (ad *admin) Init(bk *Broker) {
	ad.bk = bk

	e := echo.New()
	e.POST("/admin/create/topic", ad.createTopic)
	e.Logger.Fatal(e.Start(Conf.Admin.Addr))
}

func (ad *admin) createTopic(c echo.Context) error {
	topic := c.FormValue("topic")
	if topic == "" {
		return c.String(200, "topic cant be empty")
	}
	isPush, err := strconv.ParseBool(c.FormValue("ispush"))
	if err != nil {
		return c.String(200, err.Error())
	}

	fromOldest, err := strconv.ParseBool(c.FormValue("from_oldest"))
	if err != nil {
		return c.String(200, err.Error())
	}

	ackStrategy, err := strconv.Atoi(c.FormValue("ack_strategy"))
	if err != nil {
		return c.String(200, err.Error())
	}
	if (ackStrategy != proto.TopicPropAckSet) && (ackStrategy != proto.TopicPropAckDel) && (ackStrategy != proto.TopicPropAckIgnore) {
		return c.String(200, "ack strategy invalid")
	}

	getMsgStrategy, err := strconv.Atoi(c.FormValue("get_msg_strategy"))
	if err != nil {
		return c.String(200, err.Error())
	}

	if (getMsgStrategy != proto.TopicPropGetFilterAck) && (getMsgStrategy != proto.TopicPropGetAll) {
		return c.String(200, "ack strategy invalid")
	}

	_, err = parseTopic([]byte(topic), true)
	if err != nil {
		return c.String(200, "topic invalid")
	}

	err = ad.bk.store.SetTopicProp([]byte(topic), proto.TopicProp{isPush, fromOldest, int8(ackStrategy), int8(getMsgStrategy)})
	if err != nil {
		return c.String(200, err.Error())
	}

	return c.String(200, "topic created ok")
}
