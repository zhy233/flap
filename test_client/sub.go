package main

import (
	"fmt"

	meq "github.com/meqio/go-meq"
	"github.com/meqio/meq/proto"
)

func sub(conn *meq.Connection) {
	err := conn.Subscribe([]byte(topic), []byte("robot1"), func(m *proto.PubMsg) {
		if m.ID[len(m.ID)-1] == 48 && m.ID[len(m.ID)-2] == 48 && m.ID[len(m.ID)-3] == 48 && m.ID[len(m.ID)-4] == 48 {
			fmt.Println("收到消息：", string(m.ID), m.QoS, m.Acked)
		}
	})

	if err != nil {
		panic(err)
	}
	select {}

	// fmt.Println("累积消费未ACK消息数：", n1)
}
