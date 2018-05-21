package main

import (
	"fmt"
	"sync"
	"sync/atomic"

	meq "github.com/meqio/go-meq"
	"github.com/meqio/meq/proto"
)

func pub(conns []*meq.Connection) {
	wg := &sync.WaitGroup{}
	wg.Add(len(conns))

	var pushed int64
	for i, conn := range conns {
		go func(i int, conn *meq.Connection) {
			defer wg.Done()
			n := 1
			cache := make([]*proto.PubMsg, 0, 10000)
			for {
				if n > 100000 {
					break
				}
				// 27
				m := &proto.PubMsg{
					ID:      []byte(fmt.Sprintf("%d-%010d", i, n)),
					Topic:   []byte(topic),
					Payload: []byte("123456789"),
					Type:    1,
					QoS:     1,
				}
				if len(cache) < 500 {
					cache = append(cache, m)
				} else {
					cache = append(cache, m)
					conn.Publish(cache)
					atomic.AddInt64(&pushed, int64(len(cache)))
					cache = cache[:0]
				}
				n++
			}
		}(i, conn)
		wg.Wait()

		fmt.Println("共计推送消息：", pushed)
	}
}

// func pubTimer(conn net.Conn) {

// 	m := proto.TimerMsg{
// 		ID:      []byte(fmt.Sprintf("%010d", 1)),
// 		Topic:   []byte(topic),
// 		Payload: []byte("1234567891234567"),
// 		Trigger: time.Now().Add(60 * time.Second).Unix(),
// 		Delay:   30,
// 	}
// 	msg := proto.PackTimerMsg(&m, proto.MSG_PUB_RESTORE)
// 	_, err := conn.Write(msg)
// 	if err != nil {
// 		panic(err)
// 	}

// }
