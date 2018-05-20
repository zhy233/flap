package main

import (
	"flag"
	"fmt"
	"net"
	"time"

	"github.com/chaingod/talent"
	"github.com/meqio/meq/proto"
)

var topic = "/test/mp/1"
var host = "localhost:"

var op = flag.String("op", "", "")
var port = flag.String("p", "", "")
var thread = flag.Int("t", 1, "")

func main() {
	flag.Parse()
	if *op == "test" {
		test()
		return
	}

	if *port == "" {
		panic("port invalid")
	}

	conns := connect()
	switch *op {
	case "pub":
		pub(conns)
	case "pub_timer":
		pubTimer(conns[0])
	case "sub":
		sub(conns[0])
	}
}

func connect() []net.Conn {
	n := 0
	var conns []net.Conn
	for {
		if n >= *thread {
			break
		}
		conn, err := net.Dial("tcp", host+*port)
		if err != nil {
			panic(err)
		}
		msg := proto.PackConnect()
		conn.Write(msg)

		_, err = talent.ReadFull(conn, msg, 0)
		if err != nil {
			panic(err)
		}

		if msg[4] != proto.MSG_CONNECT_OK {
			panic("connect failed")
		}
		go ping(conn)

		conns = append(conns, conn)
		n++
	}

	return conns
}

func ping(conn net.Conn) {
	for {
		msg := proto.PackPing()
		conn.Write(msg)
		time.Sleep(30 * time.Second)
	}
}

type sess struct {
	a uint64
	b uint64
}

func test() {
	ts := time.Now()
	s := make([]int, 0, 400000)
	for i := 0; i < 400000; i++ {
		s = append(s, i)
	}
	fmt.Println(time.Now().Sub(ts).Nanoseconds())
}
