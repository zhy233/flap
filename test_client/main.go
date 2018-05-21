package main

import (
	"flag"
	"fmt"
	"net"
	"time"

	meq "github.com/meqio/go-meq"
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
		// pubTimer(conns[0])
	case "sub":
		sub(conns[0])
	}
}

func connect() []*meq.Connection {
	n := 0
	var conns []*meq.Connection
	for {
		if n >= *thread {
			break
		}

		conf := &meq.ConfigOption{
			Hosts: []string{host + *port},
		}
		conn, err := meq.Connect(conf)
		if err != nil {
			panic(err)
		}
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
