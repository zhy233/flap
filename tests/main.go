package main

import (
	"flag"
	"fmt"
	"time"

	meq "flap_zhy/sdks/go-meq"
)

var topic = "/1234567890/22/chat/001"
var host = "localhost:"

var op = flag.String("op", "", "")
var port = flag.String("p", "", "")
var thread = flag.Int("t", 1, "")
var user = flag.String("u", "", "")

func main() {
	flag.Parse()
	if *op == "test" {
		test()
		return
	}

	if *port == "" {
		panic("port invalid")
	}

	if *user == "" {
		panic("user invalid")
	}

	conns := connect(*user)
	switch *op {
	case "pub":
		pub(conns)
	case "pub_timer":
		// pubTimer(conns[0])
	case "sub":
		sub(conns[0])
	}
}

func connect(user string) []*meq.Connection {
	n := 0
	var conns []*meq.Connection
	for {
		if n >= *thread {
			break
		}

		conf := &meq.ConfigOption{
			Username: user,
			Hosts:    []string{host + *port},
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

type sess struct {
	a int
	b int
}

func test() {
	ts := time.Now()
	// 111微秒
	// var s []sess
	// for i := 0; i < 2000; i++ {
	// 	s = append(s, sess{i, i})
	// }

	s := make(map[sess]struct{})
	for i := 0; i < 2000; i++ {
		s[sess{i, i}] = struct{}{}
	}
	fmt.Println(time.Now().Sub(ts).Nanoseconds() / 1e3)
}
