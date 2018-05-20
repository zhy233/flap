package service

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/weaveworks/mesh"
	"go.uber.org/zap"
)

type cluster struct {
	bk     *Broker
	closed chan struct{}
	peer   *peer
}

func (c *cluster) Init() {
	c.closed = make(chan struct{})
	peers := stringset{}
	hwaddr := Conf.Cluster.HwAddr
	meshListen := net.JoinHostPort("0.0.0.0", Conf.Cluster.Port)
	channel := "default"
	nickname := mustHostname()

	for _, peer := range Conf.Cluster.SeedPeers {
		peers[peer] = struct{}{}
	}

	host, portStr, err := net.SplitHostPort(meshListen)
	if err != nil {
		L.Fatal("cluster address invalid", zap.Error(err), zap.String("listen_addr", meshListen))
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		L.Fatal("cluter port invalid", zap.Error(err), zap.String("listen_port", portStr))
	}

	name, err := mesh.PeerNameFromString(hwaddr)
	if err != nil {
		L.Fatal("hardware addr invalid", zap.Error(err), zap.String("hardware_addr", hwaddr))
	}

	router, err := mesh.NewRouter(mesh.Config{
		Host:               host,
		Port:               port,
		ProtocolMinVersion: mesh.ProtocolMinVersion,
		ConnLimit:          64,
		PeerDiscovery:      true,
		TrustedSubnets:     []*net.IPNet{},
	}, name, nickname, mesh.NullOverlay{}, log.New(ioutil.Discard, "", 0))

	if err != nil {
		L.Fatal("Could not create cluster", zap.Error(err))
	}

	peer := newPeer(name, c.bk)
	gossip, err := router.NewGossip(channel, peer)
	if err != nil {
		L.Fatal("Could not create cluster gossip", zap.Error(err))
	}
	peer.register(gossip)
	c.peer = peer

	ms, ok := c.bk.store.(*MemStore)
	if ok {
		fmt.Println("init mem cluster-channel")
		ms.pn = name
		g, err := router.NewGossip("mem-store", ms)
		if err != nil {
			L.Fatal("Could not create cluster gossip", zap.Error(err))
		}
		ms.register(g)
	}

	go func() {
		L.Debug("cluster starting", zap.String("listen_addr", meshListen))
		router.Start()
	}()
	defer func() {
		L.Debug("cluster stopping", zap.String("listen_addr", meshListen))
		router.Stop()
	}()

	router.ConnectionMaker.InitiateConnections(peers.slice(), true)

	// loop to get the running time of other nodes
	go func() {
		submsg := SubMessage{CLUSTER_RUNNING_TIME, nil, nil, 0}

		syncmsg := make([]byte, 5)
		syncmsg[4] = ROUTER_SUBS_SYNC_REQ
		c.bk.cluster.peer.longestRunningTime = uint64(c.bk.runningTime.Unix())
		n := 0
		for {
			if n > 3 {
				break
			}
			time.Sleep(5 * time.Second)
			if c.bk.subSynced {
				break
			}
			c.bk.cluster.peer.send.GossipBroadcast(submsg)
			time.Sleep(3 * time.Second)
			// sync the subs from the longest running node
			if c.bk.cluster.peer.longestRunningTime < uint64(c.bk.runningTime.Unix()) {
				c.bk.cluster.peer.send.GossipUnicast(c.bk.cluster.peer.longestRunningName, syncmsg)
				continue
			}
			// 没有节点比本地节点运行时间更久，为了以防万一，我们做4次循环
			n++
		}
	}()

	select {
	case <-c.closed:

	}
}

func (c *cluster) Close() {
	c.closed <- struct{}{}
}

// Peer encapsulates state and implements mesh.Gossiper.
// It should be passed to mesh.Router.NewGossip,
// and the resulting Gossip registered in turn,
// before calling mesh.Router.Start.
type peer struct {
	name mesh.PeerName
	bk   *Broker
	send mesh.Gossip

	longestRunningName mesh.PeerName
	longestRunningTime uint64
}

// peer implements mesh.Gossiper.
var _ mesh.Gossiper = &peer{}

// Construct a peer with empty state.
// Be sure to register a channel, later,
// so we can make outbound communication.
func newPeer(pn mesh.PeerName, b *Broker) *peer {
	p := &peer{
		name: pn,
		bk:   b,
		send: nil, // must .register() later
	}
	return p
}

// register the result of a mesh.Router.NewGossip.
func (p *peer) register(send mesh.Gossip) {
	p.send = send
}

func (p *peer) stop() {

}

// Return a copy of our complete state.
func (p *peer) Gossip() (complete mesh.GossipData) {
	return p.bk.subtrie
}

// Merge the gossiped data represented by buf into our state.
// Return the state information that was modified.
func (p *peer) OnGossip(buf []byte) (delta mesh.GossipData, err error) {
	return
}

// Merge the gossiped data represented by buf into our state.
// Return the state information that was modified.
func (p *peer) OnGossipBroadcast(src mesh.PeerName, buf []byte) (received mesh.GossipData, err error) {
	var msg SubMessage
	err = gob.NewDecoder(bytes.NewReader(buf)).Decode(&msg)
	if err != nil {
		L.Info("on gossip broadcast decode error", zap.Error(err))
		return
	}

	switch msg.TP {
	case CLUSTER_SUB:
		fmt.Println("recv sub:", string(msg.Topic), string(msg.Group), msg.Cid)
		p.bk.subtrie.Subscribe(msg.Topic, msg.Group, msg.Cid, src)
	case CLUSTER_UNSUB:
		fmt.Println("recv unsub:", string(msg.Topic), string(msg.Group), msg.Cid)
		p.bk.subtrie.UnSubscribe(msg.Topic, msg.Group, msg.Cid, src)
	case CLUSTER_RUNNING_TIME:
		t := make([]byte, 13)
		t[4] = ROUTER_RUNNING_TIME
		binary.PutUvarint(t[5:], uint64(p.bk.runningTime.Unix()))
		p.send.GossipUnicast(src, t)
	}

	return
}

// Merge the gossiped data represented by buf into our state.
func (p *peer) OnGossipUnicast(src mesh.PeerName, buf []byte) error {
	if buf[4] == ROUTER_RUNNING_TIME {
		t, _ := binary.Uvarint(buf[5:])
		if t < p.longestRunningTime {
			p.longestRunningName = src
			p.longestRunningTime = t
		}
		return nil
	}

	if buf[4] == ROUTER_SUBS_SYNC_REQ {
		b := p.bk.subtrie.Encode()[0]
		p.send.GossipUnicast(src, b)
		return nil
	}

	if buf[4] == ROUTER_SUBS_SYNC {
		set := NewSubTrie()
		err := gob.NewDecoder(bytes.NewReader(buf[5:])).Decode(&set)
		if err != nil {
			L.Info("on gossip decode error", zap.Error(err))
			return err
		}
		p.bk.subtrie = set

		fmt.Printf("recv subs sync %v from %v,that node starts running at %v\n", p.bk.subtrie, src, time.Unix(int64(p.longestRunningTime), 0))
		p.bk.subSynced = true
		return nil
	}
	p.bk.router.recvRoute(src, buf)
	return nil
}

type stringset map[string]struct{}

func (ss stringset) Set(value string) error {
	ss[value] = struct{}{}
	return nil
}

func (ss stringset) String() string {
	return strings.Join(ss.slice(), ",")
}

func (ss stringset) slice() []string {
	slice := make([]string, 0, len(ss))
	for k := range ss {
		slice = append(slice, k)
	}
	sort.Strings(slice)
	return slice
}

func mustHardwareAddr() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		panic(err)
	}
	for _, iface := range ifaces {
		if s := iface.HardwareAddr.String(); s != "" {
			return s
		}
	}
	panic("no valid network interfaces")
}

func mustHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hostname
}
