package broker

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/mafanr/g"
	"github.com/mafanr/meq/broker/config"
	"github.com/mafanr/meq/internal/network/listener"
	"github.com/mafanr/meq/internal/network/websocket"

	"go.uber.org/zap"

	"github.com/kelindar/tcp"
)

type Broker struct {
	context context.Context
	cancel  context.CancelFunc

	http *http.Server
	tcp  *tcp.Server

	wg          *sync.WaitGroup
	running     bool
	runningTime time.Time
	listener    net.Listener

	clients map[uint64]*client

	sync.RWMutex
}

func New(path string) *Broker {
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		context: ctx,
		cancel:  cancel,
		wg:      &sync.WaitGroup{},
		clients: make(map[uint64]*client),
	}
	// init base config
	config.Init(path)

	mux := http.NewServeMux()
	// mux.HandleFunc("/keygen", s.onHTTPKeyGen)
	mux.HandleFunc("/", b.onRequest)
	b.http.Handler = mux

	g.InitLogger(config.Conf.Common.LogLevel)
	g.L.Info("base configuration loaded")

	return b
}

func (b *Broker) Start() {
	b.running = true
	b.runningTime = time.Now()

	go b.listen()

	// init store
	switch config.Conf.Storage.Provider {
	case "memory":

	case "fdb":

	}
}

func (b *Broker) Shutdown() {
	b.running = false
	b.listener.Close()

	for _, c := range b.clients {
		c.conn.Close()
	}

	g.L.Sync()
	b.wg.Wait()
}

var uid uint64

func (b *Broker) process(conn net.Conn, id uint64) {
	defer func() {
		b.Lock()
		delete(b.clients, id)
		b.Unlock()
		conn.Close()
		g.L.Info("client closed", zap.Uint64("conn_id", id))
	}()

	c := newClient(id, conn, b)

	b.Lock()
	b.clients[id] = c
	b.Unlock()

	c.start()
}

func (b *Broker) onRequest(w http.ResponseWriter, r *http.Request) {
	if conn, ok := websocket.TryUpgrade(w, r); ok {
		atomic.AddUint64(&uid, 1)
		go b.process(conn, uid)
	}
}

// Listen starts the service.
func (b *Broker) listen() {
	l, err := listener.New(config.Conf.Broker.Listen, nil)
	if err != nil {
		g.L.Fatal("listen error", zap.Error(err))
	}

	// Set the read timeout on our mux listener
	l.SetReadTimeout(120 * time.Second)

	// Configure the matchers
	l.ServeAsync(listener.MatchHTTP(), b.http.Serve)
	l.ServeAsync(listener.MatchAny(), b.tcp.Serve)
	l.Serve()
}
