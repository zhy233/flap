package service

import (
	"sync"
	"time"
)

const (
	// ACCEPT_MIN_SLEEP is the minimum acceptable sleep times on temporary errors.
	ACCEPT_MIN_SLEEP = 10 * time.Millisecond
	// ACCEPT_MAX_SLEEP is the maximum acceptable sleep times on temporary errors
	ACCEPT_MAX_SLEEP = 1 * time.Second

	MAX_IDLE_TIME = 60

	WRITE_DEADLINE = 2 * time.Second

	MAX_MESSAGE_BATCH      = 200
	MAX_MESSAGE_PULL_COUNT = 200
)

var (
	CLUSTER_SUB          = 1
	CLUSTER_UNSUB        = 2
	CLUSTER_RUNNING_TIME = 3
)

var glock = &sync.RWMutex{}
