package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type peer struct {
	ID       string
	RaftAddr string
	HTTPAddr string
}

// parsePeers turns "n1:7001:8001,n2:7002:8002" into peers on localhost.
func parsePeers(s string) ([]peer, error) {
	var out []peer
	for _, part := range strings.Split(s, ",") {
		f := strings.Split(strings.TrimSpace(part), ":")
		if len(f) != 3 {
			return nil, fmt.Errorf("bad peer %q, want id:raftPort:httpPort", part)
		}
		out = append(out, peer{ID: f[0], RaftAddr: "127.0.0.1:" + f[1], HTTPAddr: "127.0.0.1:" + f[2]})
	}
	return out, nil
}

func main() {
	id := flag.String("id", "", "this node's id (n1, n2 or n3)")
	peersFlag := flag.String("peers", "n1:7001:8001,n2:7002:8002,n3:7003:8003", "cluster members as id:raftPort:httpPort")
	dataDir := flag.String("data", "", "data directory (default data/<id>)")
	maxInflight := flag.Int("max-inflight", 64, "max concurrent writes before returning 429 (backpressure)")
	loseAcks := flag.Float64("lose-acks", 0, "chaos: probability (0-1) of applying a write but pretending the response got lost")
	logLevel := flag.String("log", "warn", "raft log level: debug | info | warn | error")
	flag.Parse()

	peers, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatal(err)
	}
	var me *peer
	for i := range peers {
		if peers[i].ID == *id {
			me = &peers[i]
		}
	}
	if me == nil {
		log.Fatalf("-id %q not found in -peers", *id)
	}
	if *dataDir == "" {
		*dataDir = filepath.Join("data", *id)
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatal(err)
	}

	// Raft config. Timeouts are shorter than the defaults so failover is snappy to watch.
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(me.ID)
	cfg.HeartbeatTimeout = 500 * time.Millisecond
	cfg.ElectionTimeout = 500 * time.Millisecond
	cfg.LeaderLeaseTimeout = 250 * time.Millisecond
	cfg.SnapshotThreshold = 2048
	cfg.Logger = hclog.New(&hclog.LoggerOptions{Name: "raft-" + me.ID, Level: hclog.LevelFromString(*logLevel)})

	// Durable log + metadata (current term, who we voted for) in BoltDB.
	store, err := raftboltdb.NewBoltStore(filepath.Join(*dataDir, "raft.db"))
	if err != nil {
		log.Fatal(err)
	}
	snaps, err := raft.NewFileSnapshotStore(*dataDir, 2, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}
	addr, err := net.ResolveTCPAddr("tcp", me.RaftAddr)
	if err != nil {
		log.Fatal(err)
	}
	transport, err := raft.NewTCPTransport(me.RaftAddr, addr, 3, 5*time.Second, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}

	fsm := NewFSM()
	r, err := raft.NewRaft(cfg, fsm, store, store, snaps, transport)
	if err != nil {
		log.Fatal(err)
	}

	// First boot: every node bootstraps with the same static membership.
	// hashicorp/raft says this is safe when the config is identical everywhere.
	hasState, err := raft.HasExistingState(store, store, snaps)
	if err != nil {
		log.Fatal(err)
	}
	if !hasState {
		var servers []raft.Server
		for _, p := range peers {
			servers = append(servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.RaftAddr)})
		}
		if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
			log.Printf("bootstrap: %v", err)
		}
	}

	s := &Server{
		raft:     r,
		fsm:      fsm,
		self:     *me,
		peers:    peers,
		sem:      make(chan struct{}, *maxInflight),
		loseAcks: *loseAcks,
	}

	log.Printf("%s up: raft=%s http=http://%s data=%s", me.ID, me.RaftAddr, me.HTTPAddr, *dataDir)
	go s.logLeaderChanges()
	log.Fatal(http.ListenAndServe(me.HTTPAddr, s.routes()))
}
