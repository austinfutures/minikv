package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/hashicorp/raft"
)

type Server struct {
	raft     *raft.Raft
	fsm      *FSM
	self     peer
	peers    []peer
	sem      chan struct{} // bounded in-flight writes = backpressure
	loseAcks float64
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("PUT /kv/{key}", s.handlePut)
	mux.HandleFunc("DELETE /kv/{key}", s.handleDelete)
	mux.HandleFunc("POST /kv/{key}/cas", s.handleCAS)
	mux.HandleFunc("POST /kv/{key}/incr", s.handleIncr)
	mux.HandleFunc("GET /status", s.handleStatus)
	return mux
}

// leaderHTTP returns the HTTP address of whoever this node thinks is leader.
func (s *Server) leaderHTTP() string {
	_, id := s.raft.LeaderWithID()
	for _, p := range s.peers {
		if raft.ServerID(p.ID) == id {
			return p.HTTPAddr
		}
	}
	return ""
}

// notLeader tells the client to go elsewhere. 421 = "Misdirected Request".
func (s *Server) notLeader(w http.ResponseWriter) {
	writeJSON(w, http.StatusMisdirectedRequest, map[string]string{"error": "not leader", "leader": s.leaderHTTP()})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	// ?stale=true reads whatever this node has locally. Fast, works on followers,
	// but can return old data (try it on a partitioned/killed-and-restarted node).
	if r.URL.Query().Get("stale") != "true" {
		if s.raft.State() != raft.Leader {
			s.notLeader(w)
			return
		}
		// Linearizable read: confirm with a quorum that we're still the leader
		// before answering, so a deposed leader can't serve stale data.
		if err := s.raft.VerifyLeader().Error(); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
	}

	v, ok := s.fsm.Get(key)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": v})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.apply(w, r, Command{Op: "put", Key: r.PathValue("key"), Value: string(body)})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	s.apply(w, r, Command{Op: "delete", Key: r.PathValue("key")})
}

func (s *Server) handleIncr(w http.ResponseWriter, r *http.Request) {
	s.apply(w, r, Command{Op: "incr", Key: r.PathValue("key")})
}

// body: {"expected": "old" (or null / omitted = must not exist), "value": "new"}
func (s *Server) handleCAS(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Expected *string `json:"expected"`
		Value    string  `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.apply(w, r, Command{Op: "cas", Key: r.PathValue("key"), Value: req.Value, Expected: req.Expected})
}

// apply pushes a write through Raft and waits for it to be committed + applied.
func (s *Server) apply(w http.ResponseWriter, r *http.Request, c Command) {
	if s.raft.State() != raft.Leader {
		s.notLeader(w)
		return
	}

	// Backpressure: if too many writes are already in flight, refuse right away
	// instead of queueing forever. Client should back off and retry.
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	default:
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "overloaded"})
		return
	}

	c.ReqID = r.Header.Get("Idempotency-Key")
	data, _ := json.Marshal(c)

	f := s.raft.Apply(data, 2*time.Second)
	if err := f.Error(); err != nil {
		// ErrLeadershipLost is the nasty one: the write MAY have committed.
		// We can't know, so the client has to retry, and only the idempotency
		// key keeps that retry from double-applying.
		code := http.StatusServiceUnavailable
		if errors.Is(err, raft.ErrNotLeader) {
			s.notLeader(w)
			return
		}
		writeJSON(w, code, map[string]string{"error": err.Error()})
		return
	}
	res := f.Response().(Result)

	// Chaos knob: the write is committed, but we tell the client it failed.
	// This is exactly what a network blip after commit looks like.
	if s.loseAcks > 0 && rand.Float64() < s.loseAcks {
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "simulated lost ack (write actually applied)"})
		return
	}

	code := http.StatusOK
	if !res.OK && res.Error == "cas mismatch" {
		code = http.StatusConflict
	} else if !res.OK {
		code = http.StatusBadRequest
	}
	writeJSON(w, code, res)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"id":           s.self.ID,
		"state":        s.raft.State().String(),
		"leader":       s.leaderHTTP(),
		"term":         s.raft.Stats()["term"],
		"applied":      s.raft.AppliedIndex(),
		"keys":         s.fsm.Len(),
		"inflight":     len(s.sem),
		"max_inflight": cap(s.sem),
	})
}

func (s *Server) logLeaderChanges() {
	for isLeader := range s.raft.LeaderCh() {
		if isLeader {
			log.Printf("*** %s is now LEADER", s.self.ID)
		} else {
			log.Printf("--- %s lost leadership", s.self.ID)
		}
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
