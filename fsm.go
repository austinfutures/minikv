package main

import (
	"encoding/json"
	"io"
	"strconv"
	"sync"

	"github.com/hashicorp/raft"
)

// Command is what gets written into the Raft log. Every node applies the
// same commands in the same order, so every node ends up with the same map.
type Command struct {
	Op       string  `json:"op"` // put | delete | cas | incr
	Key      string  `json:"key"`
	Value    string  `json:"value,omitempty"`
	Expected *string `json:"expected,omitempty"` // for cas; nil means "key must not exist"
	ReqID    string  `json:"req_id,omitempty"`   // idempotency key from the client
}

type Result struct {
	OK      bool   `json:"ok"`
	Value   string `json:"value,omitempty"`
	Error   string `json:"error,omitempty"`
	Deduped bool   `json:"deduped,omitempty"` // true if this was a retry we'd already applied
}

// FSM is the replicated state machine: the actual key-value data, plus a
// dedupe table of request IDs we've already applied.
type FSM struct {
	mu   sync.RWMutex
	data map[string]string
	seen map[string]Result // ReqID -> result. Grows forever; fine for a toy.
}

func NewFSM() *FSM {
	return &FSM{data: map[string]string{}, seen: map[string]Result{}}
}

func (f *FSM) Apply(l *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		return Result{Error: "bad command: " + err.Error()}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Idempotency: if we've seen this request ID, return the original result
	// instead of applying it again. This is what makes retries safe.
	if c.ReqID != "" {
		if r, ok := f.seen[c.ReqID]; ok {
			r.Deduped = true
			return r
		}
	}

	var r Result
	switch c.Op {
	case "put":
		f.data[c.Key] = c.Value
		r = Result{OK: true, Value: c.Value}
	case "delete":
		delete(f.data, c.Key)
		r = Result{OK: true}
	case "cas":
		cur, exists := f.data[c.Key]
		match := (c.Expected == nil && !exists) || (c.Expected != nil && exists && cur == *c.Expected)
		if match {
			f.data[c.Key] = c.Value
			r = Result{OK: true, Value: c.Value}
		} else {
			r = Result{OK: false, Value: cur, Error: "cas mismatch"}
		}
	case "incr":
		// Deliberately NOT idempotent on its own: applying it twice adds 2.
		// That makes duplicate writes visible in the load test.
		n, _ := strconv.Atoi(f.data[c.Key])
		n++
		f.data[c.Key] = strconv.Itoa(n)
		r = Result{OK: true, Value: f.data[c.Key]}
	default:
		r = Result{Error: "unknown op " + c.Op}
	}

	if c.ReqID != "" {
		f.seen[c.ReqID] = r
	}
	return r
}

func (f *FSM) Get(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.data[key]
	return v, ok
}

func (f *FSM) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.data)
}

// --- snapshots: lets Raft compact its log instead of keeping it forever ---

type snapshotState struct {
	Data map[string]string `json:"data"`
	Seen map[string]Result `json:"seen"`
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s := snapshotState{Data: make(map[string]string, len(f.data)), Seen: make(map[string]Result, len(f.seen))}
	for k, v := range f.data {
		s.Data[k] = v
	}
	for k, v := range f.seen {
		s.Seen[k] = v
	}
	return &fsmSnapshot{s}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var s snapshotState
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return err
	}
	if s.Data == nil {
		s.Data = map[string]string{}
	}
	if s.Seen == nil {
		s.Seen = map[string]Result{}
	}
	f.mu.Lock()
	f.data, f.seen = s.Data, s.Seen
	f.mu.Unlock()
	return nil
}

type fsmSnapshot struct{ s snapshotState }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.s); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
