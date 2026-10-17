// loadtest hammers the cluster with INCR operations, then checks the final
// counter. INCR is not idempotent by itself, so if any retry gets applied
// twice the counter ends up higher than the number of acknowledged ops.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type client struct {
	nodes   []string
	leader  atomic.Value // string: last known leader address
	http    *http.Client
	retries atomic.Int64
	r429    atomic.Int64
	opTime  time.Duration // give up on a single op after this long
}

func (c *client) target(attempt int) string {
	if l, _ := c.leader.Load().(string); l != "" {
		return l
	}
	// No known leader: pick a random node instead of always trying n1 first.
	return c.nodes[rand.Intn(len(c.nodes))]
}

// do sends one logical request, retrying with exponential backoff + jitter
// until it gets a definitive answer or runs out of time. The SAME idempotency
// key is sent on every attempt; that's the whole trick.
func (c *client) do(method, path, idemKey string) (int, []byte, error) {
	deadline := time.Now().Add(c.opTime)
	backoff := 25 * time.Millisecond
	var lastErr error

	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			c.retries.Add(1)
		}
		node := c.target(attempt)
		req, _ := http.NewRequest(method, "http://"+node+path, nil)
		if idemKey != "" {
			req.Header.Set("Idempotency-Key", idemKey)
		}

		resp, err := c.http.Do(req)
		retryNow := false
		if err != nil {
			lastErr = err
			// Only forget the leader if it's the node that just failed. Clearing it
			// unconditionally let one worker's failure against a dead node wipe a
			// good leader hint for every worker (see README: the bug I found).
			c.leader.CompareAndSwap(node, "")
		} else {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			switch resp.StatusCode {
			case 200, 404, 409:
				c.leader.Store(node)
				return resp.StatusCode, body, nil
			case 421: // not leader: follow the hint
				var hint struct{ Leader string }
				json.Unmarshal(body, &hint)
				c.leader.Store(hint.Leader)
				retryNow = hint.Leader != ""
				lastErr = fmt.Errorf("not leader")
			case 429:
				c.r429.Add(1)
				lastErr = fmt.Errorf("429 overloaded")
			case 503, 504:
				lastErr = fmt.Errorf("%d %s", resp.StatusCode, strings.TrimSpace(string(body)))
			default:
				return resp.StatusCode, body, fmt.Errorf("non-retryable %d: %s", resp.StatusCode, body)
			}
		}

		if time.Now().After(deadline) {
			return 0, nil, fmt.Errorf("gave up after %d attempts: %v", attempt+1, lastErr)
		}
		if !retryNow {
			// full jitter: sleep a random amount up to the current backoff
			time.Sleep(time.Duration(rand.Int63n(int64(backoff))))
			backoff = min(backoff*2, time.Second)
		}
	}
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

func main() {
	nodes := flag.String("nodes", "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003", "node HTTP addresses")
	workers := flag.Int("workers", 16, "concurrent clients")
	ops := flag.Int("ops", 5000, "total INCR operations")
	noIdem := flag.Bool("no-idem", false, "don't send idempotency keys (watch duplicates appear)")
	opTimeout := flag.Duration("op-timeout", 20*time.Second, "give up on a single op after this long")
	flag.Parse()

	c := &client{
		nodes:  strings.Split(*nodes, ","),
		http:   &http.Client{Timeout: 2 * time.Second},
		opTime: *opTimeout,
	}
	c.leader.Store("")

	runID := fmt.Sprintf("run%d", time.Now().UnixNano())
	key := "counter-" + runID
	fmt.Printf("key=%s workers=%d ops=%d idempotency=%v\n", key, *workers, *ops, !*noIdem)
	fmt.Println("tip: Ctrl+C the leader's terminal while this runs, then restart it")

	var (
		next    atomic.Int64
		acked   atomic.Int64
		failed  atomic.Int64
		mu      sync.Mutex
		lats    []time.Duration
		wg      sync.WaitGroup
		done    = make(chan struct{})
		started = time.Now()
	)

	// progress ticker: you'll see throughput drop to 0 during failover, then recover
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		var lastAcked, lastRetries int64
		for {
			select {
			case <-done:
				return
			case <-t.C:
				a, r := acked.Load(), c.retries.Load()
				l, _ := c.leader.Load().(string)
				if l == "" {
					l = "?"
				}
				fmt.Printf("  t=%3.0fs  acked=%6d (+%5d/s)  retries=+%-4d  failed=%d  leader=%s\n",
					time.Since(started).Seconds(), a, a-lastAcked, r-lastRetries, failed.Load(), l)
				lastAcked, lastRetries = a, r
			}
		}
	}()

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var mine []time.Duration
			for {
				i := next.Add(1)
				if i > int64(*ops) {
					break
				}
				idem := ""
				if !*noIdem {
					idem = fmt.Sprintf("%s-%d", runID, i)
				}
				t0 := time.Now()
				_, _, err := c.do("POST", "/kv/"+key+"/incr", idem)
				if err != nil {
					failed.Add(1)
					fmt.Fprintf(os.Stderr, "  op %d failed: %v\n", i, err)
					continue
				}
				acked.Add(1)
				mine = append(mine, time.Since(t0))
			}
			mu.Lock()
			lats = append(lats, mine...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(done)
	elapsed := time.Since(started)

	// Final linearizable read of the counter.
	code, body, err := c.do("GET", "/kv/"+key, "")
	var final int64
	if err == nil && code == 200 {
		var v struct{ Value string }
		json.Unmarshal(body, &v)
		fmt.Sscan(v.Value, &final)
	} else if err != nil {
		fmt.Println("final read failed:", err)
		os.Exit(1)
	}

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	a, f := acked.Load(), failed.Load()

	fmt.Println()
	fmt.Println("=== results ===")
	fmt.Printf("duration      %v (%.0f ops/s)\n", elapsed.Round(time.Millisecond), float64(a)/elapsed.Seconds())
	fmt.Printf("acked         %d\n", a)
	fmt.Printf("failed        %d (gave up; may or may not have applied)\n", f)
	fmt.Printf("retries       %d  (of which 429 backpressure: %d)\n", c.retries.Load(), c.r429.Load())
	fmt.Printf("latency       p50=%v  p99=%v  p99.9=%v  max=%v\n",
		pct(lats, .50).Round(time.Microsecond), pct(lats, .99).Round(time.Microsecond),
		pct(lats, .999).Round(time.Microsecond), pct(lats, 1).Round(time.Microsecond))
	fmt.Printf("counter       %d\n", final)
	fmt.Printf("expected      %d..%d\n", a, a+f)

	switch {
	case final > a+f:
		fmt.Printf("VERDICT       DUPLICATES: %d writes applied more than once\n", final-(a+f))
	case final < a:
		fmt.Printf("VERDICT       LOST WRITES: %d acked writes are missing\n", a-final)
	default:
		fmt.Println("VERDICT       OK: every acked write applied exactly once")
	}
}
