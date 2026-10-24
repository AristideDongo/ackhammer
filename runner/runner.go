package runner

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/AristideDongo/ackhammer"
	"github.com/AristideDongo/ackhammer/invariant"
	"github.com/AristideDongo/ackhammer/ledger"
	"github.com/AristideDongo/ackhammer/probe"
	"github.com/AristideDongo/ackhammer/report"
)

// Run executes scenario.
func Run(t *testing.T, target ackhammer.Target, sc ackhammer.Scenario) {
	l := ledger.New()
	p := probe.New(l)
	rnd := rand.New(rand.NewSource(sc.Seed))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Publish messages
	for i := 0; i < sc.Messages; i++ {
		msg := ackhammer.Message{
			ID:          fmt.Sprintf("msg-%d", i+1),
			Key:         keyFor(i, sc.Keys),
			Traceparent: fmt.Sprintf("00-%016x-%016x-01", rnd.Uint64(), rnd.Uint64()),
			Payload:     []byte(fmt.Sprintf("payload-%d", i+1)),
			Attributes:  map[string]string{},
		}
		l.RecordPublished(msg)
		if err := target.Publish(ctx, msg); err != nil {
			t.Fatalf("publish failed: %v", err)
		}
		_ = rnd
	}

	// Start consumer
	handler := func(ctx context.Context, msg ackhammer.Message) error {
		return nil
	}

	go func() {
		_ = target.Consume(ctx, p.Wrap(handler))
	}()

	time.Sleep(2 * time.Second)

	// Drain
	time.Sleep(3 * time.Second)

	// Check invariants
	results := invariant.Check(l, sc.Expect)
	rep := &report.Report{
		Name:    sc.Name,
		Seed:    sc.Seed,
		Target:  target.Name(),
		Results: results,
		Events:  l.Events(),
		Replay:  fmt.Sprintf("ackhammer replay --seed %d --only %s", sc.Seed, sc.Name),
	}

	out := rep.PrintText()
	t.Log(out)

	for _, res := range results {
		if !res.Pass && res.Message != "SKIP" {
			t.Errorf("invariant %s failed: %s", res.Name, res.Message)
		}
	}

	_ = cancel
}

func keyFor(i int, keys []string) string {
	if len(keys) == 0 {
		return fmt.Sprintf("key-%d", i%4)
	}
	return keys[i%len(keys)]
}
