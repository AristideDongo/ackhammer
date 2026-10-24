// Package memtarget is an in-memory broker and consumer implementing every
// Ackhammer target interface. It needs no Docker and serves three purposes:
// trying Ackhammer in a few seconds, showing how to write a Target, and
// testing Ackhammer itself.
//
// Its default behaviour is a correct at-least-once broker: a failed delivery
// is nacked and redelivered at once, a delivery that outlives AckDeadline is
// redelivered, and a message that fails PoisonAfter times goes to the poison
// queue. Bugs reproduces classic consumer bugs on purpose, to see what
// Ackhammer reports for them.
package memtarget

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/AristideDongo/ackhammer/core"
)

// Config configures a Target.
type Config struct {
	// AckDeadline is how long a delivery may last before the message is
	// delivered again. 0 means no deadline.
	AckDeadline time.Duration
	// Concurrency is the number of messages handled in parallel (default 1).
	Concurrency int
	// PoisonAfter routes a message to the poison queue after that many
	// failures. 0 means no poison queue: failed messages are retried forever.
	PoisonAfter int
	// Bugs turns on known bug classes.
	Bugs Bugs
}

// Bugs reproduces bug classes seen in real consumers.
type Bugs struct {
	// DropBatchOnDeadline: when a delivery outlives AckDeadline, every
	// message still waiting in the queue is dropped (silent loss, like
	// watermill #700 where one slow message loses the rest of the batch).
	DropBatchOnDeadline bool
	// AbandonOnCancel: when Consume is cancelled, the messages already
	// pulled from the broker but not yet handed to the handler are dropped
	// without a nack, so they come back only after AckDeadline (like
	// cloudevents/sdk-go #1234). Here, every queued message counts as pulled.
	AbandonOnCancel bool
	// RedeliverAckedOnRestart: Restart delivers already acked messages
	// again (like watermill #671).
	RedeliverAckedOnRestart bool
}

// Target is an in-memory broker plus consumer.
type Target struct {
	cfg Config

	mu       sync.Mutex
	queue    []*entry          // ready for delivery
	inflight map[string]*entry // delivered, waiting for ack/nack
	acked    []*entry
	poisoned []string
	token    uint64
	stop     chan struct{} // closed by StopConsuming
	closed   bool
}

type entry struct {
	msg      core.Message
	failures int
	token    uint64
	deadline time.Time
}

var (
	_ core.Restartable     = (*Target)(nil)
	_ core.ConsumerStopper = (*Target)(nil)
	_ core.PoisonInspector = (*Target)(nil)
)

// ErrClosed is returned after Close.
var ErrClosed = errors.New("memtarget: closed")

// New returns an empty in-memory target.
func New(cfg Config) *Target {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	return &Target{cfg: cfg, inflight: map[string]*entry{}}
}

func (t *Target) Name() string { return "memtarget" }

func (t *Target) Publish(_ context.Context, msg core.Message) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}
	t.queue = append(t.queue, &entry{msg: msg})
	return nil
}

func (t *Target) Consume(ctx context.Context, h core.Handler) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ErrClosed
	}
	stop := make(chan struct{})
	t.stop = stop
	t.mu.Unlock()

	var wg sync.WaitGroup
	for range t.cfg.Concurrency {
		wg.Go(func() { t.work(ctx, stop, h) })
	}
	wg.Wait()
	if ctx.Err() != nil && t.cfg.Bugs.AbandonOnCancel {
		t.abandonQueued()
	}
	return ctx.Err()
}

// abandonQueued moves queued messages in flight without delivering them, so
// they wait for the ack deadline (forever without one).
func (t *Target) abandonQueued() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, e := range t.queue {
		t.token++
		e.token = t.token
		e.deadline = time.Time{}
		if t.cfg.AckDeadline > 0 {
			e.deadline = time.Now().Add(t.cfg.AckDeadline)
		}
		t.inflight[e.msg.ID] = e
	}
	t.queue = nil
}

func (t *Target) work(ctx context.Context, stop <-chan struct{}, h core.Handler) {
	idle := time.NewTicker(time.Millisecond)
	defer idle.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		default:
		}
		e, token := t.next()
		if e == nil {
			select {
			case <-ctx.Done():
			case <-stop:
			case <-idle.C:
			}
			continue
		}
		t.settle(e, token, h(ctx, e.msg))
	}
}

// next pops the next deliverable message, first moving expired deliveries
// back to the queue.
func (t *Target) next() (*entry, uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	for id, e := range t.inflight {
		if e.deadline.IsZero() || now.Before(e.deadline) {
			continue
		}
		delete(t.inflight, id)
		t.queue = append(t.queue, e)
	}
	if len(t.queue) == 0 {
		return nil, 0
	}
	e := t.queue[0]
	t.queue = t.queue[1:]
	t.token++
	e.token = t.token
	if t.cfg.AckDeadline > 0 {
		e.deadline = now.Add(t.cfg.AckDeadline)
	}
	t.inflight[e.msg.ID] = e
	return e, e.token
}

// settle acks or nacks a delivery. A late answer, for a delivery that was
// already handed out again, is ignored, as real brokers do.
func (t *Target) settle(e *entry, token uint64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, ok := t.inflight[e.msg.ID]; !ok || cur.token != token {
		return
	}
	delete(t.inflight, e.msg.ID)
	if !e.deadline.IsZero() && time.Now().After(e.deadline) {
		// Too late: the broker already considers the delivery lost.
		if t.cfg.Bugs.DropBatchOnDeadline {
			t.acked = append(t.acked, e)
			t.queue = nil
			return
		}
		t.queue = append(t.queue, e)
		return
	}
	if err == nil {
		t.acked = append(t.acked, e)
		return
	}
	e.failures++
	if t.cfg.PoisonAfter > 0 && e.failures >= t.cfg.PoisonAfter {
		t.poisoned = append(t.poisoned, e.msg.ID)
		return
	}
	t.queue = append(t.queue, e)
}

// StopConsuming makes the running Consume return once in-flight deliveries
// are settled.
func (t *Target) StopConsuming(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stop != nil {
		close(t.stop)
		t.stop = nil
	}
	return nil
}

// Restart recreates the consumer. With Bugs.RedeliverAckedOnRestart, acked
// messages are queued again.
func (t *Target) Restart(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cfg.Bugs.RedeliverAckedOnRestart {
		t.queue = append(t.queue, t.acked...)
		t.acked = nil
	}
	return nil
}

// PoisonedIDs lists the messages in the poison queue.
func (t *Target) PoisonedIDs(context.Context) ([]string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.poisoned...), nil
}

func (t *Target) Close(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}
