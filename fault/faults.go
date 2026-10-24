package fault

import (
	"context"
	"math/rand"
	"time"

	"github.com/AristideDongo/ackhammer/core"
)

// Selector picks messages.
type Selector interface {
	Select(msg core.Message, idx int, r *rand.Rand) bool
}

type onMessage struct{ n int }
func (o onMessage) Select(_ core.Message, idx int, _ *rand.Rand) bool { return idx == o.n-1 || idx == o.n }

type onKey struct{ k string }
func (o onKey) Select(m core.Message, _ int, _ *rand.Rand) bool { return m.Key == o.k }

type everyNth struct{ n int }
func (e everyNth) Select(_ core.Message, idx int, _ *rand.Rand) bool { return (idx+1)%e.n == 0 }

type randomSel struct{ p float64 }
func (r randomSel) Select(_ core.Message, _ int, rnd *rand.Rand) bool { return rnd.Float64() < r.p }

// OnMessage selects message number.
func OnMessage(n int) Selector { return onMessage{n} }

// OnKey selects by key.
func OnKey(k string) Selector { return onKey{k} }

// EveryNth selects every nth.
func EveryNth(n int) Selector { return everyNth{n} }

// Random selects with probability.
func Random(p float64) Selector { return randomSel{p} }

// SlowHandler delays handler.
type SlowHandler struct {
	sel Selector
	d   time.Duration
}

func (s *SlowHandler) Name() string { return "slow_handler" }
func (s *SlowHandler) When(r *core.RandShim) time.Duration { return 0 }
func (s *SlowHandler) Apply(ctx *core.FaultContext) error { time.Sleep(s.d); return nil }

// SlowHandler creates fault.
func SlowHandler(sel Selector, d time.Duration) *SlowHandler { return &SlowHandler{sel: sel, d: d} }

// HandlerError returns error n times.
type HandlerError struct {
	sel Selector
	n   int
}

func (h *HandlerError) Name() string { return "handler_error" }
func (h *HandlerError) When(r *core.RandShim) time.Duration { return 0 }
func (h *HandlerError) Apply(ctx *core.FaultContext) error { return context.DeadlineExceeded }

// HandlerError creates fault.
func HandlerError(sel Selector, n int) *HandlerError { return &HandlerError{sel: sel, n: n} }

// HandlerPanic panics.
type HandlerPanic struct{ sel Selector }
func (h *HandlerPanic) Name() string { return "handler_panic" }
func (h *HandlerPanic) When(r *core.RandShim) time.Duration { return 0 }
func (h *HandlerPanic) Apply(ctx *core.FaultContext) error { panic("ackhammer: handler panic") }
func HandlerPanic(sel Selector) *HandlerPanic { return &HandlerPanic{sel: sel} }

// CancelDuringReceive cancels context during receive.
type CancelDuringReceive struct{ at time.Duration }
func (c *CancelDuringReceive) Name() string { return "cancel_during_receive" }
func (c *CancelDuringReceive) When(r *core.RandShim) time.Duration { return c.at }
func (c *CancelDuringReceive) Apply(ctx *core.FaultContext) error { return nil }
func CancelDuringReceive(at time.Duration) *CancelDuringReceive { return &CancelDuringReceive{at: at} }

// GracefulStop calls Close.
type GracefulStop struct{ at time.Duration }
func (g *GracefulStop) Name() string { return "graceful_stop" }
func (g *GracefulStop) When(r *core.RandShim) time.Duration { return g.at }
func (g *GracefulStop) Apply(ctx *core.FaultContext) error {
	if ctx.Target != nil {
		return ctx.Target.Close(context.Background())
	}
	return nil
}
func GracefulStop(at time.Duration) *GracefulStop { return &GracefulStop{at: at} }

// RestartConsumer restarts consumer.
type RestartConsumer struct{ at time.Duration }
func (r *RestartConsumer) Name() string { return "restart_consumer" }
func (r *RestartConsumer) When(sh *core.RandShim) time.Duration { return r.at }
func (r *RestartConsumer) Apply(ctx *core.FaultContext) error {
	if ctx.Restartable != nil {
		return ctx.Restartable.Restart(context.Background())
	}
	return nil
}
func RestartConsumer(at time.Duration) *RestartConsumer { return &RestartConsumer{at: at} }

// NetworkCut cuts network via toxiproxy.
type NetworkCut struct {
	at time.Duration
	d  time.Duration
}
func (n *NetworkCut) Name() string { return "network_cut" }
func (n *NetworkCut) When(r *core.RandShim) time.Duration { return n.at }
func (n *NetworkCut) Apply(ctx *core.FaultContext) error { time.Sleep(n.d); return nil }
func NetworkCut(at, d time.Duration) *NetworkCut { return &NetworkCut{at: at, d: d} }

// NetworkLatency adds latency.
type NetworkLatency struct{ d time.Duration }
func (n *NetworkLatency) Name() string { return "network_latency" }
func (n *NetworkLatency) When(r *core.RandShim) time.Duration { return 0 }
func (n *NetworkLatency) Apply(ctx *core.FaultContext) error { return nil }
func NetworkLatency(d time.Duration) *NetworkLatency { return &NetworkLatency{d: d} }
