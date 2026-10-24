// Package fault provides the faults Ackhammer can inject and the selectors
// that pick which messages they hit.
//
// Handler faults (SlowHandler, HandlerError, HandlerPanic) run inside the
// probe. Timed faults (CancelDuringReceive, GracefulStop, RestartConsumer)
// fire once, at a given offset after the first message is published.
package fault

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/AristideDongo/ackhammer/core"
)

// Selector picks the messages a handler fault applies to. Selection is
// decided once, before the run, from the scenario seed.
type Selector interface {
	String() string
	selected(r *rand.Rand, msgs []core.Message) map[string]bool
}

type selectorFunc struct {
	name string
	pick func(i int, m core.Message, r *rand.Rand) bool
}

func (s selectorFunc) String() string { return s.name }

func (s selectorFunc) selected(r *rand.Rand, msgs []core.Message) map[string]bool {
	out := map[string]bool{}
	for i, m := range msgs {
		if s.pick(i+1, m, r) {
			out[m.ID] = true
		}
	}
	return out
}

// OnMessage selects the n-th published message (1-based).
func OnMessage(n int) Selector {
	return selectorFunc{fmt.Sprintf("OnMessage(%d)", n), func(i int, _ core.Message, _ *rand.Rand) bool { return i == n }}
}

// OnKey selects every message with key k.
func OnKey(k string) Selector {
	return selectorFunc{fmt.Sprintf("OnKey(%q)", k), func(_ int, m core.Message, _ *rand.Rand) bool { return m.Key == k }}
}

// EveryNth selects messages n, 2n, 3n…
func EveryNth(n int) Selector {
	return selectorFunc{fmt.Sprintf("EveryNth(%d)", n), func(i int, _ core.Message, _ *rand.Rand) bool { return n > 0 && i%n == 0 }}
}

// Random selects each message with probability p, using the scenario seed.
func Random(p float64) Selector {
	return selectorFunc{fmt.Sprintf("Random(%g)", p), func(_ int, _ core.Message, r *rand.Rand) bool { return r.Float64() < p }}
}

// handlerFault carries selection state shared by handler faults.
type handlerFault struct {
	sel    Selector
	chosen map[string]bool
}

func (h *handlerFault) Plan(r *rand.Rand, msgs []core.Message) { h.chosen = h.sel.selected(r, msgs) }

// SlowHandlerFault makes the first delivery of selected messages last d.
type SlowHandlerFault struct {
	handlerFault
	d time.Duration
}

// SlowHandler sleeps d on the first delivery of the selected messages,
// before the user handler runs. The sleep stops early if the delivery
// context is cancelled.
func SlowHandler(sel Selector, d time.Duration) *SlowHandlerFault {
	return &SlowHandlerFault{handlerFault{sel: sel}, d}
}

func (f *SlowHandlerFault) Name() string { return fmt.Sprintf("SlowHandler(%s, %s)", f.sel, f.d) }

func (f *SlowHandlerFault) Applies(m core.Message, attempt int) bool {
	return attempt == 1 && f.chosen[m.ID]
}

func (f *SlowHandlerFault) Apply(ctx context.Context, _ core.Message) error {
	t := time.NewTimer(f.d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
	return nil
}

// ErrInjected is the error returned by HandlerError.
var ErrInjected = errors.New("ackhammer: injected handler error")

// HandlerErrorFault fails the first n deliveries of selected messages.
type HandlerErrorFault struct {
	handlerFault
	n int
}

// HandlerError makes the first n deliveries of the selected messages fail
// with ErrInjected, without calling the user handler.
func HandlerError(sel Selector, n int) *HandlerErrorFault {
	return &HandlerErrorFault{handlerFault{sel: sel}, n}
}

func (f *HandlerErrorFault) Name() string { return fmt.Sprintf("HandlerError(%s, %d)", f.sel, f.n) }

func (f *HandlerErrorFault) Applies(m core.Message, attempt int) bool {
	return attempt <= f.n && f.chosen[m.ID]
}

func (f *HandlerErrorFault) Apply(context.Context, core.Message) error { return ErrInjected }

// HandlerPanicFault panics on the first delivery of selected messages.
type HandlerPanicFault struct{ handlerFault }

// HandlerPanic panics on the first delivery of the selected messages. The
// probe recovers the panic and reports the delivery as failed, so the target
// sees an error.
func HandlerPanic(sel Selector) *HandlerPanicFault {
	return &HandlerPanicFault{handlerFault{sel: sel}}
}

func (f *HandlerPanicFault) Name() string { return fmt.Sprintf("HandlerPanic(%s)", f.sel) }

func (f *HandlerPanicFault) Applies(m core.Message, attempt int) bool {
	return attempt == 1 && f.chosen[m.ID]
}

func (f *HandlerPanicFault) Apply(context.Context, core.Message) error {
	panic("ackhammer: injected panic")
}

// timedFault is a fault that fires once at a fixed offset.
type timedFault struct {
	name   string
	at     time.Duration
	inject func(core.Controller, context.Context) error
}

func (f *timedFault) Name() string      { return fmt.Sprintf("%s(%s)", f.name, f.at) }
func (f *timedFault) At() time.Duration { return f.at }
func (f *timedFault) Inject(ctx context.Context, c core.Controller) error {
	return f.inject(c, ctx)
}

// CancelDuringReceive cancels the context given to Target.Consume at the
// given offset, then resumes consuming.
func CancelDuringReceive(at time.Duration) core.TimedFault {
	return &timedFault{"CancelDuringReceive", at, core.Controller.Cancel}
}

// GracefulStop stops the consumer gracefully at the given offset, then
// resumes consuming. The target must implement core.ConsumerStopper.
func GracefulStop(at time.Duration) core.TimedFault {
	return &timedFault{"GracefulStop", at, core.Controller.Stop}
}

// KillProcess kills the consumer process abruptly at the given offset, then
// starts a new one. Deliveries in flight are neither acked nor nacked. The
// target must implement core.Killer (see package subprocess).
func KillProcess(at time.Duration) core.TimedFault {
	return &timedFault{"KillProcess", at, func(c core.Controller, ctx context.Context) error {
		k, ok := c.Target().(core.Killer)
		if !ok {
			return core.ErrUnsupported
		}
		return c.Break(ctx, func(context.Context) error { return k.Kill() })
	}}
}

// RestartConsumer closes and recreates the consumer at the given offset.
// The target must implement core.Restartable.
func RestartConsumer(at time.Duration) core.TimedFault {
	return &timedFault{"RestartConsumer", at, core.Controller.Restart}
}
