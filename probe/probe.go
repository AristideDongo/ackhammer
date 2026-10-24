// Package probe wraps the user handler and records every delivery in the
// ledger. Handler-level faults run here.
package probe

import (
	"context"
	"fmt"
	"sync"

	"github.com/AristideDongo/ackhammer/core"
	"github.com/AristideDongo/ackhammer/ledger"
)

// Probe records deliveries into a ledger.
type Probe struct {
	ledger   *ledger.Ledger
	faults   []core.HandlerFault
	mu       sync.Mutex
	attempts map[string]int
}

// New returns a probe writing to l and applying faults.
func New(l *ledger.Ledger, faults []core.HandlerFault) *Probe {
	return &Probe{ledger: l, faults: faults, attempts: map[string]int{}}
}

// Wrap returns the handler given to Target.Consume. h may be nil.
func (p *Probe) Wrap(h core.Handler) core.Handler {
	return func(ctx context.Context, msg core.Message) (err error) {
		p.mu.Lock()
		p.attempts[msg.ID]++
		attempt := p.attempts[msg.ID]
		p.mu.Unlock()

		base := ledger.Event{MsgID: msg.ID, Key: msg.Key, Attempt: attempt, Traceparent: msg.Traceparent}
		p.record(base, ledger.Received, "")

		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("ackhammer: handler panic: %v", r)
				p.record(base, ledger.Panicked, fmt.Sprint(r))
			}
			if err != nil {
				p.record(base, ledger.Failed, err.Error())
			} else {
				p.record(base, ledger.Succeeded, "")
			}
		}()

		for _, f := range p.faults {
			if !f.Applies(msg, attempt) {
				continue
			}
			p.record(base, ledger.FaultInjected, f.Name())
			if err := f.Apply(ctx, msg); err != nil {
				return err
			}
		}
		if h == nil {
			return nil
		}
		ctx = core.WithEffectSink(ctx, func(effect string) {
			p.record(base, ledger.Effect, effect)
		})
		return h(ctx, msg)
	}
}

func (p *Probe) record(base ledger.Event, t ledger.Type, detail string) {
	base.Type = t
	base.Detail = detail
	p.ledger.Append(base)
}
