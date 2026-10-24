package probe

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/AristideDongo/ackhammer/core"
	"github.com/AristideDongo/ackhammer/ledger"
)

// Probe wraps handlers to record events.
type Probe struct {
	ledger *ledger.Ledger
	next   int64
}

// New creates a probe.
func New(l *ledger.Ledger) *Probe {
	return &Probe{ledger: l}
}

// Wrap wraps a handler.
func (p *Probe) Wrap(h core.Handler) core.Handler {
	return func(ctx context.Context, msg core.Message) error {
		id := atomic.AddInt64(&p.next, 1)
		p.ledger.Append(ledger.Event{
			ID:          "recv-" + itoa(id),
			MsgID:       msg.ID,
			Key:         msg.Key,
			Type:        ledger.Received,
			Time:        time.Now(),
			Traceparent: msg.Traceparent,
		})
		err := h(ctx, msg)
		p.ledger.Append(ledger.Event{
			ID:          "proc-" + itoa(id),
			MsgID:       msg.ID,
			Key:         msg.Key,
			Type:        ledger.Processed,
			Time:        time.Now(),
			Traceparent: msg.Traceparent,
			Meta:        map[string]string{"err": boolStr(err != nil)},
		})
		if err != nil {
			p.ledger.Append(ledger.Event{
				ID:    "nack-" + itoa(id),
				MsgID: msg.ID,
				Type:  ledger.Nacked,
				Time:  time.Now(),
			})
		} else {
			p.ledger.Append(ledger.Event{
				ID:    "ack-" + itoa(id),
				MsgID: msg.ID,
				Type:  ledger.Acked,
				Time:  time.Now(),
			})
		}
		return err
	}
}

func itoa(i int64) string { return fmtInt(i) }
func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func fmtInt(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [32]byte
	b := len(buf)
	for i > 0 {
		b--
		buf[b] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		b--
		buf[b] = '-'
	}
	return string(buf[b:])
}
