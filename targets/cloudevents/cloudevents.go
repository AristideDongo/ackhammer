// Package cloudevents plugs CloudEvents sdk-go clients into Ackhammer.
//
// Each Ackhammer message becomes a CloudEvent whose id is the message ID,
// with the partitionkey and traceparent extensions set. Consume runs
// client.StartReceiver; a handler error is answered with a NACK.
package cloudevents

import (
	"context"
	"fmt"
	"strings"
	"sync"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/protocol"

	"github.com/AristideDongo/ackhammer/core"
)

// Extension names set on every event.
const (
	PartitionKeyExtension = "partitionkey"
	TraceparentExtension  = "traceparent"
)

// Config configures a Target.
type Config struct {
	// Sender publishes the events.
	Sender ce.Client
	// Receiver consumes them. Defaults to Sender.
	Receiver ce.Client
	// NewReceiver recreates the receiver. Required by RestartConsumer.
	NewReceiver func() (ce.Client, error)
	// Close releases the underlying protocols. Optional.
	Close func(ctx context.Context) error

	Source string // event source (default "ackhammer")
	Type   string // event type (default "dev.ackhammer.test")
}

// Target runs a CloudEvents receiver as the consumer under test.
type Target struct {
	cfg      Config
	mu       sync.Mutex
	receiver ce.Client
}

var _ core.Restartable = (*Target)(nil)

// New returns a CloudEvents target.
func New(cfg Config) *Target {
	if cfg.Receiver == nil {
		cfg.Receiver = cfg.Sender
	}
	if cfg.Source == "" {
		cfg.Source = "ackhammer"
	}
	if cfg.Type == "" {
		cfg.Type = "dev.ackhammer.test"
	}
	return &Target{cfg: cfg, receiver: cfg.Receiver}
}

func (t *Target) Name() string { return "cloudevents" }

func (t *Target) Publish(ctx context.Context, msg core.Message) error {
	e := ce.NewEvent()
	e.SetID(msg.ID)
	e.SetSource(t.cfg.Source)
	e.SetType(t.cfg.Type)
	e.SetExtension(PartitionKeyExtension, msg.Key)
	e.SetExtension(TraceparentExtension, msg.Traceparent)
	for k, v := range msg.Attributes {
		if name := extensionName(k); name != "" {
			e.SetExtension(name, v)
		}
	}
	if err := e.SetData("application/octet-stream", msg.Payload); err != nil {
		return err
	}
	if res := t.cfg.Sender.Send(ctx, e); ce.IsUndelivered(res) || ce.IsNACK(res) {
		return fmt.Errorf("cloudevents: send %s: %w", msg.ID, res)
	}
	return nil
}

// FromEvent converts a CloudEvent back to an Ackhammer message.
func FromEvent(e event.Event) core.Message {
	attrs := map[string]string{}
	var key, traceparent string
	for k, v := range e.Extensions() {
		s := fmt.Sprint(v)
		switch k {
		case PartitionKeyExtension:
			key = s
		case TraceparentExtension:
			traceparent = s
		default:
			attrs[k] = s
		}
	}
	return core.Message{ID: e.ID(), Key: key, Traceparent: traceparent, Payload: e.Data(), Attributes: attrs}
}

func (t *Target) Consume(ctx context.Context, h core.Handler) error {
	t.mu.Lock()
	r := t.receiver
	t.mu.Unlock()
	err := r.StartReceiver(ctx, func(ctx context.Context, e event.Event) protocol.Result {
		if err := h(ctx, FromEvent(e)); err != nil {
			return protocol.NewReceipt(false, "%v", err)
		}
		return protocol.ResultACK
	})
	if err != nil {
		return err
	}
	return ctx.Err()
}

// Restart replaces the receiver with one built by NewReceiver.
func (t *Target) Restart(context.Context) error {
	if t.cfg.NewReceiver == nil {
		return fmt.Errorf("%w: cloudevents.Config.NewReceiver is not set", core.ErrUnsupported)
	}
	r, err := t.cfg.NewReceiver()
	if err != nil {
		return err
	}
	t.mu.Lock()
	t.receiver = r
	t.mu.Unlock()
	return nil
}

func (t *Target) Close(ctx context.Context) error {
	if t.cfg.Close == nil {
		return nil
	}
	return t.cfg.Close(ctx)
}

// extensionName turns an attribute key into a valid CloudEvents extension
// name (lower-case letters and digits), or "" if nothing is left.
func extensionName(k string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(k) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}
