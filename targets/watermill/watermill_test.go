package watermill_test

import (
	"testing"
	"time"

	wm "github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"

	"github.com/AristideDongo/ackhammer"
	"github.com/AristideDongo/ackhammer/fault"
	"github.com/AristideDongo/ackhammer/targets/watermill"
)

func newPubSub() *gochannel.GoChannel {
	return gochannel.NewGoChannel(gochannel.Config{Persistent: true}, wm.NopLogger{})
}

func TestGoChannel(t *testing.T) {
	ps := newPubSub()
	ackhammer.Run(t, watermill.New(watermill.Config{Publisher: ps, Subscriber: ps, Topic: "orders"}), ackhammer.Scenario{
		Name:     "handler-faults",
		Messages: 20,
		Faults: []ackhammer.Fault{
			fault.HandlerError(fault.EveryNth(3), 2),
			fault.HandlerPanic(fault.OnMessage(5)),
			fault.SlowHandler(fault.OnKey("key-b"), 20*time.Millisecond),
		},
		Expect: ackhammer.AtLeastOnce().WithRedeliveryWithin(time.Second),
		Seed:   1,
	})
}

func TestGoChannelCancel(t *testing.T) {
	ps := newPubSub()
	ackhammer.Run(t, watermill.New(watermill.Config{Publisher: ps, Subscriber: ps, Topic: "orders"}), ackhammer.Scenario{
		Name:   "cancel-and-stop",
		Faults: []ackhammer.Fault{fault.CancelDuringReceive(50 * time.Millisecond), fault.GracefulStop(150 * time.Millisecond)},
		Expect: ackhammer.AtLeastOnce(),
		Seed:   2,
	})
}

func TestPoisonQueueMiddleware(t *testing.T) {
	ps := newPubSub()
	poison, err := middleware.PoisonQueue(ps, "orders_poison")
	if err != nil {
		t.Fatal(err)
	}
	retry := middleware.Retry{MaxRetries: 2}.Middleware

	ackhammer.Run(t, watermill.New(watermill.Config{
		Publisher:        ps,
		Subscriber:       ps,
		Topic:            "orders",
		Middlewares:      []message.HandlerMiddleware{poison, retry},
		PoisonTopic:      "orders_poison",
		PoisonSubscriber: ps,
	}), ackhammer.Scenario{
		Name:   "retry-then-poison",
		Faults: []ackhammer.Fault{fault.HandlerError(fault.OnMessage(2), 100)},
		// Retry calls the handler 3 times, then the poison middleware acks.
		Expect: ackhammer.AtLeastOnce().WithPoisonAfter(3).WithMaxRedeliveries(0),
		Seed:   3,
	})
}

// TestTraceThroughRetryAndPoison checks that the traceparent metadata
// survives Watermill's Retry and PoisonQueue middlewares, and that a
// middleware rebuilding the message without metadata is caught.
func TestTraceThroughRetryAndPoison(t *testing.T) {
	sc := ackhammer.Scenario{
		Name:   "trace",
		Faults: []ackhammer.Fault{fault.HandlerError(fault.OnMessage(2), 100), fault.HandlerError(fault.EveryNth(4), 1)},
		Expect: ackhammer.AtLeastOnce().WithPoisonAfter(3).WithTraceContinuity(),
		Seed:   4,
	}
	target := func(extra ...message.HandlerMiddleware) ackhammer.Target {
		ps := newPubSub()
		poison, err := middleware.PoisonQueue(ps, "orders_poison")
		if err != nil {
			t.Fatal(err)
		}
		return watermill.New(watermill.Config{
			Publisher: ps, Subscriber: ps, Topic: "orders",
			Middlewares:      append([]message.HandlerMiddleware{poison, middleware.Retry{MaxRetries: 2}.Middleware}, extra...),
			PoisonTopic:      "orders_poison",
			PoisonSubscriber: ps,
		})
	}

	ackhammer.Run(t, target(), sc)

	// Rebuilds the message from its payload only: metadata is lost.
	stripMetadata := func(h message.HandlerFunc) message.HandlerFunc {
		return func(m *message.Message) ([]*message.Message, error) {
			c := message.NewMessage(m.UUID, m.Payload)
			c.SetContext(m.Context())
			c.Metadata.Set(watermill.KeyMetadata, m.Metadata.Get(watermill.KeyMetadata))
			return h(c)
		}
	}
	rep, err := ackhammer.Execute(t.Context(), target(stripMetadata), sc)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rep.Results {
		if r.Name == "TraceContinuity" && r.Status != "FAIL" {
			t.Errorf("metadata loss not detected\n%s", rep.Text())
		}
	}
}
