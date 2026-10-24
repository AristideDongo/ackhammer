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
