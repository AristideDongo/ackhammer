package upstream

import (
	"testing"
	"time"

	"github.com/AristideDongo/ackhammer"
	"github.com/AristideDongo/ackhammer/fault"
)

// watermill #700: PostgreSQL adapter - slow message in batch causes loss
func TestWatermill700SlowMessageInBatch(t *testing.T) {
	t.Skip("requires docker/brokers")
	_ = ackhammer.Run
	_ = fault.SlowHandler
	_ = time.Second
	sc := ackhammer.Scenario{
		Name:     "watermill-700-slow-message-in-batch",
		Messages: 10,
		Faults: []ackhammer.Fault{
			fault.SlowHandler(fault.OnMessage(3), time.Second),
		},
		Expect: ackhammer.AtLeastOnce(),
		Seed:   42,
	}
	_ = sc
}
