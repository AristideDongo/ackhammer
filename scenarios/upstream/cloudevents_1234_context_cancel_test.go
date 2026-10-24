package upstream

import (
	"testing"
	"time"

	"github.com/AristideDongo/ackhammer"
	"github.com/AristideDongo/ackhammer/fault"
)

// sdk-go #1234: context cancelled messages abandoned without Nack()
func TestCloudEvents1234ContextCancel(t *testing.T) {
	t.Skip("requires docker/brokers")
	_ = ackhammer.Run
	_ = fault.CancelDuringReceive
	_ = time.Second
	sc := ackhammer.Scenario{
		Name:     "cloudevents-1234-context-cancel",
		Messages: 5,
		Faults: []ackhammer.Fault{
			fault.CancelDuringReceive(500 * time.Millisecond),
		},
		Expect: ackhammer.AtLeastOnce(),
		Seed:   1234,
	}
	_ = sc
}
