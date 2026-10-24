# Ackhammer

*Hammer your event consumers until they break — before production does.*

Ackhammer is a fault-injection test harness for Go event consumers (Watermill, CloudEvents sdk-go, etc.). It publishes traceable messages, injects realistic failures, and verifies delivery guarantees via a ledger + invariants.

## Quick start

```go
package orders_test

import (
	"testing"
	"time"

	"github.com/AristideDongo/ackhammer"
	"github.com/AristideDongo/ackhammer/fault"
	"github.com/AristideDongo/ackhammer/targets/watermill"
)

func TestSlowMessageInSQLBatch(t *testing.T) {
	target := watermill.New(watermill.Config{
		Publisher:  pub,
		Subscriber: sub,
		Topic:      "orders",
	})

	ackhammer.Run(t, target, ackhammer.Scenario{
		Name:     "slow-message-in-batch",
		Messages: 10,
		Faults: []ackhammer.Fault{
			fault.SlowHandler(fault.OnMessage(3), time.Second),
		},
		Expect: ackhammer.AtLeastOnce(),
		Seed:   42,
	})
}
```

## Features (v0.1)

- **Deterministic faults** seeded for replay
- **Ledger**: append-only, concurrent-safe truth journal
- **Probe**: wraps handlers to record received/processed/acked/nacked/errors
- **Invariants**: `NoLoss`, `DuplicatesWithinGuarantee`, `PoisonRouted`, `RedeliveryLatency`, `NoStall`
- **Targets**: Watermill (SQL Postgres, NATS JetStream), CloudEvents sdk-go
- **Reports**: text, JSON, JUnit, HTML

## License

Apache 2.0
