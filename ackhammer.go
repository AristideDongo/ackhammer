package ackhammer

import (
	"testing"

	"github.com/AristideDongo/ackhammer/core"
	"github.com/AristideDongo/ackhammer/runner"
)

// Re-exports
type Message = core.Message
type Handler = core.Handler
type Delivery = core.Delivery
type Ordering = core.Ordering
type Guarantee = core.Guarantee
type Target = core.Target
type Restartable = core.Restartable
type PoisonInspector = core.PoisonInspector
type EffectRecorder = core.EffectRecorder

const (
	AtLeastOnce     = core.AtLeastOnce
	AtMostOnce      = core.AtMostOnce
	EffectivelyOnce = core.EffectivelyOnce
	NoOrder         = core.NoOrder
	PerKey          = core.PerKey
	Total           = core.Total
)

// Scenario defines a fault injection test.
type Scenario struct {
	Name     string
	Messages int
	Keys     []string
	Faults   []Fault
	Expect   Guarantee
	Seed     int64
	Timeout  int
}

// AtLeastOnce returns common at-least-once guarantee.
func AtLeastOnce() Guarantee {
	return Guarantee{Delivery: AtLeastOnce, Ordering: NoOrder}
}

// EffectivelyOnce returns effectively-once guarantee.
func EffectivelyOnce() Guarantee {
	return Guarantee{Delivery: EffectivelyOnce, Ordering: NoOrder}
}

// OrderedPerKey returns per-key ordering with at-least-once.
func OrderedPerKey() Guarantee {
	return Guarantee{Delivery: AtLeastOnce, Ordering: PerKey}
}

// WithPoison returns guarantee with poison queue.
func WithPoison(after int) Guarantee {
	return Guarantee{Delivery: AtLeastOnce, Ordering: NoOrder, PoisonAfter: after}
}

// Fault is injected by the fault planner.
type Fault = core.Fault

// Run executes a scenario.
func Run(t *testing.T, target Target, sc Scenario) {
	runner.Run(t, target, sc)
}
