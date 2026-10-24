package core

import "time"

// Fault represents an injectable fault.
type Fault interface {
	Name() string
	When(r *Rand) time.Duration
	Apply(ctx *FaultContext) error
}

// FaultContext provides fault injection context.
type FaultContext struct {
	Target      Target
	Restartable Restartable
	Probe       interface{}
	At          time.Time
}

// Rand provides deterministic randomness.
type Rand interface {
	Intn(n int) int
	Float64() float64
	Seed(s int64)
}
