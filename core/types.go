package core

import (
	"context"
	"time"
)

// Message is a traceable message published by Ackhammer.
type Message struct {
	ID          string
	Key         string
	Traceparent string
	Payload     []byte
	Attributes  map[string]string
}

// Handler processes a message.
type Handler func(context.Context, Message) error

// Delivery guarantee.
type Delivery string

const (
	AtLeastOnce      Delivery = "at_least_once"
	AtMostOnce       Delivery = "at_most_once"
	EffectivelyOnce  Delivery = "effectively_once"
)

// Ordering guarantee.
type Ordering string

const (
	NoOrder    Ordering = "no_order"
	PerKey     Ordering = "per_key"
	Total      Ordering = "total"
)

// Guarantee describes expected behavior.
type Guarantee struct {
	Delivery        Delivery
	Ordering        Ordering
	MaxRedeliveries int
	RedeliveryWithin time.Duration
	PoisonAfter     int
}

// Target is the system under test.
type Target interface {
	Name() string
	Publish(ctx context.Context, msg Message) error
	Consume(ctx context.Context, h Handler) error
	Close(ctx context.Context) error
}

// Restartable targets support consumer restart.
type Restartable interface {
	Target
	Restart(ctx context.Context) error
}

// PoisonInspector can inspect poison queue.
type PoisonInspector interface {
	Target
	PoisonCount(ctx context.Context) (int, error)
}

// EffectRecorder allows recording business effects for EffectOnce.
type EffectRecorder interface {
	RecordEffect(id string)
	Effects() []string
}
