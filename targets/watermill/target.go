package watermill

import (
	"context"

	"github.com/AristideDongo/ackhammer/core"
)

// Config for Watermill target.
type Config struct {
	Publisher  interface{}
	Subscriber interface{}
	Topic      string
}

// Target implements core.Target for Watermill.
type Target struct {
	cfg Config
}

func New(cfg Config) *Target { return &Target{cfg: cfg} }
func (t *Target) Name() string { return "watermill" }
func (t *Target) Publish(ctx context.Context, msg core.Message) error { return nil }
func (t *Target) Consume(ctx context.Context, h core.Handler) error { <-ctx.Done(); return ctx.Err() }
func (t *Target) Close(ctx context.Context) error { return nil }
func (t *Target) Restart(ctx context.Context) error { return nil }
