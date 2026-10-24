package cloudevents

import (
	"context"

	"github.com/AristideDongo/ackhammer/core"
)

// Config for CloudEvents target.
type Config struct {
	ProjectID string
	Topic     string
	Port      int
}

// Target implements core.Target for CloudEvents sdk-go.
type Target struct {
	cfg Config
}

func New(cfg Config) *Target { return &Target{cfg: cfg} }
func (t *Target) Name() string { return "cloudevents" }
func (t *Target) Publish(ctx context.Context, msg core.Message) error { return nil }
func (t *Target) Consume(ctx context.Context, h core.Handler) error { <-ctx.Done(); return ctx.Err() }
func (t *Target) Close(ctx context.Context) error { return nil }
