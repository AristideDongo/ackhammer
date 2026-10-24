// Package watermill plugs a Watermill publisher and subscriber into
// Ackhammer.
//
// Consume runs a real message.Router with the middlewares you pass, so that
// retry, poison queue and deduplication middlewares are tested exactly as
// they run in production.
package watermill

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	wm "github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/AristideDongo/ackhammer/core"
)

// Metadata keys used to carry Ackhammer fields on Watermill messages.
const (
	KeyMetadata         = "ackhammer_key"
	TraceparentMetadata = "traceparent"
)

// Config configures a Target.
type Config struct {
	Publisher  message.Publisher
	Subscriber message.Subscriber
	Topic      string

	// NewSubscriber recreates the subscriber. Required by RestartConsumer.
	NewSubscriber func() (message.Subscriber, error)

	// Middlewares are added to the router, in order, around the handler.
	Middlewares []message.HandlerMiddleware

	// PoisonTopic and PoisonSubscriber let Ackhammer read the poison queue
	// filled by your PoisonQueue middleware. Required by PoisonRouted.
	PoisonTopic      string
	PoisonSubscriber message.Subscriber

	// CloseTimeout bounds the router graceful close (default 5s).
	CloseTimeout time.Duration
	// Logger defaults to a no-op logger.
	Logger wm.LoggerAdapter
}

// Target runs a Watermill router as the consumer under test.
type Target struct {
	cfg Config

	mu         sync.Mutex
	subscriber message.Subscriber
	router     *message.Router

	poisonOnce   sync.Once
	poisonErr    error
	poisonCancel context.CancelFunc
	poisonDone   chan struct{}
	poisonMu     sync.Mutex
	poisoned     []string
}

var (
	_ core.Restartable     = (*Target)(nil)
	_ core.ConsumerStopper = (*Target)(nil)
)

// New returns a Watermill target. When PoisonTopic is set the returned
// target also implements core.PoisonInspector.
func New(cfg Config) core.Target {
	if cfg.CloseTimeout == 0 {
		cfg.CloseTimeout = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = wm.NopLogger{}
	}
	t := &Target{cfg: cfg, subscriber: cfg.Subscriber}
	if cfg.PoisonTopic != "" {
		return &poisonTarget{t}
	}
	return t
}

func (t *Target) Name() string { return "watermill" }

func (t *Target) Publish(_ context.Context, msg core.Message) error {
	m := message.NewMessage(msg.ID, msg.Payload)
	for k, v := range msg.Attributes {
		m.Metadata.Set(k, v)
	}
	m.Metadata.Set(KeyMetadata, msg.Key)
	m.Metadata.Set(TraceparentMetadata, msg.Traceparent)
	return t.cfg.Publisher.Publish(t.cfg.Topic, m)
}

// FromWatermill converts a Watermill message back to an Ackhammer message.
func FromWatermill(m *message.Message) core.Message {
	attrs := map[string]string{}
	for k, v := range m.Metadata {
		if k != KeyMetadata && k != TraceparentMetadata {
			attrs[k] = v
		}
	}
	return core.Message{
		ID:          m.UUID,
		Key:         m.Metadata.Get(KeyMetadata),
		Traceparent: m.Metadata.Get(TraceparentMetadata),
		Payload:     m.Payload,
		Attributes:  attrs,
	}
}

func (t *Target) Consume(ctx context.Context, h core.Handler) error {
	if err := t.startPoisonReader(); err != nil {
		return err
	}
	router, err := message.NewRouter(message.RouterConfig{CloseTimeout: t.cfg.CloseTimeout}, t.cfg.Logger)
	if err != nil {
		return err
	}
	router.AddMiddleware(t.cfg.Middlewares...)
	t.mu.Lock()
	sub := t.subscriber
	t.router = router
	t.mu.Unlock()

	router.AddConsumerHandler("ackhammer", t.cfg.Topic, nonClosing{sub}, func(m *message.Message) error {
		return h(m.Context(), FromWatermill(m))
	})

	go func() {
		<-ctx.Done()
		_ = router.Close()
	}()
	if err := router.Run(ctx); err != nil {
		return err
	}
	return ctx.Err()
}

// StopConsuming closes the router gracefully.
func (t *Target) StopConsuming(context.Context) error {
	t.mu.Lock()
	r := t.router
	t.mu.Unlock()
	if r == nil {
		return nil
	}
	return r.Close()
}

// Restart closes the subscriber and creates a new one with NewSubscriber.
func (t *Target) Restart(context.Context) error {
	if t.cfg.NewSubscriber == nil {
		return fmt.Errorf("%w: watermill.Config.NewSubscriber is not set", core.ErrUnsupported)
	}
	sub, err := t.cfg.NewSubscriber()
	if err != nil {
		return err
	}
	t.mu.Lock()
	old := t.subscriber
	t.subscriber = sub
	t.mu.Unlock()
	return old.Close()
}

func (t *Target) Close(context.Context) error {
	if t.poisonCancel != nil {
		t.poisonCancel()
		<-t.poisonDone
	}
	t.mu.Lock()
	sub := t.subscriber
	t.mu.Unlock()
	return errors.Join(sub.Close(), t.cfg.Publisher.Close())
}

// startPoisonReader subscribes to the poison topic once, before the first
// message can be routed there.
func (t *Target) startPoisonReader() error {
	if t.cfg.PoisonTopic == "" {
		return nil
	}
	t.poisonOnce.Do(func() {
		if t.cfg.PoisonSubscriber == nil {
			t.poisonErr = errors.New("watermill: PoisonTopic set without PoisonSubscriber")
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		msgs, err := t.cfg.PoisonSubscriber.Subscribe(ctx, t.cfg.PoisonTopic)
		if err != nil {
			cancel()
			t.poisonErr = err
			return
		}
		t.poisonCancel, t.poisonDone = cancel, make(chan struct{})
		go func() {
			defer close(t.poisonDone)
			for m := range msgs {
				t.poisonMu.Lock()
				t.poisoned = append(t.poisoned, m.UUID)
				t.poisonMu.Unlock()
				m.Ack()
			}
		}()
	})
	return t.poisonErr
}

// poisonTarget adds PoisonInspector when a poison topic is configured.
type poisonTarget struct{ *Target }

var _ core.PoisonInspector = (*poisonTarget)(nil)

// PoisonedIDs lists the messages read from the poison topic so far. It
// waits briefly for messages published just before the end of the run.
func (t *poisonTarget) PoisonedIDs(ctx context.Context) ([]string, error) {
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
	}
	t.poisonMu.Lock()
	defer t.poisonMu.Unlock()
	return append([]string(nil), t.poisoned...), nil
}

// nonClosing stops the router from closing the subscriber, so that Consume
// can be called again after a cancel or a graceful stop.
type nonClosing struct{ message.Subscriber }

func (nonClosing) Close() error { return nil }
