// Package runner runs the execution cycle of a scenario:
// consume → publish → inject faults → drain → settle → check → report.
package runner

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AristideDongo/ackhammer/core"
	"github.com/AristideDongo/ackhammer/invariant"
	"github.com/AristideDongo/ackhammer/ledger"
	"github.com/AristideDongo/ackhammer/probe"
	"github.com/AristideDongo/ackhammer/report"
)

// SeedEnv overrides Scenario.Seed when set.
const SeedEnv = "ACKHAMMER_SEED"

// StopTimeout is how long Consume has to return after its context is
// cancelled before the run records the consumer as stuck.
const StopTimeout = 5 * time.Second

const (
	defaultMessages     = 10
	defaultDrainTimeout = 30 * time.Second
	defaultSettle       = 300 * time.Millisecond
	pollInterval        = 10 * time.Millisecond
)

// Execute runs sc against target and returns the report. An error means the
// run itself could not happen (invalid scenario, publish failure, unsupported
// fault); a broken guarantee is reported through the invariant results.
func Execute(ctx context.Context, target core.Target, sc core.Scenario) (*report.Report, error) {
	sc, err := normalize(sc)
	if err != nil {
		return nil, err
	}
	rnd := rand.New(rand.NewPCG(uint64(sc.Seed), uint64(sc.Seed)))
	msgs := generate(rnd, sc)

	var handlerFaults []core.HandlerFault
	var timedFaults []core.TimedFault
	for _, f := range sc.Faults {
		switch f := f.(type) {
		case core.HandlerFault:
			f.Plan(rnd, msgs)
			handlerFaults = append(handlerFaults, f)
		case core.TimedFault:
			timedFaults = append(timedFaults, f)
		default:
			return nil, fmt.Errorf("ackhammer: fault %s is neither a HandlerFault nor a TimedFault", f.Name())
		}
	}
	sort.SliceStable(timedFaults, func(i, j int) bool { return timedFaults[i].At() < timedFaults[j].At() })
	if err := checkCapabilities(target, timedFaults); err != nil {
		return nil, err
	}

	ctx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	l := ledger.New()
	c := &consumer{target: target, handler: probe.New(l, handlerFaults).Wrap(sc.Handler), ledger: l}
	started := time.Now()
	c.start()

	faultErr := make(chan error, 1)
	faultsDone := make(chan struct{})
	go func() {
		defer close(faultsDone)
		for _, f := range timedFaults {
			select {
			case <-time.After(time.Until(started.Add(f.At()))):
			case <-ctx.Done():
				return
			}
			l.Append(ledger.Event{Type: ledger.FaultInjected, Detail: f.Name()})
			if err := f.Inject(ctx, c); err != nil {
				faultErr <- fmt.Errorf("ackhammer: fault %s: %w", f.Name(), err)
				return
			}
		}
	}()

	for i, m := range msgs {
		l.Append(ledger.Event{Type: ledger.Published, MsgID: m.ID, Key: m.Key, Index: i + 1, Traceparent: m.Traceparent})
		if err := target.Publish(ctx, m); err != nil {
			cancelRun()
			<-faultsDone
			c.stop()
			_ = target.Close(context.WithoutCancel(ctx))
			return nil, fmt.Errorf("ackhammer: publish %s: %w", m.ID, err)
		}
	}

	drain(ctx, l, sc, faultsDone)
	sleep(ctx, sc.Settle)
	c.stop()
	<-faultsDone
	select {
	case err := <-faultErr:
		_ = target.Close(context.WithoutCancel(ctx))
		return nil, err
	default:
	}

	pi, poisonSupported := target.(core.PoisonInspector)
	if poisonSupported {
		msgs, err := pi.PoisonedMessages(ctx)
		if err != nil {
			return nil, fmt.Errorf("ackhammer: read poison queue: %w", err)
		}
		for _, m := range msgs {
			l.Append(ledger.Event{Type: ledger.Poisoned, MsgID: m.ID, Key: m.Key, Traceparent: m.Traceparent})
		}
	}
	if err := target.Close(context.WithoutCancel(ctx)); err != nil {
		return nil, fmt.Errorf("ackhammer: close target: %w", err)
	}

	events := l.Events()
	return &report.Report{
		Scenario: sc.Name,
		Target:   target.Name(),
		Seed:     sc.Seed,
		Started:  started,
		Duration: time.Since(started),
		Results: invariant.Check(invariant.Input{
			View:            ledger.NewView(events),
			Guarantee:       sc.Expect,
			PoisonSupported: poisonSupported,
		}),
		Events:      events,
		Environment: environment(),
	}, nil
}

// libraries are the module prefixes reported in Report.Environment.
var libraries = []string{
	"github.com/AristideDongo/ackhammer",
	"github.com/ThreeDotsLabs/",
	"github.com/cloudevents/",
	"github.com/nats-io/",
	"cloud.google.com/go/pubsub",
	"github.com/twmb/franz-go",
	"github.com/rabbitmq/",
	"github.com/eclipse/paho",
	"github.com/jackc/pgx",
}

func environment() map[string]string {
	env := map[string]string{"go": runtime.Version(), "os/arch": runtime.GOOS + "/" + runtime.GOARCH}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return env
	}
	for _, dep := range info.Deps {
		for _, prefix := range libraries {
			if strings.HasPrefix(dep.Path, prefix) {
				v := dep.Version
				if dep.Replace != nil {
					v += " => " + dep.Replace.Path + " " + dep.Replace.Version
				}
				env[dep.Path] = strings.TrimSpace(v)
			}
		}
	}
	return env
}

func normalize(sc core.Scenario) (core.Scenario, error) {
	if sc.Name == "" {
		return sc, errors.New("ackhammer: scenario needs a Name")
	}
	if sc.Messages < 0 {
		return sc, errors.New("ackhammer: Messages must be positive")
	}
	if sc.Messages == 0 {
		sc.Messages = defaultMessages
	}
	if len(sc.Keys) == 0 {
		sc.Keys = []string{"key-a", "key-b", "key-c", "key-d"}
	}
	if sc.Expect.Delivery == "" {
		sc.Expect.Delivery = core.AtLeastOnce
	}
	if sc.Expect.Ordering == "" {
		sc.Expect.Ordering = core.NoOrder
	}
	if sc.DrainTimeout == 0 {
		sc.DrainTimeout = defaultDrainTimeout
	}
	if sc.Settle == 0 {
		sc.Settle = defaultSettle
	}
	if s := os.Getenv(SeedEnv); s != "" {
		seed, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return sc, fmt.Errorf("ackhammer: invalid %s=%q", SeedEnv, s)
		}
		sc.Seed = seed
	}
	if sc.Seed == 0 {
		sc.Seed = time.Now().UnixNano()
	}
	return sc, nil
}

func generate(rnd *rand.Rand, sc core.Scenario) []core.Message {
	msgs := make([]core.Message, sc.Messages)
	for i := range msgs {
		msgs[i] = core.Message{
			ID:          fmt.Sprintf("msg-%d", i+1),
			Key:         sc.Keys[i%len(sc.Keys)],
			Traceparent: fmt.Sprintf("00-%016x%016x-%016x-01", rnd.Uint64(), rnd.Uint64(), rnd.Uint64()),
			Payload:     fmt.Appendf(nil, `{"seq":%d}`, i+1),
			Attributes:  map[string]string{"ackhammer-scenario": sc.Name},
		}
	}
	return msgs
}

// checkCapabilities fails fast when a fault needs an interface the target
// lacks, instead of failing halfway through the run.
func checkCapabilities(target core.Target, faults []core.TimedFault) error {
	dry := dryRun{target}
	for _, f := range faults {
		if err := f.Inject(context.Background(), dry); errors.Is(err, core.ErrUnsupported) {
			return fmt.Errorf("%w: %s on target %s", core.ErrUnsupported, f.Name(), target.Name())
		}
	}
	return nil
}

// dryRun is a Controller that only checks capabilities.
type dryRun struct{ target core.Target }

func (d dryRun) Target() core.Target          { return d.target }
func (d dryRun) Cancel(context.Context) error { return nil }
func (d dryRun) Break(context.Context, func(context.Context) error) error {
	return nil
}

func (d dryRun) Stop(context.Context) error {
	if _, ok := d.target.(core.ConsumerStopper); !ok {
		return core.ErrUnsupported
	}
	return nil
}

func (d dryRun) Restart(context.Context) error {
	if _, ok := d.target.(core.Restartable); !ok {
		return core.ErrUnsupported
	}
	return nil
}

// drain waits until every message is handled or poisoned, all timed faults
// fired, or the drain timeout expires.
func drain(ctx context.Context, l *ledger.Ledger, sc core.Scenario, faultsDone <-chan struct{}) {
	deadline := time.After(sc.DrainTimeout)
	tick := time.NewTicker(pollInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			return
		case <-tick.C:
		}
		select {
		case <-faultsDone:
		default:
			continue
		}
		if settled(ledger.NewView(l.Events()), sc.Expect) {
			return
		}
	}
}

func settled(v *ledger.View, g core.Guarantee) bool {
	for _, p := range v.Published {
		if v.Count(p.MsgID, ledger.Succeeded) > 0 {
			continue
		}
		if g.PoisonAfter > 0 && v.Count(p.MsgID, ledger.Failed) >= g.PoisonAfter {
			continue
		}
		return false
	}
	return true
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

// consumer runs Target.Consume in the background and implements
// core.Controller for timed faults.
type consumer struct {
	target  core.Target
	handler core.Handler
	ledger  *ledger.Ledger

	mu       sync.Mutex
	cancel   context.CancelFunc
	done     chan struct{}
	stopping bool // a graceful stop is in progress: returning is expected
}

func (c *consumer) start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	c.cancel, c.done, c.stopping = cancel, done, false
	go func() {
		defer close(done)
		err := c.target.Consume(ctx, c.handler)
		c.mu.Lock()
		expected := c.stopping
		c.mu.Unlock()
		if ctx.Err() == nil && !expected {
			detail := "returned nil"
			if err != nil {
				detail = err.Error()
			}
			c.ledger.Append(ledger.Event{Type: ledger.ConsumerExit, Detail: detail})
		}
	}()
}

// stop cancels Consume and waits for it to return, at most StopTimeout.
func (c *consumer) stop() {
	c.mu.Lock()
	cancel, done := c.cancel, c.done
	c.mu.Unlock()
	cancel()
	c.wait(done)
}

func (c *consumer) wait(done chan struct{}) {
	select {
	case <-done:
	case <-time.After(StopTimeout):
		c.ledger.Append(ledger.Event{Type: ledger.ConsumerStuck, Detail: fmt.Sprintf("Consume still running %s after cancel", StopTimeout)})
	}
}

func (c *consumer) Target() core.Target { return c.target }

func (c *consumer) Cancel(context.Context) error {
	c.stop()
	c.start()
	return nil
}

func (c *consumer) Stop(ctx context.Context) error {
	s, ok := c.target.(core.ConsumerStopper)
	if !ok {
		return core.ErrUnsupported
	}
	return c.Break(ctx, s.StopConsuming)
}

func (c *consumer) Break(ctx context.Context, action func(context.Context) error) error {
	c.mu.Lock()
	done := c.done
	c.stopping = true
	c.mu.Unlock()
	if err := action(ctx); err != nil {
		return err
	}
	c.wait(done)
	c.stop() // releases the context; no-op if Consume already returned
	c.start()
	return nil
}

func (c *consumer) Restart(ctx context.Context) error {
	r, ok := c.target.(core.Restartable)
	if !ok {
		return core.ErrUnsupported
	}
	c.stop()
	if err := r.Restart(ctx); err != nil {
		return err
	}
	c.start()
	return nil
}
