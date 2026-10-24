package ackhammer_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/AristideDongo/ackhammer"
	"github.com/AristideDongo/ackhammer/core"
	"github.com/AristideDongo/ackhammer/fault"
	"github.com/AristideDongo/ackhammer/invariant"
	"github.com/AristideDongo/ackhammer/ledger"
	"github.com/AristideDongo/ackhammer/memtarget"
)

// execute runs a scenario that is expected to complete and returns the
// status of each invariant.
func execute(t *testing.T, target ackhammer.Target, sc ackhammer.Scenario) (*ackhammer.Report, map[string]invariant.Status) {
	t.Helper()
	if sc.Seed == 0 {
		sc.Seed = 42
	}
	rep, err := ackhammer.Execute(t.Context(), target, sc)
	if err != nil {
		t.Fatal(err)
	}
	status := map[string]invariant.Status{}
	for _, r := range rep.Results {
		status[r.Name] = r.Status
	}
	return rep, status
}

func expectStatus(t *testing.T, rep *ackhammer.Report, got map[string]invariant.Status, want map[string]invariant.Status) {
	t.Helper()
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s = %s, want %s\n%s", name, got[name], w, rep.Text())
		}
	}
}

func TestHealthyTargetPasses(t *testing.T) {
	ackhammer.Run(t, memtarget.New(memtarget.Config{}), ackhammer.Scenario{
		Name:     "no-fault",
		Messages: 20,
		Expect:   ackhammer.OrderedPerKey().WithMaxRedeliveries(0),
		Seed:     1,
	})
}

func TestHealthyTargetSurvivesEveryFault(t *testing.T) {
	target := memtarget.New(memtarget.Config{AckDeadline: 500 * time.Millisecond, PoisonAfter: 3})
	ackhammer.Run(t, target, ackhammer.Scenario{
		Name:     "every-fault",
		Messages: 30,
		Faults: []ackhammer.Fault{
			fault.SlowHandler(fault.OnMessage(2), 50*time.Millisecond),
			fault.HandlerError(fault.EveryNth(5), 1),
			fault.HandlerError(fault.OnMessage(7), 3),
			fault.HandlerPanic(fault.Random(0.1)),
			fault.CancelDuringReceive(20 * time.Millisecond),
			fault.GracefulStop(60 * time.Millisecond),
			fault.RestartConsumer(100 * time.Millisecond),
		},
		Expect: ackhammer.AtLeastOnce().WithPoisonAfter(3).WithRedeliveryWithin(time.Second),
		Seed:   7,
	})
}

func TestDetectsLossOnAckDeadline(t *testing.T) { // watermill #700 class
	target := memtarget.New(memtarget.Config{
		AckDeadline: 100 * time.Millisecond,
		Bugs:        memtarget.Bugs{DropBatchOnDeadline: true},
	})
	rep, got := execute(t, target, ackhammer.Scenario{
		Name:   "slow-message-in-batch",
		Faults: []ackhammer.Fault{fault.SlowHandler(fault.OnMessage(3), 300*time.Millisecond)},
		Expect: ackhammer.AtLeastOnce(),
		// The dropped messages never come back: the drain must give up.
		DrainTimeout: time.Second,
	})
	expectStatus(t, rep, got, map[string]invariant.Status{"NoLoss": invariant.Fail})
	want := []string{"msg-4", "msg-5", "msg-6", "msg-7", "msg-8", "msg-9", "msg-10"}
	if r := rep.Results[0]; !slices.Equal(r.MsgIDs, want) {
		t.Errorf("NoLoss reports %v, want %v", r.MsgIDs, want)
	}
}

func TestDetectsLateRedeliveryAfterCancel(t *testing.T) { // sdk-go #1234 class
	target := memtarget.New(memtarget.Config{
		AckDeadline: time.Second,
		Bugs:        memtarget.Bugs{AbandonOnCancel: true},
	})
	rep, got := execute(t, target, ackhammer.Scenario{
		Name: "abandoned-on-cancel",
		Faults: []ackhammer.Fault{
			fault.SlowHandler(fault.OnMessage(1), 5*time.Second),
			fault.CancelDuringReceive(100 * time.Millisecond),
		},
		Expect: ackhammer.AtLeastOnce().WithRedeliveryWithin(300 * time.Millisecond),
	})
	expectStatus(t, rep, got, map[string]invariant.Status{
		"NoLoss":            invariant.Pass,
		"RedeliveryLatency": invariant.Fail,
	})
}

func TestDetectsRedeliveryOfAckedMessagesOnRestart(t *testing.T) { // watermill #671 class
	target := memtarget.New(memtarget.Config{Bugs: memtarget.Bugs{RedeliverAckedOnRestart: true}})
	rep, got := execute(t, target, ackhammer.Scenario{
		Name:   "acked-redelivered-on-restart",
		Faults: []ackhammer.Fault{fault.RestartConsumer(200 * time.Millisecond)},
		Expect: ackhammer.AtLeastOnce().WithMaxRedeliveries(0),
	})
	expectStatus(t, rep, got, map[string]invariant.Status{
		"NoLoss":                    invariant.Pass,
		"DuplicatesWithinGuarantee": invariant.Fail,
	})
}

func TestEffectOnce(t *testing.T) {
	buggy := func() ackhammer.Target {
		return memtarget.New(memtarget.Config{Bugs: memtarget.Bugs{RedeliverAckedOnRestart: true}})
	}
	sc := ackhammer.Scenario{
		Name:   "effect-once",
		Faults: []ackhammer.Fault{fault.RestartConsumer(200 * time.Millisecond)},
		Expect: ackhammer.EffectivelyOnce(),
	}

	t.Run("naive handler", func(t *testing.T) {
		sc.Handler = func(ctx context.Context, m ackhammer.Message) error {
			ackhammer.RecordEffect(ctx, "charge:"+m.ID)
			return nil
		}
		rep, got := execute(t, buggy(), sc)
		expectStatus(t, rep, got, map[string]invariant.Status{"EffectOnce": invariant.Fail})
	})

	t.Run("idempotent handler", func(t *testing.T) {
		var mu sync.Mutex
		done := map[string]bool{}
		sc.Handler = func(ctx context.Context, m ackhammer.Message) error {
			mu.Lock()
			defer mu.Unlock()
			if !done[m.ID] {
				done[m.ID] = true
				ackhammer.RecordEffect(ctx, "charge:"+m.ID)
			}
			return nil
		}
		rep, got := execute(t, buggy(), sc)
		expectStatus(t, rep, got, map[string]invariant.Status{"EffectOnce": invariant.Pass})
	})
}

func TestPoisonRouted(t *testing.T) {
	sc := ackhammer.Scenario{
		Name:   "poison",
		Faults: []ackhammer.Fault{fault.HandlerError(fault.OnMessage(2), 10)},
	}

	sc.Expect = ackhammer.AtLeastOnce().WithPoisonAfter(3)
	rep, got := execute(t, memtarget.New(memtarget.Config{PoisonAfter: 3}), sc)
	expectStatus(t, rep, got, map[string]invariant.Status{"NoLoss": invariant.Pass, "PoisonRouted": invariant.Pass})

	// The broker routes after 4 failures while the user believes 3.
	sc.Expect = ackhammer.AtLeastOnce().WithPoisonAfter(3)
	rep, got = execute(t, memtarget.New(memtarget.Config{PoisonAfter: 4}), sc)
	expectStatus(t, rep, got, map[string]invariant.Status{"PoisonRouted": invariant.Fail})
}

// consumeOnly hides every optional interface of the wrapped target.
type consumeOnly struct{ core.Target }

func TestUnsupportedFaultFailsFast(t *testing.T) {
	for _, f := range []ackhammer.Fault{fault.RestartConsumer(0), fault.GracefulStop(0), fault.KillProcess(0)} {
		_, err := ackhammer.Execute(t.Context(), consumeOnly{memtarget.New(memtarget.Config{})}, ackhammer.Scenario{
			Name:   "unsupported",
			Faults: []ackhammer.Fault{f},
			Seed:   1,
		})
		if !errors.Is(err, core.ErrUnsupported) {
			t.Errorf("%s: err = %v, want ErrUnsupported", f.Name(), err)
		}
	}
}

func TestSeedMakesFaultSelectionReproducible(t *testing.T) {
	selected := func(seed int64) []string {
		rep, err := ackhammer.Execute(t.Context(), memtarget.New(memtarget.Config{}), ackhammer.Scenario{
			Name:     "random",
			Messages: 50,
			Faults:   []ackhammer.Fault{fault.HandlerError(fault.Random(0.3), 1)},
			Seed:     seed,
		})
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, e := range rep.Events {
			if e.Type == ledger.FaultInjected && e.MsgID != "" {
				ids = append(ids, e.MsgID)
			}
		}
		slices.Sort(ids)
		return ids
	}
	a, b, c := selected(99), selected(99), selected(100)
	if !slices.Equal(a, b) {
		t.Errorf("same seed, different selection:\n%v\n%v", a, b)
	}
	if slices.Equal(a, c) {
		t.Errorf("different seeds, same selection: %v", a)
	}
}

func TestSeedFromEnvironment(t *testing.T) {
	t.Setenv("ACKHAMMER_SEED", "1234")
	rep, err := ackhammer.Execute(t.Context(), memtarget.New(memtarget.Config{}), ackhammer.Scenario{Name: "env-seed", Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Seed != 1234 {
		t.Errorf("seed = %d, want 1234", rep.Seed)
	}
}
