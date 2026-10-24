package invariant

import (
	"strings"
	"testing"
	"time"

	"github.com/AristideDongo/ackhammer/core"
	"github.com/AristideDongo/ackhammer/ledger"
)

// history builds a ledger view from a compact script. Each step is
// (type, message id, offset in ms); faults without a message use "".
type step struct {
	typ ledger.Type
	id  string
	ms  int
}

func history(steps ...step) *ledger.View {
	t0 := time.Unix(0, 0)
	keys := map[string]string{"m1": "a", "m2": "a", "m3": "b"}
	var events []ledger.Event
	for i, s := range steps {
		events = append(events, ledger.Event{
			Seq: i + 1, Type: s.typ, MsgID: s.id, Key: keys[s.id],
			Time: t0.Add(time.Duration(s.ms) * time.Millisecond),
		})
	}
	return ledger.NewView(events)
}

func pub(id string) step            { return step{ledger.Published, id, 0} }
func recv(id string, ms int) step   { return step{ledger.Received, id, ms} }
func ok(id string, ms int) step     { return step{ledger.Succeeded, id, ms} }
func fail(id string, ms int) step   { return step{ledger.Failed, id, ms} }
func timedFault(ms int) step        { return step{ledger.FaultInjected, "", ms} }
func effect(id string, ms int) step { return step{ledger.Effect, id, ms} }
func poisoned(id string) step       { return step{ledger.Poisoned, id, 0} }
func check(f func(Input) Result, v *ledger.View, g core.Guarantee) Result {
	return f(Input{View: v, Guarantee: g, PoisonSupported: true})
}

var atLeastOnce = core.Guarantee{Delivery: core.AtLeastOnce, Ordering: core.NoOrder, MaxRedeliveries: core.Unlimited}

func TestNoLoss(t *testing.T) {
	v := history(pub("m1"), pub("m2"), recv("m1", 1), ok("m1", 2))
	r := check(NoLoss, v, atLeastOnce)
	if r.Status != Fail || len(r.MsgIDs) != 1 || r.MsgIDs[0] != "m2" {
		t.Errorf("got %+v", r)
	}
	v = history(pub("m1"), recv("m1", 1), fail("m1", 2), poisoned("m1"))
	if r := check(NoLoss, v, atLeastOnce.WithPoisonAfter(1)); r.Status != Pass {
		t.Errorf("poisoned message counted as lost: %+v", r)
	}
	if r := check(NoLoss, history(pub("m1")), core.Guarantee{Delivery: core.AtMostOnce}); r.Status != Skip {
		t.Errorf("at-most-once: %+v", r)
	}
}

func TestDuplicatesWithinGuarantee(t *testing.T) {
	retried := history(pub("m1"), recv("m1", 1), fail("m1", 2), recv("m1", 3), ok("m1", 4))
	if r := check(DuplicatesWithinGuarantee, retried, atLeastOnce.WithMaxRedeliveries(0)); r.Status != Pass {
		t.Errorf("redelivery after failure is not a duplicate: %+v", r)
	}
	dup := history(pub("m1"), recv("m1", 1), ok("m1", 2), recv("m1", 3), ok("m1", 4))
	if r := check(DuplicatesWithinGuarantee, dup, atLeastOnce.WithMaxRedeliveries(0)); r.Status != Fail {
		t.Errorf("redelivery after success: %+v", r)
	}
	if r := check(DuplicatesWithinGuarantee, dup, atLeastOnce.WithMaxRedeliveries(1)); r.Status != Pass {
		t.Errorf("one allowed redelivery: %+v", r)
	}
	if r := check(DuplicatesWithinGuarantee, dup, atLeastOnce); r.Status != Skip {
		t.Errorf("unlimited: %+v", r)
	}
	if r := check(DuplicatesWithinGuarantee, retried, core.Guarantee{Delivery: core.AtMostOnce}); r.Status != Fail {
		t.Errorf("at-most-once with two deliveries: %+v", r)
	}
}

func TestEffectOnce(t *testing.T) {
	g := core.Guarantee{Delivery: core.EffectivelyOnce}
	v := history(pub("m1"), recv("m1", 1), effect("m1", 2), ok("m1", 3), recv("m1", 4), effect("m1", 5))
	v.Events[2].Detail, v.Events[5].Detail = "charge", "charge"
	if r := check(EffectOnce, v, g); r.Status != Fail {
		t.Errorf("double effect: %+v", r)
	}
	if r := check(EffectOnce, history(pub("m1"), recv("m1", 1), ok("m1", 2)), g); r.Status != Fail {
		t.Errorf("no effect recorded must fail loudly: %+v", r)
	}
}

func TestPerKeyOrder(t *testing.T) {
	g := atLeastOnce.WithOrdering(core.PerKey)
	inOrder := history(pub("m1"), pub("m2"), pub("m3"), ok("m3", 1), ok("m1", 2), ok("m2", 3))
	if r := check(PerKeyOrder, inOrder, g); r.Status != Pass {
		t.Errorf("other keys may interleave: %+v", r)
	}
	swapped := history(pub("m1"), pub("m2"), ok("m2", 1), ok("m1", 2))
	if r := check(PerKeyOrder, swapped, g); r.Status != Fail {
		t.Errorf("m2 before m1 on key a: %+v", r)
	}
}

func TestPoisonRouted(t *testing.T) {
	g := atLeastOnce.WithPoisonAfter(2)
	lost := history(pub("m1"), recv("m1", 1), fail("m1", 2), recv("m1", 3), fail("m1", 4))
	if r := check(PoisonRouted, lost, g); r.Status != Fail {
		t.Errorf("failed twice, not in poison queue: %+v", r)
	}
	early := history(pub("m1"), recv("m1", 1), fail("m1", 2), poisoned("m1"))
	if r := check(PoisonRouted, early, g); r.Status != Fail {
		t.Errorf("poisoned after one failure: %+v", r)
	}
	if r := PoisonRouted(Input{View: lost, Guarantee: g}); r.Status != Fail {
		t.Errorf("unsupported target must fail, not skip: %+v", r)
	}
}

func TestRedeliveryLatency(t *testing.T) {
	g := atLeastOnce.WithRedeliveryWithin(100 * time.Millisecond)
	fast := history(pub("m1"), recv("m1", 10), fail("m1", 20), recv("m1", 50), ok("m1", 60))
	if r := check(RedeliveryLatency, fast, g); r.Status != Pass {
		t.Errorf("%+v", r)
	}
	slow := history(pub("m1"), recv("m1", 10), fail("m1", 20), recv("m1", 500), ok("m1", 510))
	if r := check(RedeliveryLatency, slow, g); r.Status != Fail {
		t.Errorf("%+v", r)
	}
	// m1 was in flight when the fault fired and came back 1s later.
	afterFault := history(pub("m1"), pub("m2"), recv("m1", 10), ok("m2", 15), timedFault(20), recv("m1", 1020), ok("m1", 1030))
	r := check(RedeliveryLatency, afterFault, g)
	if r.Status != Fail || len(r.MsgIDs) != 1 || r.MsgIDs[0] != "m1" {
		t.Errorf("%+v", r)
	}
}

func TestNoStall(t *testing.T) {
	stuck := history(pub("m1"), recv("m1", 1), ok("m1", 2), step{ledger.ConsumerStuck, "", 3})
	if r := check(NoStall, stuck, atLeastOnce); r.Status != Fail {
		t.Errorf("stuck consumer: %+v", r)
	}
	exited := history(pub("m1"), step{ledger.ConsumerExit, "", 1})
	if r := check(NoStall, exited, atLeastOnce); r.Status != Fail || !strings.Contains(r.Summary, "returned on its own") {
		t.Errorf("consumer exit: %+v", r)
	}
	frozen := history(pub("m1"), pub("m2"), ok("m1", 1), timedFault(2))
	if r := check(NoStall, frozen, atLeastOnce); r.Status != Fail {
		t.Errorf("no progress after fault: %+v", r)
	}
	progressing := history(pub("m1"), pub("m2"), pub("m3"), timedFault(1), ok("m1", 2))
	if r := check(NoStall, progressing, atLeastOnce); r.Status != Pass {
		t.Errorf("progress after fault: %+v", r)
	}
}
