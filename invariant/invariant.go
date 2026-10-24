// Package invariant checks properties on the ledger once a run is over.
//
// Invariants are pure functions of the ledger and the declared guarantee:
// they never act on the target.
package invariant

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/AristideDongo/ackhammer/core"
	"github.com/AristideDongo/ackhammer/ledger"
)

// Status is the outcome of one invariant.
type Status string

const (
	Pass Status = "PASS"
	Fail Status = "FAIL"
	Skip Status = "SKIP" // not applicable to the declared guarantee
)

// Result is the outcome of one invariant.
type Result struct {
	Name    string   `json:"name"`
	Status  Status   `json:"status"`
	Summary string   `json:"summary,omitempty"`
	MsgIDs  []string `json:"msg_ids,omitempty"` // messages that break the invariant
}

// Input is what invariants are checked against.
type Input struct {
	View      *ledger.View
	Guarantee core.Guarantee
	// PoisonSupported is true when the target implements PoisonInspector,
	// so that the absence of Poisoned events is meaningful.
	PoisonSupported bool
}

// Check runs every invariant.
func Check(in Input) []Result {
	return []Result{
		NoLoss(in),
		DuplicatesWithinGuarantee(in),
		EffectOnce(in),
		PerKeyOrder(in),
		PoisonRouted(in),
		RedeliveryLatency(in),
		NoStall(in),
	}
}

func skip(name, why string) Result { return Result{Name: name, Status: Skip, Summary: why} }

func verdict(name string, bad []string, summary string) Result {
	if len(bad) == 0 {
		return Result{Name: name, Status: Pass}
	}
	return Result{Name: name, Status: Fail, Summary: summary, MsgIDs: bad}
}

// accounted reports whether a message reached a final state: handled
// successfully, or found in the poison queue.
func accounted(v *ledger.View, id string) bool {
	return v.Count(id, ledger.Succeeded) > 0 || v.Count(id, ledger.Poisoned) > 0
}

// NoLoss: every published message is handled at least once, or is in the
// poison queue, by the end of the drain.
func NoLoss(in Input) Result {
	const name = "NoLoss"
	if in.Guarantee.Delivery == core.AtMostOnce {
		return skip(name, "at-most-once allows loss")
	}
	var bad []string
	for _, p := range in.View.Published {
		if !accounted(in.View, p.MsgID) {
			bad = append(bad, p.MsgID)
		}
	}
	return verdict(name, bad, fmt.Sprintf("%d/%d messages never handled", len(bad), len(in.View.Published)))
}

// DuplicatesWithinGuarantee: deliveries respect the declared limit.
// At-most-once allows a single delivery. Otherwise, deliveries after the
// first success must not exceed MaxRedeliveries.
func DuplicatesWithinGuarantee(in Input) Result {
	const name = "DuplicatesWithinGuarantee"
	g := in.Guarantee
	var bad []string
	switch {
	case g.Delivery == core.AtMostOnce:
		for _, p := range in.View.Published {
			if in.View.Count(p.MsgID, ledger.Received) > 1 {
				bad = append(bad, p.MsgID)
			}
		}
		return verdict(name, bad, fmt.Sprintf("%d messages delivered more than once", len(bad)))
	case g.MaxRedeliveries == core.Unlimited:
		return skip(name, "MaxRedeliveries is Unlimited")
	}
	for _, p := range in.View.Published {
		first, ok := in.View.First(p.MsgID, ledger.Succeeded)
		if !ok {
			continue
		}
		after := 0
		for _, e := range in.View.ByMsg[p.MsgID] {
			if e.Type == ledger.Received && e.Seq > first.Seq {
				after++
			}
		}
		if after > g.MaxRedeliveries {
			bad = append(bad, p.MsgID)
		}
	}
	return verdict(name, bad, fmt.Sprintf("%d messages redelivered after success more than %d time(s)", len(bad), g.MaxRedeliveries))
}

// EffectOnce: with EffectivelyOnce, each business effect is recorded once.
func EffectOnce(in Input) Result {
	const name = "EffectOnce"
	if in.Guarantee.Delivery != core.EffectivelyOnce {
		return skip(name, "only for EffectivelyOnce")
	}
	count := map[string]int{}
	owner := map[string]string{}
	for _, e := range in.View.Events {
		if e.Type == ledger.Effect {
			count[e.Detail]++
			owner[e.Detail] = e.MsgID
		}
	}
	if len(count) == 0 {
		return Result{Name: name, Status: Fail, Summary: "no effect recorded: call ackhammer.RecordEffect in the handler"}
	}
	seen := map[string]bool{}
	var bad []string
	dup := 0
	for effect, n := range count {
		if n > 1 {
			dup++
			if id := owner[effect]; !seen[id] {
				seen[id] = true
				bad = append(bad, id)
			}
		}
	}
	sort.Strings(bad)
	return verdict(name, bad, fmt.Sprintf("%d effects applied more than once", dup))
}

// PerKeyOrder: for each key, messages are first handled successfully in
// publication order.
func PerKeyOrder(in Input) Result {
	const name = "PerKeyOrder"
	if in.Guarantee.Ordering != core.PerKey {
		return skip(name, "ordering not declared")
	}
	last := map[string]int{} // key -> seq of the last first-success
	var bad []string
	for _, p := range in.View.Published {
		first, ok := in.View.First(p.MsgID, ledger.Succeeded)
		if !ok {
			continue
		}
		if first.Seq < last[p.Key] {
			bad = append(bad, p.MsgID)
			continue
		}
		last[p.Key] = first.Seq
	}
	return verdict(name, bad, fmt.Sprintf("%d messages handled before an earlier message with the same key", len(bad)))
}

// PoisonRouted: a message that failed PoisonAfter times is in the poison
// queue, and every message there failed exactly PoisonAfter times.
func PoisonRouted(in Input) Result {
	const name = "PoisonRouted"
	n := in.Guarantee.PoisonAfter
	if n == 0 {
		return skip(name, "no poison queue declared")
	}
	if !in.PoisonSupported {
		return Result{Name: name, Status: Fail, Summary: "target does not implement PoisonInspector: poison routing cannot be verified"}
	}
	var missing, wrong []string
	for _, p := range in.View.Published {
		failures := in.View.Count(p.MsgID, ledger.Failed)
		poisoned := in.View.Count(p.MsgID, ledger.Poisoned) > 0
		switch {
		case failures >= n && !poisoned:
			missing = append(missing, p.MsgID)
		case poisoned && failures != n:
			wrong = append(wrong, p.MsgID)
		}
	}
	bad := append(missing, wrong...)
	return verdict(name, bad, fmt.Sprintf("%d messages missing from the poison queue, %d routed after a number of failures other than %d", len(missing), len(wrong), n))
}

// RedeliveryLatency: after a handler failure or an injected fault, the
// affected messages are delivered again within RedeliveryWithin.
//
// A message is affected by a timed fault if it was published before the
// fault and had not reached a final state. It recovers when it is delivered
// again or when the delivery in flight completes.
func RedeliveryLatency(in Input) Result {
	const name = "RedeliveryLatency"
	within := in.Guarantee.RedeliveryWithin
	if within == 0 {
		return skip(name, "RedeliveryWithin not declared")
	}
	v := in.View
	badSet := map[string]bool{}
	var worst time.Duration
	// check finds the next delivery (or the completion of the delivery in
	// flight) after afterSeq and compares its delay with the limit.
	check := func(id string, since time.Time, afterSeq int, resolved ...ledger.Type) {
		for _, e := range v.ByMsg[id] {
			if e.Seq > afterSeq && slices.Contains(resolved, e.Type) {
				if d := e.Time.Sub(since); d > within {
					badSet[id] = true
					worst = max(worst, d)
				}
				return
			}
		}
		badSet[id] = true // never delivered again
	}

	for _, p := range v.Published {
		events := v.ByMsg[p.MsgID]
		failures := 0
		for _, e := range events {
			if e.Type != ledger.Failed {
				continue
			}
			failures++
			if in.Guarantee.PoisonAfter > 0 && failures >= in.Guarantee.PoisonAfter {
				break // routed to poison, no redelivery expected
			}
			check(p.MsgID, e.Time, e.Seq, ledger.Received)
		}
	}
	for _, f := range v.Faults {
		for _, p := range v.Published {
			if p.Seq > f.Seq {
				continue
			}
			if settledBefore(v, p.MsgID, f.Seq, in.Guarantee.PoisonAfter) {
				continue
			}
			check(p.MsgID, f.Time, f.Seq, ledger.Received, ledger.Succeeded)
		}
	}

	bad := make([]string, 0, len(badSet))
	for _, p := range v.Published {
		if badSet[p.MsgID] {
			bad = append(bad, p.MsgID)
		}
	}
	summary := fmt.Sprintf("%d messages not redelivered within %s", len(bad), within)
	if worst > 0 {
		summary += fmt.Sprintf(" (worst %s)", worst.Round(time.Millisecond))
	}
	return verdict(name, bad, summary)
}

// settledBefore reports whether a message reached a final state (handled,
// or failed enough times to be poisoned) before event seq.
func settledBefore(v *ledger.View, id string, seq, poisonAfter int) bool {
	failures := 0
	for _, e := range v.ByMsg[id] {
		if e.Seq > seq {
			break
		}
		switch e.Type {
		case ledger.Succeeded:
			return true
		case ledger.Failed:
			failures++
		}
	}
	return poisonAfter > 0 && failures >= poisonAfter
}

// NoStall: the consumer keeps making progress. It fails if Consume did not
// return when cancelled, if it returned without being asked to, or if messages are left pending and nothing was
// handled after the last fault.
func NoStall(in Input) Result {
	const name = "NoStall"
	v := in.View
	for _, e := range v.Events {
		switch e.Type {
		case ledger.ConsumerStuck:
			return Result{Name: name, Status: Fail, Summary: "Consume did not return after its context was cancelled"}
		case ledger.ConsumerExit:
			return Result{Name: name, Status: Fail, Summary: "Consume returned on its own: " + e.Detail}
		}
	}
	var pending []string
	for _, p := range v.Published {
		if !accounted(v, p.MsgID) {
			pending = append(pending, p.MsgID)
		}
	}
	if len(pending) == 0 {
		return Result{Name: name, Status: Pass}
	}
	since := 0
	if len(v.Faults) > 0 {
		since = v.Faults[len(v.Faults)-1].Seq
	}
	for _, e := range v.Events {
		if e.Seq > since && e.Type == ledger.Succeeded {
			return Result{Name: name, Status: Pass}
		}
	}
	return Result{Name: name, Status: Fail, Summary: fmt.Sprintf("no progress after the last fault, %d messages pending", len(pending)), MsgIDs: pending}
}
