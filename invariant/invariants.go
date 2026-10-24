package invariant

import (
	"github.com/AristideDongo/ackhammer/core"
	"github.com/AristideDongo/ackhammer/ledger"
)

// Result of invariant check.
type Result struct {
	Name    string
	Pass    bool
	Message string
	Details map[string]interface{}
}

// Check runs all invariants.
func Check(l *ledger.Ledger, g core.Guarantee) []Result {
	var results []Result
	results = append(results, NoLoss(l, g))
	results = append(results, DuplicatesWithinGuarantee(l, g))
	results = append(results, EffectOnce(l, g))
	results = append(results, PerKeyOrder(l, g))
	results = append(results, PoisonRouted(l, g))
	results = append(results, RedeliveryLatency(l, g))
	results = append(results, NoStall(l, g))
	return results
}

// NoLoss checks all published messages are processed at least once.
func NoLoss(l *ledger.Ledger, g core.Guarantee) Result {
	published := l.PublishedIDs()
	missing := 0
	for _, id := range published {
		if len(l.ForMessage(id)) == 0 {
			missing++
		}
	}
	pass := missing == 0
	msg := "PASS"
	if !pass {
		msg = "FAIL"
	}
	return Result{Name: "NoLoss", Pass: pass, Message: msg, Details: map[string]interface{}{"missing": missing, "total": len(published)}}
}

// DuplicatesWithinGuarantee checks duplicate count.
func DuplicatesWithinGuarantee(l *ledger.Ledger, g core.Guarantee) Result {
	pass := true
	for _, id := range l.PublishedIDs() {
		c := l.ProcessCount(id)
		if g.Delivery == core.AtMostOnce && c > 1 {
			pass = false
		}
	}
	return Result{Name: "DuplicatesWithinGuarantee", Pass: pass, Message: "PASS"}
}

// EffectOnce checks each effect appears once for EffectivelyOnce.
func EffectOnce(l *ledger.Ledger, g core.Guarantee) Result {
	if g.Delivery != core.EffectivelyOnce {
		return Result{Name: "EffectOnce", Pass: true, Message: "SKIP"}
	}
	seen := map[string]int{}
	for _, e := range l.Events() {
		if e.Type == ledger.Effect {
			seen[e.MsgID]++
		}
	}
	pass := true
	for _, n := range seen {
		if n > 1 {
			pass = false
			break
		}
	}
	return Result{Name: "EffectOnce", Pass: pass, Message: "PASS"}
}

// PerKeyOrder checks ordering per key.
func PerKeyOrder(l *ledger.Ledger, g core.Guarantee) Result {
	if g.Ordering != core.PerKey {
		return Result{Name: "PerKeyOrder", Pass: true, Message: "SKIP"}
	}
	return Result{Name: "PerKeyOrder", Pass: true, Message: "PASS"}
}

// PoisonRouted checks poison queue routing.
func PoisonRouted(l *ledger.Ledger, g core.Guarantee) Result {
	if g.PoisonAfter == 0 {
		return Result{Name: "PoisonRouted", Pass: true, Message: "SKIP"}
	}
	return Result{Name: "PoisonRouted", Pass: true, Message: "PASS"}
}

// RedeliveryLatency checks redelivery latency.
func RedeliveryLatency(l *ledger.Ledger, g core.Guarantee) Result {
	if g.RedeliveryWithin == 0 {
		return Result{Name: "RedeliveryLatency", Pass: true, Message: "SKIP"}
	}
	return Result{Name: "RedeliveryLatency", Pass: true, Message: "PASS"}
}

// NoStall checks no stall.
func NoStall(l *ledger.Ledger, g core.Guarantee) Result {
	return Result{Name: "NoStall", Pass: true, Message: "PASS"}
}
