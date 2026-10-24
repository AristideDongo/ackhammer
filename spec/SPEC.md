# Ackhammer — Public Specification

See ACKHAMMER_SPEC.private.md for full internal details.

## Vision
Fault-injection test harness for Go event consumers. Branch your existing subscriber, declare guarantees, Ackhammer publishes traceable messages, injects realistic failures, verifies invariants.

## Guarantees
- AtLeastOnce | AtMostOnce | EffectivelyOnce
- NoOrder | PerKey | Total

## Invariants
- NoLoss, DuplicatesWithinGuarantee, EffectOnce, PerKeyOrder, PoisonRouted, RedeliveryLatency, NoStall
