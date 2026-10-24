// Package ackhammer is a fault-injection test bench for Go event consumers.
//
// Plug your existing subscriber into a Target, declare the guarantee you
// think you have, and Ackhammer publishes traceable messages, injects
// faults, checks invariants on what really happened, and prints a report
// you can replay with a seed.
//
//	ackhammer.Run(t, target, ackhammer.Scenario{
//		Name:     "slow-message-in-batch",
//		Messages: 10,
//		Faults:   []ackhammer.Fault{fault.SlowHandler(fault.OnMessage(3), time.Second)},
//		Expect:   ackhammer.AtLeastOnce(),
//		Seed:     42,
//	})
package ackhammer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/AristideDongo/ackhammer/core"
	"github.com/AristideDongo/ackhammer/internal/runner"
	"github.com/AristideDongo/ackhammer/report"
)

type (
	Message         = core.Message
	Handler         = core.Handler
	Target          = core.Target
	Restartable     = core.Restartable
	ConsumerStopper = core.ConsumerStopper
	PoisonInspector = core.PoisonInspector
	Guarantee       = core.Guarantee
	Delivery        = core.Delivery
	Ordering        = core.Ordering
	Fault           = core.Fault
	Scenario        = core.Scenario
	Report          = report.Report
)

// Unlimited disables the MaxRedeliveries check.
const Unlimited = core.Unlimited

// ReportDirEnv, when set, makes Run write every report as JSON in that
// directory.
const ReportDirEnv = "ACKHAMMER_REPORT_DIR"

// AtLeastOnce: every message is handled at least once; duplicates are
// allowed and not counted.
func AtLeastOnce() Guarantee {
	return Guarantee{Delivery: core.AtLeastOnce, Ordering: core.NoOrder, MaxRedeliveries: core.Unlimited}
}

// AtMostOnce: no message is delivered twice; loss is allowed.
func AtMostOnce() Guarantee {
	return Guarantee{Delivery: core.AtMostOnce, Ordering: core.NoOrder}
}

// EffectivelyOnce: transport duplicates are allowed, but each business
// effect recorded with RecordEffect must appear only once.
func EffectivelyOnce() Guarantee {
	return Guarantee{Delivery: core.EffectivelyOnce, Ordering: core.NoOrder, MaxRedeliveries: core.Unlimited}
}

// OrderedPerKey: at-least-once, and messages with the same key are handled
// in publication order.
func OrderedPerKey() Guarantee {
	return AtLeastOnce().WithOrdering(core.PerKey)
}

// RecordEffect records a business effect for the EffectOnce invariant. Call
// it from the handler, with the handler's context, once the effect is
// applied. The effect name must identify the effect, for example
// "charge:order-42".
func RecordEffect(ctx context.Context, effect string) { core.RecordEffect(ctx, effect) }

// Execute runs a scenario outside of go test and returns its report.
func Execute(ctx context.Context, target Target, sc Scenario) (*Report, error) {
	return runner.Execute(ctx, target, sc)
}

// Run runs sc as a subtest of t. Each failed invariant fails the subtest,
// and the text report is logged with a replay command.
func Run(t *testing.T, target Target, sc Scenario) {
	t.Helper()
	t.Run(sc.Name, func(t *testing.T) {
		rep, err := runner.Execute(t.Context(), target, sc)
		if err != nil {
			t.Fatal(err)
		}
		rep.Replay = fmt.Sprintf("%s=%d go test -run '^%s$'", runner.SeedEnv, rep.Seed, regexp.QuoteMeta(t.Name()))
		if dir := os.Getenv(ReportDirEnv); dir != "" {
			if err := writeJSON(dir, t.Name(), rep); err != nil {
				t.Errorf("ackhammer: write report: %v", err)
			}
		}
		if rep.Failed() {
			t.Error("\n" + rep.Text())
		} else {
			t.Log("\n" + rep.Text())
		}
	})
}

func writeJSON(dir, name string, rep *Report) error {
	data, err := rep.JSON()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	file := regexp.MustCompile(`[^A-Za-z0-9_.-]+`).ReplaceAllString(name, "_") + ".json"
	return os.WriteFile(filepath.Join(dir, file), data, 0o644)
}
