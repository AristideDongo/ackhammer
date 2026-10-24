// Package upstream reproduces real bugs reported in event consumer
// libraries. Each scenario names the issue and the affected version, and
// checks that Ackhammer detects the bug; when a fix or a workaround exists,
// a second subtest checks that Ackhammer turns green with it.
//
// If an "affected" subtest starts failing after a dependency upgrade, the bug
// was probably fixed upstream: move the scenario to the fixed version.
package upstream

import (
	"testing"

	"github.com/AristideDongo/ackhammer"
	"github.com/AristideDongo/ackhammer/invariant"
)

func execute(t *testing.T, target ackhammer.Target, sc ackhammer.Scenario) *ackhammer.Report {
	t.Helper()
	rep, err := ackhammer.Execute(t.Context(), target, sc)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + rep.Text())
	return rep
}

func result(rep *ackhammer.Report, name string) invariant.Result {
	for _, r := range rep.Results {
		if r.Name == name {
			return r
		}
	}
	return invariant.Result{Name: name}
}
