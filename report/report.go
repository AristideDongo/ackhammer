package report

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/AristideDongo/ackhammer/invariant"
	"github.com/AristideDongo/ackhammer/ledger"
)

// Report contains test results.
type Report struct {
	Name     string
	Seed     int64
	Target   string
	Results  []invariant.Result
	Events   []ledger.Event
	Replay   string
}

// PrintText prints text report.
func (r *Report) PrintText() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("✗ %s  (seed %d, target %s)\n\n", r.Name, r.Seed, r.Target))
	for _, res := range r.Results {
		status := "PASS"
		if !res.Pass && res.Message != "SKIP" {
			status = "FAIL"
		}
		if res.Message == "SKIP" {
			status = "SKIP"
		}
		sb.WriteString(fmt.Sprintf("  %-24s %s\n", res.Name+" .........", status))
	}
	if r.Replay != "" {
		sb.WriteString(fmt.Sprintf("\n  Replay : %s\n", r.Replay))
	}
	return sb.String()
}

// ToJSON returns JSON.
func (r *Report) ToJSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// ToJUnit returns JUnit XML.
func (r *Report) ToJUnit() string {
	return `<?xml version="1.0" encoding="UTF-8"?><testsuite></testsuite>`
}

// ToHTML returns HTML.
func (r *Report) ToHTML() string {
	return "<html><body>ackhammer report</body></html>"
}

// ToIssue returns GitHub issue markdown.
func (r *Report) ToIssue() string {
	var sb strings.Builder
	sb.WriteString("## Bug repro with Ackhammer\n\n")
	sb.WriteString(fmt.Sprintf("Scenario: %s (seed %d)\n\n", r.Name, r.Seed))
	sb.WriteString(r.PrintText())
	return sb.String()
}
