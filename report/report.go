// Package report formats the outcome of a run. It never recomputes a
// verdict: everything comes from the invariant results and the ledger.
package report

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/AristideDongo/ackhammer/invariant"
	"github.com/AristideDongo/ackhammer/ledger"
)

// Report is the outcome of one scenario run.
type Report struct {
	Scenario string             `json:"scenario"`
	Target   string             `json:"target"`
	Seed     int64              `json:"seed"`
	Started  time.Time          `json:"started"`
	Duration time.Duration      `json:"duration_ns"`
	Results  []invariant.Result `json:"results"`
	Events   []ledger.Event     `json:"events"`
	Replay   string             `json:"replay,omitempty"`
	// Environment holds the Go version and the versions of the messaging
	// libraries linked in the test binary.
	Environment map[string]string `json:"environment,omitempty"`
}

// Read decodes a report written by JSON.
func Read(r io.Reader) (*Report, error) {
	var rep Report
	if err := json.NewDecoder(r).Decode(&rep); err != nil {
		return nil, err
	}
	return &rep, nil
}

// Failed reports whether at least one invariant failed.
func (r *Report) Failed() bool {
	for _, res := range r.Results {
		if res.Status == invariant.Fail {
			return true
		}
	}
	return false
}

// MaxTimelines is the number of failing messages shown in the text report.
const MaxTimelines = 3

// Text renders the report for a terminal.
func (r *Report) Text() string {
	var b strings.Builder
	mark := "✓"
	if r.Failed() {
		mark = "✗"
	}
	fmt.Fprintf(&b, "%s %s  (seed %d, target %s, %s)\n\n", mark, r.Scenario, r.Seed, r.Target, r.Duration.Round(time.Millisecond))
	for _, res := range r.Results {
		line := fmt.Sprintf("  %s %s %s", res.Name, strings.Repeat(".", max(2, 28-len(res.Name))), res.Status)
		if res.Summary != "" {
			line += "  " + res.Summary
		}
		b.WriteString(line + "\n")
	}

	shown := map[string]bool{}
	for _, res := range r.Results {
		if res.Status != invariant.Fail {
			continue
		}
		for _, id := range res.MsgIDs {
			if len(shown) == MaxTimelines {
				break
			}
			if !shown[id] {
				shown[id] = true
				b.WriteString("\n" + r.Timeline(id))
			}
		}
	}
	if r.Replay != "" && r.Failed() {
		fmt.Fprintf(&b, "\n  Replay : %s\n", r.Replay)
	}
	return b.String()
}

// Timeline renders the events of one message, interleaved with the timed
// faults, with times relative to the start of the run.
func (r *Report) Timeline(id string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "  Timeline %s :\n", id)
	delivered := false
	for _, e := range r.Events {
		timed := e.Type == ledger.FaultInjected && e.MsgID == ""
		if e.MsgID != id && !timed && e.Type != ledger.ConsumerStuck {
			continue
		}
		label := string(e.Type)
		if e.Attempt > 0 {
			label += fmt.Sprintf(" #%d", e.Attempt)
		}
		if e.Detail != "" {
			label += "  " + e.Detail
		}
		if timed {
			label = "── " + label
		}
		fmt.Fprintf(&b, "    %+8dms  %s\n", e.Time.Sub(r.Started).Milliseconds(), label)
		delivered = delivered || (e.MsgID == id && e.Type == ledger.Received)
	}
	if !delivered {
		fmt.Fprintf(&b, "    %10s  never delivered\n", "—")
	}
	return b.String()
}

// JSON renders the report for machines and replay.
func (r *Report) JSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}
