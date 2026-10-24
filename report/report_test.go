package report

import (
	"bytes"
	"encoding/xml"
	"strings"
	"testing"
	"time"

	"github.com/AristideDongo/ackhammer/invariant"
	"github.com/AristideDongo/ackhammer/ledger"
)

func sample() *Report {
	t0 := time.Unix(1000, 0)
	return &Report{
		Scenario: "slow-<batch>",
		Target:   "memtarget",
		Seed:     42,
		Started:  t0,
		Duration: 1500 * time.Millisecond,
		Results: []invariant.Result{
			{Name: "NoLoss", Status: invariant.Fail, Summary: "1/2 messages never handled", MsgIDs: []string{"msg-2"}},
			{Name: "PerKeyOrder", Status: invariant.Skip, Summary: "ordering not declared"},
			{Name: "NoStall", Status: invariant.Pass},
		},
		Events: []ledger.Event{
			{Seq: 1, Type: ledger.Published, MsgID: "msg-1", Time: t0},
			{Seq: 2, Type: ledger.Published, MsgID: "msg-2", Time: t0},
			{Seq: 3, Type: ledger.FaultInjected, Detail: "CancelDuringReceive(10ms)", Time: t0.Add(10 * time.Millisecond)},
		},
		Replay:      "ACKHAMMER_SEED=42 go test -run '^TestX$'",
		Environment: map[string]string{"go": "go1.27.2"},
	}
}

func TestText(t *testing.T) {
	out := sample().Text()
	for _, want := range []string{"✗ slow-<batch>", "NoLoss", "FAIL  1/2 messages never handled", "Timeline msg-2", "never delivered", "── fault  CancelDuringReceive", "Replay : ACKHAMMER_SEED=42"} {
		if !strings.Contains(out, want) {
			t.Errorf("text report misses %q:\n%s", want, out)
		}
	}
}

func TestJSONRoundTrip(t *testing.T) {
	data, err := sample().JSON()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if got.Seed != 42 || len(got.Results) != 3 || got.Duration != 1500*time.Millisecond || got.Environment["go"] != "go1.27.2" {
		t.Errorf("round trip lost data: %+v", got)
	}
}

func TestJUnit(t *testing.T) {
	data, err := JUnit(sample(), sample())
	if err != nil {
		t.Fatal(err)
	}
	var doc junitSuites
	if err := xml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("invalid XML: %v\n%s", err, data)
	}
	s := doc.Suites[0]
	if len(doc.Suites) != 2 || s.Tests != 3 || s.Failures != 1 || s.Skipped != 1 || s.Time != "1.500" {
		t.Errorf("suite = %+v", s)
	}
	if f := s.Cases[0].Failure; f == nil || !strings.Contains(f.Body, "never delivered") {
		t.Errorf("failure body = %+v", f)
	}
}

func TestIssue(t *testing.T) {
	out := sample().Issue()
	for _, want := range []string{"### Ackhammer: `slow-<batch>`", "| go | `go1.27.2` |", "| NoLoss | FAIL | 1/2 messages never handled |", "```sh\nACKHAMMER_SEED=42"} {
		if !strings.Contains(out, want) {
			t.Errorf("issue misses %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "PerKeyOrder") {
		t.Error("skipped invariants must not appear in the issue")
	}
}

func TestHTMLEscapes(t *testing.T) {
	var buf bytes.Buffer
	if err := HTML(&buf, sample()); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if strings.Contains(out, "slow-<batch>") || !strings.Contains(out, "slow-&lt;batch&gt;") {
		t.Error("scenario name not escaped")
	}
	if !strings.Contains(out, `class="FAIL"`) || !strings.Contains(out, "prefers-color-scheme:dark") {
		t.Error("missing status class or dark mode")
	}
}
