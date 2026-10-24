package cloudevents_test

import (
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/protocol/gochan"

	"github.com/AristideDongo/ackhammer"
	"github.com/AristideDongo/ackhammer/fault"
	"github.com/AristideDongo/ackhammer/invariant"
	"github.com/AristideDongo/ackhammer/targets/cloudevents"
)

func newTarget(t *testing.T) *cloudevents.Target {
	t.Helper()
	c, err := ce.NewClient(gochan.New())
	if err != nil {
		t.Fatal(err)
	}
	return cloudevents.New(cloudevents.Config{Sender: c})
}

func TestGoChan(t *testing.T) {
	ackhammer.Run(t, newTarget(t), ackhammer.Scenario{
		Name:     "healthy",
		Messages: 50,
		Faults:   []ackhammer.Fault{fault.SlowHandler(fault.EveryNth(10), 20*time.Millisecond)},
		Expect:   ackhammer.AtLeastOnce().WithMaxRedeliveries(0),
		Seed:     1,
	})
}

func TestMessageFieldsSurviveTheRoundTrip(t *testing.T) {
	rep, err := ackhammer.Execute(t.Context(), newTarget(t), ackhammer.Scenario{Name: "fields", Messages: 3, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	published := map[string]string{}
	for _, e := range rep.Events {
		switch e.Type {
		case "published":
			published[e.MsgID] = e.Key + "|" + e.Traceparent
		case "received":
			if got := e.Key + "|" + e.Traceparent; got != published[e.MsgID] {
				t.Errorf("%s: received %q, published %q", e.MsgID, got, published[e.MsgID])
			}
		}
	}
}

// The gochan protocol drops nacked events: Ackhammer must report the loss.
func TestGoChanLosesNackedEvents(t *testing.T) {
	rep, err := ackhammer.Execute(t.Context(), newTarget(t), ackhammer.Scenario{
		Name:         "nack-is-loss",
		Faults:       []ackhammer.Fault{fault.HandlerError(fault.OnMessage(2), 1)},
		Expect:       ackhammer.AtLeastOnce(),
		Seed:         1,
		DrainTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if r := rep.Results[0]; r.Name != "NoLoss" || r.Status != invariant.Fail {
		t.Errorf("want NoLoss FAIL\n%s", rep.Text())
	}
}
