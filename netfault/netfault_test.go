package netfault_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/AristideDongo/ackhammer"
	"github.com/AristideDongo/ackhammer/brokers"
	"github.com/AristideDongo/ackhammer/fault"
	"github.com/AristideDongo/ackhammer/ledger"
	"github.com/AristideDongo/ackhammer/memtarget"
	"github.com/AristideDongo/ackhammer/netfault"
)

// remoteTarget publishes to a memtarget broker directly and consumes it
// over TCP through Toxiproxy.
type remoteTarget struct {
	*memtarget.Target
	remote *memtarget.Remote
}

func (r remoteTarget) Name() string { return "memtarget+toxiproxy" }

func (r remoteTarget) Consume(ctx context.Context, h ackhammer.Handler) error {
	return r.remote.Consume(ctx, h)
}

func setup(t *testing.T, cfg memtarget.Config) (ackhammer.Target, *netfault.Proxy) {
	t.Helper()
	brokers.Require(t)
	broker := memtarget.New(cfg)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go broker.Serve(l)

	tp := brokers.Toxiproxy(t, nil, brokers.Proxy{Name: "broker", HostPort: l.Addr().(*net.TCPAddr).Port})
	p, err := netfault.Connect(tp.API, "broker")
	if err != nil {
		t.Fatal(err)
	}
	remote, err := memtarget.Dial(tp.Endpoint("broker"), 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { remote.Close() })
	return remoteTarget{broker, remote}, p
}

func TestCutReconnects(t *testing.T) {
	target, p := setup(t, memtarget.Config{AckDeadline: 300 * time.Millisecond})
	rep, err := ackhammer.Execute(t.Context(), target, ackhammer.Scenario{
		Name:     "network-cut",
		Messages: 30,
		Faults: []ackhammer.Fault{
			fault.SlowHandler(fault.EveryNth(5), 100*time.Millisecond),
			netfault.Cut(p, 150*time.Millisecond, 500*time.Millisecond),
		},
		Expect: ackhammer.AtLeastOnce().WithRedeliveryWithin(3 * time.Second),
		Seed:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Failed() {
		t.Fatal("\n" + rep.Text())
	}
	// The ack of the delivery in flight is lost with the connection.
	redelivered := false
	for _, e := range rep.Events {
		redelivered = redelivered || (e.Type == ledger.Received && e.Attempt > 1)
	}
	if !redelivered {
		t.Errorf("the cut had no visible effect:\n%s", rep.Text())
	}
}

func TestLatency(t *testing.T) {
	target, p := setup(t, memtarget.Config{AckDeadline: time.Second})
	ackhammer.Run(t, target, ackhammer.Scenario{
		Name:     "network-latency",
		Messages: 20,
		Faults:   []ackhammer.Fault{netfault.Latency(p, 0, 50*time.Millisecond, 10*time.Millisecond, 400*time.Millisecond)},
		Expect:   ackhammer.OrderedPerKey().WithMaxRedeliveries(0),
		Seed:     2,
	})
}
