package subprocess_test

import (
	"context"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/AristideDongo/ackhammer"
	"github.com/AristideDongo/ackhammer/fault"
	"github.com/AristideDongo/ackhammer/invariant"
	"github.com/AristideDongo/ackhammer/memtarget"
	"github.com/AristideDongo/ackhammer/subprocess"
)

const addrEnv = "MEMTARGET_ADDR"

func TestMain(m *testing.M) {
	if subprocess.IsChild("memtarget") {
		os.Exit(subprocess.Serve(func(ctx context.Context, h ackhammer.Handler) error {
			r, err := memtarget.Dial(os.Getenv(addrEnv), 1)
			if err != nil {
				return err
			}
			defer r.Close()
			return r.Consume(ctx, h)
		}))
	}
	os.Exit(m.Run())
}

// newTarget serves a memtarget broker in this process and consumes it from
// a child process.
func newTarget(t *testing.T, cfg memtarget.Config) ackhammer.Target {
	t.Helper()
	broker := memtarget.New(cfg)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go broker.Serve(l)
	child := subprocess.Self("memtarget")
	return subprocess.New(subprocess.Config{
		Target: broker,
		Command: func() *exec.Cmd {
			cmd := child()
			cmd.Env = append(cmd.Env, addrEnv+"="+l.Addr().String())
			return cmd
		},
	})
}

func TestConsumerInChildProcess(t *testing.T) {
	ackhammer.Run(t, newTarget(t, memtarget.Config{}), ackhammer.Scenario{
		Name:     "child-consumer",
		Messages: 20,
		Faults: []ackhammer.Fault{
			fault.HandlerError(fault.EveryNth(4), 1),
			fault.CancelDuringReceive(50 * time.Millisecond),
		},
		Expect: ackhammer.OrderedPerKey().WithRedeliveryWithin(2 * time.Second),
		Seed:   1,
	})
}

func TestKillProcessRedeliversAfterAckDeadline(t *testing.T) {
	ackhammer.Run(t, newTarget(t, memtarget.Config{AckDeadline: 300 * time.Millisecond}), ackhammer.Scenario{
		Name: "kill-mid-processing",
		Faults: []ackhammer.Fault{
			fault.SlowHandler(fault.OnMessage(1), 5*time.Second),
			fault.KillProcess(200 * time.Millisecond),
		},
		Expect: ackhammer.AtLeastOnce().WithRedeliveryWithin(2 * time.Second),
		Seed:   2,
	})
}

// A crash after the effect but before the ack applies the effect twice: a
// naive handler breaks effectively-once.
func TestKillProcessDuplicatesNaiveEffects(t *testing.T) {
	rep, err := ackhammer.Execute(t.Context(), newTarget(t, memtarget.Config{AckDeadline: 300 * time.Millisecond}), ackhammer.Scenario{
		Name: "kill-after-effect",
		Handler: func(ctx context.Context, m ackhammer.Message) error {
			ackhammer.RecordEffect(ctx, "charge:"+m.ID)
			return nil
		},
		Faults: []ackhammer.Fault{
			fault.SlowHandler(fault.OnMessage(1), 5*time.Second),
			fault.KillProcess(200 * time.Millisecond),
		},
		Expect: ackhammer.EffectivelyOnce(),
		Seed:   3,
	})
	if err != nil {
		t.Fatal(err)
	}
	status := map[string]invariant.Status{}
	for _, r := range rep.Results {
		status[r.Name] = r.Status
	}
	t.Log("\n" + rep.Text())
	if status["NoLoss"] != invariant.Pass || status["EffectOnce"] != invariant.Fail {
		t.Errorf("want NoLoss PASS and EffectOnce FAIL\n%s", rep.Text())
	}
}
