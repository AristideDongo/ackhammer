// Package subprocess runs the consumer under test in a child process, so
// that KillProcess can kill it abruptly, exactly like a crash in production.
//
// The child runs the real consumer client (subscriber, receiver…). For each
// delivery it asks the parent to run the handler, then acks or nacks
// according to the answer. When the child is killed, the deliveries it held
// are never acked: the broker must redeliver them.
//
// The usual setup re-executes the test binary:
//
//	func TestMain(m *testing.M) {
//		if subprocess.IsChild("orders") {
//			os.Exit(subprocess.Serve(consumeOrders)) // builds the subscriber, calls Consume
//		}
//		os.Exit(m.Run())
//	}
//
//	target := subprocess.New(subprocess.Config{
//		Target:  publishingTarget, // Publish, Close and poison inspection
//		Command: subprocess.Self("orders"),
//	})
package subprocess

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/AristideDongo/ackhammer/core"
)

// ChildEnv names the child to run in a re-executed test binary.
const ChildEnv = "ACKHAMMER_SUBPROCESS"

// StopTimeout is how long the child has to exit after its stdin is closed
// before it is killed.
const StopTimeout = 5 * time.Second

// Config configures a Target.
type Config struct {
	// Target publishes, closes and inspects the poison queue. Its Consume
	// is never called.
	Target core.Target
	// Command builds the child command. It is called once per Consume.
	Command func() *exec.Cmd
}

// Target consumes through a child process.
type Target struct {
	cfg Config

	mu  sync.Mutex
	cmd *exec.Cmd
}

var _ core.Killer = (*Target)(nil)

// New returns a subprocess target. It implements core.PoisonInspector when
// cfg.Target does.
func New(cfg Config) core.Target {
	t := &Target{cfg: cfg}
	if _, ok := cfg.Target.(core.PoisonInspector); ok {
		return &poisonTarget{t}
	}
	return t
}

func (t *Target) Name() string { return t.cfg.Target.Name() + "+subprocess" }

func (t *Target) Publish(ctx context.Context, msg core.Message) error {
	return t.cfg.Target.Publish(ctx, msg)
}

func (t *Target) Close(ctx context.Context) error { return t.cfg.Target.Close(ctx) }

// Kill sends SIGKILL to the running child, if any.
func (t *Target) Kill() error {
	t.mu.Lock()
	cmd := t.cmd
	t.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return errors.New("subprocess: no consumer process running")
	}
	return cmd.Process.Kill()
}

type delivery struct {
	ID  uint64        `json:"id"`
	Msg *core.Message `json:"msg,omitempty"`
	Err *string       `json:"err,omitempty"`
}

// Consume starts the child and runs h for every delivery it reports. It
// returns when ctx is done (after the child exits) or when the child dies.
func (t *Target) Consume(ctx context.Context, h core.Handler) error {
	cmd := t.cfg.Command()
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if cmd.Stderr == nil {
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("subprocess: start: %w", err)
	}
	t.mu.Lock()
	t.cmd = cmd
	t.mu.Unlock()

	// Handlers run in this process but belong to the child: when the child
	// dies, their context is cancelled, as if they died with it.
	hctx, hcancel := context.WithCancel(ctx)
	defer hcancel()
	var (
		wmu sync.Mutex
		enc = json.NewEncoder(stdin)
		wg  sync.WaitGroup
	)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		dec := json.NewDecoder(bufio.NewReader(stdout))
		for {
			var d delivery
			if err := dec.Decode(&d); err != nil {
				return
			}
			if d.Msg == nil {
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				res := delivery{ID: d.ID}
				if err := h(hctx, *d.Msg); err != nil {
					s := err.Error()
					res.Err = &s
				}
				wmu.Lock()
				_ = enc.Encode(res) // fails if the child died: the ack is lost, as in a crash
				wmu.Unlock()
			}()
		}
	}()

	exited := make(chan error, 1)
	go func() {
		<-readDone
		hcancel()
		wg.Wait()
		exited <- cmd.Wait()
	}()

	select {
	case err := <-exited:
		t.clear(cmd)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("subprocess: consumer exited: %v", err)
	case <-ctx.Done():
	}
	_ = stdin.Close() // asks the child to stop
	select {
	case <-exited:
	case <-time.After(StopTimeout):
		_ = cmd.Process.Kill()
		<-exited
	}
	t.clear(cmd)
	return ctx.Err()
}

func (t *Target) clear(cmd *exec.Cmd) {
	t.mu.Lock()
	if t.cmd == cmd {
		t.cmd = nil
	}
	t.mu.Unlock()
}

type poisonTarget struct{ *Target }

func (p *poisonTarget) PoisonedMessages(ctx context.Context) ([]core.Message, error) {
	return p.cfg.Target.(core.PoisonInspector).PoisonedMessages(ctx)
}

// Self returns a Command that re-executes the current binary as the child
// named name. Use it from tests, with IsChild and Serve in TestMain.
func Self(name string) func() *exec.Cmd {
	return func() *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^$")
		cmd.Env = append(os.Environ(), ChildEnv+"="+name)
		return cmd
	}
}

// IsChild reports whether this process was started by Self(name).
func IsChild(name string) bool { return os.Getenv(ChildEnv) == name }

// Serve runs in the child. consume must build the real consumer and call
// its Consume-like loop with the given handler until ctx is done. Serve
// returns the process exit code.
func Serve(consume func(ctx context.Context, h core.Handler) error) int {
	return serve(os.Stdin, os.Stdout, consume)
}

func serve(in io.Reader, out io.Writer, consume func(context.Context, core.Handler) error) int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu      sync.Mutex
		next    uint64
		waiting = map[uint64]chan error{}
		enc     = json.NewEncoder(out)
	)
	go func() {
		defer cancel() // stdin closed: the parent asks us to stop
		dec := json.NewDecoder(bufio.NewReader(in))
		for {
			var d delivery
			if err := dec.Decode(&d); err != nil {
				return
			}
			mu.Lock()
			ch := waiting[d.ID]
			delete(waiting, d.ID)
			mu.Unlock()
			if ch == nil {
				continue
			}
			if d.Err != nil {
				ch <- errors.New(*d.Err)
			} else {
				ch <- nil
			}
		}
	}()

	h := func(hctx context.Context, msg core.Message) error {
		ch := make(chan error, 1)
		mu.Lock()
		next++
		id := next
		waiting[id] = ch
		err := enc.Encode(delivery{ID: id, Msg: &msg})
		mu.Unlock()
		if err != nil {
			return err
		}
		select {
		case err := <-ch:
			return err
		case <-hctx.Done():
			return hctx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := consume(ctx, h); err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, "subprocess: consume:", err)
		return 1
	}
	return 0
}
