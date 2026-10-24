package memtarget

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/AristideDongo/ackhammer/core"
)

// Serve exposes the broker over net/rpc on l, so that a consumer running in
// another process (see package subprocess) can use Dial. It returns when l
// is closed.
func (t *Target) Serve(l net.Listener) {
	s := rpc.NewServer()
	_ = s.RegisterName("Memtarget", &rpcBroker{t})
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		go s.ServeConn(conn)
	}
}

type rpcBroker struct{ t *Target }

// NextReply is the reply of Memtarget.Next.
type NextReply struct {
	OK    bool
	Msg   core.Message
	Token uint64
}

// SettleArgs are the arguments of Memtarget.Settle.
type SettleArgs struct {
	ID    string
	Token uint64
	Err   string // empty means ack
}

func (b *rpcBroker) Next(_ int, r *NextReply) error {
	e, token := b.t.next()
	if e != nil {
		r.OK, r.Msg, r.Token = true, e.msg, token
	}
	return nil
}

func (b *rpcBroker) Settle(a SettleArgs, _ *int) error {
	var err error
	if a.Err != "" {
		err = errors.New(a.Err)
	}
	b.t.settle(a.ID, a.Token, err)
	return nil
}

// Remote is a consumer connected to a broker served by Target.Serve.
type Remote struct {
	client      *rpc.Client
	concurrency int
}

// Dial connects to a broker served by Target.Serve.
func Dial(addr string, concurrency int) (*Remote, error) {
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Remote{client: c, concurrency: max(1, concurrency)}, nil
}

// Consume delivers messages to h until ctx is done.
func (r *Remote) Consume(ctx context.Context, h core.Handler) error {
	var wg sync.WaitGroup
	for range r.concurrency {
		wg.Go(func() {
			for ctx.Err() == nil {
				var next NextReply
				if err := r.client.Call("Memtarget.Next", 0, &next); err != nil {
					return
				}
				if !next.OK {
					select {
					case <-ctx.Done():
					case <-time.After(time.Millisecond):
					}
					continue
				}
				args := SettleArgs{ID: next.Msg.ID, Token: next.Token}
				if err := h(ctx, next.Msg); err != nil {
					args.Err = err.Error()
				}
				_ = r.client.Call("Memtarget.Settle", args, new(int))
			}
		})
	}
	wg.Wait()
	return ctx.Err()
}

// Close closes the connection.
func (r *Remote) Close() error { return r.client.Close() }
