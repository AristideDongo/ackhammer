// Package netfault injects network faults between the consumer and its
// broker with Toxiproxy: connection cuts and latency.
//
// Connect the consumer to the broker through a Toxiproxy proxy (see
// brokers.Toxiproxy), then use the proxy in faults:
//
//	p, _ := netfault.Connect(tp.API, "pg")
//	Faults: []ackhammer.Fault{netfault.Cut(p, 200*time.Millisecond, time.Second)}
package netfault

import (
	"context"
	"fmt"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"

	"github.com/AristideDongo/ackhammer/core"
)

// Proxy is a Toxiproxy proxy between the consumer and its broker.
type Proxy struct {
	name   string
	client *toxiproxy.Client
	proxy  *toxiproxy.Proxy
}

// Connect returns the proxy called name on the Toxiproxy at api.
func Connect(api, name string) (*Proxy, error) {
	c := toxiproxy.NewClient(api)
	p, err := c.Proxy(name)
	if err != nil {
		return nil, fmt.Errorf("netfault: proxy %s: %w", name, err)
	}
	return &Proxy{name: name, client: c, proxy: p}, nil
}

// Name returns the proxy name.
func (p *Proxy) Name() string { return p.name }

// Reset enables the proxy and removes every toxic.
func (p *Proxy) Reset() error { return p.client.ResetState() }

type fault struct {
	name   string
	at     time.Duration
	inject func(context.Context) error
}

func (f *fault) Name() string                                        { return f.name }
func (f *fault) At() time.Duration                                   { return f.at }
func (f *fault) Inject(ctx context.Context, _ core.Controller) error { return f.inject(ctx) }

// Cut closes every connection through p at the given offset and refuses new
// ones for d, then restores the network. The consumer is expected to
// reconnect by itself.
func Cut(p *Proxy, at, d time.Duration) core.TimedFault {
	return &fault{fmt.Sprintf("NetworkCut(%s, %s, %s)", p.name, at, d), at, func(ctx context.Context) error {
		if err := p.proxy.Disable(); err != nil {
			return err
		}
		wait(ctx, d)
		return p.proxy.Enable()
	}}
}

// Latency adds latency (± jitter) to both directions through p at the given
// offset, for d.
func Latency(p *Proxy, at, latency, jitter, d time.Duration) core.TimedFault {
	name := fmt.Sprintf("NetworkLatency(%s, %s, %s±%s, %s)", p.name, at, latency, jitter, d)
	return &fault{name, at, func(ctx context.Context) error {
		attrs := toxiproxy.Attributes{"latency": latency.Milliseconds(), "jitter": jitter.Milliseconds()}
		for _, stream := range []string{"upstream", "downstream"} {
			if _, err := p.proxy.AddToxic("ackhammer-latency-"+stream, "latency", stream, 1, attrs); err != nil {
				return err
			}
		}
		wait(ctx, d)
		for _, stream := range []string{"upstream", "downstream"} {
			if err := p.proxy.RemoveToxic("ackhammer-latency-" + stream); err != nil {
				return err
			}
		}
		return nil
	}}
}

func wait(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
