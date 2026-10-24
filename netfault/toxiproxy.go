package netfault

import "time"

// Proxy controls Toxiproxy
type Proxy struct{}

func New(addr string) *Proxy { return &Proxy{} }
func (p *Proxy) Cut(d time.Duration) error { return nil }
func (p *Proxy) Latency(d time.Duration) error { return nil }
func (p *Proxy) Close() error { return nil }
