package nats

// Broker manages NATS
type Broker struct{}

func New() *Broker { return &Broker{} }
func (b *Broker) Start() error { return nil }
func (b *Broker) Stop() error { return nil }
func (b *Broker) URL() string { return "" }
