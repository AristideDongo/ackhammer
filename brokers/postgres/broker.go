package postgres

// Broker manages postgres via testcontainers
type Broker struct{}

func New() *Broker { return &Broker{} }
func (b *Broker) Start() error { return nil }
func (b *Broker) Stop() error { return nil }
func (b *Broker) DSN() string { return "" }
