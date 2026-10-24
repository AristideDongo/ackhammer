// Package brokers starts real brokers in Docker containers for Ackhammer
// scenarios, with testcontainers-go.
//
// Every helper skips the test when Docker is not available or when tests run
// with -short, and removes the container when the test ends.
//
// To inject network faults, put the broker and a Toxiproxy on the same
// Network and connect the consumer through the proxy:
//
//	n := brokers.NewNetwork(t)
//	brokers.Postgres(t, brokers.OnNetwork(n, "postgres"))
//	tp := brokers.Toxiproxy(t, n, brokers.Proxy{Name: "pg", Upstream: "postgres:5432"})
//	dsn := brokers.PostgresDSN(tp.Endpoint("pg")) // goes through the proxy
package brokers

import (
	"fmt"
	"net"
	"strconv"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/gcloud/pubsub"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tctoxiproxy "github.com/testcontainers/testcontainers-go/modules/toxiproxy"
	"github.com/testcontainers/testcontainers-go/network"
)

// Images used by the helpers. Override them before starting a broker.
var (
	PostgresImage  = "postgres:16-alpine"
	NATSImage      = "nats:2.11-alpine"
	PubSubImage    = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	ToxiproxyImage = "ghcr.io/shopify/toxiproxy:2.12.0"
)

// Postgres credentials.
const (
	PostgresUser     = "ackhammer"
	PostgresPassword = "ackhammer"
	PostgresDB       = "ackhammer"
)

// Require skips t when containers cannot run.
func Require(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("brokers: skipped with -short")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
}

// Network is a Docker network shared by brokers and proxies.
type Network struct{ nw *testcontainers.DockerNetwork }

// NewNetwork creates a Docker network removed when t ends.
func NewNetwork(t *testing.T) *Network {
	t.Helper()
	Require(t)
	nw, err := network.New(t.Context())
	testcontainers.CleanupNetwork(t, nw)
	if err != nil {
		t.Fatalf("brokers: network: %v", err)
	}
	return &Network{nw}
}

// Option configures a broker container.
type Option func(*[]testcontainers.ContainerCustomizer)

// OnNetwork attaches the container to n under the given alias.
func OnNetwork(n *Network, alias string) Option {
	return func(opts *[]testcontainers.ContainerCustomizer) {
		*opts = append(*opts, network.WithNetwork([]string{alias}, n.nw))
	}
}

func customizers(opts []Option) []testcontainers.ContainerCustomizer {
	var c []testcontainers.ContainerCustomizer
	for _, o := range opts {
		o(&c)
	}
	return c
}

// Postgres starts PostgreSQL and returns a DSN reachable from the host.
func Postgres(t *testing.T, opts ...Option) string {
	t.Helper()
	Require(t)
	c, err := tcpostgres.Run(t.Context(), PostgresImage, append([]testcontainers.ContainerCustomizer{
		tcpostgres.WithDatabase(PostgresDB),
		tcpostgres.WithUsername(PostgresUser),
		tcpostgres.WithPassword(PostgresPassword),
		tcpostgres.BasicWaitStrategies(),
	}, customizers(opts)...)...)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("brokers: postgres: %v", err)
	}
	endpoint, err := c.PortEndpoint(t.Context(), "5432/tcp", "")
	if err != nil {
		t.Fatalf("brokers: postgres endpoint: %v", err)
	}
	return PostgresDSN(endpoint)
}

// PostgresDSN returns the DSN of the Postgres started by this package at
// host:port (for example a Toxiproxy endpoint).
func PostgresDSN(hostport string) string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", PostgresUser, PostgresPassword, hostport, PostgresDB)
}

// NATS starts a NATS server with JetStream and returns its URL.
func NATS(t *testing.T, opts ...Option) string {
	t.Helper()
	Require(t)
	c, err := tcnats.Run(t.Context(), NATSImage, customizers(opts)...)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("brokers: nats: %v", err)
	}
	url, err := c.ConnectionString(t.Context())
	if err != nil {
		t.Fatalf("brokers: nats url: %v", err)
	}
	return url
}

// PubSubEmulator is a running Google Cloud Pub/Sub emulator.
type PubSubEmulator struct {
	Endpoint  string // host:port, for PUBSUB_EMULATOR_HOST
	ProjectID string
}

// PubSub starts the Google Cloud Pub/Sub emulator.
func PubSub(t *testing.T, opts ...Option) PubSubEmulator {
	t.Helper()
	Require(t)
	const project = "ackhammer"
	c, err := pubsub.Run(t.Context(), PubSubImage, append([]testcontainers.ContainerCustomizer{
		pubsub.WithProjectID(project),
	}, customizers(opts)...)...)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("brokers: pubsub: %v", err)
	}
	return PubSubEmulator{Endpoint: c.URI(), ProjectID: c.ProjectID()}
}

// Proxy declares a Toxiproxy proxy. Set Upstream to reach a container on
// the Network (alias:port), or HostPort to reach a port of the host (for
// example a broker running in the test process).
type Proxy struct {
	Name     string
	Upstream string
	HostPort int
}

// ToxiproxyServer is a running Toxiproxy.
type ToxiproxyServer struct {
	// API is the URL of the control API, for the toxiproxy client.
	API       string
	endpoints map[string]string
}

// Endpoint returns the host:port that reaches the upstream of the named
// proxy through Toxiproxy.
func (s *ToxiproxyServer) Endpoint(name string) string { return s.endpoints[name] }

// Toxiproxy starts Toxiproxy with the given proxies. n may be nil when every
// proxy targets a HostPort.
func Toxiproxy(t *testing.T, n *Network, proxies ...Proxy) *ToxiproxyServer {
	t.Helper()
	Require(t)
	var opts []testcontainers.ContainerCustomizer
	if n != nil {
		opts = append(opts, network.WithNetwork([]string{"toxiproxy"}, n.nw))
	}
	for _, p := range proxies {
		upstream := p.Upstream
		if p.HostPort != 0 {
			upstream = net.JoinHostPort(testcontainers.HostInternal, strconv.Itoa(p.HostPort))
			opts = append(opts, testcontainers.WithHostPortAccess(p.HostPort))
		}
		opts = append(opts, tctoxiproxy.WithProxy(p.Name, upstream))
	}
	c, err := tctoxiproxy.Run(t.Context(), ToxiproxyImage, opts...)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("brokers: toxiproxy: %v", err)
	}
	api, err := c.URI(t.Context())
	if err != nil {
		t.Fatalf("brokers: toxiproxy api: %v", err)
	}
	s := &ToxiproxyServer{API: api, endpoints: map[string]string{}}
	for i, p := range proxies {
		host, port, err := c.ProxiedEndpoint(8666 + i)
		if err != nil {
			t.Fatalf("brokers: toxiproxy endpoint %s: %v", p.Name, err)
		}
		s.endpoints[p.Name] = net.JoinHostPort(host, port)
	}
	return s
}
