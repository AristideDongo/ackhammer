package brokers_test

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/AristideDongo/ackhammer/brokers"
)

func dial(t *testing.T, hostport string) {
	t.Helper()
	c, err := net.DialTimeout("tcp", hostport, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", hostport, err)
	}
	c.Close()
}

func TestPostgresThroughToxiproxy(t *testing.T) {
	n := brokers.NewNetwork(t)
	dsn := brokers.Postgres(t, brokers.OnNetwork(n, "postgres"))
	if !strings.HasPrefix(dsn, "postgres://") {
		t.Fatalf("dsn = %q", dsn)
	}
	tp := brokers.Toxiproxy(t, n, brokers.Proxy{Name: "pg", Upstream: "postgres:5432"})
	dial(t, tp.Endpoint("pg"))
	if tp.API == "" {
		t.Fatal("empty toxiproxy API")
	}
}

func TestNATS(t *testing.T) {
	url := brokers.NATS(t)
	dial(t, strings.TrimPrefix(url, "nats://"))
}
