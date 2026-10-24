package upstream

import (
	stdsql "database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	wm "github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/AristideDongo/ackhammer"
	"github.com/AristideDongo/ackhammer/brokers"
	"github.com/AristideDongo/ackhammer/fault"
	"github.com/AristideDongo/ackhammer/invariant"
	"github.com/AristideDongo/ackhammer/targets/watermill"
)

// Watermill700 reproduces ThreeDotsLabs/watermill#700 on watermill-sql
// v4.1.5: with PostgreSQLQueueOffsetsAdapter, one message that misses the
// ack deadline makes the subscriber delete every later row of the batch.
//
// https://github.com/ThreeDotsLabs/watermill/issues/700
func TestWatermill700(t *testing.T) {
	scenario := ackhammer.Scenario{
		Name:         "watermill-700-slow-message-in-batch",
		Messages:     10,
		Faults:       []ackhammer.Fault{fault.SlowHandler(fault.OnMessage(3), time.Second)},
		Expect:       ackhammer.AtLeastOnce(),
		Seed:         700,
		DrainTimeout: 5 * time.Second,
	}

	t.Run("affected", func(t *testing.T) {
		rep := execute(t, postgresQueueTarget(t, sql.PostgreSQLQueueOffsetsAdapter{DeleteOnAck: true}), scenario)
		// The messages fetched in the same batch after msg-3 are lost: from
		// msg-4 to the end of the batch (msg-10 unless the first query ran
		// while messages were still being published).
		noLoss := result(rep, "NoLoss")
		lost := noLoss.MsgIDs
		if noLoss.Status != invariant.Fail || len(lost) < 2 || lost[0] != "msg-4" || !consecutive(lost) {
			t.Errorf("bug not detected (fixed upstream? update this scenario): NoLoss = %s %v\n%s", noLoss.Status, lost, rep.Text())
		}
	})

	// The workaround from the issue: ack only the rows up to the last acked
	// one.
	t.Run("workaround", func(t *testing.T) {
		target := postgresQueueTarget(t, ackedRowsOnly{sql.PostgreSQLQueueOffsetsAdapter{DeleteOnAck: true}})
		ackhammer.Run(t, target, scenario)
	})
}

// ackedRowsOnly keeps only the rows up to LastRow in the ack query.
type ackedRowsOnly struct {
	sql.PostgreSQLQueueOffsetsAdapter
}

func (a ackedRowsOnly) AckMessageQuery(p sql.AckMessageQueryParams) (sql.Query, error) {
	p.Rows = slices.DeleteFunc(slices.Clone(p.Rows), func(r sql.Row) bool { return r.Offset > p.LastRow.Offset })
	return a.PostgreSQLQueueOffsetsAdapter.AckMessageQuery(p)
}

// postgresQueueTarget is a Watermill target on watermill-sql with the queue
// schema, as in the issue's reproduction.
func postgresQueueTarget(t *testing.T, offsets sql.OffsetsAdapter) ackhammer.Target {
	t.Helper()
	db, err := stdsql.Open("pgx", brokers.Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	logger := wm.NopLogger{}
	schema := sql.PostgreSQLQueueSchema{}
	pub, err := sql.NewPublisher(sql.BeginnerFromStdSQL(db), sql.PublisherConfig{SchemaAdapter: schema, AutoInitializeSchema: true}, logger)
	if err != nil {
		t.Fatal(err)
	}
	ackDeadline := 100 * time.Millisecond
	sub, err := sql.NewSubscriber(sql.BeginnerFromStdSQL(db), sql.SubscriberConfig{
		SchemaAdapter:    schema,
		OffsetsAdapter:   offsets,
		InitializeSchema: true,
		AckDeadline:      &ackDeadline,
		// Lets every message land before the first non-empty query, so
		// they are all in one batch.
		PollInterval: 500 * time.Millisecond,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	// Create the tables before the run: publisher and subscriber creating
	// them concurrently fail on Postgres' catalog unique index.
	if err := sub.SubscribeInitialize("orders"); err != nil {
		t.Fatal(err)
	}
	return watermill.New(watermill.Config{Publisher: pub, Subscriber: sub, Topic: "orders", Logger: logger})
}

// consecutive reports whether ids are msg-N, msg-N+1, …
func consecutive(ids []string) bool {
	for i := 1; i < len(ids); i++ {
		var a, b int
		fmt.Sscanf(ids[i-1], "msg-%d", &a)
		fmt.Sscanf(ids[i], "msg-%d", &b)
		if b != a+1 {
			return false
		}
	}
	return true
}
