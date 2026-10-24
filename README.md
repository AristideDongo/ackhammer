# Ackhammer

*Hammer your event consumers until they break — before production does.*

[![ci](https://github.com/AristideDongo/ackhammer/actions/workflows/ci.yml/badge.svg)](https://github.com/AristideDongo/ackhammer/actions/workflows/ci.yml)

Ackhammer is a fault-injection test bench for Go event consumers. It does not replace your messaging library: it tests it.

You plug in your existing subscriber (Watermill, CloudEvents sdk-go, or your own), declare the guarantee you believe you have, and Ackhammer:

1. publishes traceable messages (ID, key, W3C `traceparent`);
2. injects faults: slow handler, handler error, panic, cancelled context, graceful stop, consumer restart;
3. checks invariants on what really happened: no loss, duplicates within the guarantee, per-key order, poison routing, redelivery delay, no stall;
4. prints a report with a per-message timeline and a seed to replay the run.

> **Status: early development (v0.1 in progress).** The API may change. Docker-based brokers, the CLI and the upstream bug reproductions are not there yet.

## Why

The worst consumer bugs are not missing features. They are messages lost or redelivered silently during a failure, invisible until a production incident. For example, in [watermill#700](https://github.com/ThreeDotsLabs/watermill/issues/700) a single slow message makes the PostgreSQL adapter drop the rest of the batch: 7 messages out of 10 lost, no error. Proving it took the reporter 80 lines of Go and a docker-compose file. Ackhammer makes that harness reusable.

## Quick start

```sh
go get github.com/AristideDongo/ackhammer
```

```go
func TestSlowMessageInBatch(t *testing.T) {
	target := memtarget.New(memtarget.Config{
		AckDeadline: 100 * time.Millisecond,
		Bugs:        memtarget.Bugs{DropBatchOnDeadline: true}, // reproduce watermill#700
	})

	ackhammer.Run(t, target, ackhammer.Scenario{
		Name:     "slow-message-in-batch",
		Messages: 10,
		Faults:   []ackhammer.Fault{fault.SlowHandler(fault.OnMessage(3), 300*time.Millisecond)},
		Expect:   ackhammer.AtLeastOnce(),
		Seed:     42,
	})
}
```

```
✗ slow-message-in-batch  (seed 42, target memtarget, 1.302s)

  NoLoss ...................... FAIL  7/10 messages never handled
  DuplicatesWithinGuarantee ... SKIP  MaxRedeliveries is Unlimited
  EffectOnce .................. SKIP  only for EffectivelyOnce
  PerKeyOrder ................. SKIP  ordering not declared
  PoisonRouted ................ SKIP  no poison queue declared
  RedeliveryLatency ........... SKIP  RedeliveryWithin not declared
  NoStall ..................... PASS

  Timeline msg-4 :
          +0ms  published
             —  never delivered

  Replay : ACKHAMMER_SEED=42 go test -run '^TestSlowMessageInBatch/slow-message-in-batch$'
```

`memtarget` is an in-memory broker that needs no Docker. It is correct by default; its `Bugs` options reproduce known bug classes so you can see what Ackhammer reports.

## Testing a Watermill consumer

```sh
go get github.com/AristideDongo/ackhammer/targets/watermill
```

The target runs a real `message.Router` with your middlewares, so retry and poison-queue middlewares are tested as they run in production.

```go
poison, _ := middleware.PoisonQueue(pub, "orders_poison")

target := watermill.New(watermill.Config{
	Publisher:        pub,
	Subscriber:       sub,
	Topic:            "orders",
	Middlewares:      []message.HandlerMiddleware{poison, middleware.Retry{MaxRetries: 2}.Middleware},
	PoisonTopic:      "orders_poison",
	PoisonSubscriber: sub,
	NewSubscriber:    newSubscriber, // enables RestartConsumer
})

ackhammer.Run(t, target, ackhammer.Scenario{
	Name:    "retry-then-poison",
	Handler: myHandler,
	Faults:  []ackhammer.Fault{fault.HandlerError(fault.OnMessage(2), 100)},
	Expect:  ackhammer.AtLeastOnce().WithPoisonAfter(3),
})
```

## Testing a CloudEvents sdk-go consumer

```sh
go get github.com/AristideDongo/ackhammer/targets/cloudevents
```

```go
target := cloudevents.New(cloudevents.Config{
	Sender:      sender,
	Receiver:    receiver,
	NewReceiver: newReceiver, // enables RestartConsumer
})
```

Each message becomes a CloudEvent whose `id` is the message ID, with the `partitionkey` and `traceparent` extensions. A handler error is answered with a NACK.

## Killing the consumer mid-processing

`subprocess` runs the real consumer client in a child process (usually the test binary re-executed), so `KillProcess` can kill it like a production crash: the deliveries it held are never acked and the broker must redeliver them.

```go
func TestMain(m *testing.M) {
	if subprocess.IsChild("orders") {
		os.Exit(subprocess.Serve(func(ctx context.Context, h ackhammer.Handler) error {
			return newOrdersConsumer().Consume(ctx, h) // the real subscriber
		}))
	}
	os.Exit(m.Run())
}

target := subprocess.New(subprocess.Config{
	Target:  publisher,                 // publishes and inspects the poison queue
	Command: subprocess.Self("orders"), // re-executes this test binary
})
```

A crash after the business effect but before the ack applies the effect twice: with `EffectivelyOnce`, a naive handler fails `EffectOnce`.

## Your own handler and effects

`Scenario.Handler` is your consumer logic; Ackhammer runs it behind the probe. To check idempotence, declare `EffectivelyOnce` and record each business effect:

```go
Handler: func(ctx context.Context, m ackhammer.Message) error {
	if err := charge(ctx, m); err != nil {
		return err
	}
	ackhammer.RecordEffect(ctx, "charge:"+m.ID)
	return nil
},
Expect: ackhammer.EffectivelyOnce(),
```

## Guarantees

| Shortcut | Meaning |
|---|---|
| `AtLeastOnce()` | Every message handled at least once. Duplicates allowed. |
| `AtMostOnce()` | No message delivered twice. Loss allowed. |
| `EffectivelyOnce()` | Transport duplicates allowed, each recorded effect applied once. |
| `OrderedPerKey()` | At-least-once, and messages with the same key handled in publication order. |

Refine with `.WithMaxRedeliveries(n)` (deliveries allowed after a success), `.WithRedeliveryWithin(d)` and `.WithPoisonAfter(n)`.

## Faults

| Fault | Effect | Target must implement |
|---|---|---|
| `SlowHandler(sel, d)` | First delivery of selected messages lasts `d` | — |
| `HandlerError(sel, n)` | First `n` deliveries of selected messages fail | — |
| `HandlerPanic(sel)` | First delivery of selected messages panics | — |
| `CancelDuringReceive(at)` | Cancels the `Consume` context, then resumes | — |
| `GracefulStop(at)` | Stops consuming gracefully, then resumes | `ConsumerStopper` |
| `RestartConsumer(at)` | Recreates the consumer | `Restartable` |
| `KillProcess(at)` | Kills the consumer process with SIGKILL, then starts a new one | `Killer` (package `subprocess`) |
| `netfault.Cut(proxy, at, d)` | Cuts the connection to the broker for `d` (Toxiproxy) | consumer connected through the proxy |
| `netfault.Latency(proxy, at, l, jitter, d)` | Adds network latency for `d` (Toxiproxy) | consumer connected through the proxy |

Selectors: `OnMessage(n)`, `OnKey(k)`, `EveryNth(n)`, `Random(p)`. Random selection uses the scenario seed.

## Invariants

| Invariant | Rule |
|---|---|
| `NoLoss` | Every message is handled at least once or is in the poison queue. |
| `DuplicatesWithinGuarantee` | Deliveries after a success do not exceed `MaxRedeliveries`. |
| `EffectOnce` | With `EffectivelyOnce`, each recorded effect appears once. |
| `PerKeyOrder` | Same-key messages are first handled in publication order. |
| `PoisonRouted` | A message that failed `PoisonAfter` times is in the poison queue (needs `PoisonInspector`). |
| `RedeliveryLatency` | After a failure or a fault, the next delivery comes within `RedeliveryWithin`. |
| `NoStall` | `Consume` returns when cancelled, and progress continues after the last fault. |

## Replay and reports

Every run is seeded. A failing run prints the command to replay it; `ACKHAMMER_SEED` overrides the seed. Set `ACKHAMMER_REPORT_DIR` to write each report as JSON. The `report` package renders reports as text, JSON (`report.Read` loads them back), JUnit XML (`report.JUnit`), a self-contained HTML page (`report.HTML`) and Markdown ready to paste into a GitHub issue (`Report.Issue`, with the Go and library versions). Ackhammer is not a deterministic simulator: it drives real libraries and real timing, so keep explicit margins on time-based guarantees.

## Roadmap

- **v0.1**: Postgres and Pub/Sub emulator brokers via testcontainers, reproductions of watermill#700, #636, #671 and sdk-go#1234 in `scenarios/upstream`.
- **v0.2**: per-key order on real brokers, `TraceContinuity`, GitHub Action, YAML scenarios and CLI.
- **v0.3**: a resume contract for SSE, WebSocket and MQTT, with conformance scenarios.

## License

[Apache 2.0](LICENSE)
