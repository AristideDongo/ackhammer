// Package ledger is Ackhammer's journal of truth: everything that was
// published and everything the probe observed, timestamped and ordered.
package ledger

import (
	"sync"
	"time"
)

// Type is the kind of a ledger event.
type Type string

const (
	Published     Type = "published"      // message handed to Target.Publish
	Received      Type = "received"       // delivery reached the probe
	Succeeded     Type = "succeeded"      // handler returned nil (ack requested)
	Failed        Type = "failed"         // handler returned an error (nack requested)
	Panicked      Type = "panicked"       // handler panicked, recovered by the probe
	Effect        Type = "effect"         // business effect recorded by the handler
	FaultInjected Type = "fault"          // a fault fired
	ConsumerExit  Type = "consumer_exit"  // Consume returned on its own
	ConsumerStuck Type = "consumer_stuck" // Consume did not return after cancel
	Poisoned      Type = "poisoned"       // message found in the poison queue
)

// Event is one ledger entry.
type Event struct {
	Seq         int       `json:"seq"`
	Type        Type      `json:"type"`
	Time        time.Time `json:"time"`
	MsgID       string    `json:"msg_id,omitempty"`
	Key         string    `json:"key,omitempty"`
	Index       int       `json:"index,omitempty"`   // publication order, 1-based (Published only)
	Attempt     int       `json:"attempt,omitempty"` // delivery number, 1-based
	Traceparent string    `json:"traceparent,omitempty"`
	Detail      string    `json:"detail,omitempty"` // fault name, error text, effect name…
}

// Ledger is an append-only, concurrency-safe event journal.
type Ledger struct {
	mu     sync.Mutex
	events []Event
	now    func() time.Time
}

// New returns an empty ledger.
func New() *Ledger { return &Ledger{now: time.Now} }

// Append stamps e with a sequence number and the current time (unless set)
// and stores it.
func (l *Ledger) Append(e Event) Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	e.Seq = len(l.events) + 1
	if e.Time.IsZero() {
		e.Time = l.now()
	}
	l.events = append(l.events, e)
	return e
}

// Events returns a copy of all events in append order.
func (l *Ledger) Events() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Event(nil), l.events...)
}

// Len returns the number of events.
func (l *Ledger) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.events)
}

// View indexes a snapshot of events for querying.
type View struct {
	Events    []Event
	Published []Event            // Published events in publication order
	ByMsg     map[string][]Event // all events per message, in order
	Faults    []Event            // FaultInjected events with no message
}

// NewView indexes events.
func NewView(events []Event) *View {
	v := &View{Events: events, ByMsg: map[string][]Event{}}
	for _, e := range events {
		if e.MsgID != "" {
			v.ByMsg[e.MsgID] = append(v.ByMsg[e.MsgID], e)
		}
		switch {
		case e.Type == Published:
			v.Published = append(v.Published, e)
		case e.Type == FaultInjected && e.MsgID == "":
			v.Faults = append(v.Faults, e)
		}
	}
	return v
}

// Count returns how many events of type t exist for message id.
func (v *View) Count(id string, t Type) int {
	n := 0
	for _, e := range v.ByMsg[id] {
		if e.Type == t {
			n++
		}
	}
	return n
}

// First returns the first event of type t for message id.
func (v *View) First(id string, t Type) (Event, bool) {
	for _, e := range v.ByMsg[id] {
		if e.Type == t {
			return e, true
		}
	}
	return Event{}, false
}
