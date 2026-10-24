package ledger

import (
	"sync"
	"time"

	"github.com/AristideDongo/ackhammer/core"
)

// EventType represents ledger event types.
type EventType string

const (
	Published EventType = "published"
	Received  EventType = "received"
	Processed EventType = "processed"
	Acked     EventType = "acked"
	Nacked    EventType = "nacked"
	Errored   EventType = "errored"
	Effect    EventType = "effect"
	Fault     EventType = "fault"
)

// Event is a ledger entry.
type Event struct {
	ID        string
	MsgID     string
	Key       string
	Type      EventType
	Time      time.Time
	Traceparent string
	Meta      map[string]string
}

// Ledger is append-only, concurrent-safe truth journal.
type Ledger struct {
	mu     sync.RWMutex
	events []Event
	byMsg  map[string][]Event
}

// New creates a new ledger.
func New() *Ledger {
	return &Ledger{
		events: make([]Event, 0, 1024),
		byMsg:  make(map[string][]Event),
	}
}

// Append adds an event.
func (l *Ledger) Append(e Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
	if e.MsgID != "" {
		l.byMsg[e.MsgID] = append(l.byMsg[e.MsgID], e)
	}
}

// Events returns all events.
func (l *Ledger) Events() []Event {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]Event, len(l.events))
	copy(out, l.events)
	return out
}

// ForMessage returns events for a message.
func (l *Ledger) ForMessage(id string) []Event {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]Event, len(l.byMsg[id]))
	copy(out, l.byMsg[id])
	return out
}

// PublishedIDs returns all published message IDs.
func (l *Ledger) PublishedIDs() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	seen := map[string]bool{}
	var ids []string
	for _, e := range l.events {
		if e.Type == Published && !seen[e.MsgID] {
			ids = append(ids, e.MsgID)
			seen[e.MsgID] = true
		}
	}
	return ids
}

// ProcessCount returns number of times a message was processed.
func (l *Ledger) ProcessCount(id string) int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	n := 0
	for _, e := range l.byMsg[id] {
		if e.Type == Processed || e.Type == Received {
			n++
		}
	}
	return n
}

// RecordPublished records a published message.
func (l *Ledger) RecordPublished(msg core.Message) {
	l.Append(Event{
		ID:          "pub-" + msg.ID,
		MsgID:       msg.ID,
		Key:         msg.Key,
		Type:        Published,
		Time:        time.Now(),
		Traceparent: msg.Traceparent,
	})
}

// RecordEffect records a business effect.
func (l *Ledger) RecordEffect(msgID, effectID string) {
	l.Append(Event{
		ID:    "eff-" + effectID,
		MsgID: msgID,
		Type:  Effect,
		Time:  time.Now(),
	})
}
