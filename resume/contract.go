package resume

// Cursor is opaque.
type Cursor []byte

// Checkpointer handles checkpoints.
type Checkpointer interface {
	Save(cursor Cursor) error
	Load() (Cursor, error)
}
