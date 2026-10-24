package core

import "math/rand"

// RandShim wraps rand.Rand.
type RandShim struct {
	*rand.Rand
}

// NewRand creates seeded rand.
func NewRand(seed int64) *RandShim {
	return &RandShim{rand.New(rand.NewSource(seed))}
}
