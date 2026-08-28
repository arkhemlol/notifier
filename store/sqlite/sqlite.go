// Package sqlite implements core.Store on caller-owned, driver-neutral SQLite.
// Connections must enable foreign keys and a positive busy timeout; New verifies
// both without changing connection or journal settings.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var (
	// ErrInvalidConfig marks invalid constructor arguments or connection settings.
	ErrInvalidConfig = errors.New("invalid sqlite store configuration")

	// ErrCodec marks payload encoding or decoding failures.
	ErrCodec = errors.New("sqlite store codec failure")

	// ErrSchema marks an incompatible private store schema.
	ErrSchema = errors.New("sqlite store schema incompatible")
)

// Codec converts payloads to deterministic bytes without retaining input or output buffers.
type Codec[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(data []byte) (T, error)
}

// FailureClass classifies driver-specific infrastructure failures.
type FailureClass uint8

const (
	// FailureClassUnknown is treated as unavailable.
	FailureClassUnknown FailureClass = iota

	// FailureClassBusy marks transient writer contention.
	FailureClassBusy

	// FailureClassUnavailable marks unavailable persistence infrastructure.
	FailureClassUnavailable
)

// FailureClassifier maps driver errors without adding a driver dependency.
type FailureClassifier func(error) FailureClass

// Store persists notification state in SQLite.
type Store[T any] struct {
	db         *sql.DB
	codec      Codec[T]
	classifier FailureClassifier
}

// New constructs a Store and initializes its private schema. classifier maps
// driver-specific errors to FailureClass; omit it to treat every infrastructure
// failure as unavailable.
func New[T any](db *sql.DB, codec Codec[T], classifier ...FailureClassifier) (*Store[T], error) {
	return NewContext(context.Background(), db, codec, classifier...)
}

// NewContext is New with caller-controlled initialization cancellation.
func NewContext[T any](
	ctx context.Context,
	db *sql.DB,
	codec Codec[T],
	classifier ...FailureClassifier,
) (*Store[T], error) {
	switch {
	case ctx == nil:
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidConfig)
	case db == nil:
		return nil, fmt.Errorf("%w: database is nil", ErrInvalidConfig)
	case codec == nil:
		return nil, fmt.Errorf("%w: codec is nil", ErrInvalidConfig)
	case len(classifier) > 1:
		return nil, fmt.Errorf("%w: at most one error classifier", ErrInvalidConfig)
	case len(classifier) == 1 && classifier[0] == nil:
		return nil, fmt.Errorf("%w: error classifier is nil", ErrInvalidConfig)
	}

	store := &Store[T]{db: db, codec: codec}

	if len(classifier) == 1 {
		store.classifier = classifier[0]
	}

	if err := store.verifyConnectionSettings(ctx); err != nil {
		return nil, err
	}

	if err := store.initialize(ctx); err != nil {
		return nil, err
	}

	return store, nil
}
