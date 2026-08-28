package notifier

import (
	"errors"

	"github.com/arkhemlol/notifier/internal/core"
)

var (
	// ErrInvalidDispatcherConfig marks invalid dispatcher configuration.
	ErrInvalidDispatcherConfig = errors.New("invalid dispatcher configuration")
	// ErrInvalidDestinationBinding marks an invalid destination list.
	ErrInvalidDestinationBinding = errors.New("invalid destination binding")
)

var (
	// ErrStoreStaleLeaseToken marks an outcome written after losing its lease.
	ErrStoreStaleLeaseToken = core.ErrStoreStaleLeaseToken
	// ErrStorePayloadConflict marks reuse of an Item.ID with a different payload.
	ErrStorePayloadConflict = core.ErrStorePayloadConflict
	// ErrStoreWorkDoesntExist marks an outcome for an unknown batch.
	ErrStoreWorkDoesntExist = core.ErrStoreWorkDoesntExist
	// ErrStoreInvalidTransition marks a write that contradicts stored state.
	ErrStoreInvalidTransition = core.ErrStoreInvalidTransition
	// ErrStoreBusy marks transient backend contention, which is retried.
	ErrStoreBusy = core.ErrStoreBusy
	// ErrStoreUnavailable marks unavailable persistence infrastructure.
	ErrStoreUnavailable = core.ErrStoreUnavailable
)

// ErrDestinationImplementationMissing marks work with no bound destination.
var ErrDestinationImplementationMissing = errors.New(
	"dispatcher destination implementation missing",
)

// Delivery errors retain the provider cause for errors.Is and errors.As.
// ErrQuarantine also matches ErrPermanent.
var (
	// ErrRetryable marks a failure that may succeed on a later attempt.
	ErrRetryable = core.ErrRetryable
	// ErrPermanent marks a failure that will not succeed on a later attempt.
	ErrPermanent = core.ErrPermanent
	// ErrQuarantine marks a permanent failure that disables the destination.
	ErrQuarantine = core.ErrQuarantine
)
