// Package core defines notifier's internal persistence model.
package core

import (
	"context"
	"errors"
	"time"
)

// Only ErrStoreBusy and ErrStoreUnavailable are retried.
var (
	ErrStoreStaleLeaseToken   = errors.New("store stale lease token")
	ErrStorePayloadConflict   = errors.New("store payload conflict")
	ErrStoreWorkDoesntExist   = errors.New("store work does not exist")
	ErrStoreInvalidTransition = errors.New("store invalid transition")
	ErrStoreBusy              = errors.New("store busy")
	ErrStoreUnavailable       = errors.New("store unavailable")
)

// ErrInvalidPlan marks a plan that cannot be admitted, such as one with an empty ID.
var ErrInvalidPlan = errors.New("invalid plan")

// DestinationID identifies one delivery destination within a plan.
type DestinationID string

// Item is one payload enqueued for delivery, keyed by ID for deduplication.
type Item[T any] struct {
	ID      int64
	Payload T
}

// WorkID identifies one claimed batch of work.
type WorkID string

// LeaseToken fences a claimed batch so only its current holder can resolve it.
type LeaseToken string

// Work is a leased batch of items claimed for delivery to one destination.
type Work[T any] struct {
	ID          WorkID
	Plan        PlanID
	Destination DestinationID
	Items       []Item[T]
	Attempt     int
	Lease       LeaseToken
	LeaseUntil  time.Time
}

// Outcome is the result a resolution reports for a leased batch.
type Outcome int

// Outcome values, from unresolved through the three terminal dispositions a resolution can report.
const (
	OutcomeUnknown Outcome = iota
	OutcomeDelivered
	OutcomeRetryableFailure
	OutcomeFailedPermanent

	OutcomeDeliveredUnrecorded
)

func (o Outcome) String() string {
	switch o {
	case OutcomeUnknown:
		return "unknown"
	case OutcomeDelivered:
		return "delivered"
	case OutcomeRetryableFailure:
		return "retryable failure"
	case OutcomeFailedPermanent:
		return "failed permanently"
	case OutcomeDeliveredUnrecorded:
		return "delivered unrecorded"
	default:
		return "invalid outcome"
	}
}

// Prober checks whether a destination is reachable before work is scheduled against it.
type Prober interface {
	Probe(ctx context.Context) error
}

// Resolution reports the outcome of a leased batch back to the store.
type Resolution struct {
	Work       WorkID
	Lease      LeaseToken
	Outcome    Outcome
	Scope      FailureScope
	RetryAfter time.Duration
	Failure    string
}

// ClaimRequest bounds how much work to lease from a plan in one Claim call.
type ClaimRequest struct {
	Plan            PlanID
	MaxWork         int
	MaxItemsPerWork int
	LeaseDuration   time.Duration
}

// Store persists plans, items, leases, outcomes, and destination states.
type Store[T any] interface {
	// Admit registers the plan on first sight, then creates one delivery
	// obligation per destination for each item. Item.ID is the deduplication
	// key: re-admitting a stored ID is a no-op, and reusing one with a different
	// payload must fail.
	Admit(ctx context.Context, plan Plan, items []Item[T]) error
	// Claim hands out up to MaxWork batches and marks them as taken; returning
	// fewer, including none, is normal. Each carries a LeaseUntil deadline,
	// after which the store offers it to someone else, and a fresh LeaseToken.
	Claim(ctx context.Context, request ClaimRequest) ([]Work[T], error)
	// Resolve applies a delivery outcome to one leased batch, rejecting any
	// resolution whose token is not the batch's current lease. Repeating an
	// identical resolution succeeds; contradicting an applied one fails.
	Resolve(ctx context.Context, resolution Resolution) error
	// QuarantineDestination blocks a destination from receiving new work and
	// terminalizes what it already holds, recording failure against it.
	QuarantineDestination(
		ctx context.Context,
		plan PlanID,
		destination DestinationID,
		failure string,
	) error
	// ActivateDestination lets a destination receive work again. Work
	// terminalized by an earlier quarantine stays terminal.
	ActivateDestination(
		ctx context.Context,
		plan PlanID,
		destination DestinationID,
	) error
	// PendingPlans returns every plan that still holds unfinished work,
	// including previous plans not part of this dispatcher's process
	PendingPlans(ctx context.Context) ([]PlanID, error)
}
