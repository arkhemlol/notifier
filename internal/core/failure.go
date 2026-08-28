package core

import (
	"errors"
	"fmt"
	"time"
)

// Sentinels errors.Is matches against the classification a destination applied via Retryable, Permanent, or Quarantine.
var (
	ErrRetryable  = errors.New("destination failure is retryable")
	ErrPermanent  = errors.New("destination failure is permanent")
	ErrQuarantine = errors.New("destination is unusable")
)

// FailureScope describes whether a failure affects one delivery or a destination.
type FailureScope int

// FailureScope values, from unclassified to the delivery- and destination-wide scopes a failure can carry.
const (
	FailureScopeUnknown FailureScope = iota
	FailureScopeDelivery
	FailureScopeDestination
)

type failureClass int

const (
	failureClassUnknown failureClass = iota
	failureClassRetryable
	failureClassPermanent
)

type destinationError struct {
	class      failureClass
	scope      FailureScope
	retryAfter time.Duration
	err        error
}

func (e *destinationError) Error() string {
	message := "destination failed"

	switch {
	case e.class == failureClassRetryable && e.retryAfter > 0:
		message = fmt.Sprintf("destination delivery delayed for %s", e.retryAfter)
	case e.class == failureClassRetryable:
		message = "destination delivery failed temporarily"
	case e.class == failureClassPermanent && e.scope == FailureScopeDelivery:
		message = "destination delivery failed permanently"
	case e.class == failureClassPermanent && e.scope == FailureScopeDestination:
		message = "destination quarantined"
	}

	if e.err == nil {
		return message
	}

	return message + ": " + e.err.Error()
}

func (e *destinationError) Unwrap() error {
	return e.err
}

func (e *destinationError) Is(target error) bool {
	switch target {
	case ErrRetryable:
		return e.class == failureClassRetryable
	case ErrPermanent:
		return e.class == failureClassPermanent
	case ErrQuarantine:
		return e.class == failureClassPermanent &&
			e.scope == FailureScopeDestination
	default:
		return false
	}
}

// Retryable classifies err as a retryable delivery-scoped failure.
func Retryable(err error) error {
	return newDestinationError(err, failureClassRetryable, FailureScopeDelivery, 0)
}

// RetryableAfter classifies err with a provider-specified minimum retry delay.
func RetryableAfter(err error, delay time.Duration) error {
	if delay <= 0 {
		return Retryable(err)
	}

	return newDestinationError(err, failureClassRetryable, FailureScopeDelivery, delay)
}

// Permanent classifies err as a permanent failure for one delivery.
func Permanent(err error) error {
	return newDestinationError(err, failureClassPermanent, FailureScopeDelivery, 0)
}

// Quarantine classifies err as a permanent destination-wide failure.
func Quarantine(err error) error {
	return newDestinationError(err, failureClassPermanent, FailureScopeDestination, 0)
}

func newDestinationError(
	err error,
	class failureClass,
	scope FailureScope,
	retryAfter time.Duration,
) error {
	if err == nil {
		return nil
	}

	return &destinationError{
		class:      class,
		scope:      scope,
		retryAfter: retryAfter,
		err:        err,
	}
}

// Failure is the classification FailureOf reads back off a destination error.
type Failure struct {
	Permanent bool
	Scope     FailureScope
	// RetryAfter is the minimum delay the provider asked for, or zero.
	RetryAfter time.Duration
}

// FailureOf reads the classification a destination applied to err.
// It reports false for an unclassified error, which the dispatcher then treats as retryable.
func FailureOf(err error) (Failure, bool) {
	var classified *destinationError
	if !errors.As(err, &classified) || classified == nil {
		return Failure{}, false
	}

	return Failure{
		Permanent:  classified.class == failureClassPermanent,
		Scope:      classified.scope,
		RetryAfter: classified.retryAfter,
	}, true
}
