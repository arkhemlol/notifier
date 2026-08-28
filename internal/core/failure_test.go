package core

import (
	"errors"
	"testing"
	"time"
)

func TestDestinationErrorConstructorsPreserveNil(t *testing.T) {
	t.Parallel()

	constructors := []struct {
		name string
		call func() error
	}{
		{name: "retryable", call: func() error { return Retryable(nil) }},
		{
			name: "retryable after",
			call: func() error { return RetryableAfter(nil, time.Minute) },
		},
		{name: "permanent", call: func() error { return Permanent(nil) }},
		{name: "quarantine", call: func() error { return Quarantine(nil) }},
	}

	for _, constructor := range constructors {
		t.Run(constructor.name, func(t *testing.T) {
			t.Parallel()

			if err := constructor.call(); err != nil {
				t.Errorf("constructor error = %v, want nil", err)
			}
		})
	}
}

func TestDestinationErrorClassification(t *testing.T) {
	t.Parallel()

	cause := errors.New("provider rejected request")
	tests := []struct {
		name          string
		err           error
		wantIs        []error
		wantIsNot     []error
		wantDelay     time.Duration
		wantPermanent bool
		wantScope     FailureScope
		wantErrorText string
	}{
		{
			name:          "retryable",
			err:           Retryable(cause),
			wantIs:        []error{ErrRetryable},
			wantIsNot:     []error{ErrPermanent, ErrQuarantine},
			wantScope:     FailureScopeDelivery,
			wantErrorText: "destination delivery failed temporarily: provider rejected request",
		},
		{
			name:          "retryable after",
			err:           RetryableAfter(cause, 30*time.Second),
			wantIs:        []error{ErrRetryable},
			wantIsNot:     []error{ErrPermanent, ErrQuarantine},
			wantDelay:     30 * time.Second,
			wantScope:     FailureScopeDelivery,
			wantErrorText: "destination delivery delayed for 30s: provider rejected request",
		},
		{
			name:          "permanent",
			err:           Permanent(cause),
			wantIs:        []error{ErrPermanent},
			wantIsNot:     []error{ErrRetryable, ErrQuarantine},
			wantPermanent: true,
			wantScope:     FailureScopeDelivery,
			wantErrorText: "destination delivery failed permanently: provider rejected request",
		},
		{
			name:          "quarantine is also permanent",
			err:           Quarantine(cause),
			wantIs:        []error{ErrPermanent, ErrQuarantine},
			wantIsNot:     []error{ErrRetryable},
			wantPermanent: true,
			wantScope:     FailureScopeDestination,
			wantErrorText: "destination quarantined: provider rejected request",
		},
		{
			name:          "non-positive delay",
			err:           RetryableAfter(cause, 0),
			wantIs:        []error{ErrRetryable},
			wantIsNot:     []error{ErrPermanent, ErrQuarantine},
			wantScope:     FailureScopeDelivery,
			wantErrorText: "destination delivery failed temporarily: provider rejected request",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if !errors.Is(test.err, cause) {
				t.Fatal("classified error does not preserve its cause")
			}

			for _, sentinel := range test.wantIs {
				if !errors.Is(test.err, sentinel) {
					t.Errorf("errors.Is(_, %v) = false, want true", sentinel)
				}
			}

			for _, sentinel := range test.wantIsNot {
				if errors.Is(test.err, sentinel) {
					t.Errorf("errors.Is(_, %v) = true, want false", sentinel)
				}
			}

			failure, classified := FailureOf(test.err)
			if !classified {
				t.Fatal("FailureOf() did not recognise a classified error")
			}

			if failure.RetryAfter != test.wantDelay {
				t.Errorf(
					"FailureOf().RetryAfter = %v, want %v",
					failure.RetryAfter,
					test.wantDelay,
				)
			}

			if failure.Permanent != test.wantPermanent {
				t.Errorf(
					"FailureOf().Permanent = %v, want %v",
					failure.Permanent,
					test.wantPermanent,
				)
			}

			if failure.Scope != test.wantScope {
				t.Errorf("FailureOf().Scope = %v, want %v", failure.Scope, test.wantScope)
			}

			if test.err.Error() != test.wantErrorText {
				t.Errorf("Error() = %q, want %q", test.err, test.wantErrorText)
			}
		})
	}
}

// The public constructors never produce a *destinationError with a nil cause
// (newDestinationError returns nil outright when err is nil), so this
// defensive branch of Error() is only reachable through the struct literal
// directly - confirm it still formats sensibly instead of panicking.
func TestDestinationErrorFormatsWithoutACause(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  *destinationError
		want string
	}{
		{
			name: "retryable without delay",
			err:  &destinationError{class: failureClassRetryable},
			want: "destination delivery failed temporarily",
		},
		{
			name: "retryable with delay",
			err:  &destinationError{class: failureClassRetryable, retryAfter: time.Minute},
			want: "destination delivery delayed for 1m0s",
		},
		{
			name: "permanent delivery scope",
			err:  &destinationError{class: failureClassPermanent, scope: FailureScopeDelivery},
			want: "destination delivery failed permanently",
		},
		{
			name: "permanent destination scope",
			err:  &destinationError{class: failureClassPermanent, scope: FailureScopeDestination},
			want: "destination quarantined",
		},
		{
			name: "unclassified",
			err:  &destinationError{},
			want: "destination failed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := test.err.Error(); got != test.want {
				t.Errorf("Error() = %q, want %q", got, test.want)
			}

			if got := test.err.Unwrap(); got != nil {
				t.Errorf("Unwrap() = %v, want nil", got)
			}
		})
	}
}

func TestFailureOfIgnoresUnclassifiedErrors(t *testing.T) {
	t.Parallel()

	if _, ok := FailureOf(errors.New("plain")); ok {
		t.Error("FailureOf() matched an unclassified error")
	}

	if _, ok := FailureOf(nil); ok {
		t.Error("FailureOf(nil) reported a classification")
	}
}
