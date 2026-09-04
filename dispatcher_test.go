package notifier

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arkhemlol/notifier/internal/core"
)

type dispatcherStore[T any] struct {
	claimFunc    func(context.Context, core.ClaimRequest) ([]core.Work[T], error)
	resolveFunc  func(context.Context, core.Resolution) error
	enqueueErr   error
	pendingPlans []core.PlanID
	claimCalls   atomic.Int32
	resolutions  chan core.Resolution

	admitCalls atomic.Int32
	admitItems []Item[T]
}

func (s *dispatcherStore[T]) Admit(_ context.Context, _ core.Plan, items []Item[T]) error {
	s.admitCalls.Add(1)
	s.admitItems = items

	return s.enqueueErr
}

func (s *dispatcherStore[T]) Claim(
	ctx context.Context,
	request core.ClaimRequest,
) ([]core.Work[T], error) {
	s.claimCalls.Add(1)

	if s.claimFunc == nil {
		return []core.Work[T]{}, nil
	}

	return s.claimFunc(ctx, request)
}

func (s *dispatcherStore[T]) Resolve(
	ctx context.Context,
	resolution core.Resolution,
) error {
	if s.resolutions != nil {
		s.resolutions <- resolution
	}

	if s.resolveFunc == nil {
		return nil
	}

	return s.resolveFunc(ctx, resolution)
}

func (*dispatcherStore[T]) QuarantineDestination(
	context.Context,
	core.PlanID,
	core.DestinationID,
	string,
) error {
	return nil
}

func (*dispatcherStore[T]) ActivateDestination(
	context.Context,
	core.PlanID,
	core.DestinationID,
) error {
	return nil
}

func (s *dispatcherStore[T]) PendingPlans(context.Context) ([]core.PlanID, error) {
	return s.pendingPlans, nil
}

type destinationFunc[T any] struct {
	id   core.DestinationID
	send func(context.Context, []T) error
}

func (d *destinationFunc[T]) ID() core.DestinationID {
	return d.id
}

func (d *destinationFunc[T]) Send(ctx context.Context, batch []T) error {
	return d.send(ctx, batch)
}

//nolint:funlen // The table intentionally keeps all constructor cases together.
func TestNewDispatcher(t *testing.T) {
	t.Parallel()

	store := &dispatcherStore[int]{}
	valid := &destinationFunc[int]{
		id:   "destination:one",
		send: func(context.Context, []int) error { return nil },
	}

	tests := []struct {
		name         string
		store        Store[int]
		config       DispatcherConfig
		destinations []Destination[int]
		wantErr      error
	}{
		{
			name:         "valid",
			store:        store,
			config:       DispatcherConfig{Workers: 1},
			destinations: []Destination[int]{valid},
		},
		{
			name:         "no destinations",
			store:        store,
			config:       DispatcherConfig{Workers: 1},
			destinations: []Destination[int]{},
			wantErr:      ErrInvalidDestinationBinding,
		},
		{
			name:   "empty destination id",
			store:  store,
			config: DispatcherConfig{Workers: 1},
			destinations: []Destination[int]{
				&destinationFunc[int]{
					send: func(context.Context, []int) error { return nil },
				},
			},
			wantErr: ErrInvalidDestinationBinding,
		},
		{
			name:    "nil store",
			config:  DispatcherConfig{Workers: 1},
			wantErr: ErrInvalidDispatcherConfig,
		},
		{
			// Zero selects the GOMAXPROCS default rather than failing.
			name:         "zero workers",
			store:        store,
			config:       DispatcherConfig{},
			destinations: []Destination[int]{valid},
		},
		{
			name:    "negative workers",
			store:   store,
			config:  DispatcherConfig{Workers: -1},
			wantErr: ErrInvalidDispatcherConfig,
		},
		{
			name:  "tuned retry envelope",
			store: store,
			config: DispatcherConfig{
				Workers:        1,
				AttemptLimit:   3,
				AttemptTimeout: time.Second,
				InitialBackoff: 10 * time.Millisecond,
				JitterPercent:  50,
				ResolveTimeout: time.Second,
			},
			destinations: []Destination[int]{valid},
		},
		{
			name:    "attempt limit above maximum",
			store:   store,
			config:  DispatcherConfig{Workers: 1, AttemptLimit: maxAttemptLimit + 1},
			wantErr: ErrInvalidDispatcherConfig,
		},
		{
			name:    "negative attempt limit",
			store:   store,
			config:  DispatcherConfig{Workers: 1, AttemptLimit: -1},
			wantErr: ErrInvalidDispatcherConfig,
		},
		{
			name:    "initial backoff above maximum",
			store:   store,
			config:  DispatcherConfig{Workers: 1, InitialBackoff: maxInitialBackoff + 1},
			wantErr: ErrInvalidDispatcherConfig,
		},
		{
			name:    "jitter above one hundred percent",
			store:   store,
			config:  DispatcherConfig{Workers: 1, JitterPercent: 101},
			wantErr: ErrInvalidDispatcherConfig,
		},
		{
			name:   "nil destination",
			store:  store,
			config: DispatcherConfig{Workers: 1},
			destinations: []Destination[int]{
				nil,
			},
			wantErr: ErrInvalidDestinationBinding,
		},
		{
			name:   "duplicate destination",
			store:  store,
			config: DispatcherConfig{Workers: 1},
			destinations: []Destination[int]{
				valid,
				valid,
			},
			wantErr: ErrInvalidDestinationBinding,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			dispatcher, err := NewDispatcher(
				test.store,
				test.config,
				test.destinations...,
			)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("NewDispatcher() error = %v, want %v", err, test.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("NewDispatcher() error = %v", err)
			}

			if dispatcher == nil {
				t.Fatal("NewDispatcher() returned nil dispatcher")
			}
		})
	}
}

func TestDispatcherEnqueue(t *testing.T) {
	t.Parallel()

	t.Run("empty call is a no-op", func(t *testing.T) {
		t.Parallel()

		store := &dispatcherStore[int]{}
		dispatcher := mustDispatcher(t, store, 1, successfulDestination[int]("destination:one"))

		if err := dispatcher.Enqueue(t.Context()); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}

		if calls := store.admitCalls.Load(); calls != 0 {
			t.Fatalf("Admit() called %d times, want 0 for an empty Enqueue", calls)
		}
	})

	t.Run("forwards items to the store", func(t *testing.T) {
		t.Parallel()

		store := &dispatcherStore[int]{}
		dispatcher := mustDispatcher(t, store, 1, successfulDestination[int]("destination:one"))

		items := []Item[int]{{ID: 1, Payload: 7}, {ID: 2, Payload: 8}}

		if err := dispatcher.Enqueue(t.Context(), items...); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}

		if calls := store.admitCalls.Load(); calls != 1 {
			t.Fatalf("Admit() called %d times, want 1", calls)
		}

		if !slices.Equal(store.admitItems, items) {
			t.Fatalf("Admit() items = %+v, want %+v", store.admitItems, items)
		}
	})

	t.Run("wraps a store failure as an enqueue error", func(t *testing.T) {
		t.Parallel()

		failure := errors.New("boom")
		store := &dispatcherStore[int]{enqueueErr: failure}
		dispatcher := mustDispatcher(t, store, 1, successfulDestination[int]("destination:one"))

		err := dispatcher.Enqueue(t.Context(), Item[int]{ID: 1, Payload: 1})

		var dispatchErr *core.Error
		if !errors.As(err, &dispatchErr) || dispatchErr.Op != core.OpEnqueue {
			t.Fatalf("Enqueue() error = %v, want a core.Error with Op = OpEnqueue", err)
		}

		if !errors.Is(err, failure) {
			t.Fatalf("Enqueue() error = %v, want the store failure preserved", err)
		}
	})
}

func TestDispatcherMaximumBackoffWaitMatchesSchedule(t *testing.T) {
	t.Parallel()

	referenceWait := func(d *Dispatcher[int], nextAttempt int) time.Duration {
		var total time.Duration

		for attempt := nextAttempt; attempt < d.attemptLimit-1; attempt++ {
			delay := d.initialBackoff << attempt
			total += delay + delay*time.Duration(d.jitterPercent)/100
		}

		return total
	}

	configs := []DispatcherConfig{
		{Workers: 1},
		{Workers: 1, AttemptLimit: 1},
		{Workers: 1, AttemptLimit: 2, JitterPercent: 100},
		{Workers: 1, AttemptLimit: 12, InitialBackoff: 250 * time.Millisecond},
		{Workers: 1, AttemptLimit: maxAttemptLimit, InitialBackoff: time.Millisecond},
	}

	for _, config := range configs {
		dispatcher, err := NewDispatcher(
			&dispatcherStore[int]{},
			config,
			successfulDestination[int]("destination:one"),
		)
		if err != nil {
			t.Fatalf("NewDispatcher(%+v) error = %v", config, err)
		}

		for nextAttempt := range dispatcher.attemptLimit + 2 {
			want := referenceWait(dispatcher, nextAttempt)
			if got := dispatcher.maximumBackoffWait(nextAttempt); got != want {
				t.Errorf(
					"maximumBackoffWait(%d) with %+v = %v, want %v",
					nextAttempt,
					config,
					got,
					want,
				)
			}
		}
	}
}

func TestDispatcherDerivesClaimRequest(t *testing.T) {
	t.Parallel()

	var seen core.ClaimRequest

	store := &dispatcherStore[int]{
		claimFunc: func(_ context.Context, request core.ClaimRequest) ([]core.Work[int], error) {
			seen = request

			return []core.Work[int]{}, nil
		},
	}
	dispatcher := mustDispatcher(t, store, 2, successfulDestination[int]("destination:one"))

	if _, err := dispatcher.Run(t.Context()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if seen.Plan != dispatcher.plan.ID() {
		t.Errorf("Plan = %q, want %q", seen.Plan, dispatcher.plan.ID())
	}

	if seen.MaxWork != dispatcher.workers {
		t.Errorf("MaxWork = %d, want %d", seen.MaxWork, dispatcher.workers)
	}

	if seen.MaxItemsPerWork != defaultMaxItemsPerWork {
		t.Errorf("MaxItemsPerWork = %d, want %d", seen.MaxItemsPerWork, defaultMaxItemsPerWork)
	}

	want := dispatcher.requiredLease(0, dispatcher.workers)
	if seen.LeaseDuration != want {
		t.Errorf("LeaseDuration = %v, want %v", seen.LeaseDuration, want)
	}
}

func TestDispatcherRetriesRawErrorsFiveTimes(t *testing.T) {
	t.Parallel()

	providerErr := errors.New("unclassified provider failure")

	var attempts atomic.Int32

	destination := &destinationFunc[int]{
		id: "destination:one",
		send: func(context.Context, []int) error {
			attempts.Add(1)

			return providerErr
		},
	}
	resolutions := make(chan core.Resolution, 1)
	store := &dispatcherStore[int]{resolutions: resolutions}
	dispatcher := mustDispatcher(t, store, 1, destination)
	makeFast(dispatcher)
	store.claimFunc = oneWork(dispatcher, "destination:one")

	report, err := dispatcher.Run(t.Context())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if store.claimCalls.Load() != 1 {
		t.Errorf("Claim() calls = %d, want 1", store.claimCalls.Load())
	}

	if attempts.Load() != defaultAttemptLimit {
		t.Errorf("Send() attempts = %d, want %d", attempts.Load(), defaultAttemptLimit)
	}

	if len(report.Results) != 1 {
		t.Fatalf("Results length = %d, want 1", len(report.Results))
	}

	result := report.Results[0]
	if result.Outcome != OutcomeRetryableFailure {
		t.Errorf("Outcome = %v, want OutcomeRetryableFailure", result.Outcome)
	}

	if result.Attempts != defaultAttemptLimit {
		t.Errorf("Attempts = %d, want %d", result.Attempts, defaultAttemptLimit)
	}

	if !errors.Is(result.SendErr, providerErr) {
		t.Errorf("SendErr = %v, want provider error", result.SendErr)
	}

	resolution := <-resolutions
	if resolution.Outcome != OutcomeRetryableFailure || resolution.Scope != core.FailureScopeDelivery {
		t.Errorf("resolution = %+v, want retryable delivery failure", resolution)
	}

	if resolution.Failure != "" {
		t.Errorf("resolution failure = %q, want sanitized empty diagnostic", resolution.Failure)
	}
}

func TestDispatcherReportsSendDuration(t *testing.T) {
	t.Parallel()

	const sendDelay = 20 * time.Millisecond

	destination := &destinationFunc[int]{
		id: "destination:one",
		send: func(context.Context, []int) error {
			time.Sleep(sendDelay)

			return nil
		},
	}
	store := &dispatcherStore[int]{}
	dispatcher := mustDispatcher(t, store, 1, destination)
	makeFast(dispatcher)
	store.claimFunc = oneWork(dispatcher, "destination:one")

	report, err := dispatcher.Run(t.Context())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(report.Results) != 1 {
		t.Fatalf("Results length = %d, want 1", len(report.Results))
	}

	if duration := report.Results[0].Duration; duration < sendDelay {
		t.Errorf("Duration = %v, want at least %v", duration, sendDelay)
	}
}

func TestDispatcherUsesFreshAttemptTimeouts(t *testing.T) {
	t.Parallel()

	if defaultAttemptTimeout != 30*time.Second {
		t.Fatalf("default attempt timeout = %v, want 30s", defaultAttemptTimeout)
	}

	attemptDone := make([]<-chan struct{}, 0, 3)
	remaining := make([]time.Duration, 0, 3)
	missingDeadline := false
	destination := &destinationFunc[int]{
		id: "destination:one",
		send: func(ctx context.Context, _ []int) error {
			deadline, ok := ctx.Deadline()
			if !ok {
				missingDeadline = true
			}

			attemptDone = append(attemptDone, ctx.Done())
			remaining = append(remaining, time.Until(deadline))

			if len(attemptDone) < 3 {
				return errors.New("retry attempt")
			}

			return nil
		},
	}
	store := &dispatcherStore[int]{}
	dispatcher := mustDispatcher(t, store, 1, destination)
	makeFast(dispatcher)
	dispatcher.attemptTimeout = 50 * time.Millisecond
	store.claimFunc = oneWork(dispatcher, "destination:one")

	report, err := dispatcher.Run(t.Context())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if report.Results[0].Attempts != 3 {
		t.Fatalf("Attempts = %d, want 3", report.Results[0].Attempts)
	}

	if missingDeadline {
		t.Fatal("attempt context has no deadline")
	}

	for i, timeout := range remaining {
		if timeout <= 0 || timeout > dispatcher.attemptTimeout {
			t.Errorf("attempt %d timeout = %v, want (0, %v]", i+1, timeout, dispatcher.attemptTimeout)
		}

		if i > 0 && attemptDone[i] == attemptDone[i-1] {
			t.Errorf("attempt %d reused the previous attempt context", i+1)
		}
	}
}

func TestDispatcherProviderDelayIsMinimum(t *testing.T) {
	t.Parallel()

	const providerDelay = 5 * time.Millisecond

	var attempts atomic.Int32

	destination := &destinationFunc[int]{
		id: "destination:one",
		send: func(context.Context, []int) error {
			if attempts.Add(1) == 1 {
				return core.RetryableAfter(errors.New("rate limited"), providerDelay)
			}

			return nil
		},
	}
	store := &dispatcherStore[int]{}
	dispatcher := mustDispatcher(t, store, 1, destination)
	makeFast(dispatcher)
	store.claimFunc = oneWorkWithLease(
		dispatcher,
		"destination:one",
		providerDelay+time.Hour,
	)

	report, err := dispatcher.Run(t.Context())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if report.Results[0].Outcome != OutcomeDelivered {
		t.Errorf("Outcome = %v, want OutcomeDelivered", report.Results[0].Outcome)
	}

	if report.Results[0].Attempts != 2 {
		t.Errorf("Attempts = %d, want 2", report.Results[0].Attempts)
	}
}

func TestDispatcherAttemptCancellationIsAlwaysRetryable(t *testing.T) {
	t.Parallel()

	destination := &destinationFunc[int]{
		id: "destination:one",
		send: func(ctx context.Context, _ []int) error {
			<-ctx.Done()

			return core.Permanent(ctx.Err())
		},
	}
	resolutions := make(chan core.Resolution, 1)
	store := &dispatcherStore[int]{resolutions: resolutions}
	dispatcher := mustDispatcher(t, store, 1, destination)
	makeFast(dispatcher)
	dispatcher.attemptTimeout = time.Millisecond
	store.claimFunc = oneWork(dispatcher, "destination:one")

	report, err := dispatcher.Run(t.Context())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if report.Results[0].Outcome != OutcomeRetryableFailure {
		t.Errorf("Outcome = %v, want OutcomeRetryableFailure", report.Results[0].Outcome)
	}

	resolution := <-resolutions
	if resolution.Outcome != OutcomeRetryableFailure ||
		resolution.Scope != core.FailureScopeDelivery {
		t.Errorf("resolution = %+v, want retryable delivery failure", resolution)
	}
}

func TestDispatcherPermanentFailureDoesNotRetry(t *testing.T) {
	t.Parallel()

	providerErr := errors.New("recipient rejected")

	var attempts atomic.Int32

	destination := &destinationFunc[int]{
		id: "destination:one",
		send: func(context.Context, []int) error {
			attempts.Add(1)

			return core.Permanent(providerErr)
		},
	}
	resolutions := make(chan core.Resolution, 1)
	store := &dispatcherStore[int]{resolutions: resolutions}
	dispatcher := mustDispatcher(t, store, 1, destination)
	makeFast(dispatcher)
	store.claimFunc = oneWork(dispatcher, "destination:one")

	report, err := dispatcher.Run(t.Context())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if attempts.Load() != 1 {
		t.Errorf("Send() attempts = %d, want 1", attempts.Load())
	}

	if report.Results[0].Outcome != OutcomeFailedPermanent {
		t.Errorf("Outcome = %v, want OutcomeFailedPermanent", report.Results[0].Outcome)
	}

	resolution := <-resolutions
	if resolution.Scope != core.FailureScopeDelivery {
		t.Errorf("resolution scope = %v, want core.FailureScopeDelivery", resolution.Scope)
	}

	if resolution.Failure != permanentDeliveryFailure {
		t.Errorf("resolution failure = %q, want sanitized diagnostic", resolution.Failure)
	}
}

func TestDispatcherQuarantineUsesDestinationScope(t *testing.T) {
	t.Parallel()

	providerErr := errors.New("destination access revoked")
	destination := &destinationFunc[int]{
		id: "destination:one",
		send: func(context.Context, []int) error {
			return core.Quarantine(providerErr)
		},
	}
	resolutions := make(chan core.Resolution, 1)
	store := &dispatcherStore[int]{resolutions: resolutions}
	dispatcher := mustDispatcher(t, store, 1, destination)
	makeFast(dispatcher)
	store.claimFunc = oneWork(dispatcher, "destination:one")

	report, err := dispatcher.Run(t.Context())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if !errors.Is(report.Results[0].SendErr, providerErr) {
		t.Errorf("SendErr = %v, want provider cause", report.Results[0].SendErr)
	}

	resolution := <-resolutions
	if resolution.Outcome != OutcomeFailedPermanent ||
		resolution.Scope != core.FailureScopeDestination {
		t.Errorf("resolution = %+v, want destination quarantine", resolution)
	}

	if resolution.Failure != permanentDestinationError {
		t.Errorf("resolution failure = %q, want sanitized diagnostic", resolution.Failure)
	}
}

func TestDispatcherTerminalizesUnboundDestination(t *testing.T) {
	t.Parallel()

	store := &dispatcherStore[int]{resolutions: make(chan core.Resolution, 1)}
	dispatcher := mustDispatcher(t, store, 1, successfulDestination[int]("destination:one"))
	makeFast(dispatcher)
	store.claimFunc = oneWork(dispatcher, "destination:unbound")

	report, err := dispatcher.Run(t.Context())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(report.Results) != 1 {
		t.Fatalf("Results length = %d, want 1", len(report.Results))
	}

	result := report.Results[0]
	if result.Outcome != OutcomeFailedPermanent {
		t.Errorf("Outcome = %v, want OutcomeFailedPermanent", result.Outcome)
	}

	if !errors.Is(result.SendErr, ErrDestinationImplementationMissing) {
		t.Errorf("SendErr = %v, want ErrDestinationImplementationMissing", result.SendErr)
	}

	resolution := <-store.resolutions
	if resolution.Outcome != OutcomeFailedPermanent ||
		resolution.Failure != unboundDestinationFailure {
		t.Errorf("resolution = %+v, want a permanent unbound failure", resolution)
	}
}

func TestDispatcherInsufficientWorkLeaseSkipsSend(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	destination := &destinationFunc[int]{
		id: "destination:one",
		send: func(context.Context, []int) error {
			attempts.Add(1)

			return nil
		},
	}
	resolutions := make(chan core.Resolution, 1)
	store := &dispatcherStore[int]{resolutions: resolutions}
	dispatcher := mustDispatcher(t, store, 1, destination)
	makeFast(dispatcher)
	store.claimFunc = oneWorkWithLease(dispatcher, "destination:one", time.Nanosecond)

	report, err := dispatcher.Run(t.Context())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if attempts.Load() != 0 {
		t.Errorf("Send() attempts = %d, want 0", attempts.Load())
	}

	if !errors.Is(report.Results[0].SendErr, errInsufficientLeaseBudget) {
		t.Errorf("SendErr = %v, want insufficient lease budget", report.Results[0].SendErr)
	}

	resolution := <-resolutions
	if resolution.Outcome != OutcomeRetryableFailure {
		t.Errorf("resolution outcome = %v, want retryable", resolution.Outcome)
	}
}

// TestDispatcherFullWaveClaimSurvivesDispatchLatency reproduces a store that
// grants exactly the requested LeaseDuration, as sqlite's does: with a fully
// packed wave (claimed batches == Workers), requiredLease computes the same
// budget at claim and at the first withinLease check, so a naive recheck of
// that same budget would fail on any wall-clock gap between the two (even a
// scheduling delay) before the batch is ever sent. The first attempt must
// only require the lease to still be alive, not the full budget again — so
// this holds regardless of how much of the lease that gap eats into, not
// just for delays under some margin.
func TestDispatcherFullWaveClaimSurvivesDispatchLatency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// leaseFraction of the granted lease is spent simulating latency
		// between claim and the first send, e.g. 0.01 for a brief scheduling
		// delay, 0.9 for one that nearly exhausts the lease.
		leaseFraction float64
	}{
		{name: "brief scheduling delay", leaseFraction: 0.01},
		{name: "delay nearly exhausts the lease", leaseFraction: 0.9},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var attempts atomic.Int32

			destination := &destinationFunc[int]{
				id: "destination:one",
				send: func(context.Context, []int) error {
					attempts.Add(1)

					return nil
				},
			}
			store := &dispatcherStore[int]{resolutions: make(chan core.Resolution, 1)}
			dispatcher := mustDispatcher(t, store, 1, destination)
			makeFast(dispatcher)

			store.claimFunc = func(
				_ context.Context, request core.ClaimRequest,
			) ([]core.Work[int], error) {
				leaseUntil := time.Now().Add(request.LeaseDuration)

				// Simulate latency between claim and send: a DB round trip
				// or goroutine scheduling delay, sized against this run's
				// own lease rather than a fixed guess.
				time.Sleep(time.Duration(float64(request.LeaseDuration) * test.leaseFraction))

				return []core.Work[int]{
					newWork(dispatcher, "work-1", "destination:one", time.Until(leaseUntil)),
				}, nil
			}

			report, err := dispatcher.Run(t.Context())
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}

			if attempts.Load() != 1 {
				t.Errorf("Send() attempts = %d, want 1", attempts.Load())
			}

			if report.Results[0].SendErr != nil {
				t.Errorf("SendErr = %v, want nil", report.Results[0].SendErr)
			}
		})
	}
}

func TestDispatcherJoinsResolveErrors(t *testing.T) {
	t.Parallel()

	firstErr := errors.New("first resolution failed")
	secondErr := errors.New("second resolution failed")
	store := &dispatcherStore[int]{}
	destination := successfulDestination[int]("destination:one")
	dispatcher := mustDispatcher(t, store, 2, destination)
	makeFast(dispatcher)

	store.claimFunc = func(context.Context, core.ClaimRequest) ([]core.Work[int], error) {
		return []core.Work[int]{
			newWork(dispatcher, "work-1", "destination:one", time.Hour),
			newWork(dispatcher, "work-2", "destination:one", time.Hour),
		}, nil
	}
	store.resolveFunc = func(_ context.Context, resolution core.Resolution) error {
		if resolution.Work == "work-1" {
			return firstErr
		}

		return secondErr
	}

	report, err := dispatcher.Run(t.Context())
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("Run() error = %v, want both resolution errors", err)
	}

	if len(report.Results) != 2 {
		t.Fatalf("Results length = %d, want 2", len(report.Results))
	}

	for _, result := range report.Results {
		if result.Outcome != OutcomeDeliveredUnrecorded || result.ResolveErr == nil {
			t.Errorf("result = %+v, want delivered-unrecorded with resolve error", result)
		}
	}
}

func TestDispatcherRetriesTransientResolveFailures(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	store := &dispatcherStore[int]{}
	destination := successfulDestination[int]("destination:one")
	dispatcher := mustDispatcher(t, store, 1, destination)
	makeFast(dispatcher)
	store.claimFunc = oneWork(dispatcher, "destination:one")
	store.resolveFunc = func(context.Context, core.Resolution) error {
		if attempts.Add(1) == 1 {
			return fmt.Errorf("write conflict: %w", core.ErrStoreBusy)
		}

		return nil
	}

	report, err := dispatcher.Run(t.Context())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if attempts.Load() != 2 {
		t.Errorf("Resolve() attempts = %d, want 2", attempts.Load())
	}

	if result := report.Results[0]; result.ResolveErr != nil ||
		result.Outcome != OutcomeDelivered {
		t.Errorf("result = %+v, want a clean retry", result)
	}
}

func TestDispatcherDoesNotRetryPermanentResolveFailures(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	storeErr := errors.New("contradicts persisted state")
	store := &dispatcherStore[int]{}
	destination := successfulDestination[int]("destination:one")
	dispatcher := mustDispatcher(t, store, 1, destination)
	makeFast(dispatcher)
	store.claimFunc = oneWork(dispatcher, "destination:one")
	store.resolveFunc = func(context.Context, core.Resolution) error {
		attempts.Add(1)

		return storeErr
	}

	report, err := dispatcher.Run(t.Context())
	if !errors.Is(err, storeErr) {
		t.Fatalf("Run() error = %v, want the store error", err)
	}

	if attempts.Load() != 1 {
		t.Errorf("Resolve() attempts = %d, want 1", attempts.Load())
	}

	if report.Results[0].Outcome != OutcomeDeliveredUnrecorded {
		t.Errorf("result = %+v, want OutcomeDeliveredUnrecorded", report.Results[0])
	}
}

func TestDispatcherBoundsSendsAndSerializesResolves(t *testing.T) {
	t.Parallel()

	const workers = 4

	entered := make(chan struct{}, workers)
	release := make(chan struct{})

	var (
		activeSends atomic.Int32
		maxSends    atomic.Int32
	)

	destination := &destinationFunc[int]{
		id: "destination:one",
		send: func(context.Context, []int) error {
			active := activeSends.Add(1)
			updateMaximum(&maxSends, active)

			entered <- struct{}{}

			<-release
			activeSends.Add(-1)

			return nil
		},
	}

	var (
		activeResolves atomic.Int32
		maxResolves    atomic.Int32
	)

	store := &dispatcherStore[int]{}
	store.resolveFunc = func(context.Context, core.Resolution) error {
		active := activeResolves.Add(1)
		updateMaximum(&maxResolves, active)
		time.Sleep(time.Millisecond)
		activeResolves.Add(-1)

		return nil
	}
	dispatcher := mustDispatcher(t, store, workers, destination)
	makeFast(dispatcher)

	store.claimFunc = func(context.Context, core.ClaimRequest) ([]core.Work[int], error) {
		work := make([]core.Work[int], 0, workers)
		for i := range workers {
			work = append(
				work,
				newWork(
					dispatcher,
					core.WorkID(fmt.Sprintf("work-%d", i)),
					"destination:one",
					time.Hour,
				),
			)
		}

		return work, nil
	}

	done := make(chan error, 1)

	go func() {
		_, err := dispatcher.Run(t.Context())
		done <- err
	}()

	for range workers {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for bounded sends to start")
		}
	}

	close(release)

	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if maxSends.Load() != workers {
		t.Errorf("maximum concurrent sends = %d, want %d", maxSends.Load(), workers)
	}

	if maxResolves.Load() != 1 {
		t.Errorf("maximum concurrent resolves = %d, want 1", maxResolves.Load())
	}
}

func TestDispatcherSerializesConcurrentRuns(t *testing.T) {
	t.Parallel()

	claimStarted := make(chan struct{})
	releaseClaim := make(chan struct{})
	store := &dispatcherStore[int]{}
	store.claimFunc = func(context.Context, core.ClaimRequest) ([]core.Work[int], error) {
		close(claimStarted)
		<-releaseClaim

		return []core.Work[int]{}, nil
	}
	dispatcher := mustDispatcher(t, store, 1, successfulDestination[int]("destination:one"))
	makeFast(dispatcher)

	firstDone := make(chan error, 1)

	go func() {
		_, err := dispatcher.Run(t.Context())
		firstDone <- err
	}()

	<-claimStarted

	secondCtx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	_, secondErr := dispatcher.Run(secondCtx)
	if !errors.Is(secondErr, context.DeadlineExceeded) {
		t.Fatalf("second Run() error = %v, want context deadline", secondErr)
	}

	if store.claimCalls.Load() != 1 {
		t.Errorf("Claim() calls while first run is active = %d, want 1", store.claimCalls.Load())
	}

	close(releaseClaim)

	if err := <-firstDone; err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
}

func TestDispatcherCancellationResolvesWithCleanupContext(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	destination := &destinationFunc[int]{
		id: "destination:one",
		send: func(ctx context.Context, _ []int) error {
			close(started)
			<-ctx.Done()

			return ctx.Err()
		},
	}
	resolved := make(chan core.Resolution, 1)
	store := &dispatcherStore[int]{resolutions: resolved}
	dispatcher := mustDispatcher(t, store, 1, destination)
	makeFast(dispatcher)
	store.claimFunc = oneWork(dispatcher, "destination:one")

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() {
		_, err := dispatcher.Run(ctx)
		done <- err
	}()

	<-started
	cancel()

	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}

	resolution := <-resolved
	if resolution.Outcome != OutcomeRetryableFailure ||
		resolution.Scope != core.FailureScopeDelivery {
		t.Errorf("resolution = %+v, want retryable delivery failure", resolution)
	}
}

func mustDispatcher[T any](
	t *testing.T,
	store Store[T],
	workers int,
	destinations ...Destination[T],
) *Dispatcher[T] {
	t.Helper()

	dispatcher, err := NewDispatcher(
		store,
		DispatcherConfig{Workers: workers},
		destinations...,
	)
	if err != nil {
		t.Fatalf("NewDispatcher() error = %v", err)
	}

	return dispatcher
}

func successfulDestination[T any](id core.DestinationID) Destination[T] {
	return &destinationFunc[T]{
		id:   id,
		send: func(context.Context, []T) error { return nil },
	}
}

func makeFast[T any](dispatcher *Dispatcher[T]) {
	dispatcher.attemptTimeout = time.Millisecond
	dispatcher.resolveTimeout = time.Second

	dispatcher.initialBackoff = time.Millisecond
}

func oneWork[T any](
	dispatcher *Dispatcher[T],
	destination core.DestinationID,
) func(context.Context, core.ClaimRequest) ([]core.Work[T], error) {
	return oneWorkWithLease(dispatcher, destination, time.Hour)
}

func oneWorkWithLease[T any](
	dispatcher *Dispatcher[T],
	destination core.DestinationID,
	lease time.Duration,
) func(context.Context, core.ClaimRequest) ([]core.Work[T], error) {
	return func(context.Context, core.ClaimRequest) ([]core.Work[T], error) {
		return []core.Work[T]{newWork(dispatcher, "work-1", destination, lease)}, nil
	}
}

func newWork[T any](
	dispatcher *Dispatcher[T],
	id core.WorkID,
	destination core.DestinationID,
	lease time.Duration,
) core.Work[T] {
	var payload T

	return core.Work[T]{
		ID:          id,
		Plan:        dispatcher.plan.ID(),
		Destination: destination,
		Items:       []Item[T]{{ID: 42, Payload: payload}},
		Attempt:     1,
		Lease:       "lease-1",
		LeaseUntil:  time.Now().Add(lease),
	}
}

func updateMaximum(maximum *atomic.Int32, value int32) {
	for {
		current := maximum.Load()
		if value <= current || maximum.CompareAndSwap(current, value) {
			return
		}
	}
}

func TestDispatcherStartRunsImmediatelyAndStopsOnCancel(t *testing.T) {
	t.Parallel()

	var cycles atomic.Int32

	store := &dispatcherStore[int]{}
	dispatcher := mustDispatcher(t, store, 1, successfulDestination[int]("destination:one"))
	makeFast(dispatcher)

	ctx, cancel := context.WithCancel(t.Context())
	cycled := make(chan struct{}, 1)

	wait := dispatcher.Start(ctx, time.Hour, func(_ Report, err error) {
		if err != nil {
			t.Errorf("cycle error = %v", err)
		}

		cycles.Add(1)

		select {
		case cycled <- struct{}{}:
		default:
		}
	})

	// The first cycle must not wait out the interval, which is an hour here.
	select {
	case <-cycled:
	case <-time.After(2 * time.Second):
		t.Fatal("Start() did not run a cycle immediately")
	}

	cancel()

	if err := wait(t.Context()); err != nil {
		t.Fatalf("Wait() error = %v, want the loop to have stopped", err)
	}
	// Wait is a join, not a one-shot: a second call still reports stopped.
	if err := wait(t.Context()); err != nil {
		t.Fatalf("second Wait() error = %v", err)
	}

	if cycles.Load() == 0 {
		t.Error("no cycle ran")
	}
}

func TestJobWaitTimesOutWithoutCancel(t *testing.T) {
	t.Parallel()

	store := &dispatcherStore[int]{}
	dispatcher := mustDispatcher(t, store, 1, successfulDestination[int]("destination:one"))
	makeFast(dispatcher)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	wait := dispatcher.Start(ctx, time.Hour, nil)

	waitCtx, waitCancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer waitCancel()

	if err := wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait() error = %v, want deadline exceeded", err)
	}

	cancel()

	if err := wait(t.Context()); err != nil {
		t.Fatalf("Wait() after cancel error = %v", err)
	}
}

func TestDispatcherFlushesUnrecordedOnNextRun(t *testing.T) {
	t.Parallel()

	var resolves atomic.Int32

	store := &dispatcherStore[int]{}
	dispatcher := mustDispatcher(t, store, 1, successfulDestination[int]("destination:one"))
	makeFast(dispatcher)
	store.claimFunc = oneWork(dispatcher, "destination:one")
	store.resolveFunc = func(context.Context, core.Resolution) error {
		if resolves.Add(1) == 1 {
			return errors.New("store refused the write")
		}

		return nil
	}

	report, err := dispatcher.Run(t.Context())
	if err == nil {
		t.Fatal("Run() error = nil, want the resolve failure")
	}

	if report.Results[0].Outcome != OutcomeDeliveredUnrecorded {
		t.Fatalf("Outcome = %v, want OutcomeDeliveredUnrecorded", report.Results[0].Outcome)
	}

	if got := dispatcher.unrecordedCount(); got != 1 {
		t.Fatalf("unrecorded entries = %d, want 1", got)
	}

	store.claimFunc = func(context.Context, core.ClaimRequest) ([]core.Work[int], error) {
		return []core.Work[int]{}, nil
	}

	if _, err := dispatcher.Run(t.Context()); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}

	if got := dispatcher.unrecordedCount(); got != 0 {
		t.Errorf("unrecorded entries = %d, want 0 after flush", got)
	}
}

func TestDispatcherDropsUnrecordedAfterLeaseExpiry(t *testing.T) {
	t.Parallel()

	store := &dispatcherStore[int]{}
	dispatcher := mustDispatcher(t, store, 1, successfulDestination[int]("destination:one"))
	makeFast(dispatcher)

	dispatcher.deferResolution(core.Resolution{Work: "work-1"}, time.Now().Add(-time.Minute))

	if _, err := dispatcher.Run(t.Context()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := dispatcher.unrecordedCount(); got != 0 {
		t.Errorf("unrecorded entries = %d, want 0", got)
	}
}

func TestDispatcherDrainsWorkFromAnEarlierPlan(t *testing.T) {
	t.Parallel()

	stale := core.PlanID("plan-from-a-previous-destination-set")
	store := &dispatcherStore[int]{resolutions: make(chan core.Resolution, 1)}
	dispatcher := mustDispatcher(t, store, 1, successfulDestination[int]("destination:one"))
	makeFast(dispatcher)

	store.pendingPlans = []core.PlanID{stale, dispatcher.plan.ID()}
	store.claimFunc = func(_ context.Context, request core.ClaimRequest) ([]core.Work[int], error) {
		if request.Plan != stale {
			return []core.Work[int]{}, nil
		}

		work := newWork(dispatcher, "work-stale", "destination:gone", time.Hour)
		work.Plan = stale

		return []core.Work[int]{work}, nil
	}

	report, err := dispatcher.Run(t.Context())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(report.Results) != 1 {
		t.Fatalf("Results length = %d, want 1", len(report.Results))
	}

	if report.Results[0].Outcome != OutcomeFailedPermanent {
		t.Errorf("Outcome = %v, want the stale batch terminalized", report.Results[0].Outcome)
	}

	if resolution := <-store.resolutions; resolution.Failure != unboundDestinationFailure {
		t.Errorf("resolution = %+v, want an unbound-destination failure", resolution)
	}
}

func (d *Dispatcher[T]) unrecordedCount() int {
	d.unrecordedMu.Lock()
	defer d.unrecordedMu.Unlock()

	return len(d.unrecorded)
}

func TestDispatcherConfigDefaultsWorkersToGOMAXPROCS(t *testing.T) {
	t.Parallel()

	dispatcher, err := NewDispatcher(
		&dispatcherStore[int]{},
		DispatcherConfig{},
		successfulDestination[int]("destination:one"),
	)
	if err != nil {
		t.Fatalf("NewDispatcher() error = %v", err)
	}

	want := max(runtime.GOMAXPROCS(0), 1)
	if dispatcher.workers != want {
		t.Errorf("workers = %d, want GOMAXPROCS %d", dispatcher.workers, want)
	}
}

func TestDispatcherConfigNeverYieldsZeroWorkers(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{0, 1, 4} {
		config := DispatcherConfig{Workers: workers}
		if err := config.validate(); err != nil {
			t.Fatalf("validate(Workers=%d) error = %v", workers, err)
		}

		if got := config.withDefaults().Workers; got < 1 {
			t.Errorf("Workers %d defaulted to %d, want at least 1", workers, got)
		}
	}

	if err := (DispatcherConfig{Workers: -1}).validate(); !errors.Is(
		err,
		ErrInvalidDispatcherConfig,
	) {
		t.Errorf("validate(Workers=-1) error = %v, want ErrInvalidDispatcherConfig", err)
	}
}
