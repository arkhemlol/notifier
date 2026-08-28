package notifier

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/arkhemlol/notifier/internal/core"
)

type probeStore[T any] struct {
	registerErr error
	recordErr   error
	registered  atomic.Int32
	mu          sync.Mutex
	checks      []recordedCheck
}

func (s *probeStore[T]) Admit(context.Context, core.Plan, []Item[T]) error {
	s.registered.Add(1)

	return s.registerErr
}

func (*probeStore[T]) Claim(context.Context, core.ClaimRequest) ([]core.Work[T], error) {
	return []core.Work[T]{}, nil
}

func (*probeStore[T]) Resolve(context.Context, core.Resolution) error {
	return nil
}

type recordedCheck struct {
	destination core.DestinationID
	quarantined bool
	failure     string
}

func (s *probeStore[T]) QuarantineDestination(
	_ context.Context,
	_ core.PlanID,
	destination core.DestinationID,
	failure string,
) error {
	return s.record(recordedCheck{
		destination: destination,
		quarantined: true,
		failure:     failure,
	})
}

func (s *probeStore[T]) ActivateDestination(
	_ context.Context,
	_ core.PlanID,
	destination core.DestinationID,
) error {
	return s.record(recordedCheck{destination: destination})
}

func (*probeStore[T]) PendingPlans(context.Context) ([]core.PlanID, error) {
	return nil, nil
}

func (s *probeStore[T]) record(check recordedCheck) error {
	if s.recordErr != nil {
		return s.recordErr
	}

	s.mu.Lock()
	s.checks = append(s.checks, check)
	s.mu.Unlock()

	return nil
}

type probingDestination[T any] struct {
	destinationFunc[T]
	probe func(context.Context) error
}

func (d *probingDestination[T]) Probe(ctx context.Context) error {
	return d.probe(ctx)
}

func probing[T any](id core.DestinationID, probe func(context.Context) error) Destination[T] {
	return &probingDestination[T]{
		id:    id,
		send:  func(context.Context, []T) error { return nil },
		probe: probe,
	}
}

func TestRunProbesEveryDestinationThatOffersACheck(t *testing.T) {
	t.Parallel()

	store := &probeStore[int]{}

	var probeCalls atomic.Int32

	quarantining := func(context.Context) error {
		probeCalls.Add(1)

		return core.Quarantine(errors.New("safe probe failure"))
	}

	dispatcher, err := NewDispatcher(
		store,
		DispatcherConfig{Workers: 1},
		probing[int]("destination:a", quarantining),
		probing[int]("destination:b", quarantining),
		// No Probe method, so nothing is recorded for it and it keeps the
		// active baseline Admit created.
		successfulDestination[int]("destination:c"),
	)
	if err != nil {
		t.Fatalf("NewDispatcher() error = %v", err)
	}

	if _, err := dispatcher.Run(t.Context()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if store.registered.Load() != 1 {
		t.Fatalf("Admit() calls = %d, want 1", store.registered.Load())
	}

	if probeCalls.Load() != 2 {
		t.Fatalf("Probe() calls = %d, want 2", probeCalls.Load())
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if len(store.checks) != 2 {
		t.Fatalf("recorded checks = %d, want 2", len(store.checks))
	}

	if store.checks[0].destination != "destination:a" ||
		store.checks[1].destination != "destination:b" {
		t.Errorf("check order = %#v, want plan order", store.checks)
	}

	for _, check := range store.checks {
		if !check.quarantined || check.failure != destinationProbeFailure {
			t.Errorf("probe check = %#v, want sanitized quarantine", check)
		}
	}
}

func TestRunProbesOnlyOnce(t *testing.T) {
	t.Parallel()

	store := &probeStore[int]{}

	var probeCalls atomic.Int32

	dispatcher, err := NewDispatcher(
		store,
		DispatcherConfig{Workers: 1},
		probing[int]("destination:one", func(context.Context) error {
			probeCalls.Add(1)

			return nil
		}),
	)
	if err != nil {
		t.Fatalf("NewDispatcher() error = %v", err)
	}

	for range 3 {
		if _, err := dispatcher.Run(t.Context()); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	}

	if probeCalls.Load() != 1 {
		t.Errorf("Probe() calls = %d, want 1", probeCalls.Load())
	}

	if store.registered.Load() != 1 {
		t.Errorf("Admit() calls = %d, want 1", store.registered.Load())
	}
}

func TestRunSkipsProbingWhenConfigured(t *testing.T) {
	t.Parallel()

	store := &probeStore[int]{}

	var probeCalls atomic.Int32

	dispatcher, err := NewDispatcher(
		store,
		DispatcherConfig{Workers: 1, SkipProbing: true},
		probing[int]("destination:one", func(context.Context) error {
			probeCalls.Add(1)

			return nil
		}),
	)
	if err != nil {
		t.Fatalf("NewDispatcher() error = %v", err)
	}

	if _, err := dispatcher.Run(t.Context()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if probeCalls.Load() != 0 {
		t.Errorf("Probe() calls = %d, want 0", probeCalls.Load())
	}

	if store.registered.Load() != 1 {
		t.Errorf("Admit() calls = %d, want 1", store.registered.Load())
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if len(store.checks) != 0 {
		t.Errorf("recorded checks = %#v, want none", store.checks)
	}
}

func TestRunLeavesInconclusiveProbesUnrecorded(t *testing.T) {
	t.Parallel()

	store := &probeStore[int]{}

	dispatcher, err := NewDispatcher(
		store,
		DispatcherConfig{Workers: 1},
		probing[int]("destination:one", func(context.Context) error {
			return core.Retryable(errors.New("connection reset"))
		}),
		probing[int]("destination:two", func(context.Context) error {
			return errors.New("unclassified")
		}),
	)
	if err != nil {
		t.Fatalf("NewDispatcher() error = %v", err)
	}

	if _, err := dispatcher.Run(t.Context()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if len(store.checks) != 0 {
		t.Errorf("recorded checks = %#v, want none", store.checks)
	}
}

func TestRunWrapsProbeStoreFailures(t *testing.T) {
	t.Parallel()

	t.Run("register", func(t *testing.T) {
		t.Parallel()

		storeErr := errors.New("register failed")
		store := &probeStore[int]{registerErr: storeErr}

		dispatcher, err := NewDispatcher(
			store,
			DispatcherConfig{Workers: 1},
			successfulDestination[int]("destination:one"),
		)
		if err != nil {
			t.Fatalf("NewDispatcher() error = %v", err)
		}

		_, err = dispatcher.Run(t.Context())

		var probeErr *core.Error
		if !errors.As(err, &probeErr) ||
			probeErr.Op != core.OpEnqueue ||
			!errors.Is(err, storeErr) {
			t.Fatalf("Run() error = %v, want a plan registration error", err)
		}
	})

	t.Run("record", func(t *testing.T) {
		t.Parallel()

		storeErr := errors.New("record failed")
		store := &probeStore[int]{recordErr: storeErr}

		dispatcher, err := NewDispatcher(
			store,
			DispatcherConfig{Workers: 1},
			probing[int]("destination:one", func(context.Context) error { return nil }),
		)
		if err != nil {
			t.Fatalf("NewDispatcher() error = %v", err)
		}

		_, err = dispatcher.Run(t.Context())

		var probeErr *core.Error
		if !errors.As(err, &probeErr) ||
			probeErr.Op != core.OpProbe ||
			probeErr.Subject != "destination:one" ||
			!errors.Is(err, storeErr) {
			t.Fatalf("Run() error = %v, want a probe record error", err)
		}
	})
}
