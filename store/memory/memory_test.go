package memory

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/arkhemlol/notifier/internal/core"
)

var _ core.Store[int] = (*Store[int])(nil)

func mustAll(
	t *testing.T,
	_ core.PlanID,
	destinations ...core.DestinationID,
) core.Plan {
	t.Helper()

	return mustPlan(t, core.PolicyAll, destinations)
}

func mustFirstSuccess(
	t *testing.T,
	_ core.PlanID,
	destinations ...core.DestinationID,
) core.Plan {
	t.Helper()

	return mustPlan(t, core.PolicyFirstSuccess, destinations)
}

func mustPlan(
	t *testing.T,
	policy core.Policy,
	destinations []core.DestinationID,
) core.Plan {
	t.Helper()

	return core.NewPlan(policy, destinations)
}

func register[T any](t *testing.T, store *Store[T], plan core.Plan) {
	t.Helper()

	if err := store.Admit(t.Context(), plan, nil); err != nil {
		t.Fatalf("Admit(register): %v", err)
	}
}

func enqueue[T any](
	t *testing.T,
	store *Store[T],
	plan core.Plan,
	items ...core.Item[T],
) {
	t.Helper()

	if err := store.Admit(t.Context(), plan, items); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
}

func claim[T any](
	t *testing.T,
	store *Store[T],
	plan core.Plan,
	maxWork int,
	maxItems int,
	lease time.Duration,
) []core.Work[T] {
	t.Helper()

	work, err := store.Claim(t.Context(), core.ClaimRequest{
		Plan:            plan.ID(),
		MaxWork:         maxWork,
		MaxItemsPerWork: maxItems,
		LeaseDuration:   lease,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	return work
}

func resolve[T any](
	t *testing.T,
	store *Store[T],
	resolution core.Resolution,
) {
	t.Helper()

	if err := store.Resolve(t.Context(), resolution); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
}

func TestNew(t *testing.T) {
	t.Parallel()

	if New[int]() == nil {
		t.Fatal("New returned nil")
	}
}

func TestStore_EnqueueRegistersPlanOnFirstUse(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustAll(t, "operations:v1", "destination:a")
	register(t, store, plan)
	register(t, store, plan)

	changed := mustAll(t, "operations:v1", "destination:b")
	if changed.ID() == plan.ID() {
		t.Fatal("changed destinations produced the same plan id")
	}

	err := store.Admit(t.Context(), changed, nil)
	if err != nil {
		t.Fatalf("Admit(changed) error = %v", err)
	}

	changedPolicy := mustFirstSuccess(t, "operations:v1", "destination:a")

	err = store.Admit(t.Context(), changedPolicy, nil)
	if err != nil {
		t.Fatalf("Admit(changed policy) error = %v", err)
	}
}

func TestStore_Enqueue(t *testing.T) {
	t.Parallel()

	store := New[[]int]()
	plan := mustAll(t, "operations:v1", "destination:a")

	err := store.Admit(
		t.Context(),
		plan,
		[]core.Item[[]int]{{ID: 1, Payload: []int{1, 2}}},
	)
	if err != nil {
		t.Fatalf("Admit(first use) error = %v", err)
	}

	enqueue(
		t,
		store,
		plan,
		core.Item[[]int]{ID: 1, Payload: []int{1, 2}},
	)
	enqueue(
		t,
		store,
		plan,
		core.Item[[]int]{ID: 1, Payload: []int{1, 2}},
	)

	err = store.Admit(
		t.Context(),
		plan,
		[]core.Item[[]int]{{ID: 1, Payload: []int{2, 1}}},
	)
	if err != nil {
		t.Fatalf("Admit(changed payload) error = %v, want nil", err)
	}

	work := claim(t, store, plan, 1, 10, time.Minute)
	if len(work) != 1 || len(work[0].Items) != 1 {
		t.Fatalf("Claim returned %d batches, want 1 holding 1 item", len(work))
	}

	if got := work[0].Items[0].Payload; !slices.Equal(got, []int{1, 2}) {
		t.Errorf("stored payload = %v, want the first admitted %v", got, []int{1, 2})
	}

	changed := mustAll(t, "operations:v1", "destination:b")
	if err := store.Admit(t.Context(), changed, nil); err != nil {
		t.Fatalf("Admit(new plan) error = %v", err)
	}
}

func TestStore_EnqueueIsAtomicForDuplicateIDs(t *testing.T) {
	t.Parallel()

	store := New[[]int]()
	plan := mustAll(t, "operations:v1", "destination:a")
	register(t, store, plan)

	err := store.Admit(
		t.Context(),
		plan,
		[]core.Item[[]int]{
			{ID: 1, Payload: []int{1}},
			{ID: 1, Payload: []int{2}},
		},
	)
	if !errors.Is(err, core.ErrStorePayloadConflict) {
		t.Fatalf("Enqueue error = %v, want ErrStorePayloadConflict", err)
	}

	if work := claim(t, store, plan, 1, 10, time.Minute); len(work) != 0 {
		t.Fatalf("Claim returned %d batches after rejected enqueue, want 0", len(work))
	}
}

func TestStore_CanceledEnqueueDoesNotMutateState(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustAll(t, "operations:v1", "destination:a")
	register(t, store, plan)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.Admit(
		ctx,
		plan,
		[]core.Item[int]{{ID: 1, Payload: 10}},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Enqueue error = %v, want context canceled", err)
	}

	if work := claim(t, store, plan, 1, 10, time.Minute); len(work) != 0 {
		t.Fatalf("Claim returned %d batches after canceled enqueue, want 0", len(work))
	}
}

func TestStore_ConcurrentIdempotentEnqueue(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustAll(t, "operations:v1", "destination:a")
	register(t, store, plan)

	const callers = 32

	start := make(chan struct{})
	errorsByCaller := make(chan error, callers)

	var group sync.WaitGroup
	for range callers {
		group.Go(func() {
			<-start

			errorsByCaller <- store.Admit(
				context.Background(),
				plan,
				[]core.Item[int]{{ID: 1, Payload: 10}},
			)
		})
	}

	close(start)
	group.Wait()
	close(errorsByCaller)

	for err := range errorsByCaller {
		if err != nil {
			t.Fatalf("concurrent Enqueue: %v", err)
		}
	}

	work := claim(t, store, plan, 2, 10, time.Minute)
	if len(work) != 1 || len(work[0].Items) != 1 {
		t.Fatalf("Claim shape = %d batches, want one batch with one item", len(work))
	}
}

func TestStore_PendingPlansReturnsSortedPlansWithUnfinishedWork(t *testing.T) {
	t.Parallel()

	store := New[int]()

	planA := mustAll(t, "operations:v1", "destination:a")
	planB := mustAll(t, "operations:v1", "destination:b")

	enqueue(t, store, planA, core.Item[int]{ID: 1, Payload: 1})
	enqueue(t, store, planB, core.Item[int]{ID: 1, Payload: 2})

	work := claim(t, store, planA, 1, 10, time.Minute)
	resolve(t, store, core.Resolution{
		Work: work[0].ID, Lease: work[0].Lease, Outcome: core.OutcomeDelivered,
	})

	pending, err := store.PendingPlans(t.Context())
	if err != nil {
		t.Fatalf("PendingPlans() error = %v", err)
	}

	if !slices.Equal(pending, []core.PlanID{planB.ID()}) {
		t.Fatalf("PendingPlans() = %v, want only %q (planA is fully delivered)", pending, planB.ID())
	}
}

func TestStore_PendingPlansExcludesQuarantinedDeliveries(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustAll(t, "operations:v1", "destination:a")
	enqueue(t, store, plan, core.Item[int]{ID: 1, Payload: 1})

	work := claim(t, store, plan, 1, 10, time.Minute)
	resolve(t, store, core.Resolution{
		Work: work[0].ID, Lease: work[0].Lease,
		Outcome: core.OutcomeFailedPermanent, Scope: core.FailureScopeDestination,
		Failure: "boom",
	})

	pending, err := store.PendingPlans(t.Context())
	if err != nil {
		t.Fatalf("PendingPlans() error = %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("PendingPlans() = %v, want none: quarantine terminalizes every delivery", pending)
	}
}

func TestStore_PendingPlansRespectsCanceledContext(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustAll(t, "operations:v1", "destination:a")
	enqueue(t, store, plan, core.Item[int]{ID: 1, Payload: 1})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := store.PendingPlans(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("PendingPlans() error = %v, want context canceled", err)
	}
}
