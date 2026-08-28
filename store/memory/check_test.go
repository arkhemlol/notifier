package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arkhemlol/notifier/internal/core"
)

func TestStore_QuarantineAndReactivate(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustAll(t, "operations:v1", "destination:a")
	register(t, store, plan)
	enqueue(t, store, plan, core.Item[int]{ID: 1, Payload: 10})

	leased := claim(t, store, plan, 1, 10, time.Minute)[0]

	err := store.QuarantineDestination(
		t.Context(),
		plan.ID(),
		"destination:a",
		"destination unavailable",
	)
	if err != nil {
		t.Fatalf("QuarantineDestination: %v", err)
	}

	if quarantined := claim(t, store, plan, 1, 10, time.Minute); len(quarantined) != 0 {
		t.Fatalf("Claim after quarantine returned %d batches, want 0", len(quarantined))
	}

	stale := core.Resolution{
		Work:    leased.ID,
		Lease:   leased.Lease,
		Outcome: core.OutcomeDelivered,
	}
	if err := store.Resolve(t.Context(), stale); !errors.Is(err, core.ErrStoreStaleLeaseToken) {
		t.Fatalf("Resolve(fenced lease) error = %v, want ErrStoreStaleLeaseToken", err)
	}

	if stillOff := claim(t, store, plan, 1, 10, time.Minute); len(stillOff) != 0 {
		t.Fatalf("Claim after inconclusive check returned %d batches, want 0", len(stillOff))
	}

	err = store.ActivateDestination(t.Context(), plan.ID(), "destination:a")
	if err != nil {
		t.Fatalf("ActivateDestination: %v", err)
	}

	if old := claim(t, store, plan, 1, 10, time.Minute); len(old) != 0 {
		t.Fatalf("reactivation revived %d old batches, want 0", len(old))
	}

	enqueue(t, store, plan, core.Item[int]{ID: 2, Payload: 20})

	fresh := claim(t, store, plan, 1, 10, time.Minute)
	if len(fresh) != 1 {
		t.Fatalf("Claim after reactivation returned %d batches, want 1", len(fresh))
	}

	assertItemIDs(t, fresh[0].Items, 2)
}

func TestStore_QuarantineTerminalizesNewItems(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustAll(t, "operations:v1", "destination:a")
	register(t, store, plan)

	err := store.QuarantineDestination(
		t.Context(),
		plan.ID(),
		"destination:a",
		"destination unavailable",
	)
	if err != nil {
		t.Fatalf("RecordDestinationCheck: %v", err)
	}

	enqueue(t, store, plan, core.Item[int]{ID: 1, Payload: 10})

	if work := claim(t, store, plan, 1, 10, time.Minute); len(work) != 0 {
		t.Fatalf("Claim returned %d quarantined batches, want 0", len(work))
	}
}

func TestStore_ActivateRejectsUnknownDestination(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustAll(t, "operations:v1", "destination:a")
	register(t, store, plan)

	err := store.ActivateDestination(t.Context(), plan.ID(), "destination:b")
	if !errors.Is(err, core.ErrStoreInvalidTransition) {
		t.Fatalf("ActivateDestination error = %v, want invalid transition", err)
	}
}

func TestStore_CanceledDestinationCheckDoesNotMutateState(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustAll(t, "operations:v1", "destination:a")
	register(t, store, plan)
	enqueue(t, store, plan, core.Item[int]{ID: 1, Payload: 10})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.QuarantineDestination(
		ctx,
		plan.ID(),
		"destination:a",
		"destination unavailable",
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("QuarantineDestination error = %v, want context canceled", err)
	}

	if work := claim(t, store, plan, 1, 10, time.Minute); len(work) != 1 {
		t.Fatalf("Claim returned %d batches after canceled check, want 1", len(work))
	}
}

func TestStore_SweepReleasesFinishedWork(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustAll(t, "operations:v1", "destination:a")
	register(t, store, plan)
	enqueue(t, store, plan, core.Item[int]{ID: 1, Payload: 10})

	work := claim(t, store, plan, 1, 10, time.Millisecond)[0]

	resolve(t, store, core.Resolution{
		Work:    work.ID,
		Lease:   work.Lease,
		Outcome: core.OutcomeDelivered,
	})

	resolve(t, store, core.Resolution{
		Work:    work.ID,
		Lease:   work.Lease,
		Outcome: core.OutcomeDelivered,
	})

	time.Sleep(2 * time.Millisecond)

	if remaining := claim(t, store, plan, 1, 10, time.Minute); len(remaining) != 0 {
		t.Fatalf("Claim returned %d batches, want 0", len(remaining))
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	stored := store.plans[plan.ID()]

	if len(store.batches) != 0 {
		t.Errorf("store batches = %d, want 0", len(store.batches))
	}

	if len(stored.batches) != 0 {
		t.Errorf("plan batches = %d, want 0", len(stored.batches))
	}

	if len(stored.itemOrder) != 0 {
		t.Errorf("scannable items = %d, want 0", len(stored.itemOrder))
	}

	retired, ok := stored.items[1]
	if !ok {
		t.Fatal("item id was forgotten, so deduplication no longer holds")
	}

	if retired.payload != 0 || retired.deliveries != nil {
		t.Errorf("retired item = %+v, want payload and obligations released", retired)
	}
}

func TestStore_SweepKeepsDeduplication(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustAll(t, "operations:v1", "destination:a")
	register(t, store, plan)
	enqueue(t, store, plan, core.Item[int]{ID: 1, Payload: 10})

	work := claim(t, store, plan, 1, 10, time.Millisecond)[0]
	resolve(t, store, core.Resolution{
		Work:    work.ID,
		Lease:   work.Lease,
		Outcome: core.OutcomeDelivered,
	})

	time.Sleep(2 * time.Millisecond)
	claim(t, store, plan, 1, 10, time.Minute)

	enqueue(t, store, plan, core.Item[int]{ID: 1, Payload: 10})

	if revived := claim(t, store, plan, 1, 10, time.Minute); len(revived) != 0 {
		t.Fatalf("re-admitting a delivered item produced %d batches, want 0", len(revived))
	}
}
