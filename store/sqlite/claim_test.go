package sqlite

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/arkhemlol/notifier/internal/core"
)

func TestClaim_RejectsUnregisteredPlan(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")

	_, err := store.Claim(t.Context(), claimRequestFor(plan, 1, 10))
	if !errors.Is(err, core.ErrStoreInvalidTransition) {
		t.Fatalf("Claim() error = %v, want ErrStoreInvalidTransition", err)
	}
}

func TestClaim_ReturnsNothingWhenQueueIsEmpty(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")
	admit(t, store, plan, nil)

	work, err := store.Claim(t.Context(), claimRequestFor(plan, 1, 10))
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}

	if len(work) != 0 {
		t.Fatalf("Claim() = %v, want no work", work)
	}
}

func TestClaim_LeasesPendingItemsInEnqueueOrder(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")
	admit(t, store, plan, []core.Item[string]{
		{ID: 1, Payload: "a"},
		{ID: 2, Payload: "b"},
		{ID: 3, Payload: "c"},
	})

	work, err := store.Claim(t.Context(), claimRequestFor(plan, 1, 2))
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}

	if len(work) != 1 {
		t.Fatalf("Claim() returned %d batches, want 1", len(work))
	}

	batch := work[0]
	if batch.Destination != "destination:one" || batch.Attempt != 1 || batch.Lease == "" {
		t.Fatalf("Claim() batch = %+v, want bound destination, attempt 1, and a lease", batch)
	}

	if !batch.LeaseUntil.After(time.Now()) {
		t.Fatalf("Claim() lease until = %v, want a future deadline", batch.LeaseUntil)
	}

	// MaxItemsPerWork=2 bounds the batch to the two oldest items, in enqueue order.
	if len(batch.Items) != 2 || batch.Items[0].ID != 1 || batch.Items[1].ID != 2 {
		t.Fatalf("Claim() items = %+v, want items 1 and 2 in order", batch.Items)
	}

	if batch.Items[0].Payload != "a" || batch.Items[1].Payload != "b" {
		t.Fatalf("Claim() payloads = %q, %q, want round-tripped codec values",
			batch.Items[0].Payload, batch.Items[1].Payload)
	}
}

func TestClaim_BoundsBatchesByMaxWorkAcrossDestinations(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one", "destination:two", "destination:three")
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "a"}})

	work, err := store.Claim(t.Context(), claimRequestFor(plan, 2, 10))
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}

	if len(work) != 2 {
		t.Fatalf("Claim() returned %d batches, want the MaxWork bound of 2", len(work))
	}
}

func TestClaim_SkipsQuarantinedDestination(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "a"}})

	if err := store.QuarantineDestination(
		t.Context(), plan.ID(), "destination:one", "boom",
	); err != nil {
		t.Fatalf("QuarantineDestination() error = %v", err)
	}

	work, err := store.Claim(t.Context(), claimRequestFor(plan, 1, 10))
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}

	if len(work) != 0 {
		t.Fatalf("Claim() = %v, want no work from a quarantined destination", work)
	}
}

func TestClaim_ReclaimsExpiredLeaseWithBumpedAttemptAndFreshToken(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "a"}})

	request := claimRequestFor(plan, 1, 10)
	request.LeaseDuration = 15 * time.Millisecond

	first, err := store.Claim(t.Context(), request)
	if err != nil {
		t.Fatalf("Claim() first call error = %v", err)
	}

	if len(first) != 1 {
		t.Fatalf("Claim() first call returned %d batches, want 1", len(first))
	}

	time.Sleep(30 * time.Millisecond)

	request.LeaseDuration = time.Minute

	second, err := store.Claim(t.Context(), request)
	if err != nil {
		t.Fatalf("Claim() second call error = %v", err)
	}

	if len(second) != 1 || second[0].ID != first[0].ID {
		t.Fatalf("Claim() second call = %+v, want the same work id reclaimed", second)
	}

	if second[0].Attempt != first[0].Attempt+1 {
		t.Fatalf("Claim() attempt = %d, want %d", second[0].Attempt, first[0].Attempt+1)
	}

	if second[0].Lease == first[0].Lease {
		t.Fatal("Claim() reused the expired lease token")
	}
}

func TestClaim_FirstSuccessOnlyOffersTheEarliestPendingDestination(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := core.NewPlan(core.PolicyFirstSuccess, []core.DestinationID{"primary", "fallback"})
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "a"}})

	work, err := store.Claim(t.Context(), claimRequestFor(plan, 2, 10))
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}

	if len(work) != 1 || work[0].Destination != "primary" {
		t.Fatalf("Claim() = %+v, want only the primary destination claimable", work)
	}

	if err := store.Resolve(t.Context(), core.Resolution{
		Work: work[0].ID, Lease: work[0].Lease,
		Outcome: core.OutcomeFailedPermanent, Scope: core.FailureScopeDelivery,
		Failure: "primary rejected",
	}); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	fallback, err := store.Claim(t.Context(), claimRequestFor(plan, 2, 10))
	if err != nil {
		t.Fatalf("Claim() after primary settled error = %v", err)
	}

	if len(fallback) != 1 || fallback[0].Destination != "fallback" {
		t.Fatalf("Claim() = %+v, want the fallback destination once primary is settled", fallback)
	}
}

// A reclaimed lease is the mechanism that makes at-least-once delivery safe: if
// two dispatchers raced to reclaim the same expired batch, both could deliver it.
// BEGIN IMMEDIATE is what's supposed to prevent that.
func TestClaim_ConcurrentReclaimNeverDoubleAssignsTheSameBatch(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "a"}})

	request := claimRequestFor(plan, 1, 10)
	request.LeaseDuration = 20 * time.Millisecond

	if _, err := store.Claim(t.Context(), request); err != nil {
		t.Fatalf("Claim() setup error = %v", err)
	}

	time.Sleep(40 * time.Millisecond) // let the lease expire so every racer sees it as reclaimable

	const racers = 8

	var (
		group   sync.WaitGroup
		mu      sync.Mutex
		claimed []core.LeaseToken
	)

	for range racers {
		group.Go(func() {
			work, err := store.Claim(t.Context(), request)
			if err != nil {
				t.Errorf("Claim() error = %v", err)
				return
			}

			mu.Lock()
			defer mu.Unlock()

			for _, batch := range work {
				claimed = append(claimed, batch.Lease)
			}
		})
	}

	group.Wait()

	if len(claimed) != 1 {
		t.Fatalf("Claim() handed the batch to %d racers concurrently, want exactly 1", len(claimed))
	}
}
