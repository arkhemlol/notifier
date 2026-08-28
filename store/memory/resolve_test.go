package memory

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/arkhemlol/notifier/internal/core"
)

func TestStore_ResolveRetrySchedulingAndFencing(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := New[int]()
		plan := mustAll(t, "operations:v1", "destination:a")
		register(t, store, plan)
		enqueue(t, store, plan, core.Item[int]{ID: 1, Payload: 10})

		first := claim(t, store, plan, 1, 10, time.Minute)[0]
		retry := core.Resolution{
			Work:       first.ID,
			Lease:      first.Lease,
			Outcome:    core.OutcomeRetryableFailure,
			Scope:      core.FailureScopeDelivery,
			RetryAfter: time.Hour,
			Failure:    "temporary failure",
		}
		resolve(t, store, retry)
		resolve(t, store, retry)

		changed := retry

		changed.RetryAfter = 2 * time.Hour
		if err := store.Resolve(t.Context(), changed); !errors.Is(err, core.ErrStoreInvalidTransition) {
			t.Fatalf("Resolve(changed repeat) error = %v, want invalid transition", err)
		}

		time.Sleep(time.Hour - time.Nanosecond)

		if early := claim(t, store, plan, 1, 10, time.Minute); len(early) != 0 {
			t.Fatalf("Claim before retry_at returned %d batches, want 0", len(early))
		}

		time.Sleep(time.Nanosecond)

		reclaimed := claim(t, store, plan, 1, 10, time.Minute)
		if len(reclaimed) != 1 {
			t.Fatalf("Claim at retry_at returned %d batches, want 1", len(reclaimed))
		}

		if reclaimed[0].ID != first.ID || reclaimed[0].Attempt != 2 {
			t.Errorf(
				"reclaimed work = {ID:%q Attempt:%d}, want {ID:%q Attempt:2}",
				reclaimed[0].ID,
				reclaimed[0].Attempt,
				first.ID,
			)
		}

		if reclaimed[0].Lease == first.Lease {
			t.Error("retry claim did not rotate lease")
		}

		if err := store.Resolve(t.Context(), retry); !errors.Is(err, core.ErrStoreStaleLeaseToken) {
			t.Fatalf("Resolve(stale retry) error = %v, want ErrStoreStaleLeaseToken", err)
		}

		delivered := core.Resolution{
			Work:    reclaimed[0].ID,
			Lease:   reclaimed[0].Lease,
			Outcome: core.OutcomeDelivered,
		}
		resolve(t, store, delivered)
		resolve(t, store, delivered)

		if work := claim(t, store, plan, 1, 10, time.Minute); len(work) != 0 {
			t.Fatalf("Claim after delivery returned %d batches, want 0", len(work))
		}
	})
}

func TestStore_ResolveRejectsUnknownOutcome(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustAll(t, "operations:v1", "destination:a")
	register(t, store, plan)
	enqueue(t, store, plan, core.Item[int]{ID: 1, Payload: 10})

	work := claim(t, store, plan, 1, 10, time.Minute)[0]

	err := store.Resolve(t.Context(), core.Resolution{
		Work:    work.ID,
		Lease:   work.Lease,
		Outcome: core.OutcomeUnknown,
	})
	if !errors.Is(err, core.ErrStoreInvalidTransition) {
		t.Fatalf("Resolve(unknown outcome) error = %v, want invalid transition", err)
	}

	resolve(t, store, core.Resolution{
		Work:    work.ID,
		Lease:   work.Lease,
		Outcome: core.OutcomeDelivered,
	})
}

func TestStore_ResolveExpiredCurrentLease(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := New[int]()
		plan := mustAll(t, "operations:v1", "destination:a")
		register(t, store, plan)
		enqueue(t, store, plan, core.Item[int]{ID: 1, Payload: 10})

		work := claim(t, store, plan, 1, 10, time.Minute)[0]

		time.Sleep(time.Hour)
		resolve(t, store, core.Resolution{
			Work:    work.ID,
			Lease:   work.Lease,
			Outcome: core.OutcomeDelivered,
		})

		if remaining := claim(t, store, plan, 1, 10, time.Minute); len(remaining) != 0 {
			t.Fatalf("Claim after expired-token resolution returned %d batches, want 0", len(remaining))
		}
	})
}

func TestStore_ResolvePolicyAllContinuesAfterPermanentFailure(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustAll(
		t,
		"operations:v1",
		"destination:a",
		"destination:b",
	)
	register(t, store, plan)
	enqueue(t, store, plan, core.Item[int]{ID: 1, Payload: 10})

	first := claim(t, store, plan, 1, 10, time.Minute)[0]
	if first.Destination != "destination:a" {
		t.Fatalf("first destination = %q, want destination:a", first.Destination)
	}

	resolve(t, store, core.Resolution{
		Work:    first.ID,
		Lease:   first.Lease,
		Outcome: core.OutcomeFailedPermanent,
		Scope:   core.FailureScopeDelivery,
		Failure: "permanent failure",
	})

	second := claim(t, store, plan, 1, 10, time.Minute)
	if len(second) != 1 || second[0].Destination != "destination:b" {
		t.Fatalf("fallback work = %#v, want one destination:b batch", second)
	}

	resolve(t, store, core.Resolution{
		Work:    second[0].ID,
		Lease:   second[0].Lease,
		Outcome: core.OutcomeDelivered,
	})

	if remaining := claim(t, store, plan, 2, 10, time.Minute); len(remaining) != 0 {
		t.Fatalf("Claim after terminal outcomes returned %d batches, want 0", len(remaining))
	}
}

func TestStore_ResolvePolicyFirstSuccessStrictFallback(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustFirstSuccess(
		t,
		"operations:v1",
		"destination:a",
		"destination:b",
		"destination:c",
	)
	register(t, store, plan)
	enqueue(t, store, plan, core.Item[int]{ID: 1, Payload: 10})

	firstWave := claim(t, store, plan, 3, 10, time.Minute)
	if len(firstWave) != 1 || firstWave[0].Destination != "destination:a" {
		t.Fatalf("first wave = %#v, want only destination:a", firstWave)
	}

	resolve(t, store, core.Resolution{
		Work:       firstWave[0].ID,
		Lease:      firstWave[0].Lease,
		Outcome:    core.OutcomeRetryableFailure,
		Scope:      core.FailureScopeDelivery,
		RetryAfter: 0,
		Failure:    "temporary failure",
	})

	retryWave := claim(t, store, plan, 3, 10, time.Minute)
	if len(retryWave) != 1 || retryWave[0].Destination != "destination:a" {
		t.Fatalf("retry wave = %#v, want only destination:a", retryWave)
	}

	resolve(t, store, core.Resolution{
		Work:    retryWave[0].ID,
		Lease:   retryWave[0].Lease,
		Outcome: core.OutcomeFailedPermanent,
		Scope:   core.FailureScopeDelivery,
		Failure: "permanent failure",
	})

	fallback := claim(t, store, plan, 3, 10, time.Minute)
	if len(fallback) != 1 || fallback[0].Destination != "destination:b" {
		t.Fatalf("fallback wave = %#v, want only destination:b", fallback)
	}

	resolve(t, store, core.Resolution{
		Work:    fallback[0].ID,
		Lease:   fallback[0].Lease,
		Outcome: core.OutcomeDelivered,
	})

	if remaining := claim(t, store, plan, 3, 10, time.Minute); len(remaining) != 0 {
		t.Fatalf("Claim after first success returned %d batches, want 0", len(remaining))
	}
}

func TestStore_ResolveDestinationQuarantineIsAtomic(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustFirstSuccess(
		t,
		"operations:v1",
		"destination:a",
		"destination:b",
	)
	register(t, store, plan)
	enqueue(
		t,
		store,
		plan,
		core.Item[int]{ID: 1, Payload: 10},
		core.Item[int]{ID: 2, Payload: 20},
	)

	leased := claim(t, store, plan, 2, 1, time.Minute)
	if len(leased) != 2 {
		t.Fatalf("Claim returned %d batches, want 2", len(leased))
	}

	quarantine := core.Resolution{
		Work:    leased[0].ID,
		Lease:   leased[0].Lease,
		Outcome: core.OutcomeFailedPermanent,
		Scope:   core.FailureScopeDestination,
		Failure: "destination unavailable",
	}
	resolve(t, store, quarantine)
	resolve(t, store, quarantine)

	stale := core.Resolution{
		Work:    leased[1].ID,
		Lease:   leased[1].Lease,
		Outcome: core.OutcomeDelivered,
	}
	if err := store.Resolve(t.Context(), stale); !errors.Is(err, core.ErrStoreStaleLeaseToken) {
		t.Fatalf("Resolve(fenced sibling) error = %v, want ErrStoreStaleLeaseToken", err)
	}

	fallback := claim(t, store, plan, 2, 10, time.Minute)
	if len(fallback) != 1 || fallback[0].Destination != "destination:b" {
		t.Fatalf("fallback work = %#v, want one destination:b batch", fallback)
	}

	assertItemIDs(t, fallback[0].Items, 1, 2)
}
