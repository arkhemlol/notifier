package sqlite

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/arkhemlol/notifier/internal/core"
)

func TestResolve_RejectsUnknownWork(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})

	err := store.Resolve(t.Context(), core.Resolution{
		Work: "work-missing", Outcome: core.OutcomeDelivered,
	})
	if !errors.Is(err, core.ErrStoreWorkDoesntExist) {
		t.Fatalf("Resolve() error = %v, want ErrStoreWorkDoesntExist", err)
	}
}

func TestResolve_RejectsStaleLeaseToken(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "a"}})

	work := claimOne(t, store, plan)

	err := store.Resolve(t.Context(), core.Resolution{
		Work: work.ID, Lease: work.Lease + "-stale", Outcome: core.OutcomeDelivered,
	})
	if !errors.Is(err, core.ErrStoreStaleLeaseToken) {
		t.Fatalf("Resolve() error = %v, want ErrStoreStaleLeaseToken", err)
	}
}

func TestResolve_RepeatingTheSameOutcomeIsANoop(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "a"}})

	work := claimOne(t, store, plan)
	resolution := core.Resolution{Work: work.ID, Lease: work.Lease, Outcome: core.OutcomeDelivered}

	if err := store.Resolve(t.Context(), resolution); err != nil {
		t.Fatalf("Resolve() first call error = %v", err)
	}

	if err := store.Resolve(t.Context(), resolution); err != nil {
		t.Fatalf("Resolve() repeated identical call error = %v, want nil", err)
	}
}

func TestResolve_ContradictingAnAppliedOutcomeFails(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "a"}})

	work := claimOne(t, store, plan)

	if err := store.Resolve(t.Context(), core.Resolution{
		Work: work.ID, Lease: work.Lease, Outcome: core.OutcomeDelivered,
	}); err != nil {
		t.Fatalf("Resolve() first call error = %v", err)
	}

	err := store.Resolve(t.Context(), core.Resolution{
		Work: work.ID, Lease: work.Lease, Outcome: core.OutcomeRetryableFailure,
	})
	if !errors.Is(err, core.ErrStoreInvalidTransition) {
		t.Fatalf("Resolve() contradicting outcome error = %v, want ErrStoreInvalidTransition", err)
	}
}

func TestResolve_DeliveredRetiresTheItem(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "a"}})

	work := claimOne(t, store, plan)

	if err := store.Resolve(t.Context(), core.Resolution{
		Work: work.ID, Lease: work.Lease, Outcome: core.OutcomeDelivered,
	}); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	pending, err := store.PendingPlans(t.Context())
	if err != nil {
		t.Fatalf("PendingPlans() error = %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("PendingPlans() = %v, want none after full delivery", pending)
	}
}

func TestResolve_RetryableFailureReschedulesAfterRetryAfter(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "a"}})

	work := claimOne(t, store, plan)

	// Generous relative to the immediate re-claim check below: that check races
	// the wall clock against however long Resolve()+Claim() take to run, and a
	// tight margin flakes once enough parallel sqlite tests contend for disk I/O.
	const retryAfter = 300 * time.Millisecond

	if err := store.Resolve(t.Context(), core.Resolution{
		Work: work.ID, Lease: work.Lease,
		Outcome: core.OutcomeRetryableFailure, RetryAfter: retryAfter,
	}); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	immediate, err := store.Claim(t.Context(), claimRequestFor(plan, 1, 10))
	if err != nil {
		t.Fatalf("Claim() before RetryAfter elapsed error = %v", err)
	}

	if len(immediate) != 0 {
		t.Fatalf("Claim() before RetryAfter elapsed = %v, want no work", immediate)
	}

	time.Sleep(retryAfter + 100*time.Millisecond)

	retried, err := store.Claim(t.Context(), claimRequestFor(plan, 1, 10))
	if err != nil {
		t.Fatalf("Claim() after RetryAfter elapsed error = %v", err)
	}

	if len(retried) != 1 || retried[0].ID != work.ID {
		t.Fatalf("Claim() after RetryAfter elapsed = %v, want the retried batch reclaimed", retried)
	}
}

func TestResolve_PermanentDeliveryFailureLeavesDestinationActive(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "a"}})

	work := claimOne(t, store, plan)

	if err := store.Resolve(t.Context(), core.Resolution{
		Work: work.ID, Lease: work.Lease,
		Outcome: core.OutcomeFailedPermanent, Scope: core.FailureScopeDelivery,
		Failure: "rejected",
	}); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	// A delivery-scoped failure must not take the destination down: later items
	// still have to be claimable against it.
	admit(t, store, plan, []core.Item[string]{{ID: 2, Payload: "b"}})

	next, err := store.Claim(t.Context(), claimRequestFor(plan, 1, 10))
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}

	if len(next) != 1 || len(next[0].Items) != 1 || next[0].Items[0].ID != 2 {
		t.Fatalf("Claim() = %+v, want a fresh batch for item 2 on the still-active destination", next)
	}
}

func TestResolve_PermanentDestinationFailureQuarantinesAndFencesSiblingBatches(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "a"}, {ID: 2, Payload: "b"}})

	// One batch per item, both leased and outstanding when the destination
	// gets quarantined.
	work, err := store.Claim(t.Context(), claimRequestFor(plan, 2, 1))
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}

	if len(work) != 2 {
		t.Fatalf("Claim() returned %d batches, want 2", len(work))
	}

	if err := store.Resolve(t.Context(), core.Resolution{
		Work: work[0].ID, Lease: work[0].Lease,
		Outcome: core.OutcomeFailedPermanent, Scope: core.FailureScopeDestination,
		Failure: "destination unreachable",
	}); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	// The sibling batch was still leased and healthy when the quarantine hit;
	// it must be fenced so its lease can never be resolved afterward.
	err = store.Resolve(t.Context(), core.Resolution{
		Work: work[1].ID, Lease: work[1].Lease, Outcome: core.OutcomeDelivered,
	})
	if !errors.Is(err, core.ErrStoreStaleLeaseToken) {
		t.Fatalf("Resolve() sibling after quarantine error = %v, want ErrStoreStaleLeaseToken", err)
	}

	pending, err := store.PendingPlans(t.Context())
	if err != nil {
		t.Fatalf("PendingPlans() error = %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("PendingPlans() = %v, want none: quarantine terminalizes the whole destination", pending)
	}
}

func TestResolve_DeliveredCancelsFirstSuccessSiblings(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := core.NewPlan(core.PolicyFirstSuccess, []core.DestinationID{"primary", "fallback"})
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "a"}})

	work := claimOne(t, store, plan)
	if work.Destination != "primary" {
		t.Fatalf("claimed destination = %q, want primary", work.Destination)
	}

	if err := store.Resolve(t.Context(), core.Resolution{
		Work: work.ID, Lease: work.Lease, Outcome: core.OutcomeDelivered,
	}); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	pending, err := store.PendingPlans(t.Context())
	if err != nil {
		t.Fatalf("PendingPlans() error = %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("PendingPlans() = %v, want none: the never-claimed fallback must be canceled, not left pending", pending)
	}
}

func TestQuarantineDestination_RejectsUnregisteredDestination(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")
	admit(t, store, plan, nil)

	err := store.QuarantineDestination(t.Context(), plan.ID(), "destination:missing", "boom")
	if !errors.Is(err, core.ErrStoreInvalidTransition) {
		t.Fatalf("QuarantineDestination() error = %v, want ErrStoreInvalidTransition", err)
	}
}

func TestActivateDestination_RejectsUnregisteredDestination(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")
	admit(t, store, plan, nil)

	err := store.ActivateDestination(t.Context(), plan.ID(), "destination:missing")
	if !errors.Is(err, core.ErrStoreInvalidTransition) {
		t.Fatalf("ActivateDestination() error = %v, want ErrStoreInvalidTransition", err)
	}
}

func TestActivateDestination_LetsNewWorkFlowAfterAQuarantine(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "a"}})

	if err := store.QuarantineDestination(
		t.Context(), plan.ID(), "destination:one", "boom",
	); err != nil {
		t.Fatalf("QuarantineDestination() error = %v", err)
	}

	if err := store.ActivateDestination(t.Context(), plan.ID(), "destination:one"); err != nil {
		t.Fatalf("ActivateDestination() error = %v", err)
	}

	// Item 1 was terminalized by the quarantine and must stay terminal; only
	// work enqueued after reactivation is claimable.
	admit(t, store, plan, []core.Item[string]{{ID: 2, Payload: "b"}})

	work, err := store.Claim(t.Context(), claimRequestFor(plan, 1, 10))
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}

	if len(work) != 1 || len(work[0].Items) != 1 || work[0].Items[0].ID != 2 {
		t.Fatalf("Claim() after reactivation = %+v, want a fresh batch for item 2 only", work)
	}
}

func TestPendingPlans_ReturnsSortedPlansWithUnfinishedWork(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})

	planA := planFor(t, "destination:a")
	planB := planFor(t, "destination:b")

	admit(t, store, planA, []core.Item[string]{{ID: 1, Payload: "a"}})
	admit(t, store, planB, []core.Item[string]{{ID: 1, Payload: "b"}})

	work := claimOne(t, store, planA)

	if err := store.Resolve(t.Context(), core.Resolution{
		Work: work.ID, Lease: work.Lease, Outcome: core.OutcomeDelivered,
	}); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	pending, err := store.PendingPlans(t.Context())
	if err != nil {
		t.Fatalf("PendingPlans() error = %v", err)
	}

	if !slices.Equal(pending, []core.PlanID{planB.ID()}) {
		t.Fatalf("PendingPlans() = %v, want only %q (planA is fully delivered)", pending, planB.ID())
	}
}
