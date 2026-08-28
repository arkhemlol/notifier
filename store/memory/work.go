package memory

import (
	"context"
	"crypto/rand"
	"fmt"
	"strconv"
	"time"

	"github.com/arkhemlol/notifier/internal/core"
)

// Claim implements core.Store.
func (s *Store[T]) Claim(ctx context.Context, request core.ClaimRequest) ([]core.Work[T], error) {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	plan, ok := s.plans[request.Plan]
	if !ok {
		return nil, notRegistered(request.Plan)
	}

	s.sweep(plan, now)

	work := make([]core.Work[T], 0, request.MaxWork)

	for _, batch := range plan.batches {
		if len(work) == request.MaxWork {
			return work, nil
		}

		if batchEligible(batch, now) {
			work = append(work, s.lease(batch, now, request.LeaseDuration))
		}
	}

	for len(work) < request.MaxWork {
		deliveries := nextBatchDeliveries(plan, request.MaxItemsPerWork, now)
		if len(deliveries) == 0 {
			break
		}

		batch := s.createBatch(plan, deliveries)
		work = append(work, s.lease(batch, now, request.LeaseDuration))
	}

	return work, nil
}

// Resolve implements core.Store.
func (s *Store[T]) Resolve(ctx context.Context, resolution core.Resolution) error {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	batch, ok := s.batches[resolution.Work]
	if !ok {
		return fmt.Errorf("%w: work %q does not exist", core.ErrStoreWorkDoesntExist, resolution.Work)
	}

	if batch.lease != resolution.Lease {
		return fmt.Errorf("resolve work %q: %w", resolution.Work, core.ErrStoreStaleLeaseToken)
	}

	if batch.lastResolution != nil {
		if *batch.lastResolution == resolution {
			return nil
		}

		return invalidTransition(resolution.Work)
	}

	if batch.state != batchLeased || !allDeliveriesPending(batch) {
		return invalidTransition(resolution.Work)
	}

	if err := applyResolution(batch, resolution, now); err != nil {
		return err
	}

	accepted := resolution
	batch.lastResolution = &accepted

	return nil
}

func (s *Store[T]) lease(batch *workBatch[T], now time.Time, duration time.Duration) core.Work[T] {
	batch.state = batchLeased
	batch.attempt++
	batch.retryAt = time.Time{}
	batch.lease = core.LeaseToken("lease-" + rand.Text())
	batch.leaseUntil = now.Add(duration)
	batch.lastResolution = nil

	items := make([]core.Item[T], len(batch.deliveries))
	for index, candidate := range batch.deliveries {
		items[index] = core.Item[T]{
			ID:      candidate.item.id,
			Payload: candidate.item.payload,
		}
	}

	return core.Work[T]{
		ID:          batch.id,
		Plan:        batch.plan.id,
		Destination: batch.destination,
		Items:       items,
		Attempt:     batch.attempt,
		Lease:       batch.lease,
		LeaseUntil:  batch.leaseUntil,
	}
}

func (s *Store[T]) createBatch(plan *storedPlan[T], deliveries []*delivery[T]) *workBatch[T] {
	s.nextBatch++

	batch := &workBatch[T]{
		id:          core.WorkID("work-" + strconv.FormatUint(s.nextBatch, 10)),
		plan:        plan,
		destination: deliveries[0].destination,
		deliveries:  deliveries,
		state:       batchReady,
	}
	for _, candidate := range deliveries {
		candidate.batch = batch
	}

	plan.batches = append(plan.batches, batch)
	s.batches[batch.id] = batch

	return batch
}

func batchEligible[T any](batch *workBatch[T], now time.Time) bool {
	if batch.plan.destinationByID[batch.destination].quarantined {
		return false
	}

	for _, candidate := range batch.deliveries {
		if !deliveryClaimable(batch.plan, candidate, batch, now) {
			return false
		}
	}

	switch batch.state {
	case batchReady:
		return true
	case batchLeased:
		return !batch.leaseUntil.After(now)
	case batchRetryable:
		return !batch.retryAt.After(now)
	case batchTerminal:
		return false
	}

	return false
}

func nextBatchDeliveries[T any](plan *storedPlan[T], limit int, now time.Time) []*delivery[T] {
	for _, destination := range plan.destinations {
		if plan.destinationByID[destination].quarantined {
			continue
		}

		deliveries := make([]*delivery[T], 0, limit)

		for _, item := range plan.itemOrder {
			candidate := item.deliveries[destination]
			if candidate.batch != nil || !deliveryClaimable(plan, candidate, nil, now) {
				continue
			}

			deliveries = append(deliveries, candidate)
			if len(deliveries) == limit {
				return deliveries
			}
		}

		if len(deliveries) > 0 {
			return deliveries
		}
	}

	return nil
}

func deliveryClaimable[T any](plan *storedPlan[T], candidate *delivery[T], except *workBatch[T], now time.Time) bool {
	if candidate.state != deliveryPending {
		return false
	}

	switch plan.policy {
	case core.PolicyAll:
		return true
	case core.PolicyFirstSuccess:
		return firstEligibleDelivery(plan, candidate.item) == candidate &&
			!itemHasLiveLease(candidate.item, except, now)
	case core.PolicyUnknown:
		return false
	}

	return false
}

func firstEligibleDelivery[T any](plan *storedPlan[T], item *storedItem[T]) *delivery[T] {
	for _, destination := range plan.destinations {
		switch candidate := item.deliveries[destination]; candidate.state {
		case deliveryPending:
			return candidate
		case deliveryDelivered:
			return nil
		case deliveryFailedPermanent, deliveryCanceled:
			continue
		}
	}

	return nil
}

func itemHasLiveLease[T any](item *storedItem[T], except *workBatch[T], now time.Time) bool {
	for _, candidate := range item.deliveries {
		batch := candidate.batch
		if batch == nil || batch == except || batch.state != batchLeased {
			continue
		}

		if batch.leaseUntil.After(now) {
			return true
		}
	}

	return false
}

func allDeliveriesPending[T any](batch *workBatch[T]) bool {
	for _, candidate := range batch.deliveries {
		if candidate.state != deliveryPending {
			return false
		}
	}

	return true
}

func applyResolution[T any](batch *workBatch[T], resolution core.Resolution, now time.Time) error {
	switch resolution.Outcome {
	case core.OutcomeDelivered:
		terminalize(batch, deliveryDelivered, "")

		if batch.plan.policy == core.PolicyFirstSuccess {
			cancelSiblingDeliveries(batch)
		}
	case core.OutcomeRetryableFailure:
		batch.state = batchRetryable
		batch.retryAt = now.Add(max(resolution.RetryAfter, 0))
		batch.leaseUntil = time.Time{}
	case core.OutcomeFailedPermanent:
		terminalize(batch, deliveryFailedPermanent, resolution.Failure)

		if resolution.Scope == core.FailureScopeDestination {
			// batch is already batchTerminal from terminalize above, so
			// quarantineDestination's own terminal-state check skips it.
			quarantineDestination(batch.plan, batch.destination, resolution.Failure)
		}
	case core.OutcomeUnknown, core.OutcomeDeliveredUnrecorded:
		return invalidTransition(resolution.Work)
	}

	return nil
}

func terminalize[T any](batch *workBatch[T], state deliveryState, failure string) {
	for _, candidate := range batch.deliveries {
		candidate.state = state
		candidate.failure = failure
	}

	batch.state = batchTerminal
	batch.retryAt = time.Time{}

	batch.retainUntil = batch.leaseUntil
	batch.leaseUntil = time.Time{}
}

func cancelSiblingDeliveries[T any](batch *workBatch[T]) {
	for _, delivered := range batch.deliveries {
		for _, destination := range batch.plan.destinations {
			candidate := delivered.item.deliveries[destination]
			if candidate == delivered || candidate.state != deliveryPending {
				continue
			}

			candidate.state = deliveryCanceled
			candidate.failure = ""
		}
	}
}

func invalidTransition(workID core.WorkID) error {
	return fmt.Errorf("resolve work %q: %w", workID, core.ErrStoreInvalidTransition)
}

// sweep retains terminal batches through their old leases, then drops payloads
// while keeping item IDs for deduplication.
func (s *Store[T]) sweep(plan *storedPlan[T], now time.Time) {
	live := plan.batches[:0]

	for _, batch := range plan.batches {
		if batch.state != batchTerminal || batch.retainUntil.After(now) {
			live = append(live, batch)

			continue
		}

		for _, candidate := range batch.deliveries {
			candidate.batch = nil
		}

		delete(s.batches, batch.id)
	}

	clear(plan.batches[len(live):])
	plan.batches = live

	pending := plan.itemOrder[:0]

	for _, item := range plan.itemOrder {
		if !itemFinished(item) {
			pending = append(pending, item)

			continue
		}

		var zero T

		item.payload = zero
		item.deliveries = nil
	}

	clear(plan.itemOrder[len(pending):])
	plan.itemOrder = pending
}

func itemFinished[T any](item *storedItem[T]) bool {
	for _, candidate := range item.deliveries {
		if candidate.state == deliveryPending || candidate.batch != nil {
			return false
		}
	}

	return true
}
