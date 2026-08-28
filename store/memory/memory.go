// Package memory provides a process-local implementation of core.Store.
package memory

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/arkhemlol/notifier/internal/core"
)

// Store keeps delivery state in memory (doesn't survive process restarts).
type Store[T any] struct {
	mu        sync.Mutex
	plans     map[core.PlanID]*storedPlan[T]
	batches   map[core.WorkID]*workBatch[T]
	nextBatch uint64
}

type storedPlan[T any] struct {
	id              core.PlanID
	policy          core.Policy
	destinations    []core.DestinationID
	destinationByID map[core.DestinationID]*storedDestination
	items           map[int64]*storedItem[T]
	itemOrder       []*storedItem[T]
	batches         []*workBatch[T]
}

type storedDestination struct {
	quarantined bool
	failure     string
}

type storedItem[T any] struct {
	id      int64
	payload T
	// deliveries is nil once sweep has retired the item, leaving only the ID as
	// a deduplication marker.
	deliveries map[core.DestinationID]*delivery[T]
}

type deliveryState uint8

const (
	deliveryPending deliveryState = iota
	deliveryDelivered
	deliveryFailedPermanent
	deliveryCanceled
)

type delivery[T any] struct {
	item        *storedItem[T]
	destination core.DestinationID
	state       deliveryState
	failure     string
	batch       *workBatch[T]
}

type batchState uint8

const (
	batchReady batchState = iota
	batchLeased
	batchRetryable
	batchTerminal
)

type workBatch[T any] struct {
	id             core.WorkID
	plan           *storedPlan[T]
	destination    core.DestinationID
	deliveries     []*delivery[T]
	state          batchState
	attempt        int
	retryAt        time.Time
	lease          core.LeaseToken
	leaseUntil     time.Time
	retainUntil    time.Time
	lastResolution *core.Resolution
}

// New returns an empty Store.
func New[T any]() *Store[T] {
	return &Store[T]{
		plans:   map[core.PlanID]*storedPlan[T]{},
		batches: map[core.WorkID]*workBatch[T]{},
	}
}

// Admit registers the plan and creates one delivery obligation per plan destination for each item.
func (s *Store[T]) Admit(ctx context.Context, plan core.Plan, items []core.Item[T]) error {
	if plan.ID() == "" {
		return core.ErrInvalidPlan
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	stored, ok := s.plans[plan.ID()]
	if !ok {
		stored = newStoredPlan[T](plan)
		s.plans[plan.ID()] = stored
	}

	if err := validateItemIDs(items); err != nil {
		return err
	}

	for _, item := range items {
		stored.insert(item)
	}

	return nil
}

// QuarantineDestination blocks the destination and terminalizes its work.
func (s *Store[T]) QuarantineDestination(
	ctx context.Context, plan core.PlanID, destination core.DestinationID, failure string,
) error {
	return s.withDestination(ctx, plan, destination, func(stored *storedPlan[T], _ *storedDestination) {
		quarantineDestination(stored, destination, failure)
	})
}

// ActivateDestination lets the destination receive newly enqueued work again.
func (s *Store[T]) ActivateDestination(ctx context.Context, plan core.PlanID, destination core.DestinationID) error {
	return s.withDestination(ctx, plan, destination, func(_ *storedPlan[T], status *storedDestination) {
		status.quarantined = false
		status.failure = ""
	})
}

// PendingPlans returns every plan that still holds unfinished deliveries.
func (s *Store[T]) PendingPlans(ctx context.Context) ([]core.PlanID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	pending := make([]core.PlanID, 0, len(s.plans))

	for id, plan := range s.plans {
		if plan.hasPendingWork() {
			pending = append(pending, id)
		}
	}

	slices.Sort(pending)

	return pending, nil
}

func (p *storedPlan[T]) hasPendingWork() bool {
	for _, item := range p.itemOrder {
		for _, candidate := range item.deliveries {
			if candidate.state == deliveryPending {
				return true
			}
		}
	}

	return false
}

func (s *Store[T]) withDestination(
	ctx context.Context,
	plan core.PlanID,
	destination core.DestinationID,
	apply func(*storedPlan[T], *storedDestination),
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	stored, ok := s.plans[plan]
	if !ok {
		return notRegistered(plan)
	}

	status, ok := stored.destinationByID[destination]
	if !ok {
		return fmt.Errorf("%w: destination %q is not in plan %q", core.ErrStoreInvalidTransition, destination, plan)
	}

	apply(stored, status)

	return nil
}

func newStoredPlan[T any](plan core.Plan) *storedPlan[T] {
	destinations := plan.Destinations()

	byID := make(map[core.DestinationID]*storedDestination, len(destinations))
	for _, destination := range destinations {
		byID[destination] = &storedDestination{}
	}

	return &storedPlan[T]{
		id:              plan.ID(),
		policy:          plan.Policy(),
		destinations:    destinations,
		destinationByID: byID,
		items:           map[int64]*storedItem[T]{},
	}
}

func (p *storedPlan[T]) insert(item core.Item[T]) {
	if _, ok := p.items[item.ID]; ok {
		return
	}

	stored := &storedItem[T]{
		id:         item.ID,
		payload:    item.Payload,
		deliveries: make(map[core.DestinationID]*delivery[T], len(p.destinations)),
	}

	for _, destination := range p.destinations {
		obligation := &delivery[T]{
			item:        stored,
			destination: destination,
			state:       deliveryPending,
		}

		if status := p.destinationByID[destination]; status.quarantined {
			obligation.state = deliveryFailedPermanent
			obligation.failure = status.failure
		}

		stored.deliveries[destination] = obligation
	}

	p.items[item.ID] = stored
	p.itemOrder = append(p.itemOrder, stored)
}

// validateItemIDs rejects a batch that names the same item twice.
func validateItemIDs[T any](items []core.Item[T]) error {
	seen := make(map[int64]struct{}, len(items))
	for _, item := range items {
		if _, duplicate := seen[item.ID]; duplicate {
			return fmt.Errorf("%w: item %d is named twice in one admit", core.ErrStorePayloadConflict, item.ID)
		}

		seen[item.ID] = struct{}{}
	}

	return nil
}

// quarantineDestination blocks the destination and terminalizes its live batches.
// Batches already terminalized by the caller (e.g. the batch whose resolution
// triggered the quarantine) are skipped via the batchTerminal check below.
func quarantineDestination[T any](
	plan *storedPlan[T],
	destination core.DestinationID,
	failure string,
) {
	status := plan.destinationByID[destination]
	status.quarantined = true
	status.failure = failure

	for _, item := range plan.itemOrder {
		if candidate := item.deliveries[destination]; candidate.state == deliveryPending {
			candidate.state = deliveryFailedPermanent
			candidate.failure = failure
		}
	}

	for _, batch := range plan.batches {
		if batch.destination != destination || batch.state == batchTerminal {
			continue
		}

		batch.state = batchTerminal
		batch.retryAt = time.Time{}
		batch.lease = ""
		batch.retainUntil = batch.leaseUntil
		batch.leaseUntil = time.Time{}
		batch.lastResolution = nil
	}
}

func notRegistered(planID core.PlanID) error {
	return fmt.Errorf("%w: plan %q is not registered", core.ErrStoreInvalidTransition, planID)
}
