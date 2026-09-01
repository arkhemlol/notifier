package sqlite

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/arkhemlol/notifier/internal/core"
)

type reclaimableBatch struct {
	key         int64
	workID      core.WorkID
	destination core.DestinationID
	attempt     int
}

type pendingDelivery struct {
	deliveryKey int64
	itemID      int64
	payload     []byte
}

type claimRun[T any] struct {
	store          *Store[T]
	request        core.ClaimRequest
	plan           storedPlan
	leases         []core.LeaseToken
	workIDs        []core.WorkID
	now            int64
	leaseUntil     int64
	leaseUntilTime time.Time

	// exhausted remembers, by destination index, which destinations
	// createNext already found empty this transaction: claiming never adds
	// pending deliveries to a destination, so one empty once stays empty for
	// the rest of the run.
	exhausted []bool
}

func leasedBatch(attempt int, lease core.LeaseToken, leaseUntil int64) batchWrite {
	return batchWrite{
		state:      batchStateLeased,
		attempt:    attempt,
		lease:      string(lease),
		leaseUntil: leaseUntil,
	}
}

// Claim implements core.Store.
func (s *Store[T]) Claim(ctx context.Context, request core.ClaimRequest) ([]core.Work[T], error) {
	leases, workIDs := claimIdentifiers(request.MaxWork)
	nowTime := time.Now().UTC()
	leaseUntilTime := normalizeScheduledTime(nowTime.Add(request.LeaseDuration))

	run := claimRun[T]{
		store:          s,
		request:        request,
		leases:         leases,
		workIDs:        workIDs,
		now:            nowTime.UnixMicro(),
		leaseUntil:     leaseUntilTime.UnixMicro(),
		leaseUntilTime: leaseUntilTime,
	}

	var work []core.Work[T]

	err := s.writeTransaction(ctx, "claim work", func(conn *txConn) error {
		plan, err := loadStoredPlan(ctx, conn, request.Plan)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf(
				"%w: plan %q is not registered",
				core.ErrStoreInvalidTransition, request.Plan,
			)
		}

		if err != nil {
			return fmt.Errorf("load claim plan: %w", err)
		}

		run.plan = plan
		run.exhausted = make([]bool, len(plan.destinations))
		work, err = run.collect(ctx, conn)

		return err
	})
	if err != nil {
		return nil, err
	}

	return work, nil
}

func (r *claimRun[T]) collect(ctx context.Context, conn *txConn) ([]core.Work[T], error) {
	claimed, err := r.reclaim(ctx, conn)
	if err != nil {
		return nil, err
	}

	for len(claimed) < r.request.MaxWork {
		next, created, err := r.createNext(ctx, conn, len(claimed))
		if err != nil {
			return nil, err
		}

		if !created {
			break
		}

		claimed = append(claimed, next)
	}

	return claimed, nil
}

func (r *claimRun[T]) work(
	id core.WorkID, destination core.DestinationID, items []core.Item[T], attempt int,
	lease core.LeaseToken,
) core.Work[T] {
	return core.Work[T]{
		ID:          id,
		Plan:        r.request.Plan,
		Destination: destination,
		Items:       items,
		Attempt:     attempt,
		Lease:       lease,
		LeaseUntil:  r.leaseUntilTime,
	}
}

func (r *claimRun[T]) reclaim(ctx context.Context, conn *txConn) ([]core.Work[T], error) {
	reclaimable, err := selectReclaimableBatches(ctx, conn, r.plan.key, r.now, r.request.MaxWork)
	if err != nil {
		return nil, err
	}

	batchKeys := make([]int64, len(reclaimable))
	for i, batch := range reclaimable {
		batchKeys[i] = batch.key
	}

	itemsByBatch, err := r.loadBatchItems(ctx, conn, batchKeys)
	if err != nil {
		return nil, err
	}

	claimed := make([]core.Work[T], 0, r.request.MaxWork)

	for i, batch := range reclaimable {
		lease := r.leases[len(claimed)]
		attempt := batch.attempt + 1

		if err := updateBatches(
			ctx, conn, "rotate reclaimed batch lease",
			updateBatchByKey, leasedBatch(attempt, lease, r.leaseUntil), batch.key,
		); err != nil {
			return nil, err
		}

		claimed = append(claimed,
			r.work(batch.workID, batch.destination, itemsByBatch[i], attempt, lease))
	}

	return claimed, nil
}

func (r *claimRun[T]) createNext(
	ctx context.Context, conn *txConn, index int,
) (core.Work[T], bool, error) {
	for position, destination := range r.plan.destinations {
		if destination.state != destinationStateActive || r.exhausted[position] {
			continue
		}

		candidates, err := selectPendingDeliveries(
			ctx, conn, r.plan, destination, r.request.MaxItemsPerWork,
		)
		if err != nil {
			return core.Work[T]{}, false, err
		}

		if len(candidates) == 0 {
			r.exhausted[position] = true
			continue
		}

		workID, lease := r.workIDs[index], r.leases[index]

		batchKey, err := insertBatch(ctx, conn, destination.key, workID, lease, r.leaseUntil)
		if err != nil {
			return core.Work[T]{}, false, err
		}

		items, err := r.insertBatchItems(ctx, conn, batchKey, candidates)
		if err != nil {
			return core.Work[T]{}, false, err
		}

		return r.work(workID, destination.id, items, 1, lease), true, nil
	}

	return core.Work[T]{}, false, nil
}

func (s *Store[T]) decodeItem(ctx context.Context, id int64, payload []byte) (core.Item[T], error) {
	if err := ctx.Err(); err != nil {
		return core.Item[T]{}, err
	}

	value, err := s.codec.Decode(bytes.Clone(payload))
	if err != nil {
		return core.Item[T]{}, fmt.Errorf("decode claimed payload: %w: %w", ErrCodec, err)
	}

	return core.Item[T]{ID: id, Payload: value}, nil
}

func insertBatch(
	ctx context.Context, conn *txConn, destinationKey int64, workID core.WorkID,
	lease core.LeaseToken, leaseUntil int64,
) (int64, error) {
	args := append(
		[]any{destinationKey, string(workID)},
		leasedBatch(1, lease, leaseUntil).args()...,
	)

	return insertReturningKey(ctx, conn, "work batch", insertBatchQuery, args...)
}

func (r *claimRun[T]) insertBatchItems(
	ctx context.Context, conn *txConn, batchKey int64, candidates []pendingDelivery,
) ([]core.Item[T], error) {
	items := make([]core.Item[T], len(candidates))
	args := make([]any, 0, len(candidates)*3)

	for position, candidate := range candidates {
		args = append(args, batchKey, candidate.deliveryKey, position)

		item, err := r.store.decodeItem(ctx, candidate.itemID, candidate.payload)
		if err != nil {
			return nil, err
		}

		items[position] = item
	}

	query := "INSERT INTO notifier_work_batch_items(batch_key, delivery_key, position) VALUES " +
		strings.TrimSuffix(strings.Repeat("(?, ?, ?),", len(candidates)), ",")

	if _, err := conn.exec(ctx, query, args...); err != nil {
		return nil, fmt.Errorf("insert work batch items: %w", err)
	}

	return items, nil
}

type batchItemRow struct {
	batchKey int64
	itemID   int64
	payload  []byte
}

func (r *claimRun[T]) loadBatchItems(
	ctx context.Context, conn *txConn, batchKeys []int64,
) ([][]core.Item[T], error) {
	if len(batchKeys) == 0 {
		return nil, nil
	}

	args := make([]any, len(batchKeys))
	for i, key := range batchKeys {
		args[i] = key
	}

	rows, err := queryAll(ctx, conn, "batch items", `SELECT
			wbi.batch_key, i.item_id, i.payload
		FROM notifier_work_batch_items wbi
		JOIN notifier_deliveries d ON d.delivery_key = wbi.delivery_key
		JOIN notifier_items i ON i.item_key = d.item_key
		WHERE wbi.batch_key IN (`+placeholders(len(batchKeys))+`)
		ORDER BY wbi.batch_key, wbi.position`, args,
		func(rows *sql.Rows, row *batchItemRow) error {
			return rows.Scan(&row.batchKey, &row.itemID, &row.payload)
		})
	if err != nil {
		return nil, err
	}

	itemsByBatch := make([][]core.Item[T], len(batchKeys))

	position := 0

	for _, row := range rows {
		for batchKeys[position] != row.batchKey {
			position++

			if position == len(batchKeys) {
				return nil, fmt.Errorf(
					"batch item row references unrequested batch %d", row.batchKey,
				)
			}
		}

		item, err := r.store.decodeItem(ctx, row.itemID, row.payload)
		if err != nil {
			return nil, err
		}

		itemsByBatch[position] = append(itemsByBatch[position], item)
	}

	return itemsByBatch, nil
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func claimIdentifiers(maxWork int) ([]core.LeaseToken, []core.WorkID) {
	leases := make([]core.LeaseToken, maxWork)

	workIDs := make([]core.WorkID, maxWork)
	for i := range maxWork {
		leases[i] = core.LeaseToken(rand.Text())
		workIDs[i] = core.WorkID("work-" + rand.Text())
	}

	return leases, workIDs
}

func normalizeScheduledTime(value time.Time) time.Time {
	microseconds := value.UnixMicro()
	if value.Nanosecond()%int(time.Microsecond) != 0 {
		microseconds++
	}

	return time.UnixMicro(microseconds).UTC()
}

// Keep retry and lease scans separate so SQLite can use each state/time index.
const reclaimableHalf = `SELECT wb.batch_key, wb.work_id, pd.destination_id, wb.attempt_count
		FROM notifier_work_batches wb
		JOIN notifier_plan_destinations pd
			ON pd.plan_destination_key = wb.plan_destination_key
		JOIN notifier_destination_status ds
			ON ds.plan_destination_key = pd.plan_destination_key
		WHERE pd.plan_key = ? AND ds.state = ? AND wb.state = ? AND `

const selectReclaimableQuery = `SELECT
		batch_key, work_id, destination_id, attempt_count
	FROM (
		` + reclaimableHalf + `wb.retry_at <= ?
		UNION ALL
		` + reclaimableHalf + `wb.lease_until <= ?
	)
	ORDER BY batch_key
	LIMIT ?`

func selectReclaimableBatches(
	ctx context.Context, conn *txConn, planKey, now int64, limit int,
) ([]reclaimableBatch, error) {
	return queryAll(ctx, conn, "reclaimable batches", selectReclaimableQuery, []any{
		planKey, destinationStateActive, batchStateRetryable, now,
		planKey, destinationStateActive, batchStateLeased, now,
		limit,
	}, func(rows *sql.Rows, batch *reclaimableBatch) error {
		var workID, destinationID string
		if err := rows.Scan(&batch.key, &workID, &destinationID, &batch.attempt); err != nil {
			return err
		}

		batch.workID = core.WorkID(workID)
		batch.destination = core.DestinationID(destinationID)

		return nil
	})
}

const pendingDeliveriesHead = `SELECT d.delivery_key, i.item_id, i.payload
		FROM notifier_items i
		JOIN notifier_deliveries d ON d.item_key = i.item_key
		WHERE i.plan_key = ?
			AND d.plan_destination_key = ?
			AND d.state = ?
			AND NOT EXISTS (
				SELECT 1 FROM notifier_work_batch_items wbi
				WHERE wbi.delivery_key = d.delivery_key
			)`

const pendingDeliveriesPriorSettled = `
			AND NOT EXISTS (
				SELECT 1
				FROM notifier_deliveries prior
				JOIN notifier_plan_destinations prior_pd
					ON prior_pd.plan_destination_key = prior.plan_destination_key
				WHERE prior.item_key = d.item_key
					AND prior_pd.position < ?
					AND prior.state = ?
			)`

const pendingDeliveriesTail = `
		ORDER BY i.enqueued_at, i.item_key
		LIMIT ?`

const (
	pendingDeliveriesQuery = pendingDeliveriesHead + pendingDeliveriesTail

	pendingDeliveriesChainQuery = pendingDeliveriesHead +
		pendingDeliveriesPriorSettled + pendingDeliveriesTail
)

func selectPendingDeliveries(
	ctx context.Context, conn *txConn, plan storedPlan, destination storedDestination,
	limit int,
) ([]pendingDelivery, error) {
	query := pendingDeliveriesQuery
	args := []any{plan.key, destination.key, deliveryStatePending}

	if plan.policy == core.PolicyFirstSuccess {
		query = pendingDeliveriesChainQuery

		args = append(args, destination.position, deliveryStatePending)
	}

	return queryAll(ctx, conn, "pending deliveries", query, append(args, limit),
		func(rows *sql.Rows, delivery *pendingDelivery) error {
			return rows.Scan(&delivery.deliveryKey, &delivery.itemID, &delivery.payload)
		})
}
