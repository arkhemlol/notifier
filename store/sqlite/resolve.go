package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/arkhemlol/notifier/internal/core"
)

type batchWrite struct {
	state int

	attempt    any
	retryAt    any
	lease      any
	leaseUntil any
	outcome    core.Outcome
	scope      core.FailureScope
	retryAfter time.Duration
	failure    string
	at         any
}

func (b batchWrite) args() []any {
	return []any{
		b.state, b.attempt, b.retryAt, b.lease, b.leaseUntil,
		int(b.outcome), int(b.scope), int64(b.retryAfter), b.failure, b.at,
	}
}

const updateBatchPrefix = `UPDATE notifier_work_batches SET
		state = ?,
		attempt_count = COALESCE(?, attempt_count),
		retry_at = ?,
		lease_token = ?,
		lease_until = ?,
		resolution_outcome = ?,
		resolution_scope = ?,
		resolution_retry_after_ns = ?,
		last_failure = ?,
		outcome_at = ?
	WHERE `

const insertBatchQuery = `INSERT INTO notifier_work_batches(
		plan_destination_key,
		work_id,
		state,
		attempt_count,
		retry_at,
		lease_token,
		lease_until,
		resolution_outcome,
		resolution_scope,
		resolution_retry_after_ns,
		last_failure,
		outcome_at
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

func resolvedBatch(state int, resolution core.Resolution, at int64) batchWrite {
	return batchWrite{
		state:      state,
		lease:      string(resolution.Lease),
		outcome:    resolution.Outcome,
		scope:      resolution.Scope,
		retryAfter: resolution.RetryAfter,
		failure:    resolution.Failure,
		at:         at,
	}
}

func fencedBatch(
	outcome core.Outcome, scope core.FailureScope, failure string, at int64,
) batchWrite {
	return batchWrite{
		state:   batchStateTerminal,
		outcome: outcome,
		scope:   scope,
		failure: failure,
		at:      at,
	}
}

// Enumerating live states lets idx_work_batches_leased constrain destination
// and state instead of scanning a destination's history.
const fenceableBatch = "batch_key <> ? AND state IN (?, ?, ?)"

const (
	updateBatchByKey = updateBatchPrefix + "batch_key = ?"

	fenceCanceledBatches = updateBatchPrefix + fenceableBatch + `
		AND EXISTS (
			SELECT 1
			FROM notifier_work_batch_items wbi
			JOIN notifier_deliveries d ON d.delivery_key = wbi.delivery_key
			WHERE wbi.batch_key = notifier_work_batches.batch_key
				AND d.state = ?
		)`

	fenceDestinationBatches = updateBatchPrefix +
		"plan_destination_key = ? AND " + fenceableBatch
)

const settleDeliveriesPrefix = `UPDATE notifier_deliveries SET
		state = ?, failure = ?, outcome_at = ?
	WHERE `

const (
	settleBatchDeliveries = settleDeliveriesPrefix + `state = ? AND delivery_key IN (
			SELECT delivery_key FROM notifier_work_batch_items WHERE batch_key = ?
		)`

	settleSiblingDeliveries = settleDeliveriesPrefix + `state = ?
		AND item_key IN (
			SELECT d.item_key
			FROM notifier_work_batch_items wbi
			JOIN notifier_deliveries d ON d.delivery_key = wbi.delivery_key
			WHERE wbi.batch_key = ?
		)
		AND delivery_key NOT IN (
			SELECT delivery_key FROM notifier_work_batch_items WHERE batch_key = ?
		)`

	settleDestinationDeliveries = settleDeliveriesPrefix +
		"plan_destination_key = ? AND state = ?"
)

func updateBatches(
	ctx context.Context, conn *txConn, subject, query string, write batchWrite,
	args ...any,
) error {
	if _, err := conn.exec(ctx, query, append(write.args(), args...)...); err != nil {
		return fmt.Errorf("%s: %w", subject, err)
	}

	return nil
}

type storedBatchResolution struct {
	key          int64
	destination  int64
	policy       core.Policy
	state        int
	leaseToken   sql.NullString
	outcome      core.Outcome
	scope        core.FailureScope
	retryAfterNS int64
	failure      string
}

// Resolve implements core.Store.
func (s *Store[T]) Resolve(ctx context.Context, resolution core.Resolution) error {
	nowTime := time.Now().UTC()
	now := nowTime.UnixMicro()

	return s.writeTransaction(ctx, "resolve work", func(conn *txConn) error {
		return resolveTransaction(ctx, conn, resolution, nowTime, now)
	})
}

func resolveTransaction(
	ctx context.Context, conn *txConn, resolution core.Resolution, nowTime time.Time,
	now int64,
) error {
	batch, err := loadBatchResolution(ctx, conn, resolution.Work)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf(
			"%w: work %q does not exist",
			core.ErrStoreWorkDoesntExist, resolution.Work,
		)
	}

	if err != nil {
		return fmt.Errorf("load work batch: %w", err)
	}

	if !batch.leaseToken.Valid || batch.leaseToken.String != string(resolution.Lease) {
		return fmt.Errorf(
			"resolve work %q: %w",
			resolution.Work, core.ErrStoreStaleLeaseToken,
		)
	}

	if batch.state != batchStateLeased {
		if batch.matches(resolution) {
			return nil
		}

		return invalidResolutionTransition(resolution.Work)
	}

	members, allPending, err := batchDeliveries(ctx, conn, batch.key)
	if err != nil {
		return err
	}

	if !allPending {
		return invalidResolutionTransition(resolution.Work)
	}

	return applyResolution(ctx, conn, batch, members, resolution, nowTime, now)
}

func applyResolution(
	ctx context.Context, conn *txConn, batch storedBatchResolution, members int64,
	resolution core.Resolution, nowTime time.Time, now int64,
) error {
	switch resolution.Outcome {
	case core.OutcomeRetryableFailure:
		retryAt := normalizeScheduledTime(nowTime.Add(resolution.RetryAfter)).UnixMicro()
		write := resolvedBatch(batchStateRetryable, resolution, now)
		write.retryAt = retryAt

		return updateBatches(ctx, conn, "schedule batch retry",
			updateBatchByKey, write, batch.key)

	case core.OutcomeDelivered:
		if err := resolveBatchTerminal(
			ctx, conn, batch, members, resolution, now, deliveryStateDelivered,
		); err != nil {
			return err
		}

		if batch.policy != core.PolicyFirstSuccess {
			return nil
		}

		return cancelFirstSuccessSiblings(ctx, conn, batch.key, now)

	case core.OutcomeFailedPermanent:
		if err := resolveBatchTerminal(
			ctx, conn, batch, members, resolution, now, deliveryStateFailedPermanent,
		); err != nil {
			return err
		}

		if resolution.Scope != core.FailureScopeDestination {
			return nil
		}

		return quarantineDestination(
			ctx, conn, batch.destination, resolution.Failure, now, batch.key,
		)

	case core.OutcomeUnknown, core.OutcomeDeliveredUnrecorded:
		return invalidResolutionTransition(resolution.Work)

	default:
		return invalidResolutionTransition(resolution.Work)
	}
}

func invalidResolutionTransition(workID core.WorkID) error {
	return fmt.Errorf("resolve work %q: %w", workID, core.ErrStoreInvalidTransition)
}

func loadBatchResolution(
	ctx context.Context, conn *txConn, workID core.WorkID,
) (storedBatchResolution, error) {
	var (
		batch                  storedBatchResolution
		policy, outcome, scope int
	)

	if err := conn.queryRow(ctx, `SELECT
		wb.batch_key,
		wb.plan_destination_key,
		p.policy,
		wb.state,
		wb.lease_token,
		wb.resolution_outcome,
		wb.resolution_scope,
		wb.resolution_retry_after_ns,
		wb.last_failure
	FROM notifier_work_batches wb
	JOIN notifier_plan_destinations pd
		ON pd.plan_destination_key = wb.plan_destination_key
	JOIN notifier_plans p ON p.plan_key = pd.plan_key
	WHERE wb.work_id = ?`, string(workID)).Scan(
		&batch.key, &batch.destination, &policy, &batch.state, &batch.leaseToken,
		&outcome, &scope, &batch.retryAfterNS, &batch.failure,
	); err != nil {
		return storedBatchResolution{}, err
	}

	batch.policy = core.Policy(policy)
	batch.outcome = core.Outcome(outcome)
	batch.scope = core.FailureScope(scope)

	return batch, nil
}

func (batch storedBatchResolution) matches(resolution core.Resolution) bool {
	return batch.outcome == resolution.Outcome &&
		batch.scope == resolution.Scope &&
		batch.retryAfterNS == int64(resolution.RetryAfter) &&
		batch.failure == resolution.Failure
}

func batchDeliveries(
	ctx context.Context, conn *txConn, batchKey int64,
) (members int64, allPending bool, err error) {
	var pending int64
	if err := conn.queryRow(
		ctx, `SELECT
		COUNT(*),
		COALESCE(SUM(CASE WHEN d.state = ? THEN 1 ELSE 0 END), 0)
	FROM notifier_work_batch_items wbi
	JOIN notifier_deliveries d ON d.delivery_key = wbi.delivery_key
	WHERE wbi.batch_key = ?`,
		deliveryStatePending, batchKey,
	).Scan(&members, &pending); err != nil {
		return 0, false, fmt.Errorf("inspect batch deliveries: %w", err)
	}

	return members, members > 0 && members == pending, nil
}

func resolveBatchTerminal(
	ctx context.Context, conn *txConn, batch storedBatchResolution, members int64,
	resolution core.Resolution, now int64, deliveryState int,
) error {
	result, err := conn.exec(ctx, settleBatchDeliveries,
		deliveryState, resolution.Failure, now, deliveryStatePending, batch.key)
	if err != nil {
		return fmt.Errorf("resolve batch deliveries: %w", err)
	}

	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read resolved delivery count: %w", err)
	}

	if updated != members {
		return fmt.Errorf(
			"%w: batch deliveries are not pending",
			core.ErrStoreInvalidTransition,
		)
	}

	return updateBatches(ctx, conn, "terminalize work batch", updateBatchByKey,
		resolvedBatch(batchStateTerminal, resolution, now), batch.key)
}

func cancelFirstSuccessSiblings(
	ctx context.Context, conn *txConn, batchKey, now int64,
) error {
	if _, err := conn.exec(
		ctx, settleSiblingDeliveries,
		deliveryStateCanceled, "", now, deliveryStatePending, batchKey, batchKey,
	); err != nil {
		return fmt.Errorf("cancel first-success siblings: %w", err)
	}

	return updateBatches(ctx, conn, "fence canceled first-success batches",
		fenceCanceledBatches,
		fencedBatch(core.OutcomeUnknown, core.FailureScopeUnknown, "", now),
		batchKey, batchStateReady, batchStateLeased, batchStateRetryable,
		deliveryStateCanceled)
}

func quarantineDestination(
	ctx context.Context, conn *txConn, destinationKey int64, failure string, now int64,
	preserveBatchKey int64,
) error {
	if _, err := conn.exec(
		ctx, `UPDATE notifier_destination_status SET
		state = ?, failure = ?
	WHERE plan_destination_key = ?`,
		destinationStateQuarantined, failure, destinationKey,
	); err != nil {
		return fmt.Errorf("quarantine destination status: %w", err)
	}

	if _, err := conn.exec(
		ctx, settleDestinationDeliveries,
		deliveryStateFailedPermanent, failure, now, destinationKey, deliveryStatePending,
	); err != nil {
		return fmt.Errorf("terminalize quarantined deliveries: %w", err)
	}

	return updateBatches(ctx, conn, "fence quarantined batches",
		fenceDestinationBatches,
		fencedBatch(core.OutcomeFailedPermanent, core.FailureScopeDestination, failure, now),
		destinationKey, preserveBatchKey,
		batchStateReady, batchStateLeased, batchStateRetryable)
}

func (s *Store[T]) withDestination(
	ctx context.Context, operation string, plan core.PlanID, destination core.DestinationID,
	fn func(*txConn, int64) error,
) error {
	return s.writeTransaction(ctx, operation, func(conn *txConn) error {
		destinationKey, err := loadDestinationKey(ctx, conn, plan, destination)
		if err != nil {
			return err
		}

		return fn(conn, destinationKey)
	})
}

// QuarantineDestination implements core.Store.
func (s *Store[T]) QuarantineDestination(
	ctx context.Context, plan core.PlanID, destination core.DestinationID, failure string,
) error {
	now := time.Now().UTC().UnixMicro()

	return s.withDestination(ctx, "quarantine destination", plan, destination,
		func(conn *txConn, destinationKey int64) error {
			return quarantineDestination(ctx, conn, destinationKey, failure, now, 0)
		})
}

// ActivateDestination implements core.Store.
func (s *Store[T]) ActivateDestination(
	ctx context.Context, plan core.PlanID, destination core.DestinationID,
) error {
	return s.withDestination(ctx, "activate destination", plan, destination,
		func(conn *txConn, destinationKey int64) error {
			if _, err := conn.exec(
				ctx, `UPDATE notifier_destination_status SET
				state = ?, failure = ?
			WHERE plan_destination_key = ?`,
				destinationStateActive, "", destinationKey,
			); err != nil {
				return fmt.Errorf("activate destination: %w", err)
			}

			return nil
		})
}

// PendingPlans implements core.Store.
func (s *Store[T]) PendingPlans(ctx context.Context) ([]core.PlanID, error) {
	var pending []core.PlanID

	err := s.writeTransaction(ctx, "pending plans", func(conn *txConn) error {
		var err error

		pending, err = queryAll(ctx, conn, "pending plans", `SELECT DISTINCT p.plan_id
		FROM notifier_plans p
		JOIN notifier_plan_destinations pd ON pd.plan_key = p.plan_key
		JOIN notifier_deliveries d ON d.plan_destination_key = pd.plan_destination_key
		WHERE d.state = ?
		ORDER BY p.plan_id`, []any{deliveryStatePending},
			func(rows *sql.Rows, id *core.PlanID) error {
				return rows.Scan((*string)(id))
			})

		return err
	})
	if err != nil {
		return nil, err
	}

	return pending, nil
}

func loadDestinationKey(
	ctx context.Context, conn *txConn, plan core.PlanID, destination core.DestinationID,
) (int64, error) {
	var destinationKey int64

	err := conn.queryRow(
		ctx, `SELECT pd.plan_destination_key
	FROM notifier_plans p
	JOIN notifier_plan_destinations pd ON pd.plan_key = p.plan_key
	JOIN notifier_destination_status ds
		ON ds.plan_destination_key = pd.plan_destination_key
	WHERE p.plan_id = ? AND pd.destination_id = ?`,
		string(plan), string(destination),
	).Scan(&destinationKey)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf(
			"%w: destination %q is not registered in plan %q",
			core.ErrStoreInvalidTransition, destination, plan,
		)
	}

	if err != nil {
		return 0, fmt.Errorf("load destination status: %w", err)
	}

	return destinationKey, nil
}
