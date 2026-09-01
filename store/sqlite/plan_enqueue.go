package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/arkhemlol/notifier/internal/core"
)

type storedDestination struct {
	key      int64
	id       core.DestinationID
	position int
	state    int
	failure  string
}

type storedPlan struct {
	key          int64
	policy       core.Policy
	destinations []storedDestination
}

type encodedItem struct {
	id      int64
	payload []byte
}

func registerPlan(ctx context.Context, conn *txConn, plan core.Plan) (storedPlan, error) {
	planKey, err := insertReturningKey(ctx, conn, "plan",
		"INSERT INTO notifier_plans(plan_id, policy) VALUES(?, ?)",
		string(plan.ID()), int(plan.Policy()))
	if err != nil {
		return storedPlan{}, err
	}

	destinationIDs := plan.Destinations()
	destinations := make([]storedDestination, len(destinationIDs))

	for position, destinationID := range destinationIDs {
		destinationKey, err := insertReturningKey(ctx, conn, "plan destination",
			`INSERT INTO notifier_plan_destinations(
				plan_key, destination_id, position
			) VALUES(?, ?, ?)`,
			planKey, string(destinationID), position)
		if err != nil {
			return storedPlan{}, err
		}

		if _, err := conn.exec(
			ctx, `INSERT INTO notifier_destination_status(
			plan_destination_key, state, failure
		) VALUES(?, ?, ?)`,
			destinationKey, destinationStateActive, "",
		); err != nil {
			return storedPlan{}, fmt.Errorf("insert destination status: %w", err)
		}

		destinations[position] = storedDestination{
			key:      destinationKey,
			id:       destinationID,
			position: position,
			state:    destinationStateActive,
		}
	}

	return storedPlan{key: planKey, policy: plan.Policy(), destinations: destinations}, nil
}

// Admit implements core.Store.
func (s *Store[T]) Admit(ctx context.Context, plan core.Plan, items []core.Item[T]) error {
	if plan.ID() == "" {
		return core.ErrInvalidPlan
	}

	encoded, err := s.encodeItems(ctx, items)
	if err != nil {
		return err
	}

	now := time.Now().UTC().UnixMicro()

	return s.writeTransaction(ctx, "enqueue items", func(conn *txConn) error {
		stored, err := loadStoredPlan(ctx, conn, plan.ID())
		if errors.Is(err, sql.ErrNoRows) {
			stored, err = registerPlan(ctx, conn, plan)
		}

		if err != nil {
			return fmt.Errorf("load registered plan: %w", err)
		}

		for _, item := range encoded {
			itemKey, inserted, err := ensureItem(ctx, conn, stored.key, item, now)
			if err != nil {
				return err
			}

			if !inserted {
				continue
			}

			if err := insertObligations(ctx, conn, stored.destinations, itemKey, now); err != nil {
				return err
			}
		}

		return nil
	})
}

func insertObligations(
	ctx context.Context, conn *txConn, destinations []storedDestination, itemKey int64,
	now int64,
) error {
	for _, destination := range destinations {
		state, failure, outcomeAt := deliveryStatePending, "", any(nil)
		if destination.state == destinationStateQuarantined {
			state, failure, outcomeAt = deliveryStateFailedPermanent, destination.failure, now
		}

		if _, err := conn.exec(
			ctx, `INSERT INTO notifier_deliveries(
			item_key, plan_destination_key, state, failure, outcome_at
		) VALUES(?, ?, ?, ?, ?)`,
			itemKey, destination.key, state, failure, outcomeAt,
		); err != nil {
			return fmt.Errorf("insert delivery obligation: %w", err)
		}
	}

	return nil
}

func (s *Store[T]) encodeItems(ctx context.Context, items []core.Item[T]) ([]encodedItem, error) {
	encoded := make([]encodedItem, 0, len(items))

	seen := make(map[int64]int, len(items))

	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		payload, err := s.codec.Encode(item.Payload)
		if err != nil {
			return nil, fmt.Errorf("encode item payload: %w: %w", ErrCodec, err)
		}

		payload = bytes.Clone(payload)

		if index, ok := seen[item.ID]; ok {
			if !bytes.Equal(encoded[index].payload, payload) {
				return nil, fmt.Errorf(
					"%w: duplicate item %d has different encoded payload",
					core.ErrStorePayloadConflict, item.ID,
				)
			}

			continue
		}

		seen[item.ID] = len(encoded)
		encoded = append(encoded, encodedItem{id: item.ID, payload: payload})
	}

	return encoded, nil
}

func ensureItem(
	ctx context.Context, conn *txConn, planKey int64, item encodedItem, enqueuedAt int64,
) (int64, bool, error) {
	var (
		key           int64
		storedPayload []byte
	)

	err := conn.queryRow(
		ctx, `SELECT item_key, payload FROM notifier_items
		WHERE plan_key = ? AND item_id = ?`,
		planKey, item.id,
	).Scan(&key, &storedPayload)

	switch {
	case err == nil && !bytes.Equal(storedPayload, item.payload):
		return 0, false, fmt.Errorf(
			"%w: item %d already has different encoded payload",
			core.ErrStorePayloadConflict, item.id,
		)
	case err == nil:
		return key, false, nil
	case !errors.Is(err, sql.ErrNoRows):
		return 0, false, fmt.Errorf("load existing item: %w", err)
	}

	key, err = insertReturningKey(ctx, conn, "item", `INSERT INTO notifier_items(
		plan_key, item_id, payload, enqueued_at
	) VALUES(?, ?, ?, ?)`,
		planKey, item.id, item.payload, enqueuedAt)
	if err != nil {
		return 0, false, err
	}

	return key, true, nil
}

func loadStoredPlan(ctx context.Context, conn *txConn, planID core.PlanID) (storedPlan, error) {
	var (
		plan   storedPlan
		policy int
	)

	if err := conn.queryRow(ctx, `SELECT plan_key, policy FROM notifier_plans
		WHERE plan_id = ?`, string(planID)).Scan(&plan.key, &policy); err != nil {
		return storedPlan{}, err
	}

	plan.policy = core.Policy(policy)

	destinations, err := queryAll(ctx, conn, "plan destinations", `SELECT
		pd.plan_destination_key,
		pd.destination_id,
		pd.position,
		ds.state,
		ds.failure
	FROM notifier_plan_destinations pd
	JOIN notifier_destination_status ds
		ON ds.plan_destination_key = pd.plan_destination_key
	WHERE pd.plan_key = ?
	ORDER BY pd.position`, []any{plan.key},
		func(rows *sql.Rows, destination *storedDestination) error {
			var id string
			if err := rows.Scan(
				&destination.key, &id, &destination.position,
				&destination.state, &destination.failure,
			); err != nil {
				return err
			}

			destination.id = core.DestinationID(id)

			return nil
		})
	if err != nil {
		return storedPlan{}, err
	}

	plan.destinations = destinations

	return plan, nil
}
