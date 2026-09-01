package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

const schemaVersion = 1

// State values are schema-pinned and require a migration to renumber.

const (
	deliveryStatePending = iota + 1
	deliveryStateDelivered
	deliveryStateFailedPermanent
	deliveryStateCanceled
)

const (
	batchStateReady = iota + 1
	batchStateLeased
	batchStateRetryable
	batchStateTerminal
)

const (
	destinationStateActive      = 1
	destinationStateQuarantined = 2
)

var schemaStatements = [...]string{
	`CREATE TABLE notifier_plans (
		plan_key INTEGER PRIMARY KEY,
		plan_id TEXT NOT NULL UNIQUE,
		policy INTEGER NOT NULL CHECK (policy IN (1, 2))
	)`,
	`CREATE TABLE notifier_plan_destinations (
		plan_destination_key INTEGER PRIMARY KEY,
		plan_key INTEGER NOT NULL REFERENCES notifier_plans(plan_key) ON DELETE CASCADE,
		destination_id TEXT NOT NULL,
		position INTEGER NOT NULL CHECK (position >= 0),
		UNIQUE (plan_key, destination_id),
		UNIQUE (plan_key, position)
	)`,
	`CREATE TABLE notifier_items (
		item_key INTEGER PRIMARY KEY,
		plan_key INTEGER NOT NULL REFERENCES notifier_plans(plan_key) ON DELETE RESTRICT,
		item_id INTEGER NOT NULL,
		payload BLOB NOT NULL,
		enqueued_at INTEGER NOT NULL,
		UNIQUE (plan_key, item_id)
	)`,
	`CREATE TABLE notifier_deliveries (
		delivery_key INTEGER PRIMARY KEY,
		item_key INTEGER NOT NULL REFERENCES notifier_items(item_key) ON DELETE RESTRICT,
		plan_destination_key INTEGER NOT NULL REFERENCES notifier_plan_destinations(plan_destination_key) ON DELETE RESTRICT,
		state INTEGER NOT NULL CHECK (state IN (1, 2, 3, 4)),
		failure TEXT NOT NULL DEFAULT '',
		outcome_at INTEGER,
		UNIQUE (item_key, plan_destination_key)
	)`,
	`CREATE TABLE notifier_work_batches (
		batch_key INTEGER PRIMARY KEY,
		plan_destination_key INTEGER NOT NULL REFERENCES notifier_plan_destinations(plan_destination_key) ON DELETE RESTRICT,
		work_id TEXT NOT NULL UNIQUE,
		state INTEGER NOT NULL CHECK (state IN (1, 2, 3, 4)),
		attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
		retry_at INTEGER,
		lease_token TEXT,
		lease_until INTEGER,
		resolution_outcome INTEGER NOT NULL DEFAULT 0 CHECK (resolution_outcome IN (0, 1, 2, 3)),
		resolution_scope INTEGER NOT NULL DEFAULT 0 CHECK (resolution_scope IN (0, 1, 2)),
		resolution_retry_after_ns INTEGER NOT NULL DEFAULT 0 CHECK (resolution_retry_after_ns >= 0),
		last_failure TEXT NOT NULL DEFAULT '',
		outcome_at INTEGER
	)`,
	`CREATE TABLE notifier_work_batch_items (
		batch_key INTEGER NOT NULL REFERENCES notifier_work_batches(batch_key) ON DELETE CASCADE,
		delivery_key INTEGER NOT NULL UNIQUE REFERENCES notifier_deliveries(delivery_key) ON DELETE RESTRICT,
		position INTEGER NOT NULL CHECK (position >= 0),
		PRIMARY KEY (batch_key, delivery_key),
		UNIQUE (batch_key, position)
	)`,
	`CREATE TABLE notifier_destination_status (
		plan_destination_key INTEGER PRIMARY KEY REFERENCES notifier_plan_destinations(plan_destination_key) ON DELETE CASCADE,
		state INTEGER NOT NULL CHECK (state IN (1, 2)),
		failure TEXT NOT NULL DEFAULT ''
	)`,
}

var indexStatements = [...]string{
	`CREATE INDEX idx_notifier_plan_destinations_ordered
		ON notifier_plan_destinations(plan_key, position)`,
	`CREATE INDEX idx_notifier_items_enqueued
		ON notifier_items(plan_key, enqueued_at, item_key)`,
	`CREATE INDEX idx_notifier_deliveries_pending_claim
		ON notifier_deliveries(plan_destination_key, state, item_key, delivery_key)`,
	`CREATE INDEX idx_notifier_deliveries_item_siblings
		ON notifier_deliveries(item_key, plan_destination_key, state)`,
	`CREATE INDEX idx_notifier_work_batches_retryable
		ON notifier_work_batches(plan_destination_key, state, retry_at, batch_key)`,
	`CREATE INDEX idx_notifier_work_batches_leased
		ON notifier_work_batches(plan_destination_key, state, lease_until, batch_key)`,
}

func (s *Store[T]) initialize(ctx context.Context) error {
	return s.writeTransaction(ctx, "initialize sqlite store", func(conn *txConn) error {
		if _, err := conn.exec(ctx, `CREATE TABLE IF NOT EXISTS notifier_store_schema (
			singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
			version INTEGER NOT NULL
		)`); err != nil {
			return fmt.Errorf("create schema metadata: %w", err)
		}

		var version int

		err := conn.queryRow(
			ctx,
			"SELECT version FROM notifier_store_schema WHERE singleton = ?",
			1,
		).Scan(&version)
		switch {
		case err == nil:
			if version != schemaVersion {
				return fmt.Errorf("%w: unsupported version", ErrSchema)
			}

			return nil
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("read schema version: %w", err)
		}

		for _, statement := range schemaStatements {
			if _, err := conn.exec(ctx, statement); err != nil {
				return fmt.Errorf("create store table: %w", err)
			}
		}

		for _, statement := range indexStatements {
			if _, err := conn.exec(ctx, statement); err != nil {
				return fmt.Errorf("create store index: %w", err)
			}
		}

		if _, err := conn.exec(
			ctx,
			"INSERT INTO notifier_store_schema(singleton, version) VALUES(?, ?)",
			1,
			schemaVersion,
		); err != nil {
			return fmt.Errorf("record schema version: %w", err)
		}

		return nil
	})
}
