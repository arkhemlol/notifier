package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/arkhemlol/notifier/internal/core"
)

const rollbackTimeout = 5 * time.Second

const preparedStatementsHint = 12

// txConn prepares SQL on its second use and pins statements to the connection
// running BEGIN IMMEDIATE; pool-level statements could execute outside it.
type txConn struct {
	*sql.Conn

	stmts []preparedStmt
}

type preparedStmt struct {
	query string
	stmt  *sql.Stmt
}

func (t *txConn) stmt(ctx context.Context, query string) (*sql.Stmt, error) {
	for i := range t.stmts {
		if t.stmts[i].query != query {
			continue
		}

		if t.stmts[i].stmt != nil {
			return t.stmts[i].stmt, nil
		}

		stmt, err := t.PrepareContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("prepare statement: %w", err)
		}

		t.stmts[i].stmt = stmt

		return stmt, nil
	}

	t.stmts = append(t.stmts, preparedStmt{query: query})

	return nil, nil
}

func (t *txConn) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	//nolint:sqlclosecheck // owned by the transaction's cache, closed in closeStatements
	stmt, err := t.stmt(ctx, query)
	if err != nil {
		return nil, err
	}

	if stmt == nil {
		return t.ExecContext(ctx, query, args...)
	}

	return stmt.ExecContext(ctx, args...)
}

func (t *txConn) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	//nolint:sqlclosecheck // owned by the transaction's cache, closed in closeStatements
	stmt, err := t.stmt(ctx, query)
	if err != nil {
		return nil, err
	}

	if stmt == nil {
		return t.QueryContext(ctx, query, args...)
	}

	return stmt.QueryContext(ctx, args...)
}

func (t *txConn) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	//nolint:sqlclosecheck // owned by the transaction's cache, closed in closeStatements
	if stmt, err := t.stmt(ctx, query); err == nil && stmt != nil {
		return stmt.QueryRowContext(ctx, args...)
	}

	return t.QueryRowContext(ctx, query, args...)
}

func (t *txConn) closeStatements() error {
	var err error

	for _, entry := range t.stmts {
		if entry.stmt != nil {
			err = errors.Join(err, entry.stmt.Close())
		}
	}

	t.stmts = t.stmts[:0]

	return err
}

func (s *Store[T]) writeTransaction(
	ctx context.Context,
	operation string,
	fn func(*txConn) error,
) (err error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return s.storeError(operation, fmt.Errorf("pin connection: %w", err))
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			err = errors.Join(err, s.storeError(
				operation,
				fmt.Errorf("release connection: %w", closeErr),
			))
		}
	}()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return s.storeError(operation, fmt.Errorf("begin immediate transaction: %w", err))
	}

	committed := false
	defer func() {
		if committed {
			return
		}

		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
		defer cancel()

		if _, rollbackErr := conn.ExecContext(rollbackCtx, "ROLLBACK"); rollbackErr != nil {
			rollbackErr = errors.Join(rollbackErr, discardConnection(conn))
			err = errors.Join(err, s.storeError(
				operation,
				fmt.Errorf("rollback transaction: %w", rollbackErr),
			))
		}
	}()

	tx := &txConn{Conn: conn, stmts: make([]preparedStmt, 0, preparedStatementsHint)}

	defer func() {
		err = errors.Join(err, tx.closeStatements())
	}()

	if err := fn(tx); err != nil {
		return s.storeError(operation, err)
	}

	if err := tx.closeStatements(); err != nil {
		return s.storeError(operation, fmt.Errorf("close prepared statements: %w", err))
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return s.storeError(operation, fmt.Errorf("commit transaction: %w", err))
	}

	committed = true

	return nil
}

func discardConnection(conn *sql.Conn) error {
	return conn.Raw(func(any) error {
		return driver.ErrBadConn
	})
}

func closeRows(rows *sql.Rows, err error, operation string) error {
	if closeErr := rows.Close(); closeErr != nil {
		return errors.Join(err, fmt.Errorf("%s: %w", operation, closeErr))
	}

	return err
}

func queryAll[T any](
	ctx context.Context, conn *txConn, subject, query string, args []any,
	scan func(*sql.Rows, *T) error,
) (values []T, err error) {
	rows, err := conn.query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select %s: %w", subject, err)
	}
	defer func() {
		err = closeRows(rows, err, "close "+subject+" rows")
	}()

	for rows.Next() {
		var value T
		if err := scan(rows, &value); err != nil {
			return nil, fmt.Errorf("scan %s: %w", subject, err)
		}

		values = append(values, value)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s: %w", subject, err)
	}

	return values, nil
}

func insertReturningKey(
	ctx context.Context, conn *txConn, subject, query string, args ...any,
) (int64, error) {
	result, err := conn.exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("insert %s: %w", subject, err)
	}

	key, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("read inserted %s key: %w", subject, err)
	}

	return key, nil
}

func (s *Store[T]) verifyConnectionSettings(ctx context.Context) (err error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("%w: pin connection: %w", ErrInvalidConfig, err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("release connection: %w", closeErr))
		}
	}()

	var foreignKeys int
	if err := conn.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		return fmt.Errorf("verify foreign key enforcement: %w", err)
	}

	if foreignKeys != 1 {
		return fmt.Errorf("%w: foreign key enforcement is disabled", ErrInvalidConfig)
	}

	var busyTimeout int64
	if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		return fmt.Errorf("verify busy timeout: %w", err)
	}

	if busyTimeout <= 0 {
		return fmt.Errorf("%w: busy timeout must be positive", ErrInvalidConfig)
	}

	return nil
}

func (s *Store[T]) storeError(operation string, err error) error {
	if err == nil {
		return nil
	}

	if isKnownError(err) {
		return fmt.Errorf("%s: %w", operation, err)
	}

	class := FailureClassUnknown
	if s.classifier != nil {
		class = s.classifier(err)
	}

	sentinel := core.ErrStoreUnavailable
	if class == FailureClassBusy {
		sentinel = core.ErrStoreBusy
	}

	return fmt.Errorf("%s: %w: %w", operation, sentinel, err)
}

func isKnownError(err error) bool {
	known := [...]error{
		context.Canceled,
		context.DeadlineExceeded,
		ErrInvalidConfig,
		ErrSchema,
		core.ErrInvalidPlan,
		core.ErrStoreStaleLeaseToken,
		core.ErrStorePayloadConflict,
		core.ErrStoreWorkDoesntExist,
		core.ErrStoreInvalidTransition,
		core.ErrStoreBusy,
		core.ErrStoreUnavailable,
		ErrCodec,
	}
	for _, target := range known {
		if errors.Is(err, target) {
			return true
		}
	}

	return false
}
