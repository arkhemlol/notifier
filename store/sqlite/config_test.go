package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"modernc.org/sqlite"

	"github.com/arkhemlol/notifier/internal/core"
)

// https://www.sqlite.org/rescode.html#busy - a stable, public SQLite result code.
const sqliteBusyResultCode = 5

var _ core.Store[string] = (*Store[string])(nil)

type stringCodec struct{}

func (stringCodec) Encode(value string) ([]byte, error) { return []byte(value), nil }

func (stringCodec) Decode(data []byte) (string, error) { return string(data), nil }

func TestNew_ValidatesConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		build func(t *testing.T) error
	}{
		{
			name: "nil database",
			build: func(*testing.T) error {
				_, err := New[string](nil, stringCodec{})

				return err
			},
		},
		{
			name: "nil codec",
			build: func(t *testing.T) error {
				t.Helper()

				_, err := New[string](openRealDB(t), nil)

				return err
			},
		},
		{
			name: "nil classifier",
			build: func(t *testing.T) error {
				t.Helper()

				_, err := New[string](openRealDB(t), stringCodec{}, nil)

				return err
			},
		},
		{
			name: "multiple classifiers",
			build: func(t *testing.T) error {
				t.Helper()

				noop := func(error) FailureClass { return FailureClassUnknown }
				_, err := New[string](openRealDB(t), stringCodec{}, noop, noop)

				return err
			},
		},
		{
			name: "nil context",
			build: func(t *testing.T) error {
				t.Helper()

				var ctx context.Context

				_, err := NewContext[string](ctx, openRealDB(t), stringCodec{})

				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if err := test.build(t); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestNew_CreatesVersionedSchemaInImmediateTransaction(t *testing.T) {
	t.Parallel()

	db := openRealDB(t)
	if _, err := New[string](db, stringCodec{}); err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// +1 for notifier_store_schema, which isn't in schemaStatements.
	if got, want := tableCount(t, db), len(schemaStatements)+1; got != want {
		t.Fatalf("created %d tables, want %d", got, want)
	}

	if got, want := indexCount(t, db), len(indexStatements); got != want {
		t.Fatalf("created %d indexes, want %d", got, want)
	}

	var version int
	if err := db.QueryRowContext(
		t.Context(), "SELECT version FROM notifier_store_schema WHERE singleton = 1",
	).Scan(&version); err != nil {
		t.Fatalf("read recorded schema version: %v", err)
	}

	if version != schemaVersion {
		t.Fatalf("recorded schema version = %d, want %d", version, schemaVersion)
	}

	if err := db.PingContext(t.Context()); err != nil {
		t.Fatalf("database unusable after New(): %v", err)
	}
}

func TestNew_RollsBackTheWholeSchemaOnFailure(t *testing.T) {
	t.Parallel()

	db := openRealDB(t)

	// Pre-create one of the store's own tables so schema creation fails partway
	// through; New must leave no trace of whatever it managed to create before
	// the failure - that's what the enclosing BEGIN IMMEDIATE transaction buys.
	if _, err := db.ExecContext(
		t.Context(), "CREATE TABLE notifier_plan_destinations (dummy INTEGER)",
	); err != nil {
		t.Fatalf("seed conflicting table: %v", err)
	}

	if _, err := New[string](db, stringCodec{}); err == nil {
		t.Fatal("New() error = nil, want a failure from the conflicting table")
	}

	if got := tableCount(t, db); got != 1 {
		t.Fatalf("tables after failed New() = %d, want only the pre-seeded one (full rollback)", got)
	}
}

func TestNew_RejectsConnectionSettingsAndSchemaVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		open func(t *testing.T) *sql.DB
		want error
	}{
		{
			name: "foreign keys disabled",
			open: func(t *testing.T) *sql.DB {
				t.Helper()

				return openRealDBWithDSN(t, "_busy_timeout=5000")
			},
			want: ErrInvalidConfig,
		},
		{
			name: "busy timeout disabled",
			open: func(t *testing.T) *sql.DB {
				t.Helper()

				return openRealDBWithDSN(t, "_fk=1")
			},
			want: ErrInvalidConfig,
		},
		{
			name: "unsupported schema version",
			open: func(t *testing.T) *sql.DB {
				t.Helper()

				db := openRealDB(t)
				seedMismatchedSchemaVersion(t, db)

				return db
			},
			want: ErrSchema,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := New[string](test.open(t), stringCodec{})
			if !errors.Is(err, test.want) {
				t.Fatalf("New() error = %v, want %v", err, test.want)
			}
		})
	}
}

func seedMismatchedSchemaVersion(t *testing.T, db *sql.DB) {
	t.Helper()

	if _, err := db.ExecContext(t.Context(), `CREATE TABLE notifier_store_schema (
		singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
		version INTEGER NOT NULL
	)`); err != nil {
		t.Fatalf("seed schema metadata table: %v", err)
	}

	if _, err := db.ExecContext(
		t.Context(),
		"INSERT INTO notifier_store_schema(singleton, version) VALUES(1, ?)",
		schemaVersion+1,
	); err != nil {
		t.Fatalf("seed mismatched schema version: %v", err)
	}
}

func TestNewContext_PreservesCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := NewContext[string](ctx, openRealDB(t), stringCodec{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("NewContext error = %v, want context.Canceled", err)
	}
}

func TestWithFailureClassifier_MapsBusyAndPreservesCause(t *testing.T) {
	t.Parallel()

	path := t.TempDir() + "/notifier.db"

	// A short busy_timeout so the contention below resolves in milliseconds
	// instead of the production default.
	storeDB := openRealDBAt(t, path, "_fk=1&_busy_timeout=100")

	store, err := New[string](storeDB, stringCodec{}, func(err error) FailureClass {
		var sqliteErr *sqlite.Error
		if errors.As(err, &sqliteErr) && sqliteErr.Code() == sqliteBusyResultCode {
			return FailureClassBusy
		}

		return FailureClassUnknown
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// A second connection to the same file, holding the write lock New()
	// itself needed a moment ago but Admit() below must contend for.
	blockerDB := openRealDBAt(t, path, "_fk=1&_busy_timeout=5000")

	blockerConn, err := blockerDB.Conn(t.Context())
	if err != nil {
		t.Fatalf("pin blocker connection: %v", err)
	}

	if _, err := blockerConn.ExecContext(t.Context(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("blocker BEGIN IMMEDIATE: %v", err)
	}

	t.Cleanup(func() {
		_, _ = blockerConn.ExecContext(context.Background(), "ROLLBACK")
		_ = blockerConn.Close()
	})

	plan := planFor(t, "destination:one")

	err = store.Admit(t.Context(), plan, nil)
	if !errors.Is(err, core.ErrStoreBusy) {
		t.Fatalf("Admit() error = %v, want core.ErrStoreBusy", err)
	}

	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) || sqliteErr.Code() != sqliteBusyResultCode {
		t.Fatalf("Admit() error = %v, want the underlying SQLITE_BUSY cause preserved", err)
	}
}

func planFor(t *testing.T, destinations ...core.DestinationID) core.Plan {
	t.Helper()

	return core.NewPlan(core.PolicyAll, destinations)
}

func tableCount(t *testing.T, db *sql.DB) int {
	t.Helper()

	return sqliteMasterCount(t, db, "table")
}

func indexCount(t *testing.T, db *sql.DB) int {
	t.Helper()

	return sqliteMasterCount(t, db, "index")
}

func sqliteMasterCount(t *testing.T, db *sql.DB, kind string) int {
	t.Helper()

	var count int
	if err := db.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type = ? AND name NOT LIKE 'sqlite_%'",
		kind,
	).Scan(&count); err != nil {
		t.Fatalf("count sqlite_master %ss: %v", kind, err)
	}

	return count
}
