package sqlite

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/arkhemlol/notifier/internal/core"
)

// newRealStore opens a fresh, file-backed SQLite database in t.TempDir() and
// builds a Store on it. The database is closed when t finishes, so every
// test gets its own on-disk file that never outlives it.
func newRealStore[T any](t *testing.T, codec Codec[T]) *Store[T] {
	t.Helper()

	store, err := New[T](openRealDB(t), codec)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	return store
}

func openRealDB(t *testing.T) *sql.DB {
	t.Helper()

	return openRealDBWithDSN(t, "_fk=1&_busy_timeout=5000")
}

// openRealDBWithDSN opens a fresh temp-file database with dsnParams verbatim,
// letting a test omit or override a pragma (e.g. to disable foreign keys or
// the busy timeout) instead of the production defaults openRealDB applies.
func openRealDBWithDSN(t *testing.T, dsnParams string) *sql.DB {
	t.Helper()

	return openRealDBAt(t, filepath.Join(t.TempDir(), "notifier.db"), dsnParams)
}

// openRealDBAt opens path with dsnParams, so a test can point a second
// connection at a database another Store or raw handle already owns.
func openRealDBAt(t *testing.T, path, dsnParams string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", "file:"+path+"?"+dsnParams)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("db.Close() error = %v", err)
		}
	})

	return db
}

func admit[T any](t *testing.T, store *Store[T], plan core.Plan, items []core.Item[T]) {
	t.Helper()

	if err := store.Admit(t.Context(), plan, items); err != nil {
		t.Fatalf("Admit() error = %v", err)
	}
}

func claimRequestFor(plan core.Plan, maxWork, maxItemsPerWork int) core.ClaimRequest {
	return core.ClaimRequest{
		Plan:            plan.ID(),
		MaxWork:         maxWork,
		MaxItemsPerWork: maxItemsPerWork,
		LeaseDuration:   time.Minute,
	}
}

// claimOne claims exactly one batch for plan and fails the test otherwise.
func claimOne[T any](t *testing.T, store *Store[T], plan core.Plan) core.Work[T] {
	t.Helper()

	work, err := store.Claim(t.Context(), claimRequestFor(plan, 1, 10))
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}

	if len(work) != 1 {
		t.Fatalf("Claim() returned %d batches, want exactly 1", len(work))
	}

	return work[0]
}
