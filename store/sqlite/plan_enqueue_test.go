package sqlite

import (
	"errors"
	"testing"

	"github.com/arkhemlol/notifier/internal/core"
)

// TestAdmit_ParameterizesValuesSoAdversarialInputCannotReachSQL exercises
// values built to break a naive string-concatenated query. If plan_enqueue.go
// ever stopped binding them as parameters, this would either corrupt the
// round-tripped value or run the embedded statement against the schema.
func TestAdmit_ParameterizesValuesSoAdversarialInputCannotReachSQL(t *testing.T) {
	t.Parallel()

	db := openRealDB(t)

	store, err := New[string](db, stringCodec{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	const adversarialDestination = core.DestinationID(`destination:one'); DROP TABLE plans; --`)

	const adversarialPayload = `payload"); DROP TABLE items; --`

	plan := planFor(t, adversarialDestination)
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: adversarialPayload}})

	work := claimOne(t, store, plan)
	if work.Destination != adversarialDestination {
		t.Fatalf("claimed destination = %q, want %q round-tripped verbatim",
			work.Destination, adversarialDestination)
	}

	if len(work.Items) != 1 || work.Items[0].Payload != adversarialPayload {
		t.Fatalf("claimed items = %+v, want %q round-tripped verbatim", work.Items, adversarialPayload)
	}

	if got, want := tableCount(t, db), len(schemaStatements)+1; got != want {
		t.Fatalf("tables after admitting adversarial input = %d, want %d (schema intact)", got, want)
	}
}

func TestAdmit_ReusingAnItemIDIsANoopUnlessThePayloadDiffers(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")

	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "first"}})
	admit(t, store, plan, []core.Item[string]{{ID: 1, Payload: "first"}}) // identical re-admit: no-op

	work := claimOne(t, store, plan)
	if len(work.Items) != 1 || work.Items[0].Payload != "first" {
		t.Fatalf("claimed items = %+v, want the single original item", work.Items)
	}

	if err := store.Resolve(t.Context(), core.Resolution{
		Work: work.ID, Lease: work.Lease, Outcome: core.OutcomeDelivered,
	}); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	err := store.Admit(t.Context(), plan, []core.Item[string]{{ID: 1, Payload: "second"}})
	if !errors.Is(err, core.ErrStorePayloadConflict) {
		t.Fatalf("Admit() with a different payload for the same id error = %v, want ErrStorePayloadConflict", err)
	}
}

func TestAdmit_RejectsDuplicateItemIDsWithDifferentPayloadsWithinOneCall(t *testing.T) {
	t.Parallel()

	store := newRealStore(t, stringCodec{})
	plan := planFor(t, "destination:one")

	err := store.Admit(t.Context(), plan, []core.Item[string]{
		{ID: 1, Payload: "a"},
		{ID: 1, Payload: "b"},
	})
	if !errors.Is(err, core.ErrStorePayloadConflict) {
		t.Fatalf("Admit() error = %v, want ErrStorePayloadConflict", err)
	}
}
