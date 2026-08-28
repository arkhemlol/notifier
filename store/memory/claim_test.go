package memory

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/arkhemlol/notifier/internal/core"
)

func TestStore_ClaimCreatesOneStableBatchPerDestination(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := New[string]()
		plan := mustAll(
			t,
			"operations:v1",
			"destination:a",
			"destination:b",
		)
		register(t, store, plan)
		enqueue(
			t,
			store,
			plan,
			core.Item[string]{ID: 2, Payload: "second"},
			core.Item[string]{ID: 1, Payload: "first"},
		)

		const leaseDuration = 10 * time.Minute

		work := claim(t, store, plan, 2, 10, leaseDuration)
		if len(work) != 2 {
			t.Fatalf("Claim returned %d batches, want 2", len(work))
		}

		if work[0].Destination != "destination:a" ||
			work[1].Destination != "destination:b" {
			t.Fatalf(
				"Claim destinations = [%q %q], want [destination:a destination:b]",
				work[0].Destination,
				work[1].Destination,
			)
		}

		assertItemIDs(t, work[0].Items, 2, 1)
		assertItemIDs(t, work[1].Items, 2, 1)

		assertCountedIdentifier(t, string(work[0].ID), "work-")
		assertCountedIdentifier(t, string(work[1].ID), "work-")

		if work[0].ID == work[1].ID {
			t.Fatalf("both batches share work id %q", work[0].ID)
		}

		assertRandomIdentifier(t, string(work[0].Lease), "lease-")

		if work[0].Attempt != 1 {
			t.Fatalf("first attempt = %d, want 1", work[0].Attempt)
		}

		if want := time.Now().Add(leaseDuration); !work[0].LeaseUntil.Equal(want) {
			t.Fatalf("LeaseUntil = %v, want %v", work[0].LeaseUntil, want)
		}

		originalID := work[0].ID
		originalLease := work[0].Lease
		work[0].Items[0].ID = 999
		work[0].Items = append(
			work[0].Items,
			core.Item[string]{ID: 1000, Payload: "caller mutation"},
		)

		if live := claim(t, store, plan, 2, 10, leaseDuration); len(live) != 0 {
			t.Fatalf("Claim returned %d live batches, want 0", len(live))
		}

		time.Sleep(leaseDuration)

		reclaimed := claim(t, store, plan, 1, 1, leaseDuration)
		if len(reclaimed) != 1 {
			t.Fatalf("reclaim returned %d batches, want 1", len(reclaimed))
		}

		if reclaimed[0].ID != originalID {
			t.Errorf("reclaimed WorkID = %q, want %q", reclaimed[0].ID, originalID)
		}

		if reclaimed[0].Lease == originalLease {
			t.Error("reclaim did not rotate the lease token")
		}

		if reclaimed[0].Attempt != 2 {
			t.Errorf("reclaimed attempt = %d, want 2", reclaimed[0].Attempt)
		}

		assertItemIDs(t, reclaimed[0].Items, 2, 1)
	})
}

func TestStore_ClaimPreservesExistingBatchSize(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := New[int]()
		plan := mustAll(t, "operations:v1", "destination:a")
		register(t, store, plan)
		enqueue(
			t,
			store,
			plan,
			core.Item[int]{ID: 1, Payload: 10},
			core.Item[int]{ID: 2, Payload: 20},
			core.Item[int]{ID: 3, Payload: 30},
		)

		first := claim(t, store, plan, 1, 3, time.Minute)
		time.Sleep(time.Minute)
		reclaimed := claim(t, store, plan, 1, 1, time.Minute)

		if len(first) != 1 || len(reclaimed) != 1 {
			t.Fatalf("claim lengths = %d and %d, want 1 and 1", len(first), len(reclaimed))
		}

		if len(reclaimed[0].Items) != 3 {
			t.Fatalf("reclaimed item count = %d, want stable size 3", len(reclaimed[0].Items))
		}
	})
}

func TestStore_ClaimFirstSuccessAllowsDifferentItemsOnly(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustFirstSuccess(
		t,
		"operations:v1",
		"destination:a",
		"destination:b",
	)
	register(t, store, plan)
	enqueue(
		t,
		store,
		plan,
		core.Item[int]{ID: 1, Payload: 10},
		core.Item[int]{ID: 2, Payload: 20},
	)

	work := claim(t, store, plan, 2, 1, time.Minute)
	if len(work) != 2 {
		t.Fatalf("Claim returned %d batches, want 2", len(work))
	}

	for index, batch := range work {
		if batch.Destination != "destination:a" {
			t.Errorf("work[%d] destination = %q, want destination:a", index, batch.Destination)
		}

		if len(batch.Items) != 1 {
			t.Errorf("work[%d] item count = %d, want 1", index, len(batch.Items))
		}
	}
}

func TestStore_ConcurrentClaimLeasesWorkOnce(t *testing.T) {
	t.Parallel()

	store := New[int]()
	plan := mustAll(t, "operations:v1", "destination:a")
	register(t, store, plan)
	enqueue(t, store, plan, core.Item[int]{ID: 1, Payload: 10})

	const callers = 32

	start := make(chan struct{})
	results := make(chan []core.Work[int], callers)
	errorsByCaller := make(chan error, callers)

	var group sync.WaitGroup
	for range callers {
		group.Go(func() {
			<-start

			work, err := store.Claim(context.Background(), core.ClaimRequest{
				Plan:            plan.ID(),
				MaxWork:         1,
				MaxItemsPerWork: 10,
				LeaseDuration:   time.Minute,
			})
			results <- work

			errorsByCaller <- err
		})
	}

	close(start)
	group.Wait()
	close(results)
	close(errorsByCaller)

	for err := range errorsByCaller {
		if err != nil {
			t.Fatalf("concurrent Claim: %v", err)
		}
	}

	claimed := 0
	for work := range results {
		claimed += len(work)
	}

	if claimed != 1 {
		t.Fatalf("total claimed batches = %d, want 1", claimed)
	}
}

func assertItemIDs[T any](
	t *testing.T,
	items []core.Item[T],
	want ...int64,
) {
	t.Helper()

	if len(items) != len(want) {
		t.Fatalf("item count = %d, want %d", len(items), len(want))
	}

	for index, id := range want {
		if items[index].ID != id {
			t.Errorf("items[%d].ID = %d, want %d", index, items[index].ID, id)
		}
	}
}

// randomTextLength is the length of a crypto/rand.Text identifier suffix.
const randomTextLength = 26

func assertCountedIdentifier(t *testing.T, value, prefix string) {
	t.Helper()

	suffix, found := strings.CutPrefix(value, prefix)
	if !found {
		t.Fatalf("identifier %q does not start with %q", value, prefix)
	}

	if number, err := strconv.ParseUint(suffix, 10, 64); err != nil || number == 0 {
		t.Fatalf("identifier suffix %q is not a positive counter", suffix)
	}
}

func assertRandomIdentifier(t *testing.T, value, prefix string) {
	t.Helper()

	suffix, found := strings.CutPrefix(value, prefix)
	if !found {
		t.Fatalf("identifier %q does not start with %q", value, prefix)
	}

	if len(suffix) != randomTextLength {
		t.Fatalf("identifier suffix length = %d, want %d", len(suffix), randomTextLength)
	}

	for _, char := range suffix {
		isBase32 := (char >= 'A' && char <= 'Z') || (char >= '2' && char <= '7')
		if !isBase32 {
			t.Fatalf("identifier suffix %q is not base32", suffix)
		}
	}
}
