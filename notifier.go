/*
Package notifier provides stored, at-least-once notification delivery.

Create destinations and a dispatcher, enqueue items, then run a delivery cycle:

	transport, err := email.NewTransport[Alert](email.Config{
		Host: "smtp.example.com", Port: "465",
		Username: user, Password: pass, From: "alerts@example.com",
	})
	onCall, err := transport.Recipient("email:on-call", "on-call@example.com", render)
	dispatcher, err := notifier.NewDispatcher(
		memory.New[Alert](), notifier.DispatcherConfig{}, onCall)
	err = dispatcher.Enqueue(ctx, notifier.Item[Alert]{
		ID: 1, Payload: Alert{Text: "disk almost full"},
	})
	report, err := dispatcher.Run(ctx)

Run claims queued work, sends it, and records each outcome. Retryable failures
remain queued. Permanent destination failures quarantine that destination.

	for _, result := range report.Results {
		if errors.Is(result.SendErr, notifier.ErrQuarantine) {
			log.Printf("destination %s quarantined", result.Destination)
		}
	}

Multiple destinations must all succeed unless DispatcherConfig.FirstSuccess is
set, in which case their order defines the fallback chain. The Store holds
unfinished work; memory.New is process-local and sqlite.New survives restarts.

The first Run registers the plan and probes destinations that support checks.
If delivery succeeds but recording fails, Run reports
OutcomeDeliveredUnrecorded and the item may be delivered again.
*/
package notifier

import (
	"context"

	"github.com/arkhemlol/notifier/internal/core"
)

// Destination sends batches to one addressable endpoint.
// Use email.Transport.Recipient, telegram.Client.Chat, or another transport in this module.
type Destination[T any] interface {
	// ID returns the stable identifier used in delivery plans.
	ID() core.DestinationID

	// Send delivers one batch and returns only once the provider has accepted or rejected it.
	Send(ctx context.Context, batch []T) error
}

type (
	// Store persists queued items, leases, and outcomes.
	Store[T any] = core.Store[T]
	// Item pairs a storage-level identifier with a delivery payload.
	Item[T any] = core.Item[T]
	// Outcome describes a delivery resolution reported by Run.
	Outcome = core.Outcome
	// Prober is a Destination that can check reachability before delivery.
	// Dispatcher probes any bound destination implementing it, including a
	// wrapper embedding a plain Destination to add a custom Probe.
	Prober = core.Prober
)

const (
	// OutcomeUnknown is not a valid delivery resolution.
	OutcomeUnknown = core.OutcomeUnknown
	// OutcomeDelivered records successful provider acceptance.
	OutcomeDelivered = core.OutcomeDelivered
	// OutcomeRetryableFailure keeps the batch eligible for later delivery.
	OutcomeRetryableFailure = core.OutcomeRetryableFailure
	// OutcomeFailedPermanent terminalizes the affected failure scope.
	OutcomeFailedPermanent = core.OutcomeFailedPermanent
	// OutcomeDeliveredUnrecorded means delivery succeeded but recording failed; it may repeat.
	OutcomeDeliveredUnrecorded = core.OutcomeDeliveredUnrecorded
)
