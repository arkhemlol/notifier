package notifier

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/arkhemlol/notifier/internal/core"
)

const destinationProbeFailure = "destination probe failed permanently"

func (d *Dispatcher[T]) probe(ctx context.Context) error {
	if err := d.store.Admit(ctx, d.plan, nil); err != nil {
		return &core.Error{Op: core.OpEnqueue, Err: err}
	}

	if d.skipProbing {
		return nil
	}

	probers := make([]core.DestinationID, 0, len(d.destinations))

	for _, id := range d.plan.Destinations() {
		if _, ok := d.destinations[id].(core.Prober); ok {
			probers = append(probers, id)
		}
	}

	if len(probers) == 0 {
		return nil
	}

	results, err := d.runProbes(ctx, probers)
	if err != nil {
		return err
	}

	for index, result := range results {
		if err := d.recordProbe(ctx, probers[index], result); err != nil {
			return err
		}
	}

	return nil
}

type probeResult struct {
	healthy    bool
	quarantine bool
	failure    string
}

func (d *Dispatcher[T]) runProbes(
	ctx context.Context,
	probers []core.DestinationID,
) ([]probeResult, error) {
	results := make([]probeResult, len(probers))
	tokens := make(chan struct{}, d.probeWorkers)

	var group sync.WaitGroup

	for index, id := range probers {
		select {
		case tokens <- struct{}{}:
		case <-ctx.Done():
			group.Wait()

			return nil, ctx.Err()
		}

		group.Go(func() {
			defer func() { <-tokens }()

			probeCtx, cancel := context.WithTimeout(ctx, d.probeTimeout)
			defer cancel()

			prober, _ := d.destinations[id].(core.Prober)
			results[index] = classifyProbe(prober.Probe(probeCtx))
		})
	}

	group.Wait()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func classifyProbe(err error) probeResult {
	if err == nil {
		return probeResult{healthy: true}
	}

	if errors.Is(err, core.ErrQuarantine) {
		return probeResult{quarantine: true, failure: destinationProbeFailure}
	}

	return probeResult{}
}

func (d *Dispatcher[T]) recordProbe(
	ctx context.Context,
	destination core.DestinationID,
	outcome probeResult,
) error {
	if !outcome.healthy && !outcome.quarantine {
		return nil
	}

	var err error
	if outcome.quarantine {
		err = d.store.QuarantineDestination(ctx, d.plan.ID(), destination, outcome.failure)
	} else {
		err = d.store.ActivateDestination(ctx, d.plan.ID(), destination)
	}

	if err != nil {
		return &core.Error{
			Op:      core.OpProbe,
			Subject: string(destination),
			Err:     err,
		}
	}

	return nil
}

const (
	defaultProbeWorkers = 4
	defaultProbeTimeout = 10 * time.Second
)
