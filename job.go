package notifier

import (
	"context"
	"time"
)

// Start runs the dispatcher on a schedule until ctx is cancelled, calling onCycle on every cycle and returns
// the function that can be called to cancel the loop.
//
//	wait := dispatcher.Start(ctx, time.Minute, func(_ notifier.Report, err error) {
//		if err != nil {
//			slog.Error("delivery cycle failed", "error", err)
//		}
//	})
//	defer wait(shutdownCtx)
func (d *Dispatcher[T]) Start(
	ctx context.Context,
	interval time.Duration,
	onCycle func(Report, error),
) (wait func(context.Context) error) {
	if interval <= 0 {
		interval = defaultJobInterval
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		d.runLoop(ctx, interval, onCycle)
	}()

	return func(waitCtx context.Context) error {
		select {
		case <-done:
			return nil
		case <-waitCtx.Done():
			return waitCtx.Err()
		}
	}
}

func (d *Dispatcher[T]) runLoop(
	ctx context.Context,
	interval time.Duration,
	onCycle func(Report, error),
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		report, err := d.Run(ctx)
		if onCycle != nil {
			onCycle(report, err)
		}

		// Checked after the cycle, so a cancellation arriving mid-cycle ends the
		// loop instead of starting another one.
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
