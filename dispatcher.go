package notifier

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/sethvargo/go-retry"

	"github.com/arkhemlol/notifier/internal/core"
)

// Defaults applied to any DispatcherConfig field left at its zero value.
const (
	defaultMaxItemsPerWork = 100
	defaultAttemptLimit    = 5
	defaultAttemptTimeout  = 30 * time.Second
	defaultInitialBackoff  = time.Second
	defaultJitterPercent   = 20
	defaultResolveTimeout  = 5 * time.Second
	defaultJobInterval     = time.Minute
)

const (
	maxAttemptLimit   = 20
	maxInitialBackoff = time.Hour
)

const (
	resolveRetries   = 2
	unrecordedBuffer = 32
)

const (
	permanentDeliveryFailure  = "destination delivery failed permanently"
	permanentDestinationError = "destination quarantined"
	unboundDestinationFailure = "destination is not bound in this dispatcher"
)

var errInsufficientLeaseBudget = errors.New("insufficient lease budget")

// result describes one claimed batch.
type result struct {
	// Work identifies the batch in the store.
	Work string
	// Destination identifies the target destination.
	Destination string
	// ItemIDs preserves the order passed to Destination.Send.
	ItemIDs []int64
	// Outcome is OutcomeUnknown if delivery was not attempted.
	Outcome Outcome
	// Attempts counts calls to Destination.Send.
	Attempts int
	// SendErr retains the provider error for errors.Is and errors.As.
	SendErr error
	// ResolveErr reports failure to persist the outcome.
	ResolveErr error
}

// Report contains results from one bounded dispatch wave.
type Report struct {
	Results []result
}

// DispatcherConfig controls dispatch concurrency, retries, and destination probes.
// Zero values select the defaults below; invalid numeric values return
// ErrInvalidDispatcherConfig.
type DispatcherConfig struct {
	// Workers limits batches claimed and sent concurrently. Defaults to GOMAXPROCS.
	Workers int

	// FirstSuccess stops at the first accepting destination. By default, all must succeed.
	FirstSuccess bool

	// MaxItemsPerWork limits one Destination.Send batch. Defaults to 100.
	MaxItemsPerWork int

	// AttemptLimit includes the initial send. Defaults to 5; the maximum is 20.
	AttemptLimit int

	// AttemptTimeout bounds each send. Timeouts are retryable. Defaults to 30s.
	AttemptTimeout time.Duration

	// InitialBackoff is doubled after each retry. Defaults to 1s; the maximum is 1h.
	InitialBackoff time.Duration

	// JitterPercent varies backoff by ±N percent. Defaults to 20; the maximum is 100.
	JitterPercent int

	// DisableJitter uses the nominal backoff without variation.
	DisableJitter bool

	// PersistFailureDetail stores provider error text for permanent failures.
	// It defaults off because provider errors may contain recipient data.
	PersistFailureDetail bool

	// ResolveTimeout bounds outcome persistence, including retries. Defaults to 5s.
	ResolveTimeout time.Duration

	// ProbeWorkers limits concurrent checks before the first delivery. Defaults to 4.
	ProbeWorkers int

	// ProbeTimeout bounds each check. A timeout leaves stored state unchanged. Defaults to 10s.
	ProbeTimeout time.Duration

	// SkipProbing disables the automatic probe before the first Run. Defaults to false;
	// skipping makes delivery to an unreachable destination less reliable.
	SkipProbing bool
}

func (c DispatcherConfig) withDefaults() DispatcherConfig {
	// <= rather than ==: a dispatcher with no workers claims nothing and the
	// queue silently stops.
	if c.Workers <= 0 {
		c.Workers = max(runtime.GOMAXPROCS(0), 1)
	}

	if c.MaxItemsPerWork == 0 {
		c.MaxItemsPerWork = defaultMaxItemsPerWork
	}

	if c.AttemptLimit == 0 {
		c.AttemptLimit = defaultAttemptLimit
	}

	if c.AttemptTimeout == 0 {
		c.AttemptTimeout = defaultAttemptTimeout
	}

	if c.InitialBackoff == 0 {
		c.InitialBackoff = defaultInitialBackoff
	}

	switch {
	case c.DisableJitter:
		c.JitterPercent = 0
	case c.JitterPercent == 0:
		c.JitterPercent = defaultJitterPercent
	}

	if c.ResolveTimeout == 0 {
		c.ResolveTimeout = defaultResolveTimeout
	}

	if c.ProbeWorkers == 0 {
		c.ProbeWorkers = defaultProbeWorkers
	}

	if c.ProbeTimeout == 0 {
		c.ProbeTimeout = defaultProbeTimeout
	}

	return c
}

// zero is the default value.
//
//nolint:gocyclo // flat list of independent field validations, no branching complexity
func (c DispatcherConfig) validate() error {
	switch {
	case c.Workers < 0:
		return fmt.Errorf("%w: workers must be greater than zero", ErrInvalidDispatcherConfig)
	case c.MaxItemsPerWork < 0:
		return fmt.Errorf("%w: max items per work must not be negative", ErrInvalidDispatcherConfig)
	case c.ResolveTimeout < 0:
		return fmt.Errorf("%w: resolve timeout must not be negative", ErrInvalidDispatcherConfig)
	case c.ProbeWorkers < 0:
		return fmt.Errorf("%w: probe workers must not be negative", ErrInvalidDispatcherConfig)
	case c.ProbeTimeout < 0:
		return fmt.Errorf("%w: probe timeout must not be negative", ErrInvalidDispatcherConfig)
	case c.AttemptLimit < 0 || c.AttemptLimit > maxAttemptLimit:
		return fmt.Errorf("%w: attempt limit must be between 1 and %d",
			ErrInvalidDispatcherConfig, maxAttemptLimit)
	case c.AttemptTimeout < 0:
		return fmt.Errorf("%w: attempt timeout must not be negative", ErrInvalidDispatcherConfig)
	case c.InitialBackoff < 0 || c.InitialBackoff > maxInitialBackoff:
		return fmt.Errorf("%w: initial backoff must not be negative or exceed %s",
			ErrInvalidDispatcherConfig, maxInitialBackoff)
	case c.JitterPercent < 0 || c.JitterPercent > 100:
		return fmt.Errorf("%w: jitter percent must be between 0 and 100", ErrInvalidDispatcherConfig)
	case c.DisableJitter && c.JitterPercent != 0:
		return fmt.Errorf("%w: jitter percent must be zero when jitter is disabled",
			ErrInvalidDispatcherConfig)
	}

	return nil
}

// Dispatcher delivers queued items for one destination plan.
// Run calls are serialized; sends are concurrent and outcomes are persisted serially.
// Durable delivery state lives in the Store.
type Dispatcher[T any] struct {
	store           Store[T]
	plan            core.Plan
	workers         int
	maxItemsPerWork int
	destinations    map[core.DestinationID]Destination[T]

	attemptLimit   int
	attemptTimeout time.Duration
	initialBackoff time.Duration
	jitterPercent  int

	retries uint64

	probeWorkers int
	probeTimeout time.Duration
	skipProbing  bool

	// probed needs no lock: only the goroutine holding runGate touches it.
	probed bool

	persistFailureDetail bool
	resolveTimeout       time.Duration

	// unrecorded drops its oldest entry when full
	unrecordedMu sync.Mutex
	unrecorded   []deferredResolution

	// runGate holds one token, so a second Run waits.
	runGate chan struct{}
}

// NewDispatcher validates and binds destinations in order.
// Their IDs and order define the delivery plan and FirstSuccess fallback order.
func NewDispatcher[T any](
	store Store[T], config DispatcherConfig, destinations ...Destination[T],
) (*Dispatcher[T], error) {
	if store == nil {
		return nil, fmt.Errorf("%w: store is nil", ErrInvalidDispatcherConfig)
	}

	if err := config.validate(); err != nil {
		return nil, err
	}

	config = config.withDefaults()

	bindings, ids, err := bindDestinations(destinations)
	if err != nil {
		return nil, err
	}

	policy := core.PolicyAll
	if config.FirstSuccess {
		policy = core.PolicyFirstSuccess
	}

	return &Dispatcher[T]{
		store:                store,
		plan:                 core.NewPlan(policy, ids),
		workers:              config.Workers,
		maxItemsPerWork:      config.MaxItemsPerWork,
		destinations:         bindings,
		attemptLimit:         config.AttemptLimit,
		attemptTimeout:       config.AttemptTimeout,
		initialBackoff:       config.InitialBackoff,
		jitterPercent:        config.JitterPercent,
		retries:              uint64(max(config.AttemptLimit-1, 0)),
		probeWorkers:         config.ProbeWorkers,
		probeTimeout:         config.ProbeTimeout,
		skipProbing:          config.SkipProbing,
		resolveTimeout:       config.ResolveTimeout,
		persistFailureDetail: config.PersistFailureDetail,
		runGate:              make(chan struct{}, 1),
	}, nil
}

func bindDestinations[T any](
	destinations []Destination[T],
) (map[core.DestinationID]Destination[T], []core.DestinationID, error) {
	if len(destinations) == 0 {
		return nil, nil, fmt.Errorf("%w: destination list is empty", ErrInvalidDestinationBinding)
	}

	bindings := make(map[core.DestinationID]Destination[T], len(destinations))
	ids := make([]core.DestinationID, 0, len(destinations))

	for index, destination := range destinations {
		if destination == nil {
			return nil, nil, fmt.Errorf("%w: destination %d is nil",
				ErrInvalidDestinationBinding, index)
		}

		id := destination.ID()
		if id == "" {
			return nil, nil, fmt.Errorf("%w: destination %d has an empty id",
				ErrInvalidDestinationBinding, index)
		}

		if _, exists := bindings[id]; exists {
			return nil, nil, fmt.Errorf("%w: duplicate destination %q",
				ErrInvalidDestinationBinding, id)
		}

		bindings[id] = destination
		ids = append(ids, id)
	}

	return bindings, ids, nil
}

// Enqueue persists items for later delivery; it does not send them.
// Reusing an Item.ID with the same payload is a no-op; a different payload
// returns ErrStorePayloadConflict.
func (d *Dispatcher[T]) Enqueue(ctx context.Context, items ...Item[T]) error {
	if len(items) == 0 {
		return nil
	}

	if err := d.store.Admit(ctx, d.plan, items); err != nil {
		return dispatcherError(core.OpEnqueue, "", err)
	}

	return nil
}

// Run performs one delivery cycle. An empty queue is not an error.
// Concurrent calls wait for the active cycle or return when their context is canceled.
// The first call registers the plan and probes supported destinations before delivery.
// Before claiming, it retries still-leased writes for successful deliveries.
// Once its plan is empty, Run may drain another pending plan; unbound work fails permanently.
func (d *Dispatcher[T]) Run(ctx context.Context) (Report, error) {
	report := Report{Results: []result{}}

	select {
	case d.runGate <- struct{}{}:
		defer func() { <-d.runGate }()
	case <-ctx.Done():
		return report, dispatcherError(core.OpRun, "", ctx.Err())
	}

	if !d.probed {
		if err := d.probe(ctx); err != nil {
			return report, err
		}

		d.probed = true
	}

	d.flushUnrecorded(ctx)

	work, err := d.store.Claim(ctx, d.claimRequest())
	if err != nil {
		return report, dispatcherError(core.OpClaim, "", err)
	}
	// Nothing pending for this plan is the moment to clear out what an earlier
	// destination set left behind.
	if len(work) == 0 {
		if work, err = d.claimStale(ctx); err != nil {
			return report, err
		}
	}

	switch {
	case len(work) == 0:
		return report, nil
	case len(work) > d.workers:
		cause := fmt.Errorf("store returned %d work batches for a limit of %d",
			len(work), d.workers)

		return report, dispatcherError(core.OpRun, "", cause)
	}

	return d.dispatchWave(ctx, work)
}

func (d *Dispatcher[T]) claimStale(ctx context.Context) ([]core.Work[T], error) {
	plans, err := d.store.PendingPlans(ctx)
	if err != nil {
		return nil, dispatcherError(core.OpDrain, "", err)
	}

	for _, stale := range plans {
		if stale == d.plan.ID() {
			continue
		}

		request := d.claimRequest()
		request.Plan = stale

		work, err := d.store.Claim(ctx, request)
		if err != nil {
			return nil, dispatcherError(core.OpDrain, string(stale), err)
		}

		if len(work) > 0 {
			return work, nil
		}
	}

	return nil, nil
}

func (d *Dispatcher[T]) claimRequest() core.ClaimRequest {
	return core.ClaimRequest{
		Plan:            d.plan.ID(),
		MaxWork:         d.workers,
		MaxItemsPerWork: d.maxItemsPerWork,
		LeaseDuration:   d.requiredLease(0, d.workers),
	}
}

func (d *Dispatcher[T]) dispatchWave(ctx context.Context, work []core.Work[T]) (Report, error) {
	completed := make(chan completedWork, len(work))

	var workers sync.WaitGroup
	for _, batch := range work {
		workers.Go(func() {
			completed <- d.executeWork(ctx, batch, len(work))
		})
	}

	go func() {
		workers.Wait()
		close(completed)
	}()

	report := Report{Results: make([]result, 0, len(work))}
	dispatchErrors := make([]error, 0, len(work)+1)

	for finished := range completed {
		outcome, err := d.recordOutcome(ctx, finished)
		if err != nil {
			dispatchErrors = append(dispatchErrors, err)
		}

		report.Results = append(report.Results, outcome)
	}

	if err := ctx.Err(); err != nil {
		dispatchErrors = append(dispatchErrors, dispatcherError(core.OpRun, "", err))
	}

	return report, errors.Join(dispatchErrors...)
}

type completedWork struct {
	result     result
	resolution core.Resolution
	leaseUntil time.Time
}

func (d *Dispatcher[T]) recordOutcome(ctx context.Context, finished completedWork) (result, error) {
	err := d.resolve(ctx, finished.resolution)
	if err == nil {
		return finished.result, nil
	}

	outcome := finished.result
	outcome.ResolveErr = err
	// A delivered batch whose success went unrecorded is the only case that can
	// reach a recipient twice, so a later Run retries the write.
	if outcome.Outcome == OutcomeDelivered {
		outcome.Outcome = OutcomeDeliveredUnrecorded

		d.deferResolution(finished.resolution, finished.leaseUntil)
	}

	return outcome, dispatcherError(core.OpResolve, outcome.Work, err)
}

type deferredResolution struct {
	resolution core.Resolution
	leaseUntil time.Time
}

func (d *Dispatcher[T]) deferResolution(resolution core.Resolution, leaseUntil time.Time) {
	d.unrecordedMu.Lock()
	defer d.unrecordedMu.Unlock()

	if len(d.unrecorded) == unrecordedBuffer {
		d.unrecorded = d.unrecorded[1:]
	}

	d.unrecorded = append(d.unrecorded,
		deferredResolution{resolution: resolution, leaseUntil: leaseUntil})
}

// flushUnrecorded retries still-leased outcome writes and drops expired ones.
func (d *Dispatcher[T]) flushUnrecorded(ctx context.Context) {
	d.unrecordedMu.Lock()
	pending := d.unrecorded
	d.unrecorded = nil
	d.unrecordedMu.Unlock()

	now := time.Now()

	for _, entry := range pending {
		if !entry.leaseUntil.After(now) {
			continue
		}

		if err := d.resolve(ctx, entry.resolution); err != nil {
			d.deferResolution(entry.resolution, entry.leaseUntil)
		}
	}
}

func (d *Dispatcher[T]) executeWork(
	ctx context.Context, work core.Work[T], waveSize int,
) completedWork {
	outcome := newResult(work)

	destination, exists := d.destinations[work.Destination]
	if !exists {
		outcome.Outcome = OutcomeFailedPermanent
		outcome.SendErr = fmt.Errorf("%w: destination %q",
			ErrDestinationImplementationMissing, work.Destination)

		return completedWork{
			result: outcome,
			resolution: core.Resolution{
				Work:    work.ID,
				Lease:   work.Lease,
				Outcome: OutcomeFailedPermanent,
				Scope:   core.FailureScopeDelivery,
				Failure: unboundDestinationFailure,
			},
			leaseUntil: work.LeaseUntil,
		}
	}

	failure := d.send(ctx, work, destination, waveSize)
	outcome.Outcome = failure.outcome
	outcome.Attempts = failure.attempts
	outcome.SendErr = failure.err

	return completedWork{
		result:     outcome,
		resolution: d.newResolution(work, failure),
		leaseUntil: work.LeaseUntil,
	}
}

func newResult[T any](work core.Work[T]) result {
	itemIDs := make([]int64, 0, len(work.Items))
	for _, item := range work.Items {
		itemIDs = append(itemIDs, item.ID)
	}

	return result{
		Work:        string(work.ID),
		Destination: string(work.Destination),
		ItemIDs:     itemIDs,
		Outcome:     OutcomeUnknown,
	}
}

type sendFailure struct {
	outcome    Outcome
	scope      core.FailureScope
	retryAfter time.Duration
	attempts   int
	err        error
}

func (d *Dispatcher[T]) send(
	ctx context.Context, work core.Work[T], destination Destination[T], waveSize int,
) sendFailure {
	batch := make([]T, len(work.Items))
	for i, item := range work.Items {
		batch[i] = item.Payload
	}

	var last sendFailure

	delays := d.sendBackoff()
	backoff := retry.BackoffFunc(func() (time.Duration, bool) {
		delay, stop := delays.Next()
		if stop {
			return 0, true
		}

		delay = max(delay, last.retryAfter)

		return delay, !d.withinLease(delay, work, last, waveSize)
	})

	err := retry.Do(ctx, backoff, func(ctx context.Context) error {
		if !d.withinLease(0, work, last, waveSize) {
			last = retryableFailure(last.attempts, errInsufficientLeaseBudget, last.retryAfter)
			return last.err
		}

		last.attempts++
		attemptCtx, cancel := context.WithTimeout(ctx, d.attemptTimeout)
		sendErr := destination.Send(attemptCtx, batch)
		attemptErr := attemptCtx.Err()

		cancel()

		classified := classifyError(sendErr, attemptErr)
		classified.attempts = last.attempts
		last = classified

		if last.outcome == OutcomeRetryableFailure {
			return retry.RetryableError(last.err)
		}

		return last.err
	})
	if last.outcome == OutcomeUnknown {
		return retryableFailure(0, err, 0)
	}

	if ctx.Err() != nil {
		last.err = errors.Join(last.err, err)
	}

	return last
}

func (d *Dispatcher[T]) withinLease(delay time.Duration, work core.Work[T], lastResult sendFailure, waveSize int) bool {
	budget := delay + d.requiredLease(lastResult.attempts, waveSize)

	return !work.LeaseUntil.IsZero() && time.Until(work.LeaseUntil) >= budget
}

func (d *Dispatcher[T]) sendBackoff() retry.Backoff {
	jitter := uint64(max(d.jitterPercent, 0))
	backoff := retry.WithJitterPercent(jitter, retry.NewExponential(d.initialBackoff))

	return retry.WithMaxRetries(d.retries, backoff)
}

// attemptErr tells a provider failure apart from a timeout that cut it short.
// Only a permanent, scoped failure ends the batch; ambiguity stays retryable.
func classifyError(err, attemptErr error) sendFailure {
	if err == nil {
		return sendFailure{outcome: OutcomeDelivered}
	}

	if attemptErr != nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return retryableFailure(0, errors.Join(err, attemptErr), 0)
	}

	failure, classified := core.FailureOf(err)
	if !classified {
		return retryableFailure(0, err, 0)
	}

	scoped := failure.Scope == core.FailureScopeDelivery ||
		failure.Scope == core.FailureScopeDestination
	if failure.Permanent && scoped {
		return sendFailure{outcome: OutcomeFailedPermanent, scope: failure.Scope, err: err}
	}

	return retryableFailure(0, err, failure.RetryAfter)
}

func retryableFailure(attempts int, err error, retryAfter time.Duration) sendFailure {
	return sendFailure{
		outcome:    OutcomeRetryableFailure,
		scope:      core.FailureScopeDelivery,
		retryAfter: retryAfter,
		attempts:   attempts,
		err:        err,
	}
}

func (d *Dispatcher[T]) newResolution(work core.Work[T], failure sendFailure) core.Resolution {
	resolution := core.Resolution{
		Work:       work.ID,
		Lease:      work.Lease,
		Outcome:    failure.outcome,
		Scope:      failure.scope,
		RetryAfter: failure.retryAfter,
	}

	switch failure.scope {
	case core.FailureScopeDestination:
		resolution.Failure = permanentDestinationError
	case core.FailureScopeDelivery:
		if failure.outcome == OutcomeFailedPermanent {
			resolution.Failure = permanentDeliveryFailure
		}
	case core.FailureScopeUnknown:
		// delivered batch has unknown scope, do nothing
	}

	if d.persistFailureDetail && resolution.Failure != "" && failure.err != nil {
		resolution.Failure = failure.err.Error()
	}

	return resolution
}

// resolve ignores run cancellation after a send, but applies ResolveTimeout.
func (d *Dispatcher[T]) resolve(runCtx context.Context, resolution core.Resolution) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(runCtx), d.resolveTimeout)
	defer cancel()

	backoff := retry.WithMaxRetries(resolveRetries, retry.NewExponential(d.initialBackoff))

	var last error

	err := retry.Do(ctx, backoff, func(ctx context.Context) error {
		last = d.store.Resolve(ctx, resolution)
		if errors.Is(last, core.ErrStoreBusy) || errors.Is(last, core.ErrStoreUnavailable) {
			return retry.RetryableError(last)
		}

		return last
	})

	if last != nil {
		// A context error says nothing about why the store refused, so the
		// store's own last word wins.
		return last
	}

	return err
}

func dispatcherError(operation core.Op, subject string, err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		operation = core.OpRun
		subject = ""
	}

	return &core.Error{Op: operation, Subject: subject, Err: err}
}

// requiredLease budgets remaining sends and backoffs plus serial outcome writes.
// Each batch reserves ResolveTimeout for itself and every batch ahead of it.
func (d *Dispatcher[T]) requiredLease(nextAttempt, waveSize int) time.Duration {
	remainingAttempts := d.attemptLimit - nextAttempt
	queuedAhead := max(waveSize-1, 0)

	return time.Duration(remainingAttempts)*d.attemptTimeout +
		d.maximumBackoffWait(nextAttempt) +
		time.Duration(queuedAhead+1)*d.resolveTimeout
}

// initialBackoff*(2^retries - 2^nextAttempt), scaled by jitter.
func (d *Dispatcher[T]) maximumBackoffWait(nextAttempt int) time.Duration {
	if nextAttempt < 0 || uint64(nextAttempt) >= d.retries {
		return 0
	}

	total := d.initialBackoff * time.Duration((1<<d.retries)-(1<<uint64(nextAttempt)))

	return total + total*time.Duration(d.jitterPercent)/100
}
