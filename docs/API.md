# API description

Types prefixed `core.` live in the unexported `internal/core` package.

```go
// ---------------------------------------------------------------------------
// package notifier
// ---------------------------------------------------------------------------

// DispatcherConfig controls dispatch concurrency, retries, and destination
// probes. Every field defaults at its zero value; an invalid numeric value
// returns ErrInvalidDispatcherConfig.
type DispatcherConfig struct {
	Workers              int           // batches claimed and sent concurrently; default 16
	FirstSuccess         bool          // stop at the first accepting destination; default - all must succeed
	MaxItemsPerWork      int           // items per Destination.Send call; default 100
	AttemptLimit         int           // total retries per batch, including the first; default 5, max 20
	AttemptTimeout       time.Duration // budget for one Send call; default 30s
	InitialBackoff       time.Duration // delay before the first retry, doubled after each; default 1s, max 1h
	JitterPercent        int           // spreads backoff by ±N percent; default 20, max 100
	DisableJitter        bool          // use the nominal backoff with no random spread
	PersistFailureDetail bool          // store the provider's error text for permanent failures; default false
	ResolveTimeout       time.Duration // budget to persist one outcome, including retries; default 5s
	ProbeWorkers         int           // destination checks in flight before the first delivery; default 4
	ProbeTimeout         time.Duration // budget for one check; a timeout leaves stored state unchanged; default 10s
	SkipProbing          bool          // disables the automatic probe; default false, less reliable delivery when true
	MaxWavesPerRun       int           // waves sent before Run returns; default 0, meaning no cap
}

// Item pairs a storage-level identifier with a delivery payload. Item.ID
// deduplicates: re-enqueuing a stored ID with the same payload does nothing; a
// different payload with the same ID returns ErrStorePayloadConflict.
type Item[T any] = core.Item[T] // ID int64, Payload T

// Store persists plans, items, leases, outcomes, and destination states.
type Store[T any] interface {
	// Admit registers the plan, then creates one delivery
	// obligation per destination for each item.
	Admit(ctx context.Context, plan core.Plan, items []Item[T]) error

	// Claim hands out up to MaxWork batches and marks them as taken;
	// returning fewer, including none, is normal. Each carries a
	// LeaseUntil deadline and a fresh lease token.
	Claim(ctx context.Context, request core.ClaimRequest) ([]core.Work[T], error)

	// Resolve applies a delivery outcome to one leased batch, rejecting a
	// resolution whose token isn't the batch's current lease.
	Resolve(ctx context.Context, resolution core.Resolution) error

	// QuarantineDestination blocks a destination from new messages and
	// finalizes what it already holds.
	QuarantineDestination(ctx context.Context, plan core.PlanID, destination core.DestinationID, failure string) error

	// ActivateDestination lets a destination receive messages again.
	ActivateDestination(ctx context.Context, plan core.PlanID, destination core.DestinationID) error

	// PendingPlans returns every plan that still holds unfinished work,
	// including plans left by an earlier process or destination set.
	PendingPlans(ctx context.Context) ([]core.PlanID, error)
}

// Destination sends batches to one addressable endpoint. 
type Destination[T any] interface {
	ID() core.DestinationID
	Send(ctx context.Context, batch []T) error
}

// Prober is a Destination that can check reachability before delivery; a Dispatcher
// probes any destination implementing it before the first Run, unless SkipProbing is set.
type Prober = core.Prober

// Outcome is the delivery resolution reported for one batch.
type Outcome = core.Outcome

const (
	OutcomeUnknown             Outcome = iota // not a valid resolution
	OutcomeDelivered                          // accepted by the provider and recorded
	OutcomeRetryableFailure                   // still queued; a later Run retries it
	OutcomeFailedPermanent                    // given up on
	OutcomeDeliveredUnrecorded                // accepted by the provider, but the store lost the write; may repeat
)

// NewDispatcher validates config and binds destinations in the given order;
// their IDs and order define the delivery plan and the FirstSuccess fallback
// chain.
func NewDispatcher[T any](store Store[T], config DispatcherConfig, destinations ...Destination[T]) (*Dispatcher[T], error)

// Dispatcher delivers queued items for one destination plan. Run calls happen
// one at a time; sends within one Run are concurrent.
type Dispatcher[T any] struct{ /* unexported */ }

// Enqueue persists items for later delivery; it does not send them.
func (d *Dispatcher[T]) Enqueue(ctx context.Context, items ...Item[T]) error

// Run claims and sends waves of queued work until none is left (or
// MaxWavesPerRun is reached), resolving each outcome as it completes. An
// empty queue is not an error. The first call also registers the delivery
// plan and probes destinations that support Probe.
func (d *Dispatcher[T]) Run(ctx context.Context) (Report, error)

// Start runs Run on a schedule until ctx is cancelled, calling onCycle after
// every cycle, and returns a function that waits for the loop to stop.
// onCycle may be nil. interval defaults to one minute at zero.
func (d *Dispatcher[T]) Start(
	ctx context.Context, interval time.Duration, onCycle func(Report, error),
) (wait func(context.Context) error)

// Report holds the results of one Run.
type Report struct {
	// Results is unexported-element; range over it with := to read each one:
	//   Work        string        // batch identifier, for correlating log lines
	//   Destination string        // destination identifier
	//   ItemIDs     []int64       // item IDs in send order
	//   Outcome     Outcome
	//   Attempts    int           // Destination.Send calls made
	//   Duration    time.Duration // time spent sending, including retries and backoff
	//   SendErr     error         // provider error; matches ErrRetryable, ErrPermanent, ErrQuarantine
	//   ResolveErr  error         // set if persisting the outcome failed
	Results []struct{ /* see above */ }
}

var (
	ErrInvalidDispatcherConfig   = errors.New("...") // invalid DispatcherConfig field
	ErrInvalidDestinationBinding = errors.New("...") // empty, nil, or duplicate destination passed to NewDispatcher
)

// ErrDestinationImplementationMissing marks work claimed for a destination ID
// not set up in this dispatcher.
var ErrDestinationImplementationMissing = errors.New("...")

// Store errors, surfaced through Enqueue and Run.
var (
	ErrStoreBusy              = core.ErrStoreBusy              // another operation was using the store; the dispatcher retries
	ErrStoreUnavailable       = core.ErrStoreUnavailable       // transient infrastructure failure; the dispatcher retries
	ErrStoreStaleLeaseToken   = core.ErrStoreStaleLeaseToken   // an outcome was written after losing its lease
	ErrStorePayloadConflict   = core.ErrStorePayloadConflict   // an Item.ID was reused with a different payload
	ErrStoreWorkDoesntExist   = core.ErrStoreWorkDoesntExist   // an outcome was written for an unknown batch
	ErrStoreInvalidTransition = core.ErrStoreInvalidTransition // a write contradicted the stored state; not retried
)

// Delivery errors, matched against result.SendErr with errors.Is. They wrap
// the transport provider's own error, reachable with errors.As.
var (
	ErrRetryable  = core.ErrRetryable  // may succeed on a later attempt
	ErrPermanent  = core.ErrPermanent  // will not succeed on a later attempt
	ErrQuarantine = core.ErrQuarantine // the destination itself is unusable; also matches ErrPermanent
)

// ---------------------------------------------------------------------------
// package email
// ---------------------------------------------------------------------------

// Config defines SMTP settings. Every field but TLSServerName, TLSConfig, and
// MaxIdleConnections is required; From does not default to Username.
type Config struct {
	Host, Port, Username, Password, From string
	TLSServerName                        string      // optional SNI override
	TLSConfig                            *tls.Config // optional override; NewTransport clones it
	MaxIdleConnections                   int         // idle connections kept open for reuse; default 4
}

// Message contains rendered email content.
type Message struct {
	Subject, Text, HTML string
}

// Renderer renders a notification batch as email content.
type Renderer[T any] func(batch []T) Message

// NewTransport validates cfg and creates a Transport without connecting to anything.
func NewTransport[T any](cfg Config) (*Transport[T], error)

// Transport is an immutable SMTP client shared safely by concurrent destinations.
// It keeps a small pool of authenticated connections open between sends.
type Transport[T any] struct{ /* unexported */ }

// Recipient creates a destination for one SMTP recipient.
func (t *Transport[T]) Recipient(id, address string, renderer Renderer[T]) (notifier.Destination[T], error)

// RecipientGroup creates a destination that sends to all addresses in one SMTP transaction.
func (t *Transport[T]) RecipientGroup(id string, addresses []string, renderer Renderer[T]) (notifier.Destination[T], error)

// Close closes every idle pooled connection and stops future pooling. Connections
// already in use finish normally and are closed instead of pooled when returned.
// The Transport stays usable afterward; it just dials fresh for every send.
func (t *Transport[T]) Close() error

var (
	ErrInvalidConfig  = errors.New("...") // invalid Config or a nil renderer
	ErrInvalidAddress = errors.New("...") // malformed, duplicate, or includes a name like "Jane <jane@example.com>"
)

// ---------------------------------------------------------------------------
// package telegram
// ---------------------------------------------------------------------------

// Config defines Telegram API and update-delivery settings.
type Config struct {
	Token   string // required BotFather token; errors do not include it
	APIBase string // overrides the Bot API endpoint, e.g. for proxy

	WebhookURL     string // receives updates in webhook mode
	WebhookPattern string // route pattern returned by Routes; default "POST /telegram/webhook"
	WebhookSecret  string // authenticates webhook callbacks

	Polling    bool         // long polling instead of webhook mode
	HTTPClient *http.Client // overrides the default HTTP client (must not be mutated); e.g. for a custom timeout or transport

	ProxyCACertPath string // adds a trusted CA for the Bot API connection
	ClientCertPath  string // mTLS; set together with ClientKeyPath
	ClientKeyPath   string
}

// Renderer renders a notification batch as one Telegram message.
type Renderer[T any] func(batch []T) string

// SendOption customizes the bot.SendMessageParams used for a Chat's messages.
type SendOption func(*bot.SendMessageParams)

// WithParseMode sets the message parse mode, e.g. models.ParseModeMarkdown or models.ParseModeHTML.
func WithParseMode(mode models.ParseMode) SendOption

// NewClient validates cfg and creates a Client without connecting to anything.
func NewClient[T any](cfg Config) (*Client[T], error)

// Client is a shared Telegram bot client.
type Client[T any] struct{ /* unexported */ }

// Bot returns the underlying go-telegram/bot client so callers can register their own update handlers.
func (c *Client[T]) Bot() *bot.Bot

// Chat creates a destination for a Telegram chat. opts customize every SendMessage call,
// e.g. WithParseMode(models.ParseModeMarkdown) or setting fields directly. It probes with
// getChat before the first delivery, if SkipProbing wasn't set.
func (c *Client[T]) Chat(id, chat string, renderer Renderer[T], opts ...SendOption) (notifier.Destination[T], error)

// Routes returns the webhook route, or an empty slice in polling mode, without connecting to anything.
func (c *Client[T]) Routes() []Route

// Run registers the webhook when configured and blocks until shutdown or worker failure.
func (c *Client[T]) Run(ctx context.Context) error

// Route describes an HTTP endpoint required by a Client.
type Route struct {
	Pattern string       // a net/http ServeMux pattern
	Handler http.Handler // serves Pattern
}

var (
	ErrInvalidConfig = errors.New("...") // invalid client settings
	ErrInvalidChat   = errors.New("...") // invalid chat destination
)

// ---------------------------------------------------------------------------
// package store/sqlite
// ---------------------------------------------------------------------------

// Codec converts payloads to deterministic bytes without keeping a copy of what it's given or returns.
type Codec[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(data []byte) (T, error)
}

// FailureClass classifies driver-specific infrastructure failures.
type FailureClass uint8

const (
	FailureClassUnknown     FailureClass = iota // treated as unavailable
	FailureClassBusy                          // another writer was using the database
	FailureClassUnavailable                   // unavailable persistence infrastructure
)

// FailureClassifier maps driver errors.
type FailureClassifier func(error) FailureClass

// New constructs a Store and initializes its private schema. classifier
// tells a driver error apart from FailureClassBusy; without one, all driver
// errors are reported as FailureClassUnavailable.
func New[T any](db *sql.DB, codec Codec[T], classifier ...FailureClassifier) (*Store[T], error)

// NewContext is New, but lets the caller cancel schema initialization via ctx.
func NewContext[T any](
	ctx context.Context, db *sql.DB, codec Codec[T], classifier ...FailureClassifier,
) (*Store[T], error)

// Store persists notification state in SQLite.
type Store[T any] struct{ /* unexported */ }

var (
	ErrInvalidConfig = errors.New("...") // db or codec is nil, more than one classifier, or the database connection isn't set up to enforce foreign keys and wait when locked
	ErrCodec         = errors.New("...") // payload encoding or decoding failed
	ErrSchema        = errors.New("...") // stored schema version doesn't match this build
)

// ---------------------------------------------------------------------------
// package store/memory
// ---------------------------------------------------------------------------

// New returns an empty Store.
func New[T any]() *Store[T]

// Store keeps delivery state in memory; it doesn't survive process restarts.
type Store[T any] struct{ /* unexported */ }
```
