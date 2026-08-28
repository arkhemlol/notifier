// Package telegram sends notification batches through the Telegram Bot API.
package telegram

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/arkhemlol/notifier"
	"github.com/arkhemlol/notifier/internal/core"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

const (
	serviceName           = "telegram"
	defaultWebhookPattern = "POST /telegram/webhook"
	botPollTimeout        = time.Minute
)

var (
	// ErrInvalidConfig indicates invalid client settings.
	ErrInvalidConfig = errors.New("invalid telegram configuration")
	// ErrInvalidChat indicates an invalid chat destination.
	ErrInvalidChat = errors.New("invalid telegram chat")
)

var (
	errTelegramRateLimited        = errors.New("telegram api rate limit exceeded")
	errTelegramRequestInterrupted = errors.New("telegram request did not complete")
	errTelegramNetworkFailure     = errors.New("telegram network request failed")
	errTelegramServerFailure      = errors.New("telegram server request failed")
	errTelegramAccessDenied       = errors.New("telegram bot cannot access the chat")
	errTelegramBadRequest         = errors.New("telegram rejected the message")
	errTelegramRequestFailed      = errors.New("telegram request failed")
)

// Config defines Telegram API and update-delivery settings.
type Config struct {
	// Token is the required BotFather token. Errors do not include it.
	Token string
	// APIBase overrides the Bot API endpoint.
	APIBase string

	// WebhookURL receives updates in webhook mode.
	WebhookURL string
	// WebhookPattern is the ServeMux pattern returned by Routes.
	WebhookPattern string
	// WebhookSecret authenticates webhook callbacks.
	WebhookSecret string

	// Polling selects long polling instead of webhook mode.
	Polling bool
	// HTTPClient overrides the default HTTP client used for Bot API requests (i.e. for proxy).
	// Callers must not mutate it while in use.
	HTTPClient *http.Client

	// ProxyCACertPath adds a trusted CA for the Bot API connection.
	ProxyCACertPath string
	// ClientCertPath and ClientKeyPath configure mTLS and must be set together.
	ClientCertPath string
	ClientKeyPath  string
}

// Renderer renders a notification batch as one Telegram message.
type Renderer[T any] func(batch []T) string

// Client is a shared Telegram bot client.
type Client[T any] struct {
	cfg          Config
	bot          *bot.Bot
	apiBot       *bot.Bot
	client       *http.Client
	polling      bool
	workerErrors chan error
	webhook      http.Handler
}

// NewClient validates cfg and creates a Client without network I/O.
func NewClient[T any](cfg Config) (*Client[T], error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	cfg = normalizeConfig(cfg)

	client, err := configuredHTTPClient(cfg)
	if err != nil {
		return nil, err
	}

	if cfg.HTTPClient != nil {
		client = cfg.HTTPClient
	}

	client = withoutRedirects(client)

	workerErrors := make(chan error, 1)

	telegramBot, err := newBot(cfg, client, workerErrors)
	if err != nil {
		return nil, fmt.Errorf("%w: construct service bot", ErrInvalidConfig)
	}

	apiBot, err := newBot(cfg, client, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: construct api bot", ErrInvalidConfig)
	}

	telegramClient := &Client[T]{
		cfg:          cfg,
		bot:          telegramBot,
		apiBot:       apiBot,
		client:       client,
		polling:      cfg.Polling,
		workerErrors: workerErrors,
	}
	telegramClient.webhook = telegramClient.newWebhookHandler()

	return telegramClient, nil
}

func validateConfig(cfg Config) error {
	switch {
	case cfg.Token == "":
		return fmt.Errorf("%w: token is empty", ErrInvalidConfig)
	case strings.TrimSpace(cfg.Token) != cfg.Token:
		return fmt.Errorf("%w: token has surrounding whitespace", ErrInvalidConfig)
	}

	if cfg.APIBase != "" && strings.TrimSpace(cfg.APIBase) != cfg.APIBase {
		return fmt.Errorf("%w: APIBase URL has surrounding whitespace", ErrInvalidConfig)
	}

	if cfg.WebhookURL != "" && strings.TrimSpace(cfg.WebhookURL) != cfg.WebhookURL {
		return fmt.Errorf("%w: WebhookURL has surrounding whitespace", ErrInvalidConfig)
	}

	if (cfg.ClientCertPath == "") != (cfg.ClientKeyPath == "") {
		return fmt.Errorf("%w: client certificate and key must be configured together", ErrInvalidConfig)
	}

	if cfg.Polling && (cfg.WebhookURL != "" || cfg.WebhookSecret != "" || cfg.WebhookPattern != "") {
		return fmt.Errorf("%w: polling and webhook options cannot be combined", ErrInvalidConfig)
	}

	return nil
}

func normalizeConfig(cfg Config) Config {
	cfg.APIBase = strings.TrimRight(cfg.APIBase, "/")
	if !cfg.Polling && cfg.WebhookPattern == "" {
		cfg.WebhookPattern = defaultWebhookPattern
	}

	return cfg
}

func configuredHTTPClient(cfg Config) (*http.Client, error) {
	hasCA, hasCert := cfg.ProxyCACertPath != "", cfg.ClientCertPath != ""
	if !hasCA && !hasCert {
		return http.DefaultClient, nil
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

	if hasCA {
		pool, err := loadCertPool(cfg.ProxyCACertPath)
		if err != nil {
			return nil, err
		}

		tlsConfig.RootCAs = pool
	}

	if hasCert {
		certificate, err := tls.LoadX509KeyPair(
			filepath.Clean(cfg.ClientCertPath),
			filepath.Clean(cfg.ClientKeyPath),
		)
		if err != nil {
			return nil, fmt.Errorf("%w: load client certificate pair", ErrInvalidConfig)
		}

		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("%w: default http transport is unsupported", ErrInvalidConfig)
	}

	transport := defaultTransport.Clone()
	transport.TLSClientConfig = tlsConfig

	return &http.Client{Transport: transport}, nil
}

func loadCertPool(path string) (*x509.CertPool, error) {
	certificatePEM, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("%w: read proxy ca certificate", ErrInvalidConfig)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certificatePEM) {
		return nil, fmt.Errorf("%w: proxy ca certificate contains no valid certificates", ErrInvalidConfig)
	}

	return pool, nil
}

func newBot(cfg Config, client *http.Client, workerErrors chan<- error) (*bot.Bot, error) {
	report := func(error) {}
	if workerErrors != nil {
		report = func(err error) {
			select {
			case workerErrors <- classifyTelegramError(err).cause:
			default:
			}
		}
	}

	botOptions := []bot.Option{
		bot.WithSkipGetMe(),
		bot.WithHTTPClient(botPollTimeout, classifiedHTTPClient{client: client}),
		bot.WithErrorsHandler(report),
	}

	if cfg.APIBase != "" {
		botOptions = append(botOptions, bot.WithServerURL(cfg.APIBase))
	}

	if cfg.WebhookSecret != "" {
		botOptions = append(botOptions, bot.WithWebhookSecretToken(cfg.WebhookSecret))
	}

	telegramBot, err := bot.New(cfg.Token, botOptions...)
	if err != nil {
		return nil, err
	}

	return telegramBot, nil
}

// Chat creates a destination for a Telegram chat.
func (c *Client[T]) Chat(id, chat string, renderer Renderer[T]) (notifier.Destination[T], error) {
	if renderer == nil {
		return nil, fmt.Errorf("%w: renderer is nil", ErrInvalidChat)
	}

	resolvedChat, err := parseChat(chat)
	if err != nil {
		return nil, err
	}

	return &destination[T]{
		id:       core.DestinationID(id),
		resolved: resolvedChat,
		renderer: renderer,
		bot:      c.apiBot,
	}, nil
}

// Route describes an HTTP endpoint required by a Client.
type Route struct {
	// Pattern is a net/http ServeMux pattern.
	Pattern string
	// Handler serves Pattern.
	Handler http.Handler
}

// Routes returns the webhook route, or an empty slice in polling mode, without network I/O.
func (c *Client[T]) Routes() []Route {
	if c.polling {
		return []Route{}
	}

	return []Route{{Pattern: c.cfg.WebhookPattern, Handler: c.webhook}}
}

// Run registers the webhook when configured and blocks until shutdown or worker failure.
func (c *Client[T]) Run(ctx context.Context) error {
	select {
	case <-c.workerErrors:
	default:
	}

	if err := c.registerWebhook(ctx); err != nil {
		return err
	}

	return c.runWorker(ctx)
}

func (c *Client[T]) registerWebhook(ctx context.Context) error {
	if c.polling || c.cfg.WebhookURL == "" {
		return nil
	}

	registered, err := c.apiBot.SetWebhook(ctx, &bot.SetWebhookParams{
		URL:         c.cfg.WebhookURL,
		SecretToken: c.cfg.WebhookSecret,
	})
	if err != nil {
		return serviceError(ctx, core.OpServiceSetup, "register webhook", classifyTelegramError(err).cause)
	}

	if !registered {
		return serviceError(ctx, core.OpServiceSetup, "register webhook", errors.New("telegram rejected setup"))
	}

	return nil
}

func serviceError(ctx context.Context, op core.Op, stage string, err error) error {
	if ctx.Err() != nil {
		return nil //nolint:nilerr // Run-context cancellation is normal shutdown.
	}

	return &core.Error{
		Op:      op,
		Subject: serviceName,
		Err:     fmt.Errorf("%s: %w", stage, err),
	}
}

func (c *Client[T]) runWorker(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)

		if c.polling {
			c.bot.Start(runCtx)
			return
		}

		c.bot.StartWebhook(runCtx)
	}()

	var failure error

	select {
	case <-ctx.Done():
	case failure = <-c.workerErrors:
	case <-done:
		failure = errors.New("worker stopped unexpectedly")
	}

	cancel()
	<-done

	if failure == nil {
		return nil
	}

	return serviceError(ctx, core.OpServiceWorker, "receive updates", failure)
}

func (c *Client[T]) newWebhookHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		secret := request.Header.Get("X-Telegram-Bot-Api-Secret-Token")
		if !validWebhookSecret(c.cfg.WebhookSecret, secret) {
			w.WriteHeader(http.StatusForbidden)
			return
		}

		update := &models.Update{}
		if err := json.NewDecoder(request.Body).Decode(update); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		c.bot.ProcessUpdate(request.Context(), update)
		w.WriteHeader(http.StatusOK)
	})
}

func validWebhookSecret(expected, actual string) bool {
	return expected == "" || subtle.ConstantTimeCompare([]byte(expected), []byte(actual)) == 1
}

// destination is a Telegram destination backed by its Client.
type destination[T any] struct {
	id       core.DestinationID
	resolved any
	renderer Renderer[T]
	bot      *bot.Bot
}

var (
	_ notifier.Destination[any] = (*destination[any])(nil)
	_ core.Prober               = (*destination[any])(nil)
)

// ID returns the destination ID.
func (d *destination[T]) ID() core.DestinationID {
	return d.id
}

// Send renders batch and sends one Telegram message.
func (d *destination[T]) Send(ctx context.Context, batch []T) error {
	_, err := d.bot.SendMessage(ctx, &bot.SendMessageParams{
		ChatID: d.resolved,
		Text:   d.renderer(batch),
	})
	if err != nil {
		return classifyTelegramError(err).send()
	}

	return nil
}

// Probe checks bot access to the chat with getChat.
func (d *destination[T]) Probe(ctx context.Context) error {
	_, err := d.bot.GetChat(ctx, &bot.GetChatParams{ChatID: d.resolved})
	if err != nil {
		return classifyTelegramError(err).probe()
	}

	return nil
}

func parseChat(value string) (any, error) {
	if value == "" || strings.TrimSpace(value) != value {
		return nil, fmt.Errorf("%w: value is empty or padded with spaces", ErrInvalidChat)
	}

	if value[0] != '+' {
		if chatID, err := strconv.ParseInt(value, 10, 64); err == nil {
			return chatID, nil
		}
	}

	return value, nil
}

type telegramFailure struct {
	cause      error
	retryAfter time.Duration

	access bool

	rejected bool
}

func isType[E error](err error) bool {
	_, ok := errors.AsType[E](err)

	return ok
}

func classifyTelegramError(err error) telegramFailure {
	if rateLimit, ok := errors.AsType[*bot.TooManyRequestsError](err); ok {
		return telegramFailure{
			cause:      errTelegramRateLimited,
			retryAfter: rateLimitDelay(rateLimit.RetryAfter),
		}
	}

	switch {
	case isType[*bot.MigrateError](err),
		errors.Is(err, bot.ErrorForbidden),
		errors.Is(err, bot.ErrorUnauthorized),
		errors.Is(err, bot.ErrorNotFound):
		return telegramFailure{cause: errTelegramAccessDenied, access: true}
	case isType[*telegramServerError](err):
		return telegramFailure{cause: errTelegramServerFailure}
	case errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		return telegramFailure{cause: errTelegramRequestInterrupted}
	case errors.Is(err, bot.ErrorBadRequest):
		return telegramFailure{cause: errTelegramBadRequest, rejected: true}
	case isType[net.Error](err):
		return telegramFailure{cause: errTelegramNetworkFailure}
	default:
		return telegramFailure{cause: errTelegramRequestFailed}
	}
}

func rateLimitDelay(seconds int) time.Duration {
	const maxSeconds = (1<<63 - 1) / int64(time.Second)

	if int64(seconds) <= 0 || int64(seconds) > maxSeconds {
		return 0
	}

	return time.Duration(seconds) * time.Second
}

func (f telegramFailure) send() error {
	switch {
	case f.access:
		return core.Quarantine(f.cause)
	case f.rejected:
		return core.Permanent(f.cause)
	default:
		return core.RetryableAfter(f.cause, f.retryAfter)
	}
}

func (f telegramFailure) probe() error {
	if f.access || f.rejected {
		return core.Quarantine(f.cause)
	}

	return core.RetryableAfter(f.cause, f.retryAfter)
}

// withoutRedirects prevents forwarding bot tokens and message bodies to another host.
func withoutRedirects(client *http.Client) *http.Client {
	cloned := *client
	cloned.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	return &cloned
}

type classifiedHTTPClient struct {
	client *http.Client
}

func (c classifiedHTTPClient) Do(request *http.Request) (*http.Response, error) {
	// #nosec G704 -- The bot constructs the URL from the configured API base.
	response, err := c.client.Do(request)
	if err != nil {
		if ctxErr := request.Context().Err(); ctxErr != nil {
			return nil, ctxErr
		}

		networkError, ok := errors.AsType[net.Error](err)

		return nil, &telegramNetworkError{timeout: ok && networkError.Timeout()}
	}

	if response.StatusCode < http.StatusInternalServerError {
		return response, nil
	}

	return nil, &telegramServerError{
		statusCode: response.StatusCode,
		closeErr:   response.Body.Close(),
	}
}

type telegramNetworkError struct {
	timeout bool
}

func (e *telegramNetworkError) Error() string {
	return "telegram network request failed"
}

func (e *telegramNetworkError) Timeout() bool {
	return e.timeout
}

func (e *telegramNetworkError) Temporary() bool {
	return true
}

type telegramServerError struct {
	statusCode int
	closeErr   error
}

func (e *telegramServerError) Error() string {
	return fmt.Sprintf("telegram server returned status %d", e.statusCode)
}

func (e *telegramServerError) Unwrap() error {
	return e.closeErr
}
