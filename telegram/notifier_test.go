package telegram

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arkhemlol/notifier"
	"github.com/arkhemlol/notifier/internal/core"
)

const (
	testToken          = "123456:TEST_SECRET_TOKEN"
	testAPIBase        = "http://api.telegram.org/"
	testWebhookURL     = "https://myserver:443/"
	testDestinationID  = "telegram:alerts"
	okMessageResponse  = `{"ok":true,"result":{"message_id":1,"date":1,"chat":{"id":123,"type":"private"}}}`
	okBoolResponse     = `{"ok":true,"result":true}`
	okGetMeResponse    = `{"ok":true,"result":{"id":1,"is_bot":true,"first_name":"test"}}`
	okGetChatResponse  = `{"ok":true,"result":{"id":123,"type":"private"}}`
	okUpdatesResponse  = `{"ok":true,"result":[]}`
	badRequestResponse = `{"ok":false,"error_code":400,"description":"Bad Request: message is invalid"}`
	forbiddenResponse  = `{"ok":false,"error_code":403,"description":"Forbidden: bot cannot access chat"}`
)

type payload struct {
	Text string
}

func renderPayloads(batch []payload) string {
	texts := make([]string, 0, len(batch))
	for _, item := range batch {
		texts = append(texts, item.Text)
	}

	return strings.Join(texts, "\n")
}

func TestNewClient_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "empty token",
			cfg:  Config{},
			want: "token is empty",
		},
		{
			name: "token with leading whitespace",
			cfg:  Config{Token: " " + testToken},
			want: "token has surrounding whitespace",
		},
		{
			name: "APIBase URL with leading whitespace",
			cfg:  Config{Token: testToken, APIBase: " " + testAPIBase},
			want: "APIBase URL has surrounding whitespace",
		},
		{
			name: "WebhookURL with trailing whitespace",
			cfg:  Config{Token: testToken, WebhookURL: testWebhookURL + " "},
			want: "WebhookURL has surrounding whitespace",
		},
		{
			name: "polling with webhook url",
			cfg: Config{
				Token:      testToken,
				Polling:    true,
				WebhookURL: "https://example.test/hook",
			},
			want: "polling and webhook options cannot be combined",
		},
		{
			name: "polling with webhook secret",
			cfg: Config{
				Token:         testToken,
				Polling:       true,
				WebhookSecret: "secret",
			},
			want: "polling and webhook options cannot be combined",
		},
		{
			name: "polling with webhook pattern",
			cfg: Config{
				Token:          testToken,
				Polling:        true,
				WebhookPattern: "POST /hook",
			},
			want: "polling and webhook options cannot be combined",
		},
		{
			name: "certificate without key",
			cfg: Config{
				Token:          testToken,
				ClientCertPath: "client.pem",
			},
			want: "client certificate and key must be configured together",
		},
		{
			name: "key without certificate",
			cfg: Config{
				Token:         testToken,
				ClientKeyPath: "client.key",
			},
			want: "client certificate and key must be configured together",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewClient[payload](tt.cfg)
			if err == nil {
				t.Fatal("NewClient succeeded, want an error")
			}

			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("NewClient error = %v, want ErrInvalidConfig", err)
			}

			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("NewClient error = %q, want it to contain %q", err, tt.want)
			}

			assertErrorChainExcludes(t, err, testToken)
		})
	}
}

func TestNewClient_PerformsNoNetworkIO(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)

		_, _ = w.Write([]byte(okGetMeResponse))
	}))
	defer srv.Close()

	_, err := NewClient[payload](
		Config{Token: testToken, APIBase: srv.URL, HTTPClient: srv.Client()},
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if got := requests.Load(); got != 0 {
		t.Errorf("constructor requests = %d, want 0", got)
	}
}

func TestNewClient_ValidatesTLSFiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		configure func(*testing.T, *Config)
		want      string
	}{
		{
			name: "missing ca file",
			configure: func(t *testing.T, cfg *Config) {
				t.Helper()
				cfg.ProxyCACertPath = filepath.Join(t.TempDir(), "missing.pem")
			},
			want: "read proxy ca certificate",
		},
		{
			name: "invalid ca pem",
			configure: func(t *testing.T, cfg *Config) {
				t.Helper()

				path := filepath.Join(t.TempDir(), "ca.pem")
				if err := os.WriteFile(path, []byte("not a certificate"), 0o600); err != nil {
					t.Fatalf("write invalid ca: %v", err)
				}

				cfg.ProxyCACertPath = path
			},
			want: "proxy ca certificate contains no valid certificates",
		},
		{
			name: "invalid certificate pair",
			configure: func(t *testing.T, cfg *Config) {
				t.Helper()
				dir := t.TempDir()
				cfg.ClientCertPath = filepath.Join(dir, "client.pem")

				cfg.ClientKeyPath = filepath.Join(dir, "client.key")
				if err := os.WriteFile(cfg.ClientCertPath, []byte("invalid"), 0o600); err != nil {
					t.Fatalf("write certificate: %v", err)
				}

				if err := os.WriteFile(cfg.ClientKeyPath, []byte("invalid"), 0o600); err != nil {
					t.Fatalf("write key: %v", err)
				}
			},
			want: "load client certificate pair",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := Config{Token: testToken}
			tt.configure(t, &cfg)

			_, err := NewClient[payload](cfg)
			if err == nil {
				t.Fatal("NewClient succeeded, want a tls error")
			}

			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("NewClient error = %v, want ErrInvalidConfig", err)
			}

			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("NewClient error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestNewClient_PreservesDefaultTransportSettings(t *testing.T) {
	t.Parallel()

	caPath, certPath, keyPath := writeCertificateFiles(t)
	client := mustClient(t, Config{
		Token:           testToken,
		ProxyCACertPath: caPath,
		ClientCertPath:  certPath,
		ClientKeyPath:   keyPath,
	})

	transport, ok := client.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", client.client.Transport)
	}

	if transport == http.DefaultTransport {
		t.Fatal("transport reuses http.DefaultTransport instead of cloning it")
	}

	if transport.TLSClientConfig == nil {
		t.Fatal("TLSClientConfig is nil")
	}

	if transport.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf(
			"minimum tls version = %#x, want TLS 1.2",
			transport.TLSClientConfig.MinVersion,
		)
	}

	if len(transport.TLSClientConfig.Certificates) != 1 {
		t.Errorf(
			"client certificates = %d, want 1",
			len(transport.TLSClientConfig.Certificates),
		)
	}

	if transport.TLSClientConfig.RootCAs == nil {
		t.Error("RootCAs is nil")
	}

	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatalf("default transport type = %T, want *http.Transport", http.DefaultTransport)
	}

	if transport.ForceAttemptHTTP2 != defaultTransport.ForceAttemptHTTP2 {
		t.Error("ForceAttemptHTTP2 was not preserved")
	}

	if transport.DisableCompression != defaultTransport.DisableCompression {
		t.Error("DisableCompression was not preserved")
	}
}

//nolint:funlen // Covers all chat syntax cases.
func TestClient_ChatValidation(t *testing.T) {
	t.Parallel()

	client := mustClient(t, Config{Token: testToken})
	tests := []struct {
		name     string
		id       string
		chat     string
		renderer Renderer[payload]
		valid    bool
	}{
		{
			name:     "numeric private chat",
			id:       "telegram:private",
			chat:     "123456",
			renderer: renderPayloads,
			valid:    true,
		},
		{
			name:     "numeric supergroup",
			id:       "telegram:group",
			chat:     "-1001234567890",
			renderer: renderPayloads,
			valid:    true,
		},
		{
			name:     "public username",
			id:       "telegram:public",
			chat:     "@Channel_123",
			renderer: renderPayloads,
			valid:    true,
		},
		{
			name: "nil renderer",
			id:   testDestinationID,
			chat: "123",
		},
		{
			name:     "empty chat",
			id:       testDestinationID,
			chat:     "",
			renderer: renderPayloads,
		},
		{
			name:     "padded chat",
			id:       testDestinationID,
			chat:     " 123 ",
			renderer: renderPayloads,
		},

		{
			name:     "positive sign is not an integer chat id",
			id:       testDestinationID,
			chat:     "+123",
			renderer: renderPayloads,
			valid:    true,
		},
		{
			name:     "integer overflow falls back to a string chat",
			id:       testDestinationID,
			chat:     "9223372036854775808",
			renderer: renderPayloads,
			valid:    true,
		},
		{
			name:     "short username",
			id:       testDestinationID,
			chat:     "@abcd",
			renderer: renderPayloads,
			valid:    true,
		},
		{
			name:     "username starts with digit",
			id:       testDestinationID,
			chat:     "@1channel",
			renderer: renderPayloads,
			valid:    true,
		},
		{
			name:     "username contains punctuation",
			id:       testDestinationID,
			chat:     "@channel-name",
			renderer: renderPayloads,
			valid:    true,
		},
		{
			name:     "username contains unicode",
			id:       testDestinationID,
			chat:     "@Ašaaa",
			renderer: renderPayloads,
			valid:    true,
		},
		{
			name:     "plain username",
			id:       testDestinationID,
			chat:     "channelname",
			renderer: renderPayloads,
			valid:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			chat, err := client.Chat(tt.id, tt.chat, tt.renderer)
			if tt.valid {
				if err != nil {
					t.Fatalf("Chat: %v", err)
				}

				if string(chat.ID()) != tt.id {
					t.Errorf("ID() = %q, want %q", chat.ID(), tt.id)
				}

				return
			}

			if err == nil {
				t.Fatal("Chat succeeded, want an error")
			}

			if !errors.Is(err, ErrInvalidChat) {
				t.Errorf("Chat error = %v, want ErrInvalidChat", err)
			}

			assertErrorChainExcludes(t, err, testToken)

			if tt.chat != "" {
				assertErrorChainExcludes(t, err, tt.chat)
			}
		})
	}
}

func TestChat_SendPerformsOneRequestWithCallerContext(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32

	ctx := context.WithValue(t.Context(), contextKey{}, "caller")
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests.Add(1)

		if request.Context() != ctx {
			t.Error("Send did not propagate the caller context directly")
		}

		if request.URL.Path != "/bot"+testToken+"/sendMessage" {
			t.Errorf("request path = %q, want sendMessage", request.URL.Path)
		}
		// #nosec G120 -- Test input only.
		if err := request.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("parse form: %v", err)
		}

		if got := request.FormValue("chat_id"); got != "-1001234567890" {
			t.Errorf("chat_id = %q, want -1001234567890", got)
		}

		if got := request.FormValue("text"); got != "first\nsecond" {
			t.Errorf("text = %q, want rendered batch", got)
		}

		return jsonResponse(okMessageResponse), nil
	})

	client := mustClient(t, Config{
		Token:      testToken,
		APIBase:    "https://api.example.test",
		HTTPClient: &http.Client{Transport: transport},
	})
	chat := mustChat(t, client, testDestinationID, "-1001234567890")

	err := chat.Send(ctx, []payload{{Text: "first"}, {Text: "second"}})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	if got := requests.Load(); got != 1 {
		t.Errorf("sendMessage requests = %d, want 1", got)
	}
}

func TestChat_SendClassifiesFailures(t *testing.T) {
	t.Parallel()

	const rawChat = "-1001234567890"

	tests := []struct {
		name string

		status         int
		response       string
		transport      http.RoundTripper
		cancel         bool
		want           error
		wantOther      error
		wantRetryAfter time.Duration
	}{
		{
			name:           "rate limit",
			status:         http.StatusTooManyRequests,
			response:       `{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":5}}`,
			want:           notifier.ErrRetryable,
			wantOther:      notifier.ErrPermanent,
			wantRetryAfter: 5 * time.Second,
		},
		{
			name:      "server failure",
			status:    http.StatusInternalServerError,
			response:  "server failed " + testToken + " " + rawChat,
			want:      notifier.ErrRetryable,
			wantOther: notifier.ErrPermanent,
		},
		{
			name:      "forbidden",
			status:    http.StatusForbidden,
			response:  forbiddenResponse,
			want:      notifier.ErrQuarantine,
			wantOther: notifier.ErrRetryable,
		},
		{
			name:      "unauthorized",
			status:    http.StatusUnauthorized,
			response:  `{"ok":false,"error_code":401,"description":"Unauthorized"}`,
			want:      notifier.ErrQuarantine,
			wantOther: notifier.ErrRetryable,
		},
		{
			name:      "not found",
			status:    http.StatusNotFound,
			response:  `{"ok":false,"error_code":404,"description":"Not Found"}`,
			want:      notifier.ErrQuarantine,
			wantOther: notifier.ErrRetryable,
		},
		{
			name:      "migration",
			status:    http.StatusBadRequest,
			response:  `{"ok":false,"error_code":400,"description":"migrated","parameters":{"migrate_to_chat_id":-10042}}`,
			want:      notifier.ErrQuarantine,
			wantOther: notifier.ErrRetryable,
		},
		{
			name:      "bad request",
			status:    http.StatusBadRequest,
			response:  badRequestResponse,
			want:      notifier.ErrPermanent,
			wantOther: notifier.ErrQuarantine,
		},
		{
			name: "network timeout",
			transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, timeoutError{message: testToken + " " + rawChat}
			}),
			want:      notifier.ErrRetryable,
			wantOther: notifier.ErrPermanent,
		},
		{
			name:   "caller cancellation",
			cancel: true,
			transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				return nil, request.Context().Err()
			}),
			want:      notifier.ErrRetryable,
			wantOther: notifier.ErrPermanent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var requests atomic.Int32

			cfg := Config{Token: testToken, APIBase: "https://api.example.test"}
			transport := tt.transport

			if transport == nil {
				base, httpClient := stubAPI(t, &requests, tt.status, tt.response)
				cfg.APIBase = base
				transport = httpClient.Transport
			}

			cfg.HTTPClient = &http.Client{Transport: transport}
			client := mustClient(t, cfg)
			chat := mustChat(t, client, testDestinationID, rawChat)

			ctx := t.Context()
			if tt.cancel {
				canceled, cancel := context.WithCancel(ctx)
				cancel()

				ctx = canceled
			}

			err := chat.Send(ctx, []payload{{Text: "message"}})
			assertDestinationError(t, err, tt.want, tt.wantOther, tt.wantRetryAfter)
			assertErrorChainExcludes(t, err, testToken, rawChat)

			if tt.transport == nil && requests.Load() != 1 {
				t.Errorf("sendMessage requests = %d, want 1", requests.Load())
			}
		})
	}
}

func TestChat_ProbeUsesReadOnlyMethods(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex

	methods := []string{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		method := filepath.Base(request.URL.Path)

		mu.Lock()

		methods = append(methods, method)
		mu.Unlock()

		switch method {
		case "getChat":
			if got := request.FormValue("chat_id"); got != "@Channel_123" {
				t.Errorf("getChat chat_id = %q, want @Channel_123", got)
			}

			_, _ = w.Write([]byte(okGetChatResponse))
		default:
			t.Errorf("unexpected Telegram method %q", method)
		}
	}))
	defer srv.Close()

	client := mustClient(t, Config{Token: testToken, APIBase: srv.URL, HTTPClient: srv.Client()})
	chat := mustChat(t, client, testDestinationID, "@Channel_123")

	if err := chat.Probe(t.Context()); err != nil {
		t.Fatalf("Chat.Probe: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if got, want := strings.Join(methods, ","), "getChat"; got != want {
		t.Errorf("probe methods = %q, want %q", got, want)
	}
}

func TestChat_ProbeClassifiesFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		status    int
		response  string
		want      error
		wantOther error
	}{
		{
			name:      "shared credentials unauthorized",
			status:    http.StatusUnauthorized,
			response:  `{"ok":false,"error_code":401,"description":"Unauthorized"}`,
			want:      notifier.ErrQuarantine,
			wantOther: notifier.ErrRetryable,
		},
		{
			name:      "invalid chat",
			status:    http.StatusBadRequest,
			response:  badRequestResponse,
			want:      notifier.ErrQuarantine,
			wantOther: notifier.ErrRetryable,
		},
		{
			name:      "server failure",
			status:    http.StatusBadGateway,
			response:  "upstream failed",
			want:      notifier.ErrRetryable,
			wantOther: notifier.ErrPermanent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var requests atomic.Int32

			base, httpClient := stubAPI(t, &requests, tt.status, tt.response)
			client := mustClient(t, Config{Token: testToken, APIBase: base, HTTPClient: httpClient})
			chat := mustChat(t, client, testDestinationID, "123456")

			err := chat.Probe(t.Context())

			assertDestinationError(t, err, tt.want, tt.wantOther, 0)
			assertErrorChainExcludes(t, err, testToken, "123456")
		})
	}
}

func TestClient_RoutesAreSideEffectFree(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)

		_, _ = w.Write([]byte(okBoolResponse))
	}))
	defer srv.Close()

	client := mustClient(t, Config{
		Token:          testToken,
		APIBase:        srv.URL,
		WebhookURL:     "https://example.test/hook",
		WebhookPattern: "POST /custom/hook",
		HTTPClient:     srv.Client(),
	})

	first := client.Routes()
	second := client.Routes()

	if got := requests.Load(); got != 0 {
		t.Errorf("Routes requests = %d, want 0", got)
	}

	if len(first) != 1 || first[0].Pattern != "POST /custom/hook" || first[0].Handler == nil {
		t.Fatalf("first routes = %#v, want configured webhook route", first)
	}

	first[0].Pattern = "POST /mutated"
	if len(second) != 1 || second[0].Pattern != "POST /custom/hook" {
		t.Errorf("second routes = %#v, want independent route slice", second)
	}

	polling := mustClient(t, Config{Token: testToken, Polling: true})

	routes := polling.Routes()
	if routes == nil {
		t.Fatal("polling Routes returned nil")
	}

	if len(routes) != 0 {
		t.Errorf("polling routes = %d, want 0", len(routes))
	}
}

func TestClient_RunRegistersWebhookAndJoinsOnCancellation(t *testing.T) {
	t.Parallel()

	registered := make(chan struct{})

	var (
		once      sync.Once
		gotURL    atomic.Value
		gotSecret atomic.Value
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if filepath.Base(request.URL.Path) != "setWebhook" {
			t.Errorf("unexpected Telegram method %q", filepath.Base(request.URL.Path))
			return
		}

		gotURL.Store(request.FormValue("url"))
		gotSecret.Store(request.FormValue("secret_token"))
		once.Do(func() { close(registered) })

		_, _ = w.Write([]byte(okBoolResponse))
	}))
	defer srv.Close()

	client := mustClient(t, Config{
		Token:         testToken,
		APIBase:       srv.URL,
		WebhookURL:    "https://example.test/hook",
		WebhookSecret: "secret",
		HTTPClient:    srv.Client(),
	})
	ctx, cancel := context.WithCancel(t.Context())

	result := make(chan error, 1)
	go func() { result <- client.Run(ctx) }()

	select {
	case <-registered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for webhook registration")
	}

	cancel()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Run after exact context cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not join webhook workers")
	}

	if got := gotURL.Load(); got != "https://example.test/hook" {
		t.Errorf("registered url = %v, want configured webhook url", got)
	}

	if got := gotSecret.Load(); got != "secret" {
		t.Errorf("registered secret = %v, want configured secret", got)
	}
}

func TestClient_RunPollingJoinsOnCancellation(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})

	var once sync.Once

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if filepath.Base(request.URL.Path) != "getUpdates" {
			t.Errorf("unexpected Telegram method %q", filepath.Base(request.URL.Path))
			return
		}

		once.Do(func() { close(started) })
		<-release

		_, _ = w.Write([]byte(okUpdatesResponse))
	}))
	defer srv.Close()

	client := mustClient(t, Config{
		Token:      testToken,
		APIBase:    srv.URL,
		HTTPClient: srv.Client(),
		Polling:    true,
	})
	ctx, cancel := context.WithCancel(t.Context())

	result := make(chan error, 1)
	go func() { result <- client.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for polling")
	}

	cancel()
	close(release)

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Run after exact context cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not join polling workers")
	}
}

func TestClient_RunReturnsSetupServiceError(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32

	base, httpClient := stubAPI(
		t,
		&requests,
		http.StatusUnauthorized,
		`{"ok":false,"error_code":401,"description":"`+testToken+`"}`,
	)
	client := mustClient(t, Config{
		Token:      testToken,
		APIBase:    base,
		WebhookURL: "https://example.test/hook",
		HTTPClient: httpClient,
	})
	err := client.Run(t.Context())
	serviceError := requireServiceError(t, err, core.OpServiceSetup)
	assertErrorChainExcludes(t, serviceError, testToken)
}

func TestClient_RunRetriesTransientWorkerErrorsAndShutsDownCleanly(t *testing.T) {
	t.Parallel()

	const responseSecret = "worker-sensitive-value"

	var requests atomic.Int32

	base, httpClient := stubAPI(
		t,
		&requests,
		http.StatusInternalServerError,
		responseSecret+" "+testToken,
	)
	client := mustClient(t, Config{
		Token:      testToken,
		APIBase:    base,
		HTTPClient: httpClient,
		Polling:    true,
	})

	ctx, cancel := context.WithCancel(t.Context())

	result := make(chan error, 1)
	go func() { result <- client.Run(ctx) }()

	// Waits for a retried getUpdates request after the first transient failure.
	deadline := time.After(time.Second)

	for requests.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("worker did not retry after a transient getUpdates failure")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()

	select {
	case err := <-result:
		if err != nil {
			t.Errorf("Run() = %v, want nil after graceful shutdown", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestClient_WebhookRouteAuthenticatesRequests(t *testing.T) {
	t.Parallel()

	client := mustClient(t, Config{
		Token:         testToken,
		WebhookSecret: "expected-secret",
	})
	route := client.Routes()[0]

	for _, test := range []struct {
		name   string
		secret string
		status int
	}{
		{name: "invalid secret", secret: "wrong-secret", status: http.StatusForbidden},
		{name: "valid secret", secret: "expected-secret", status: http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			response := httptest.NewRecorder()
			route.Handler.ServeHTTP(response, webhookRequest(t, test.secret))

			if response.Code != test.status {
				t.Errorf("status = %d, want %d", response.Code, test.status)
			}
		})
	}
}

func FuzzResolveChatID(f *testing.F) {
	f.Add("123456789")
	f.Add("-1001234567890")
	f.Add("@Channel_123")
	f.Add("")
	f.Add(testToken)

	f.Fuzz(func(t *testing.T, value string) {
		resolved, err := parseChat(value)
		if err != nil {
			if !errors.Is(err, ErrInvalidChat) {
				t.Errorf("parseChat error = %v, want ErrInvalidChat", err)
			}

			if len(value) >= 8 {
				assertErrorChainExcludes(t, err, value)
			}

			return
		}

		switch typed := resolved.(type) {
		case int64:
			parsed, parseErr := strconv.ParseInt(value, 10, 64)
			if parseErr != nil || parsed != typed || strings.HasPrefix(value, "+") {
				t.Errorf("parseChat(%q) = int64(%d), want strict integer", value, typed)
			}
		case string:
			if typed != value {
				t.Errorf("parseChat(%q) = %q, want the value unchanged", value, typed)
			}
		default:
			t.Errorf("parseChat(%q) returned unexpected type %T", value, resolved)
		}
	})
}

type contextKey struct{}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type timeoutError struct {
	message string
}

func (e timeoutError) Error() string { return e.message }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func stubAPI(t *testing.T, requests *atomic.Int32, status int, body string) (string, *http.Client) {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	return srv.URL, srv.Client()
}

func jsonResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func mustClient(t *testing.T, cfg Config) *Client[payload] {
	t.Helper()

	client, err := NewClient[payload](cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	return client
}

func mustChat(
	t *testing.T,
	client *Client[payload],
	id string,
	rawChat string,
) *destination[payload] {
	t.Helper()

	dest, err := client.Chat(id, rawChat, renderPayloads)
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}

	concrete, ok := dest.(*destination[payload])
	if !ok {
		t.Fatalf("Chat: returned %T, want *destination[payload]", dest)
	}

	return concrete
}

func assertDestinationError(
	t *testing.T,
	err error,
	want error,
	wantOther error,
	wantRetryAfter time.Duration,
) {
	t.Helper()

	if err == nil {
		t.Fatal("operation succeeded, want a destination error")
	}

	if !errors.Is(err, want) {
		t.Errorf("error = %v, want errors.Is(_, %v)", err, want)
	}

	if errors.Is(err, wantOther) {
		t.Errorf("error = %v, want no match for %v", err, wantOther)
	}

	failure, classified := core.FailureOf(err)
	if !classified {
		t.Fatalf("error = %v, want a classified destination failure", err)
	}

	if failure.RetryAfter != wantRetryAfter {
		t.Errorf("retry after = %s, want %s", failure.RetryAfter, wantRetryAfter)
	}
}

func requireServiceError(t *testing.T, err error, wantOp core.Op) *core.Error {
	t.Helper()

	if err == nil {
		t.Fatal("Run succeeded, want a service error")
	}

	serviceError, ok := errors.AsType[*core.Error](err)
	if !ok {
		t.Fatalf("Run error type = %T, want *core.Error", err)
	}

	if serviceError.Subject != "telegram" {
		t.Errorf("subject = %q, want telegram", serviceError.Subject)
	}

	if serviceError.Op != wantOp {
		t.Errorf("op = %q, want %q", serviceError.Op, wantOp)
	}

	return serviceError
}

func assertErrorChainExcludes(t *testing.T, err error, values ...string) {
	t.Helper()

	for current := err; current != nil; current = errors.Unwrap(current) {
		for _, value := range values {
			if value != "" && strings.Contains(current.Error(), value) {
				t.Errorf("error chain exposes sensitive value %q in %q", value, current)
			}
		}
	}
}

func webhookRequest(t *testing.T, secret string) *http.Request {
	t.Helper()

	request := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"/telegram/webhook",
		strings.NewReader(`{"update_id":1}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)

	return request
}

func writeCertificateFiles(t *testing.T) (string, string, string) {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate private key: %v", err)
	}

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "telegram-test"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	certificateDER, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		&privateKey.PublicKey,
		privateKey,
	)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	privateKeyDER, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		t.Fatalf("marshal private key: %v", err)
	}

	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	certPath := filepath.Join(dir, "client.pem")
	keyPath := filepath.Join(dir, "client.key")
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER})
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privateKeyDER})

	if err := os.WriteFile(caPath, certificatePEM, 0o600); err != nil {
		t.Fatalf("write ca certificate: %v", err)
	}

	if err := os.WriteFile(certPath, certificatePEM, 0o600); err != nil {
		t.Fatalf("write client certificate: %v", err)
	}

	if err := os.WriteFile(keyPath, privateKeyPEM, 0o600); err != nil {
		t.Fatalf("write client key: %v", err)
	}

	return caPath, certPath, keyPath
}
