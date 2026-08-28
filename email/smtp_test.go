package email

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/textproto"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arkhemlol/notifier"
)

func TestDestination_SendAtomicTransactions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		recipients []string
	}{
		{name: "recipient", recipients: []string{"ops@example.test"}},
		{
			name:       "recipient group",
			recipients: []string{"ops@example.test", "audit@example.test", "oncall@example.test"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server := newSMTPTestServer(t, smtpTestBehavior{})
			transport := server.newTransport(t)
			renderer := func(batch []testPayload) Message {
				return Message{
					Subject: "status",
					Text:    batch[0].Body,
					HTML:    "<p>" + batch[0].Body + "</p>",
				}
			}

			var (
				destination notifier.Destination[testPayload]
				err         error
			)
			if len(test.recipients) == 1 {
				destination, err = transport.Recipient(
					"email:operations",
					test.recipients[0],
					renderer,
				)
			} else {
				destination, err = transport.RecipientGroup(
					"email:operations",
					test.recipients,
					renderer,
				)
			}

			if err != nil {
				t.Fatalf("construct destination: %v", err)
			}

			if err := destination.Send(
				context.Background(),
				[]testPayload{{Body: "accepted body"}},
			); err != nil {
				t.Fatalf("Send(): %v", err)
			}

			snapshot := server.snapshot()
			if got := countCommand(snapshot.commands, "MAIL "); got != 1 {
				t.Errorf("MAIL command count = %d, want 1", got)
			}

			if got := countCommand(snapshot.commands, "RCPT "); got != len(test.recipients) {
				t.Errorf("RCPT command count = %d, want %d", got, len(test.recipients))
			}

			if got := countCommand(snapshot.commands, "DATA"); got != 1 {
				t.Errorf("DATA command count = %d, want 1", got)
			}

			if len(snapshot.messages) != 1 {
				t.Fatalf("accepted message count = %d, want 1", len(snapshot.messages))
			}

			decoded, err := decodeBuiltMessage(snapshot.messages[0])
			if err != nil {
				t.Fatalf("decodeBuiltMessage(): %v", err)
			}

			if decoded.to != strings.Join(test.recipients, ", ") {
				t.Errorf("To = %q, want %q", decoded.to, strings.Join(test.recipients, ", "))
			}
		})
	}
}

func TestTransport_ProbeIsSafe(t *testing.T) {
	t.Parallel()

	server := newSMTPTestServer(t, smtpTestBehavior{})

	transport := server.newTransport(t)
	if err := transport.probe(context.Background()); err != nil {
		t.Fatalf("Probe(): %v", err)
	}

	snapshot := server.snapshot()
	for _, forbidden := range []string{"MAIL ", "RCPT ", "DATA"} {
		if got := countCommand(snapshot.commands, forbidden); got != 0 {
			t.Errorf("Probe issued %s command %d times", strings.TrimSpace(forbidden), got)
		}
	}

	if got := countCommand(snapshot.commands, "NOOP"); got != 1 {
		t.Errorf("NOOP command count = %d, want 1", got)
	}

	if len(snapshot.messages) != 0 {
		t.Errorf("Probe accepted %d messages, want 0", len(snapshot.messages))
	}
}

func TestSMTPErrorClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		behavior  smtpTestBehavior
		probe     bool
		want      error
		wantOther error
	}{
		{
			name:      "temporary greeting",
			behavior:  smtpTestBehavior{failCommand: "GREETING", failCode: 421},
			want:      notifier.ErrRetryable,
			wantOther: notifier.ErrPermanent,
		},
		{
			name:      "permanent greeting quarantines transport",
			behavior:  smtpTestBehavior{failCommand: "GREETING", failCode: 554},
			want:      notifier.ErrQuarantine,
			wantOther: notifier.ErrRetryable,
		},
		{
			name:      "temporary authentication",
			behavior:  smtpTestBehavior{failCommand: "AUTH", failCode: 454},
			want:      notifier.ErrRetryable,
			wantOther: notifier.ErrPermanent,
		},
		{
			name:      "bad credentials quarantine transport",
			behavior:  smtpTestBehavior{failCommand: "AUTH", failCode: 535},
			want:      notifier.ErrQuarantine,
			wantOther: notifier.ErrRetryable,
		},
		{
			name:      "sender rejected quarantines transport",
			behavior:  smtpTestBehavior{failCommand: "MAIL", failCode: 550},
			want:      notifier.ErrQuarantine,
			wantOther: notifier.ErrRetryable,
		},
		{
			name:      "recipient rejected permanently",
			behavior:  smtpTestBehavior{failCommand: "RCPT", failCode: 550},
			want:      notifier.ErrPermanent,
			wantOther: notifier.ErrQuarantine,
		},
		{
			name:      "temporary recipient rejection",
			behavior:  smtpTestBehavior{failCommand: "RCPT", failCode: 450},
			want:      notifier.ErrRetryable,
			wantOther: notifier.ErrPermanent,
		},
		{
			name:      "message rejected before data permanently",
			behavior:  smtpTestBehavior{failCommand: "DATA", failCode: 554},
			want:      notifier.ErrPermanent,
			wantOther: notifier.ErrQuarantine,
		},
		{
			name:      "message rejected after data permanently",
			behavior:  smtpTestBehavior{failCommand: "MESSAGE", failCode: 554},
			want:      notifier.ErrPermanent,
			wantOther: notifier.ErrQuarantine,
		},
		{
			name:      "temporary result after data is retryable",
			behavior:  smtpTestBehavior{failCommand: "MESSAGE", failCode: 451},
			want:      notifier.ErrRetryable,
			wantOther: notifier.ErrPermanent,
		},
		{
			name:      "probe noop rejection quarantines transport",
			behavior:  smtpTestBehavior{failCommand: "NOOP", failCode: 550},
			probe:     true,
			want:      notifier.ErrQuarantine,
			wantOther: notifier.ErrRetryable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server := newSMTPTestServer(t, test.behavior)
			transport := server.newTransport(t)

			var err error
			if test.probe {
				err = transport.probe(context.Background())
			} else {
				destination := newSMTPTestDestination(t, transport)
				err = destination.Send(context.Background(), []testPayload{{Body: "body"}})
			}

			if err == nil {
				t.Fatal("SMTP operation succeeded, want classified failure")
			}

			assertDestinationError(t, err, test.want, test.wantOther)

			for _, sensitive := range []string{
				server.config.Username,
				server.config.Password,
				server.config.From,
				"ops@example.test",
				server.behavior.responseText,
			} {
				if sensitive != "" && strings.Contains(err.Error(), sensitive) {
					t.Errorf("SMTP error leaks an address, credential, or server response")
				}
			}
		})
	}
}

func TestDestination_SendNetworkFailureIsRetryable(t *testing.T) {
	t.Parallel()

	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenConfig.Listen(): %v", err)
	}

	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("listener.Close(): %v", err)
	}

	host, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("net.SplitHostPort(): %v", err)
	}

	config := validConfig()
	config.Host = host
	config.Port = port
	config.TLSConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // The endpoint is deliberately closed before TLS.

	transport, err := NewTransport[testPayload](config)
	if err != nil {
		t.Fatalf("NewTransport(): %v", err)
	}

	destination := newSMTPTestDestination(t, transport)
	err = destination.Send(context.Background(), []testPayload{{Body: "body"}})
	assertDestinationError(
		t,
		err,
		notifier.ErrRetryable,
		notifier.ErrPermanent,
	)

	if strings.Contains(err.Error(), address) {
		t.Error("network error leaks SMTP endpoint")
	}
}

func TestDestination_SendTLSVerificationFailureQuarantines(t *testing.T) {
	t.Parallel()

	server := newSMTPTestServer(t, smtpTestBehavior{})
	config := server.config

	transport, err := NewTransport[testPayload](config)
	if err != nil {
		t.Fatalf("NewTransport(): %v", err)
	}

	destination := newSMTPTestDestination(t, transport)
	err = destination.Send(context.Background(), []testPayload{{Body: "body"}})
	assertDestinationError(
		t,
		err,
		notifier.ErrQuarantine,
		notifier.ErrRetryable,
	)
}

func TestDestination_SendContextCompletionUnblocksProtocol(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		newContext func() (context.Context, context.CancelFunc)
		wantCause  error
	}{
		{
			name: "cancellation",
			newContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantCause: context.Canceled,
		},
		{
			name: "deadline",
			newContext: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 200*time.Millisecond)
			},
			wantCause: context.DeadlineExceeded,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server := newSMTPTestServer(t, smtpTestBehavior{stallCommand: "RCPT"})
			transport := server.newTransport(t)
			destination := newSMTPTestDestination(t, transport)

			ctx, cancel := test.newContext()
			defer cancel()

			result := make(chan error, 1)
			go func() {
				result <- destination.Send(ctx, []testPayload{{Body: "body"}})
			}()

			select {
			case <-server.stalled:
			case err := <-result:
				t.Fatalf("Send() returned before SMTP server stalled: %v", err)
			case <-time.After(3 * time.Second):
				t.Fatal("Send() did not reach stalled SMTP command")
			}

			if errors.Is(test.wantCause, context.Canceled) {
				cancel()
			}

			select {
			case err := <-result:
				if !errors.Is(err, test.wantCause) {
					t.Errorf("Send() error = %v, want errors.Is(_, %v)", err, test.wantCause)
				}

				assertDestinationError(
					t,
					err,
					notifier.ErrRetryable,
					notifier.ErrPermanent,
				)
			case <-time.After(3 * time.Second):
				t.Fatal("Send() did not unblock after context completion")
			}
		})
	}
}

func TestTransport_ConcurrentDestinationSends(t *testing.T) {
	t.Parallel()

	server := newSMTPTestServer(t, smtpTestBehavior{})
	transport := server.newTransport(t)
	destination := newSMTPTestDestination(t, transport)

	const sends = 8

	errorsBySend := make(chan error, sends)

	var workers sync.WaitGroup
	for range sends {
		workers.Go(func() {
			errorsBySend <- destination.Send(
				context.Background(),
				[]testPayload{{Body: "body"}},
			)
		})
	}

	workers.Wait()
	close(errorsBySend)

	for err := range errorsBySend {
		if err != nil {
			t.Errorf("Send(): %v", err)
		}
	}

	if got := len(server.snapshot().messages); got != sends {
		t.Errorf("accepted message count = %d, want %d", got, sends)
	}
}

func newSMTPTestDestination(
	t *testing.T,
	transport *Transport[testPayload],
) notifier.Destination[testPayload] {
	t.Helper()

	destination, err := transport.Recipient(
		"email:operations",
		"ops@example.test",
		func([]testPayload) Message {
			return Message{Subject: "status", Text: "body", HTML: "<p>body</p>"}
		},
	)
	if err != nil {
		t.Fatalf("Recipient(): %v", err)
	}

	return destination
}

func assertDestinationError(t *testing.T, err, want, wantOther error) {
	t.Helper()

	if err == nil {
		t.Fatal("error = nil, want classified destination error")
	}

	if !errors.Is(err, want) {
		t.Errorf("error = %v, want errors.Is(_, %v)", err, want)
	}

	if errors.Is(err, wantOther) {
		t.Errorf("error = %v, want no match for %v", err, wantOther)
	}
}

func countCommand(commands []string, prefix string) int {
	count := 0

	for _, command := range commands {
		if strings.HasPrefix(command, prefix) {
			count++
		}
	}

	return count
}

type smtpTestBehavior struct {
	failCommand  string
	failCode     int
	stallCommand string
	responseText string
}

type smtpTestServer struct {
	listener net.Listener
	config   Config
	roots    *x509.CertPool
	behavior smtpTestBehavior
	stalled  chan struct{}
	done     chan struct{}

	mutex     sync.Mutex
	commands  []string
	messages  [][]byte
	stallOnce sync.Once
	closeOnce sync.Once
	workers   sync.WaitGroup
}

type smtpTestSnapshot struct {
	commands []string
	messages [][]byte
}

func newSMTPTestServer(t *testing.T, behavior smtpTestBehavior) *smtpTestServer {
	t.Helper()

	certificate := newSelfSignedCertificate(t)

	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("tls.Listen(): %v", err)
	}

	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		t.Fatalf("x509.ParseCertificate(): %v", err)
	}

	roots := x509.NewCertPool()
	roots.AddCert(leaf)

	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("net.SplitHostPort(): %v", err)
	}

	if behavior.responseText == "" {
		behavior.responseText = "provider response may echo sender@example.test top-secret ops@example.test"
	}

	server := &smtpTestServer{
		listener: listener,
		config: Config{
			Host:     host,
			Port:     port,
			Username: "smtp-user",
			Password: "top-secret",
			From:     "sender@example.test",
		},
		roots:    roots,
		behavior: behavior,
		stalled:  make(chan struct{}),
		done:     make(chan struct{}),
		commands: []string{},
		messages: [][]byte{},
	}

	server.workers.Go(server.serve)
	go func() {
		server.workers.Wait()
		close(server.done)
	}()

	t.Cleanup(func() { server.close(t) })

	return server
}

func (server *smtpTestServer) newTransport(t *testing.T) *Transport[testPayload] {
	t.Helper()

	config := server.config
	config.TLSConfig = &tls.Config{
		RootCAs:    server.roots,
		ServerName: server.config.Host,
		MinVersion: tls.VersionTLS12,
	}

	transport, err := NewTransport[testPayload](config)
	if err != nil {
		t.Fatalf("NewTransport(): %v", err)
	}

	return transport
}

func (server *smtpTestServer) serve() {
	for {
		connection, err := server.listener.Accept()
		if err != nil {
			return
		}

		server.workers.Go(func() {
			server.handle(connection)
		})
	}
}

func (server *smtpTestServer) handle(connection net.Conn) {
	defer func() { _ = connection.Close() }()

	reader := bufio.NewReader(connection)

	writer := bufio.NewWriter(connection)
	if !server.respond(writer, "GREETING", 220) {
		return
	}

	for {
		command, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		command = strings.TrimSuffix(strings.TrimSuffix(command, "\n"), "\r")
		server.recordCommand(command)

		commandName := strings.ToUpper(strings.SplitN(command, " ", 2)[0])
		if server.behavior.stallCommand == commandName {
			server.stallOnce.Do(func() { close(server.stalled) })

			_, _ = reader.ReadByte()

			return
		}

		if !server.handleCommand(reader, writer, commandName) {
			return
		}
	}
}

func (server *smtpTestServer) handleCommand(
	reader *bufio.Reader,
	writer *bufio.Writer,
	command string,
) bool {
	switch command {
	case "EHLO", "HELO":
		return writeSMTPCapabilities(writer)
	case "AUTH":
		server.respond(writer, "AUTH", 235)
	case "MAIL":
		server.respond(writer, "MAIL", 250)
	case "RCPT":
		server.respond(writer, "RCPT", 250)
	case "DATA":
		return server.handleData(reader, writer)
	case "NOOP":
		server.respond(writer, "NOOP", 250)
	case "QUIT":
		server.writeResponseCode(writer, 221, "bye")

		return false
	default:
		server.writeResponseCode(writer, 500, "unsupported")
	}

	return true
}

func writeSMTPCapabilities(writer *bufio.Writer) bool {
	if _, err := writer.WriteString("250-smtp.test\r\n250 AUTH PLAIN\r\n"); err != nil {
		return false
	}

	return writer.Flush() == nil
}

func (server *smtpTestServer) handleData(
	reader *bufio.Reader,
	writer *bufio.Writer,
) bool {
	if server.behavior.failCommand == "DATA" {
		server.writeResponse(writer, server.behavior.failCode)

		return true
	}

	server.writeResponseCode(writer, 354, "continue")

	message, err := textproto.NewReader(reader).ReadDotBytes()
	if err != nil {
		return false
	}

	server.recordMessage(message)

	if server.behavior.failCommand == "MESSAGE" {
		server.writeResponse(writer, server.behavior.failCode)

		return true
	}

	server.writeResponseCode(writer, 250, "accepted")

	return true
}

func (server *smtpTestServer) respond(
	writer *bufio.Writer,
	command string,
	successCode int,
) bool {
	if server.behavior.failCommand == command {
		server.writeResponse(writer, server.behavior.failCode)

		return false
	}

	server.writeResponseCode(writer, successCode, "ok")

	return true
}

func (server *smtpTestServer) writeResponse(writer *bufio.Writer, code int) {
	server.writeResponseCode(writer, code, server.behavior.responseText)
}

func (server *smtpTestServer) writeResponseCode(
	writer *bufio.Writer,
	code int,
	message string,
) {
	_, _ = fmt.Fprintf(writer, "%d %s\r\n", code, message)
	_ = writer.Flush()
}

func (server *smtpTestServer) recordCommand(command string) {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	server.commands = append(server.commands, command)
}

func (server *smtpTestServer) recordMessage(message []byte) {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	server.messages = append(server.messages, message)
}

func (server *smtpTestServer) snapshot() smtpTestSnapshot {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	commands := append([]string{}, server.commands...)

	messages := make([][]byte, len(server.messages))
	for i, message := range server.messages {
		messages[i] = append([]byte{}, message...)
	}

	return smtpTestSnapshot{commands: commands, messages: messages}
}

func (server *smtpTestServer) close(t *testing.T) {
	t.Helper()

	server.closeOnce.Do(func() {
		_ = server.listener.Close()
	})

	select {
	case <-server.done:
	case <-time.After(3 * time.Second):
		t.Fatal("SMTP test server goroutines did not stop")
	}
}

func newSelfSignedCertificate(t *testing.T) tls.Certificate {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey(): %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	certificateDER, err := x509.CreateCertificate(
		rand.Reader,
		&template,
		&template,
		&privateKey.PublicKey,
		privateKey,
	)
	if err != nil {
		t.Fatalf("x509.CreateCertificate(): %v", err)
	}

	return tls.Certificate{
		Certificate: [][]byte{certificateDER},
		PrivateKey:  privateKey,
	}
}
