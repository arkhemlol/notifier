package email

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/mail"
	"runtime"
	"strings"
	"testing"

	"github.com/arkhemlol/notifier/internal/core"
)

func TestTransport_Recipient(t *testing.T) {
	t.Parallel()

	transport := newTestTransport(t)
	renderer := func([]testPayload) Message { return Message{Subject: "status"} }

	destination, err := transport.Recipient("email:operations", "ops@example.test", renderer)
	if err != nil {
		t.Fatalf("Recipient(): %v", err)
	}

	if got := destination.ID(); got != core.DestinationID("email:operations") {
		t.Errorf("ID() = %q, want email:operations", got)
	}
}

func TestTransport_RecipientValidation(t *testing.T) {
	t.Parallel()

	transport := newTestTransport(t)
	renderer := func([]testPayload) Message { return Message{} }
	tests := []struct {
		name         string
		address      string
		renderer     Renderer[testPayload]
		wantSentinel error
	}{
		{
			name:         "nil renderer",
			address:      "ops@example.test",
			renderer:     nil,
			wantSentinel: ErrInvalidConfig,
		},
		{
			name:         "malformed address",
			address:      "not-an-address",
			renderer:     renderer,
			wantSentinel: ErrInvalidAddress,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := transport.Recipient("email:operations", test.address, test.renderer)
			if !errors.Is(err, test.wantSentinel) {
				t.Fatalf("Recipient() error = %v, want errors.Is(_, %v)", err, test.wantSentinel)
			}

			if test.address != "" && strings.Contains(err.Error(), test.address) {
				t.Error("Recipient() error leaks the rejected address")
			}
		})
	}
}

func TestTransport_RecipientGroupValidation(t *testing.T) {
	t.Parallel()

	transport := newTestTransport(t)
	renderer := func([]testPayload) Message { return Message{} }
	tests := []struct {
		name      string
		addresses []string
	}{
		{name: "empty", addresses: []string{}},
		{name: "duplicate", addresses: []string{"ops@example.test", "ops@example.test"}},
		{name: "invalid member", addresses: []string{"ops@example.test", "invalid"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := transport.RecipientGroup("email:operations", test.addresses, renderer)
			if !errors.Is(err, ErrInvalidAddress) {
				t.Fatalf("RecipientGroup() error = %v, want ErrInvalidAddress", err)
			}
		})
	}
}

func TestTransport_RecipientGroupCopiesAddresses(t *testing.T) {
	t.Parallel()

	transport := newTestTransport(t)
	addresses := []string{"ops@example.test", "audit@example.test"}

	created, err := transport.RecipientGroup(
		"email:operations",
		addresses,
		func([]testPayload) Message { return Message{} },
	)
	if err != nil {
		t.Fatalf("RecipientGroup(): %v", err)
	}

	addresses[0] = "mutated@example.test"

	concrete, ok := created.(*destination[testPayload])
	if !ok {
		t.Fatalf("destination type = %T, want *destination[testPayload]", created)
	}

	if got := concrete.recipients[0]; got != "ops@example.test" {
		t.Errorf("stored recipient = %q, want ops@example.test", got)
	}
}

func TestValidateAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		address string
		valid   bool
	}{
		{name: "bare mailbox", address: "ops@example.test", valid: true},
		{name: "empty", address: ""},
		{name: "display name", address: "Operations <ops@example.test>"},
		{name: "display name without spaces", address: "Operations<ops@example.test>"},
		{name: "leading whitespace", address: " ops@example.test"},
		{name: "embedded whitespace", address: "ops @example.test"},
		{name: "carriage return", address: "ops@example.test\rBcc:evil@example.test"},
		{name: "newline", address: "ops@example.test\nevil@example.test"},
		{name: "nul", address: "ops@example.test\x00"},
		{name: "invalid utf-8", address: string([]byte{'o', 'p', 's', 0xff})},
		{name: "multiple", address: "ops@example.test,audit@example.test"},
		{name: "missing domain", address: "ops"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateAddress(test.address)
			if test.valid && err != nil {
				t.Fatalf("validateAddress() error = %v", err)
			}

			if !test.valid && !errors.Is(err, ErrInvalidAddress) {
				t.Fatalf("validateAddress() error = %v, want ErrInvalidAddress", err)
			}

			if err != nil && test.address != "" && strings.Contains(err.Error(), test.address) {
				t.Error("validateAddress() error leaks the rejected address")
			}
		})
	}
}

func newTestTransport(t *testing.T) *Transport[testPayload] {
	t.Helper()

	transport, err := NewTransport[testPayload](validConfig())
	if err != nil {
		t.Fatalf("NewTransport(): %v", err)
	}

	return transport
}

func TestBuildMessage(t *testing.T) {
	t.Parallel()

	message := Message{
		Subject: "2 updates",
		Text:    "Build: passed\nDeploy: started\n",
		HTML:    "<h2>Build</h2><p>passed</p><h2>Deploy</h2><p>started</p>",
	}
	raw := buildMessage(
		"sender@example.test",
		[]string{"ops@example.test", "audit@example.test"},
		message,
	)

	decoded, err := decodeBuiltMessage(raw)
	if err != nil {
		t.Fatalf("decodeBuiltMessage(): %v", err)
	}

	if decoded.from != "sender@example.test" {
		t.Errorf("From = %q, want sender@example.test", decoded.from)
	}

	if decoded.to != "ops@example.test, audit@example.test" {
		t.Errorf("To = %q, want group recipients", decoded.to)
	}

	if decoded.subject != message.Subject {
		t.Errorf("Subject = %q, want %q", decoded.subject, message.Subject)
	}

	if decoded.text != message.Text {
		t.Errorf("plain body = %q, want %q", decoded.text, message.Text)
	}

	if decoded.html != message.HTML {
		t.Errorf("HTML body = %q, want %q", decoded.html, message.HTML)
	}
}

func FuzzBuildMessage(f *testing.F) {
	f.Add("Status update", "plain text", "<p>HTML</p>")
	f.Add("Привет, мир", "line one\nline two", "<strong>готово</strong>")
	f.Add("", "", "")
	f.Add("invalid\nsubject", "body", "<p>body</p>")

	f.Fuzz(func(t *testing.T, subject, text, html string) {
		message := Message{Subject: subject, Text: text, HTML: html}

		raw := buildMessage(
			"sender@example.test",
			[]string{"ops@example.test", "audit@example.test"},
			message,
		)

		decoded, err := decodeBuiltMessage(raw)
		if err != nil {
			t.Fatalf("decodeBuiltMessage(): %v", err)
		}

		if decoded.subject != subject {
			t.Errorf("Subject = %q, want %q", decoded.subject, subject)
		}

		if decoded.text != text {
			t.Errorf("plain body = %q, want %q", decoded.text, text)
		}

		if decoded.html != html {
			t.Errorf("HTML body = %q, want %q", decoded.html, html)
		}
	})
}

func BenchmarkBuildMessage(b *testing.B) {
	message := Message{
		Subject: "Daily notification summary",
		Text:    strings.Repeat("A useful plain-text notification line.\n", 25),
		HTML:    strings.Repeat("<p>A useful HTML notification line.</p>", 25),
	}
	recipients := []string{"ops@example.test", "audit@example.test"}

	b.ReportAllocs()

	var result []byte
	for b.Loop() {
		result = buildMessage("sender@example.test", recipients, message)
	}

	runtime.KeepAlive(result)
}

type decodedMessage struct {
	from    string
	to      string
	subject string
	text    string
	html    string
}

func decodeBuiltMessage(raw []byte) (decodedMessage, error) {
	decoded, reader, err := decodeMessageEnvelope(raw)
	if err != nil {
		return decodedMessage{}, err
	}

	parts, err := decodeMessageParts(reader, &decoded)
	if err != nil {
		return decodedMessage{}, err
	}

	if parts != 2 {
		return decodedMessage{}, fmt.Errorf("MIME parts = %d, want 2", parts)
	}

	return decoded, nil
}

func decodeMessageEnvelope(raw []byte) (decodedMessage, *multipart.Reader, error) {
	message, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return decodedMessage{}, nil, fmt.Errorf("read message: %w", err)
	}

	subject, err := new(mime.WordDecoder).DecodeHeader(message.Header.Get("Subject"))
	if err != nil {
		return decodedMessage{}, nil, fmt.Errorf("decode subject: %w", err)
	}

	mediaType, params, err := mime.ParseMediaType(message.Header.Get("Content-Type"))
	if err != nil {
		return decodedMessage{}, nil, fmt.Errorf("parse content type: %w", err)
	}

	if mediaType != "multipart/alternative" {
		return decodedMessage{}, nil, fmt.Errorf(
			"content type = %q, want multipart/alternative",
			mediaType,
		)
	}

	decoded := decodedMessage{
		from:    message.Header.Get("From"),
		to:      message.Header.Get("To"),
		subject: subject,
	}
	reader := multipart.NewReader(message.Body, params["boundary"])

	return decoded, reader, nil
}

func decodeMessageParts(reader *multipart.Reader, decoded *decodedMessage) (int, error) {
	parts := 0

	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			return parts, nil
		}

		if err != nil {
			return 0, fmt.Errorf("read next MIME part: %w", err)
		}

		parts++

		partType, body, err := decodeMessagePart(part)
		if err != nil {
			return 0, err
		}

		if err := setDecodedPart(decoded, partType, body); err != nil {
			return 0, err
		}
	}
}

func decodeMessagePart(part *multipart.Part) (string, string, error) {
	bodyReader := io.Reader(part)
	if part.Header.Get("Content-Transfer-Encoding") == "base64" {
		bodyReader = base64.NewDecoder(base64.StdEncoding, part)
	}

	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return "", "", fmt.Errorf("read MIME part: %w", err)
	}

	partType, _, err := mime.ParseMediaType(part.Header.Get("Content-Type"))
	if err != nil {
		return "", "", fmt.Errorf("parse MIME part content type: %w", err)
	}

	return partType, string(body), nil
}

func setDecodedPart(decoded *decodedMessage, partType, body string) error {
	switch partType {
	case "text/plain":
		decoded.text = body
	case "text/html":
		decoded.html = body
	default:
		return fmt.Errorf("unexpected MIME part content type %q", partType)
	}

	return nil
}
