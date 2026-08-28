package email

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/mail"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/arkhemlol/notifier"
	"github.com/arkhemlol/notifier/internal/core"
)

type destination[T any] struct {
	id         core.DestinationID
	transport  *Transport[T]
	recipients []string
	renderer   Renderer[T]
}

// Recipient creates a destination for one SMTP recipient.
func (transport *Transport[T]) Recipient(
	id string,
	address string,
	renderer Renderer[T],
) (notifier.Destination[T], error) {
	return transport.newDestination(id, []string{address}, renderer)
}

// RecipientGroup creates a destination that sends to all addresses in one SMTP transaction.
func (transport *Transport[T]) RecipientGroup(
	id string,
	addresses []string,
	renderer Renderer[T],
) (notifier.Destination[T], error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("%w: recipient group is empty", ErrInvalidAddress)
	}

	return transport.newDestination(id, addresses, renderer)
}

func (transport *Transport[T]) newDestination(
	id string,
	addresses []string,
	renderer Renderer[T],
) (notifier.Destination[T], error) {
	if renderer == nil {
		return nil, fmt.Errorf("%w: renderer is nil", ErrInvalidConfig)
	}

	seen := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		if err := validateAddress(address); err != nil {
			return nil, err
		}

		if _, duplicate := seen[address]; duplicate {
			return nil, fmt.Errorf("%w: recipient group contains a duplicate", ErrInvalidAddress)
		}

		seen[address] = struct{}{}
	}

	return &destination[T]{
		id:         core.DestinationID(id),
		transport:  transport,
		recipients: slices.Clone(addresses),
		renderer:   renderer,
	}, nil
}

func (destination *destination[T]) ID() core.DestinationID {
	return destination.id
}

func (destination *destination[T]) Send(ctx context.Context, batch []T) error {
	message := destination.renderer(batch)

	return destination.transport.send(ctx, destination.recipients, message)
}

// Probe authenticates once per transport without sending mail.
func (destination *destination[T]) Probe(ctx context.Context) error {
	return destination.transport.probe(ctx)
}

func validateAddress(address string) error {
	if !utf8.ValidString(address) {
		return fmt.Errorf("%w: envelope address is not valid utf-8", ErrInvalidAddress)
	}

	if strings.ContainsAny(address, "\r\n\x00") {
		return fmt.Errorf("%w: envelope address contains a prohibited control character", ErrInvalidAddress)
	}

	parsed, err := mail.ParseAddress(address)
	if err != nil {
		return fmt.Errorf("%w: envelope address is malformed", ErrInvalidAddress)
	}

	if parsed.Name != "" || parsed.Address != address {
		return fmt.Errorf("%w: envelope address contains a display name", ErrInvalidAddress)
	}

	return nil
}

var (
	_ notifier.Destination[any] = (*destination[any])(nil)
	_ core.Prober               = (*destination[any])(nil)
)

const (
	messageBoundary = "notifier-email-alternative"

	messageFixedSize = 512
)

// buildMessage base64-encodes the subject to prevent header injection.
func buildMessage(from string, recipients []string, message Message) []byte {
	to := strings.Join(recipients, ", ")
	raw := make([]byte, 0, messageSize(from, to, message))

	raw = append(raw, "From: "...)
	raw = append(raw, from...)
	raw = append(raw, "\r\nTo: "...)
	raw = append(raw, to...)
	raw = append(raw, "\r\nSubject: =?UTF-8?B?"...)
	raw = base64.StdEncoding.AppendEncode(raw, []byte(message.Subject))
	raw = append(raw, "?=\r\nMIME-Version: 1.0\r\n"...)
	raw = append(raw, "Content-Type: multipart/alternative; boundary=\""...)
	raw = append(raw, messageBoundary...)
	raw = append(raw, "\"\r\n\r\n"...)

	raw = appendMessagePart(raw, "text/plain", message.Text)
	raw = appendMessagePart(raw, "text/html", message.HTML)

	raw = append(raw, "--"...)
	raw = append(raw, messageBoundary...)

	return append(raw, "--\r\n"...)
}

func appendMessagePart(raw []byte, contentType, content string) []byte {
	raw = append(raw, "--"...)
	raw = append(raw, messageBoundary...)
	raw = append(raw, "\r\nContent-Type: "...)
	raw = append(raw, contentType...)
	raw = append(raw, "; charset=\"UTF-8\"\r\n"...)
	raw = append(raw, "Content-Transfer-Encoding: base64\r\n\r\n"...)
	raw = base64.StdEncoding.AppendEncode(raw, []byte(content))

	return append(raw, "\r\n"...)
}

func messageSize(from, to string, message Message) int {
	return messageFixedSize + len(from) + len(to) +
		base64.StdEncoding.EncodedLen(len(message.Subject)) +
		base64.StdEncoding.EncodedLen(len(message.Text)) +
		base64.StdEncoding.EncodedLen(len(message.HTML))
}
