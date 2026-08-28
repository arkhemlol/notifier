package email

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/smtp"
	"net/textproto"

	"github.com/arkhemlol/notifier/internal/core"
)

type smtpStage uint8

const (
	smtpStageConnection smtpStage = iota + 1
	smtpStageGreeting
	smtpStageAuthentication
	smtpStageSender
	smtpStageRecipient
	smtpStageData
	smtpStageMessage
	smtpStageNoop
)

type smtpOperationError struct {
	stage smtpStage
	err   error
}

var smtpStageMessages = map[smtpStage]string{
	smtpStageConnection:     "smtp connection failed",
	smtpStageGreeting:       "smtp greeting failed",
	smtpStageAuthentication: "smtp authentication failed",
	smtpStageSender:         "smtp relay rejected sender",
	smtpStageRecipient:      "smtp relay rejected recipient",
	smtpStageData:           "smtp relay rejected message",
	smtpStageMessage:        "smtp message transfer failed",
	smtpStageNoop:           "smtp noop failed",
}

func (err *smtpOperationError) Error() string {
	if message, ok := smtpStageMessages[err.stage]; ok {
		return message
	}

	return "smtp operation failed"
}

func (err *smtpOperationError) Unwrap() error {
	switch {
	case errors.Is(err.err, context.Canceled):
		return context.Canceled
	case errors.Is(err.err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (transport *Transport[T]) probe(ctx context.Context) error {
	transport.probeOnce.Do(func() {
		err := transport.withClient(ctx, func(client *smtp.Client) error {
			if err := client.Noop(); err != nil {
				return newSMTPOperationError(smtpStageNoop, err)
			}

			return nil
		})
		if err != nil {
			transport.probeErr = classifySMTP(ctx, err)
		}
	})

	return transport.probeErr
}

func (transport *Transport[T]) send(
	ctx context.Context,
	recipients []string,
	message Message,
) error {
	rawMessage := buildMessage(transport.config.From, recipients, message)

	err := transport.withClient(ctx, func(client *smtp.Client) error {
		if err := client.Mail(transport.config.From); err != nil {
			return newSMTPOperationError(smtpStageSender, err)
		}

		for _, recipient := range recipients {
			if err := client.Rcpt(recipient); err != nil {
				return newSMTPOperationError(smtpStageRecipient, err)
			}
		}

		writer, err := client.Data()
		if err != nil {
			return newSMTPOperationError(smtpStageData, err)
		}

		if _, err := writer.Write(rawMessage); err != nil {
			return newSMTPOperationError(smtpStageMessage, err)
		}

		if err := writer.Close(); err != nil {
			return newSMTPOperationError(smtpStageMessage, err)
		}

		return nil
	})
	if err == nil {
		return nil
	}

	return classifySMTP(ctx, err)
}

func (transport *Transport[T]) withClient(
	ctx context.Context,
	operation func(*smtp.Client) error,
) error {
	dialer := tls.Dialer{Config: transport.tlsConfig}

	connection, err := dialer.DialContext(ctx, "tcp", transport.endpoint)
	if err != nil {
		return newSMTPOperationError(smtpStageConnection, err)
	}

	stopOnCancel := context.AfterFunc(ctx, func() {
		_ = connection.Close()
	})
	defer func() {
		stopOnCancel()

		_ = connection.Close()
	}()

	if deadline, ok := ctx.Deadline(); ok {
		if err := connection.SetDeadline(deadline); err != nil {
			return newSMTPOperationError(smtpStageConnection, err)
		}
	}

	client, err := smtp.NewClient(connection, transport.config.Host)
	if err != nil {
		return newSMTPOperationError(smtpStageGreeting, err)
	}

	auth := smtp.PlainAuth(
		"",
		transport.config.Username,
		transport.config.Password,
		transport.config.Host,
	)
	if err := client.Auth(auth); err != nil {
		return newSMTPOperationError(smtpStageAuthentication, err)
	}

	return operation(client)
}

func newSMTPOperationError(stage smtpStage, err error) *smtpOperationError {
	return &smtpOperationError{stage: stage, err: err}
}

func classifySMTP(ctx context.Context, err error) error {
	operationError, ok := errors.AsType[*smtpOperationError](err)
	if !ok {
		operationError = newSMTPOperationError(smtpStageConnection, err)
	}

	protocolError, isProtocolError := errors.AsType[*textproto.Error](operationError.err)
	if isProtocolError {
		if protocolError.Code/100 == 5 {
			return classifyPermanentSMTP(operationError)
		}

		return core.Retryable(operationError)
	}

	if isPermanentTLSFailure(operationError.err) {
		return core.Quarantine(operationError)
	}

	if contextError := ctx.Err(); contextError != nil {
		return core.Retryable(newSMTPOperationError(
			operationError.stage,
			errors.Join(contextError, operationError.err),
		))
	}

	if isNetworkFailure(operationError.err) {
		return core.Retryable(operationError)
	}

	if operationError.stage == smtpStageAuthentication {
		return core.Quarantine(operationError)
	}

	return core.Retryable(operationError)
}

// classifyPermanentSMTP handles a 5xx reply: the relay rejected the message content
// (permanent, retrying won't help) or something about the transport itself (quarantine
// the destination for review).
func classifyPermanentSMTP(err *smtpOperationError) error {
	switch err.stage {
	case smtpStageRecipient, smtpStageData, smtpStageMessage:
		return core.Permanent(err)
	case smtpStageConnection,
		smtpStageGreeting,
		smtpStageAuthentication,
		smtpStageSender,
		smtpStageNoop:
		return core.Quarantine(err)
	default:
		return core.Quarantine(err)
	}
}

func isNetworkFailure(err error) bool {
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) {
		return true
	}

	_, ok := errors.AsType[net.Error](err)

	return ok
}

func isPermanentTLSFailure(err error) bool {
	var (
		verification *tls.CertificateVerificationError
		recordHeader tls.RecordHeaderError
		authority    x509.UnknownAuthorityError
		hostname     x509.HostnameError
		invalid      x509.CertificateInvalidError
	)

	return errors.As(err, &verification) ||
		errors.As(err, &recordHeader) ||
		errors.As(err, &authority) ||
		errors.As(err, &hostname) ||
		errors.As(err, &invalid)
}
