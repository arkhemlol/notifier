// Package email sends notification batches over implicit-TLS SMTP.
package email

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"unicode"
	"unicode/utf8"
)

var (
	// ErrInvalidConfig indicates invalid transport or destination settings.
	ErrInvalidConfig = errors.New("invalid email configuration")
	// ErrInvalidAddress indicates an invalid envelope address.
	ErrInvalidAddress = errors.New("invalid email address")
)

// Config defines SMTP settings. All fields except TLSServerName, TLSConfig, and
// MaxIdleConnections are required; From does not default to Username.
type Config struct {
	Host          string
	Port          string
	Username      string
	Password      string
	From          string
	TLSServerName string

	// TLSConfig overrides the default TLS configuration (TLS 1.2 minimum). NewTransport clones it.
	TLSConfig *tls.Config

	// MaxIdleConnections caps SMTP connections kept open for reuse between sends,
	// avoiding a full TCP+TLS+AUTH handshake on every batch. Defaults to 4.
	MaxIdleConnections int
}

// Message contains rendered email content.
type Message struct {
	Subject string
	Text    string
	HTML    string
}

// Renderer renders a notification batch as email content.
type Renderer[T any] func(batch []T) Message

// defaultMaxIdleSMTPConnections is used when Config.MaxIdleConnections is zero.
const defaultMaxIdleSMTPConnections = 4

// Transport is an immutable SMTP client shared safely by concurrent destinations.
// It keeps a small pool of authenticated connections open between sends.
type Transport[T any] struct {
	config    Config
	endpoint  string
	tlsConfig *tls.Config
	maxIdle   int

	probeOnce sync.Once
	probeErr  error

	idleMu sync.Mutex
	idle   []*idleSMTPConn
	closed bool
}

// NewTransport validates cfg and creates a Transport without network I/O.
func NewTransport[T any](cfg Config) (*Transport[T], error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	baseTLSConfig := cfg.TLSConfig
	if baseTLSConfig == nil {
		baseTLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	tlsConfig := baseTLSConfig.Clone()
	if cfg.TLSServerName != "" {
		tlsConfig.ServerName = cfg.TLSServerName
	}

	if tlsConfig.ServerName == "" {
		tlsConfig.ServerName = cfg.Host
	}

	maxIdle := cfg.MaxIdleConnections
	if maxIdle == 0 {
		maxIdle = defaultMaxIdleSMTPConnections
	}

	return &Transport[T]{
		config:    cfg,
		endpoint:  net.JoinHostPort(cfg.Host, cfg.Port),
		tlsConfig: tlsConfig,
		maxIdle:   maxIdle,
	}, nil
}

// Close closes every idle pooled connection and stops future pooling.
// Connections currently in use finish normally and are closed rather than
// pooled when they're returned. The Transport remains usable afterward; it
// just dials a fresh connection for every send from now on.
func (transport *Transport[T]) Close() error {
	transport.idleMu.Lock()
	idle := transport.idle
	transport.idle = nil
	transport.closed = true
	transport.idleMu.Unlock()

	errs := make([]error, len(idle))
	for i, conn := range idle {
		errs[i] = conn.connection.Close()
	}

	return errors.Join(errs...)
}

func validateConfig(cfg Config) error {
	if err := validateEndpointPart("host", cfg.Host, true); err != nil {
		return err
	}

	if err := validatePort(cfg.Port); err != nil {
		return err
	}

	if cfg.Username == "" {
		return fmt.Errorf("%w: username is empty", ErrInvalidConfig)
	}

	if cfg.Password == "" {
		return fmt.Errorf("%w: password is empty", ErrInvalidConfig)
	}

	if err := validateAddress(cfg.From); err != nil {
		return err
	}

	if err := validateEndpointPart("tls server name", cfg.TLSServerName, false); err != nil {
		return err
	}

	if cfg.MaxIdleConnections < 0 {
		return fmt.Errorf("%w: max idle connections must not be negative", ErrInvalidConfig)
	}

	return nil
}

func validateEndpointPart(name, value string, required bool) error {
	if value == "" {
		if required {
			return fmt.Errorf("%w: %s is empty", ErrInvalidConfig, name)
		}

		return nil
	}

	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s is not valid utf-8", ErrInvalidConfig, name)
	}

	for _, character := range value {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return fmt.Errorf("%w: %s contains whitespace or a control character", ErrInvalidConfig, name)
		}
	}

	return nil
}

func validatePort(port string) error {
	value, err := strconv.ParseUint(port, 10, 32)
	if err != nil {
		return fmt.Errorf("%w: port must be numeric", ErrInvalidConfig)
	}

	if value < 1 || value > 65535 {
		return fmt.Errorf("%w: port must be between 1 and 65535", ErrInvalidConfig)
	}

	return nil
}
