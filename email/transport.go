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

// Config defines SMTP settings. All fields except TLSServerName and TLSConfig are required;
// From does not default to Username.
type Config struct {
	Host          string
	Port          string
	Username      string
	Password      string
	From          string
	TLSServerName string

	// TLSConfig overrides the default TLS configuration (TLS 1.2 minimum). NewTransport clones it.
	TLSConfig *tls.Config
}

// Message contains rendered email content.
type Message struct {
	Subject string
	Text    string
	HTML    string
}

// Renderer renders a notification batch as email content.
type Renderer[T any] func(batch []T) Message

// Transport is an immutable SMTP client shared safely by concurrent destinations.
type Transport[T any] struct {
	config    Config
	endpoint  string
	tlsConfig *tls.Config

	probeOnce sync.Once
	probeErr  error
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

	return &Transport[T]{
		config:    cfg,
		endpoint:  net.JoinHostPort(cfg.Host, cfg.Port),
		tlsConfig: tlsConfig,
	}, nil
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
