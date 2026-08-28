package email

import (
	"crypto/tls"
	"errors"
	"strings"
	"testing"
)

type invalidTransportTest struct {
	name         string
	modify       func(*Config)
	wantSentinel error
}

func TestNewTransport(t *testing.T) {
	t.Parallel()

	for _, test := range invalidTransportTests() {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			config := validConfig()
			test.modify(&config)

			_, err := NewTransport[testPayload](config)
			if !errors.Is(err, test.wantSentinel) {
				t.Fatalf("NewTransport() error = %v, want errors.Is(_, %v)", err, test.wantSentinel)
			}

			for _, secret := range []string{config.Username, config.Password, config.From} {
				if secret != "" && strings.Contains(err.Error(), secret) {
					t.Errorf("NewTransport() error leaks configured secret or address")
				}
			}
		})
	}
}

func invalidTransportTests() []invalidTransportTest {
	return []invalidTransportTest{
		{
			name: "missing host",
			modify: func(config *Config) {
				config.Host = ""
			},
			wantSentinel: ErrInvalidConfig,
		},
		{
			name: "host whitespace",
			modify: func(config *Config) {
				config.Host = "smtp.example.test "
			},
			wantSentinel: ErrInvalidConfig,
		},
		{
			name: "invalid host utf-8",
			modify: func(config *Config) {
				config.Host = string([]byte{0xff})
			},
			wantSentinel: ErrInvalidConfig,
		},
		{
			name: "missing port",
			modify: func(config *Config) {
				config.Port = ""
			},
			wantSentinel: ErrInvalidConfig,
		},
		{
			name: "zero port",
			modify: func(config *Config) {
				config.Port = "0"
			},
			wantSentinel: ErrInvalidConfig,
		},
		{
			name: "port above maximum",
			modify: func(config *Config) {
				config.Port = "65536"
			},
			wantSentinel: ErrInvalidConfig,
		},
		{
			name: "signed port",
			modify: func(config *Config) {
				config.Port = "+465"
			},
			wantSentinel: ErrInvalidConfig,
		},
		{
			name: "port with whitespace",
			modify: func(config *Config) {
				config.Port = " 465"
			},
			wantSentinel: ErrInvalidConfig,
		},
		{
			name: "missing username",
			modify: func(config *Config) {
				config.Username = ""
			},
			wantSentinel: ErrInvalidConfig,
		},
		{
			name: "missing password",
			modify: func(config *Config) {
				config.Password = ""
			},
			wantSentinel: ErrInvalidConfig,
		},
		{
			name: "missing from has no username fallback",
			modify: func(config *Config) {
				config.From = ""
			},
			wantSentinel: ErrInvalidAddress,
		},
		{
			name: "invalid tls server name",
			modify: func(config *Config) {
				config.TLSServerName = "smtp.example.test\n"
			},
			wantSentinel: ErrInvalidConfig,
		},
	}
}

func TestNewTransport_TLSConfiguration(t *testing.T) {
	t.Parallel()

	original := &tls.Config{
		ServerName: "custom.example.test",
		MinVersion: tls.VersionTLS13,
	}

	config := validConfig()
	config.TLSConfig = original

	transport, err := NewTransport[testPayload](config)
	if err != nil {
		t.Fatalf("NewTransport(): %v", err)
	}

	if transport.tlsConfig == original {
		t.Fatal("NewTransport() retained the caller's TLS config pointer")
	}

	original.ServerName = "mutated.example.test"
	if got := transport.tlsConfig.ServerName; got != "custom.example.test" {
		t.Errorf("TLS ServerName = %q, want custom.example.test", got)
	}

	if got := transport.tlsConfig.MinVersion; got != tls.VersionTLS13 {
		t.Errorf("TLS MinVersion = %d, want %d", got, tls.VersionTLS13)
	}
}

func TestNewTransport_TLSServerNamePrecedence(t *testing.T) {
	t.Parallel()

	config := validConfig()
	config.TLSServerName = "override.example.test"
	config.TLSConfig = &tls.Config{ServerName: "custom.example.test"}

	transport, err := NewTransport[testPayload](config)
	if err != nil {
		t.Fatalf("NewTransport(): %v", err)
	}

	if got := transport.tlsConfig.ServerName; got != config.TLSServerName {
		t.Errorf("TLS ServerName = %q, want %q", got, config.TLSServerName)
	}
}

func TestNewTransport_DefaultTLSConfiguration(t *testing.T) {
	t.Parallel()

	transport, err := NewTransport[testPayload](validConfig())
	if err != nil {
		t.Fatalf("NewTransport(): %v", err)
	}

	if got := transport.tlsConfig.ServerName; got != validConfig().Host {
		t.Errorf("TLS ServerName = %q, want %q", got, validConfig().Host)
	}

	if got := transport.tlsConfig.MinVersion; got != tls.VersionTLS12 {
		t.Errorf("TLS MinVersion = %d, want %d", got, tls.VersionTLS12)
	}
}

func validConfig() Config {
	return Config{
		Host:     "smtp.example.test",
		Port:     "465",
		Username: "smtp-user",
		Password: "top-secret",
		From:     "sender@example.test",
	}
}

type testPayload struct {
	Title string
	Body  string
}
