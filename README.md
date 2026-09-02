# notifier

[![CI](https://github.com/arkhemlol/notifier/actions/workflows/ci.yml/badge.svg)](https://github.com/arkhemlol/notifier/actions/workflows/ci.yml)
[![Go version](https://img.shields.io/github/go-mod/go-version/arkhemlol/notifier)](go.mod)
[![coverage](https://raw.githubusercontent.com/arkhemlol/notifier/badges/.badges/main/coverage.svg)](https://github.com/arkhemlol/notifier/actions/workflows/ci.yml)
[![tag](https://img.shields.io/github/v/tag/arkhemlol/notifier?sort=semver)](https://github.com/arkhemlol/notifier/tags)

**Notifier** is a lightweight library that delivers queued items to destinations by transport (`Telegram` and `Email` at the moment), retrying until they land using [go-retry](https://github.com/sethvargo/go-retry). Message batches claimed from storage, sent, and resolved by pool of workers concurrently. Queued items and delivery state persisted in a pluggable store (driver-agnostic `SQLite` and in-memory at the moment). Stale deliveries are handled too. It also automatically probes destinations concurrently before sending to them.

## Table of Contents

- [Usage](#usage)
- [Structure](#structure)
- [Examples](./examples)
- [API description](./docs/API.md)
- [TODO](#todo)

## Usage

```go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
	// or any other driver of your choice which implements 'database/sql'
	_ "modernc.org/sqlite"

	"github.com/go-telegram/bot/models"

	"github.com/arkhemlol/notifier"
	// import only what you actually need
	"github.com/arkhemlol/notifier/telegram"
	"github.com/arkhemlol/notifier/email"
	"github.com/arkhemlol/notifier/store/sqlite"
)

type Alert struct {
	Text string
}

type jsonCodec struct{}

func (jsonCodec) Encode(value Alert) ([]byte, error) {
	return json.Marshal(value)
}

func (jsonCodec) Decode(data []byte) (Alert, error) {
	var value Alert
	err := json.Unmarshal(data, &value)

	return value, err
}

// error handling ommited for clarity
func main() {
	ctx := context.Background()

	client, err := telegram.NewClient[Alert](telegram.Config{Token: "bot-token"})

	alerts, err := client.Chat("telegram:alerts", "chat_id", func(batch []Alert) string {
		return batch[0].Text
	}, telegram.WithParseMode(models.ParseModeMarkdown)) // opts customize every SendMessage call

	transport, err := email.NewTransport[Alert](email.Config{
		Host: "smtp.example.com", Port: "465",
		Username: "alerts@example.com", Password: "my_password", From: "alerts@example.com",
	})

	onCall, err := transport.Recipient("email:on-call", "on-call@example.com", func(batch []Alert) email.Message {
		return email.Message{Subject: "Alert", Text: batch[0].Text}
	})

	db, err := sql.Open("sqlite", "file:notifier.db?_fk=1&_busy_timeout=5000")

	store, err := sqlite.New[Alert](db, jsonCodec{})

	dispatcher, err := notifier.NewDispatcher(
		store,
		notifier.DispatcherConfig{
			Workers:      8,                // batches claimed and sent at once; defaults to GOMAXPROCS
			FirstSuccess: false,            // false: every destination must succeed; true: delivery success for first transport is enough (others not used)
			ProbeWorkers: 4,                // check destination availability before the first delivery, this many at once
			ProbeTimeout: 10 * time.Second, // time budget for one probe check
			SkipProbing:  false,            // true skips that check; delivery to an unreachable destination is then less reliable
		},
		// Destinations. For a custom probe check per destination, implement notifier.Prober, 
		// or wrap one in a type that embeds it and adds its own Probe.
		alerts, // telegram, tried first if FirstSuccess is set
		onCall, // email
		// .. more
	)

	wait := dispatcher.Start(ctx, time.Minute, func(_ notifier.Report, err error) {
		// handle err, e.g. log it
	})
	defer wait(ctx)

	// One-shot delivery cycle instead of a recurring schedule:
	report, err := dispatcher.Run(ctx)
}
```

## Examples

More examples here: [`examples`](./examples) folder.

- [`telegram-email-sqlite`](./examples/telegram-email-sqlite) — Telegram + Email example with a SQLite store.
- [`telegram-email-memory`](./examples/telegram-email-memory) — Telegram + Email example with an in-memory store.
- [`telegram-email-direct-send`](./examples/telegram-email-direct-send) — Telegram + Email example sending directly, without a dispatcher.

## Structure

- `telegram` sends notification batches through the Telegram Bot API.
- `email` sends notification batches over implicit-TLS SMTP.
- `store/sqlite` is a driver-agnostic adapter for SQLite (uses std's `database/sql` interface).
- `store/memory` is an in-memory store, doesn't survive process restart.

See [API description](./docs/API.md) for the full public API.

## TODO

- add statistics
- more transports (Discord, Slack, grpc, etc.)
- centralized caching
- PostgreSQL adapter
- Redis adapter
