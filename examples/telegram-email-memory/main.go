// Command telegram-email-memory wires a dispatcher to Telegram and email
// destinations, backed by an in-memory store, then runs delivery continuously
// until its context is cancelled.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/arkhemlol/notifier"
	"github.com/arkhemlol/notifier/email"
	"github.com/arkhemlol/notifier/store/memory"
	"github.com/arkhemlol/notifier/telegram"
)

type Alert struct {
	Text string
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := telegram.NewClient[Alert](telegram.Config{
		Token:          "bot-token",
		WebhookURL:     "https://example.com/telegram/webhook",
		WebhookPattern: "POST /telegram/webhook",
		WebhookSecret:  "shared-secret",
	})
	// Long polling instead of a webhook:
	// client, err := telegram.NewClient[Alert](telegram.Config{
	// 	Token:   "bot-token",
	// 	Polling: true,
	// })
	if err != nil {
		logger.Error("build telegram client", "error", err)
		os.Exit(1)
	}
	// client.Bot() exposes the underlying go-telegram/bot client for registering your own update handlers.

	// Note: Chat.Send splits text over Telegram's 4096-char message limit into multiple messages.
	alerts, err := client.Chat("telegram:alerts", "your_chat_id", func(batch []Alert) string {
		return batch[0].Text
	}, telegram.WithParseMode(models.ParseModeMarkdown))
	if err != nil {
		logger.Error("build telegram destination", "error", err)
		os.Exit(1)
	}

	transport, err := email.NewTransport[Alert](email.Config{
		Host: "smtp.example.com", Port: "465",
		Username: "alerts@example.com", Password: "my_password", From: "alerts@example.com",
		MaxIdleConnections: 4, // SMTP connections kept open for reuse; defaults to 4
	})
	if err != nil {
		logger.Error("build email transport", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := transport.Close(); err != nil { // closes pooled SMTP connections on shutdown
			logger.Error("close email transport", "error", err)
		}
	}()

	team, err := transport.RecipientGroup(
		"email:team",
		[]string{"a@example.com", "b@example.com"},
		func(batch []Alert) email.Message {
			return email.Message{Subject: "Alert", Text: batch[0].Text}
		},
	)
	// One recipient instead of a group:
	// team, err := transport.Recipient("email:on-call", "on-call@example.com", func(batch []Alert) email.Message {
	// 	return email.Message{Subject: "Alert", Text: batch[0].Text}
	// })
	if err != nil {
		logger.Error("build email destination", "error", err)
		os.Exit(1)
	}

	dispatcher, err := notifier.NewDispatcher(
		memory.New[Alert](),
		notifier.DispatcherConfig{
			FirstSuccess: false,            // every destination must succeed
			ProbeWorkers: 4,                // check destination availability before the first delivery, this many at once
			ProbeTimeout: 10 * time.Second, // time budget for one check
		},
		// notifier.DispatcherConfig{FirstSuccess: true}, // stop at the first destination that accepts
		alerts,
		team,
	)
	if err != nil {
		logger.Error("build dispatcher", "error", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	for _, route := range client.Routes() {
		mux.Handle(route.Pattern, route.Handler)
	}

	go func() {
		if err := client.Run(ctx); err != nil {
			logger.Error("telegram worker stopped", "error", err)
		}
	}()

	server := &http.Server{Addr: ":8081", Handler: mux}

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server stopped", "error", err)
		}
	}()
	defer func() {
		if err := server.Close(); err != nil {
			logger.Error("close http server", "error", err)
		}
	}()

	item := notifier.Item[Alert]{ID: 1, Payload: Alert{Text: "disk almost full"}}
	if err := dispatcher.Enqueue(ctx, item); err != nil {
		logger.Error("enqueue", "error", err)
		os.Exit(1)
	}

	// The first cycle runs immediately and also probes destinations; onCycle
	// then reports every later cycle on the given interval.
	wait := dispatcher.Start(ctx, 15*time.Second, func(report notifier.Report, err error) {
		if err != nil {
			logger.Error("delivery cycle failed", "error", err)
		}

		for _, result := range report.Results {
			logger.Info(
				"delivered",
				"work", result.Work,
				"destination", result.Destination,
				"outcome", result.Outcome,
			)
		}
	})

	// One delivery cycle instead of a continuous loop:
	// report, err := dispatcher.Run(ctx)
	// if err != nil {
	// 	logger.Error("delivery cycle failed", "error", err)
	// }
	//
	// for _, result := range report.Results {
	// 	logger.Info("delivered",
	// 		"work", result.Work,
	// 		"destination", result.Destination,
	// 		"outcome", result.Outcome,
	// 	)
	// }

	<-ctx.Done() // 5s timeout above, or a shutdown signal in a real service

	stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()

	if err := wait(stopCtx); err != nil {
		logger.Warn("delivery loop did not stop in time", "error", err)
	}
}
