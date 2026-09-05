// Command telegram-email-direct-send sends through Telegram and email
// destinations directly, without a dispatcher, store, or retries.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/arkhemlol/notifier"
	"github.com/arkhemlol/notifier/email"
	"github.com/arkhemlol/notifier/telegram"
)

type Alert struct {
	Text string
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := telegram.NewClient[Alert](telegram.Config{Token: "bot-token"})
	// Long polling instead of a webhook; irrelevant for a direct send, since
	// Chat.Send talks to the Bot API directly:
	// client, err := telegram.NewClient[Alert](telegram.Config{
	// 	Token:   "bot-token",
	// 	Polling: true,
	// })
	if err != nil {
		logger.Error("build telegram client", "error", err)
		os.Exit(1)
	}

	alerts, err := client.Chat("telegram:alerts", "your_chat_id", func(batch []Alert) string {
		return batch[0].Text
	}, telegram.WithParseMode(models.ParseModeMarkdown))
	if err != nil {
		logger.Error("build telegram destination", "error", err)
		os.Exit(1)
	}

	// This example bypasses Dispatcher, which would otherwise assert Prober and
	// probe automatically; done here by hand, since not every destination supports it.
	if prober, ok := alerts.(notifier.Prober); ok {
		if err := prober.Probe(ctx); err != nil {
			logger.Error("telegram chat unreachable", "error", err)
		}
	}
	// Note: Chat.Send splits text over Telegram's 4096-char message limit into multiple messages.
	if err := alerts.Send(ctx, []Alert{{Text: "disk almost full"}}); err != nil {
		logger.Error("telegram send failed", "error", err)
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
	defer transport.Close() // closes pooled SMTP connections on shutdown

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

	if err := team.Send(ctx, []Alert{{Text: "disk almost full"}}); err != nil {
		logger.Error("email send failed", "error", err)
	}
}
