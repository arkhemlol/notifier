package core

import "fmt"

// Op names the operation an Error occurred during.
type Op string

// Operations an Error can be attributed to.
const (
	OpRun           Op = "dispatcher run"
	OpEnqueue       Op = "dispatcher enqueue"
	OpClaim         Op = "dispatcher claim work"
	OpDrain         Op = "dispatcher drain"
	OpResolve       Op = "dispatcher resolve work"
	OpProbe         Op = "dispatcher probe destination"
	OpServiceSetup  Op = "service setup"
	OpServiceWorker Op = "service worker"
)

// Error attributes a failure to an operation and, optionally, a subject.
type Error struct {
	Op      Op
	Subject string
	Err     error
}

func (e *Error) Error() string {
	prefix := string(e.Op)
	if prefix == "" {
		prefix = "notifier"
	}

	if e.Subject != "" {
		prefix = fmt.Sprintf("%s %q", prefix, e.Subject)
	}

	if e.Err == nil {
		return prefix
	}

	return prefix + ": " + e.Err.Error()
}

func (e *Error) Unwrap() error {
	return e.Err
}
