package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

const retryDelay = time.Second

type syncRequester interface {
	RequestWithContext(ctx context.Context, subj string, data []byte) (*nats.Msg, error)
}

type asyncPublisher interface {
	Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error)
}

// Notifier publishes a message to a named pipe.
type Notifier struct {
	Name            string
	Context         string
	ContextExplicit bool
	Subject         string
	Message         string
	Timeout         time.Duration
}

// Notify connects to NATS and publishes the message.
func (n *Notifier) Notify(ctx context.Context) error {
	if n.Timeout == 0 && async {
		n.Timeout = 2 * time.Second
	} else if n.Timeout == 0 {
		n.Timeout = 1 * time.Hour
	}

	if n.Message == "" {
		message, err := readMessage(os.Stdin)
		if err != nil {
			return fmt.Errorf("could not read STDIN: %w", err)
		}
		n.Message = message
	}

	compressed, err := compress(n.Message)
	if err != nil {
		return fmt.Errorf("compression failed: %w", err)
	}

	nc, err := connect(n.Context, n.ContextExplicit)
	if err != nil {
		return fmt.Errorf("could not connect to NATS: %w", err)
	}
	defer nc.Close()

	log.Debugf("Publishing to %s with a timeout of %v", n.Subject, n.Timeout)

	tctx, cancel := context.WithTimeout(ctx, n.Timeout)
	defer cancel()

	if async {
		js, jsErr := nc.JetStream()
		if jsErr != nil {
			return fmt.Errorf("JetStream is not available: %w", jsErr)
		}

		if err := createConsumer(n.Name, js); err != nil {
			return fmt.Errorf("could not create JetStream consumer: %w", err)
		}

		return publishAsync(tctx, js, n.Subject, compressed)
	}

	return publishSync(tctx, nc, n.Subject, compressed, n.Timeout)
}

func publishAsync(ctx context.Context, publisher asyncPublisher, subject string, data []byte) error {
	_, err := publisher.Publish(subject, data, nats.Context(ctx))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("async publish failed: %w", err)
	}

	return nil
}

func publishSync(ctx context.Context, requester syncRequester, subject string, data []byte, timeout time.Duration) error {
	for {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, 2*time.Second)

		log.Debugf("Sending %d bytes of data on subject %s", len(data), subject)
		_, err := requester.RequestWithContext(attemptCtx, subject, data)
		attemptCancel()

		if err == nil {
			return nil
		}

		if !requestContextDone(err) {
			log.Errorf("notification failed, will retry in a second: %s", err)
			if err := waitForRetry(ctx); err != nil {
				return timeoutError(ctx, timeout)
			}
			continue
		}

		if ctx.Err() != nil {
			return timeoutError(ctx, timeout)
		}
	}
}

func waitForRetry(ctx context.Context) error {
	timer := time.NewTimer(retryDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func timeoutError(ctx context.Context, timeout time.Duration) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	return fmt.Errorf("timeout after %v", timeout)
}

func requestContextDone(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func readMessage(reader io.Reader) (string, error) {
	text, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	return string(text), nil
}
