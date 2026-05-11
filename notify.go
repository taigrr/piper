package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

// Notifier publishes a message to a named pipe.
type Notifier struct {
	Name    string
	Context string
	Subject string
	Message string
	Timeout time.Duration
}

// Notify connects to NATS and publishes the message.
func (n *Notifier) Notify(ctx context.Context) error {
	if n.Timeout == 0 && async {
		n.Timeout = 2 * time.Second
	} else if n.Timeout == 0 {
		n.Timeout = 1 * time.Hour
	}

	log.Debugf("Publishing to %s with a timeout of %v", n.Subject, n.Timeout)

	tctx, cancel := context.WithTimeout(ctx, n.Timeout)
	defer cancel()

	nc, err := connect(n.Context)
	if err != nil {
		return fmt.Errorf("could not connect to NATS: %w", err)
	}
	defer nc.Close()

	if async {
		js, jsErr := nc.JetStream()
		if jsErr != nil {
			return fmt.Errorf("JetStream is not available: %w", jsErr)
		}

		if err := createConsumer(n.Name, js); err != nil {
			return fmt.Errorf("could not create JetStream consumer: %w", err)
		}
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

	for {
		attemptCtx, attemptCancel := context.WithTimeout(tctx, 2*time.Second)

		log.Debugf("Sending %d bytes of data compressed to %d on subject %s", len(n.Message), len(compressed), n.Subject)
		_, err = nc.RequestWithContext(attemptCtx, n.Subject, compressed)
		attemptCancel()

		if err == nil {
			return nil
		}

		if err != context.Canceled && err != context.DeadlineExceeded {
			log.Errorf("notification failed, will retry in a second: %s", err)
			time.Sleep(time.Second)
			continue
		}

		if tctx.Err() != nil {
			return fmt.Errorf("timeout after %v", n.Timeout)
		}
	}
}

func readMessage(reader io.Reader) (string, error) {
	text, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	return string(text), nil
}
