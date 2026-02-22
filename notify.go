package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

type Notifier struct {
	Name        string
	Credentials string
	Servers     string
	Subject     string
	Message     string
	Timeout     time.Duration
}

func NewNotifier() *Notifier {
	var sub string
	if async {
		sub = asyncName(name)
	} else {
		sub = syncName(name)
	}

	return &Notifier{
		Name:        name,
		Credentials: creds,
		Servers:     servers,
		Message:     notifierMessage,
		Timeout:     notifierTimeout,
		Subject:     sub,
	}
}

func (n *Notifier) Notify(ctx context.Context) error {
	if n.Timeout == 0 && async {
		n.Timeout = 2 * time.Second
	} else if n.Timeout == 0 {
		n.Timeout = 1 * time.Hour
	}

	log.Debugf("Publishing to %s with a timeout of %v", n.Name, n.Timeout)

	timeout, cancel := context.WithTimeout(ctx, n.Timeout)
	defer cancel()

	nc, err := connect(n.Credentials, n.Servers)
	if err != nil {
		return fmt.Errorf("could not connect to NATS: %w", err)
	}

	if async {
		js, jsErr := nc.JetStream()
		if jsErr != nil {
			return fmt.Errorf("could not get JetStream context: %w", jsErr)
		}

		err = createConsumer(n.Name, js)
		if err != nil {
			return fmt.Errorf("could not create JetStream Consumer: %w", err)
		}
	}

	if n.Message == "" {
		reader := bufio.NewReader(os.Stdin)
		text := make([]byte, reader.Size())
		_, err = reader.Read(text)
		if err != nil {
			return fmt.Errorf("could not read STDIN: %w", err)
		}
		n.Message = string(text)
	}

	compressed, err := compress(n.Message)
	if err != nil {
		return fmt.Errorf("compression failed: %w", err)
	}

	for {
		attemptTimeout, attemptCancel := context.WithTimeout(timeout, 2*time.Second)
		defer attemptCancel()

		log.Debugf("Sending %d bytes of data compressed to %d on subject %s", len(n.Message), len(compressed), n.Subject)
		_, err = nc.RequestWithContext(attemptTimeout, n.Subject, compressed)
		if err == nil {
			return nil
		}

		if err != context.Canceled && err != context.DeadlineExceeded {
			log.Errorf("notification failed, will retry in a second: %s", err)
			time.Sleep(time.Second)
			continue
		}

		err = timeout.Err()
		if err != nil {
			return fmt.Errorf("timeout after %v", n.Timeout)
		}
	}
}
