package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

const asyncFetchWait = time.Second

type messageFetcher interface {
	Fetch(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error)
}

// Listener waits for a message on a named pipe.
type Listener struct {
	Name     string
	Group    bool
	Context  string
	DataSubj string

	nc   *nats.Conn
	errc chan error
}

// Listen connects to NATS and waits for a single message.
func (l *Listener) Listen(ctx context.Context) error {
	var err error

	l.nc, err = connect(l.Context)
	if err != nil {
		return fmt.Errorf("could not connect to NATS: %w", err)
	}
	defer l.close()

	if async {
		js, jsErr := l.nc.JetStream()
		if jsErr != nil {
			return fmt.Errorf("could not get JetStream context: %w", jsErr)
		}

		if err := createConsumer(l.Name, js); err != nil {
			return fmt.Errorf("could not set up consumer: %w", err)
		}
	}

	switch {
	case async:
		go func() {
			js, jsErr := l.nc.JetStream()
			if jsErr != nil {
				l.errc <- fmt.Errorf("could not get JetStream context: %w", jsErr)
				return
			}

			sub, subErr := js.PullSubscribe(asyncName(l.Name), l.Name)
			if subErr != nil {
				l.errc <- fmt.Errorf("could not subscribe: %w", subErr)
				return
			}

			msg, fetchErr := waitForAsyncMessage(ctx, sub)
			if fetchErr != nil {
				if errors.Is(fetchErr, context.Canceled) {
					return
				}
				l.errc <- fmt.Errorf("async fetch failed: %w", fetchErr)
				return
			}

			l.jsHandler(msg)
		}()

	case l.Group:
		log.Debugf("Listening on %s in a work group", l.DataSubj)
		if _, err := l.nc.QueueSubscribe(l.DataSubj, "piper", l.ibHandler); err != nil {
			l.errc <- err
		}

	default:
		log.Debugf("Listening on %s", l.DataSubj)
		if _, err := l.nc.Subscribe(l.DataSubj, l.ibHandler); err != nil {
			l.errc <- err
		}
	}

	select {
	case <-ctx.Done():
		return nil
	case err = <-l.errc:
		return err
	}
}

func (l *Listener) close() {
	if err := l.nc.Flush(); err != nil {
		log.Warnf("Could not flush NATS connection: %s", err)
	}
	l.nc.Close()
}

func (l *Listener) ibHandler(m *nats.Msg) {
	if err := m.Sub.Unsubscribe(); err != nil {
		log.Warnf("Could not unsubscribe from data subject: %s", err)
	}

	if err := m.Respond([]byte{}); err != nil {
		l.errc <- fmt.Errorf("acknowledgement failed: %w", err)
		return
	}

	body, err := decompress(m.Data)
	if err != nil {
		l.errc <- fmt.Errorf("decompression failed: %w", err)
		return
	}

	fmt.Print(body)
	l.errc <- nil
}

func (l *Listener) jsHandler(m *nats.Msg) {
	if err := m.Ack(); err != nil {
		l.errc <- fmt.Errorf("acknowledgement failed: %w", err)
		return
	}

	body, err := decompress(m.Data)
	if err != nil {
		l.errc <- fmt.Errorf("decompression failed: %w", err)
		return
	}

	fmt.Print(body)
	l.errc <- nil
}

func waitForAsyncMessage(ctx context.Context, sub messageFetcher) (*nats.Msg, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		log.Debugf("Fetching 1 message from JetStream")
		msgs, err := sub.Fetch(1, nats.Context(ctx), nats.MaxWait(asyncFetchWait))
		if err == nil {
			if len(msgs) == 0 {
				continue
			}
			return msgs[0], nil
		}

		if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
			continue
		}

		return nil, err
	}
}
