package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

type Listener struct {
	Name        string
	Group       bool
	Credentials string
	Servers     string
	DataSubj    string

	nc   *nats.Conn
	errc chan error
}

func NewListener() *Listener {
	return &Listener{
		Name:        name,
		Group:       listenGroup,
		Credentials: creds,
		Servers:     servers,
		DataSubj:    "piper." + name,
		errc:        make(chan error),
	}
}

func (l *Listener) Listen(ctx context.Context) error {
	var err error

	l.nc, err = connect(l.Credentials, l.Servers)
	if err != nil {
		return fmt.Errorf("could not connect to NATS: %w", err)
	}
	defer l.close()

	if async {
		js, jsErr := l.nc.JetStream()
		if jsErr != nil {
			return fmt.Errorf("could not get JetStream context: %w", jsErr)
		}

		err = createConsumer(l.Name, js)
		if err != nil {
			return fmt.Errorf("could not set up consumer: %w", err)
		}
	}

	switch {
	case async:
		log.Debugf("Fetching 1 message from JetStream")
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

			msgs, fetchErr := sub.Fetch(1, nats.MaxWait(8760*time.Hour))
			if fetchErr != nil {
				l.errc <- fmt.Errorf("async fetch failed: %w", fetchErr)
				return
			}

			if len(msgs) > 0 {
				l.jsHandler(msgs[0])
			}
		}()

	case l.Group:
		log.Debugf("Listening on %s in a work group", l.DataSubj)
		_, err := l.nc.QueueSubscribe(l.DataSubj, "piper", l.ibHandler)
		if err != nil {
			l.errc <- err
		}

	default:
		log.Debugf("Listening on %s", l.DataSubj)
		_, err := l.nc.Subscribe(l.DataSubj, l.ibHandler)
		if err != nil {
			l.errc <- err
		}
	}

	select {
	case <-ctx.Done():
	case err = <-l.errc:
	}

	return err
}

func (l *Listener) close() {
	err := l.nc.Flush()
	if err != nil {
		log.Warnf("Could not flush NATS connection: %s", err)
	}
	l.nc.Close()
}

func (l *Listener) ibHandler(m *nats.Msg) {
	err := m.Sub.Unsubscribe()
	if err != nil {
		log.Warnf("Could not unsubscribe from data subject: %s", err)
	}

	err = m.Respond([]byte{})
	if err != nil {
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
	err := m.Ack()
	if err != nil {
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
