package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

func connect(nctx string) (*nats.Conn, error) {
	errh := func(nc *nats.Conn, sub *nats.Subscription, err error) {
		if sub != nil {
			log.Errorf("async error for sub [%s]: %v", sub.Subject, err)
			os.Exit(1)
		} else {
			log.Errorf("async error: %v", err)
			os.Exit(1)
		}
	}

	closedh := func(nc *nats.Conn) {
		err := nc.LastError()
		if err != nil {
			log.Errorf("NATS connection closed: %v", err)
			os.Exit(1)
		}
	}

	connh := func(nc *nats.Conn) {
		err := nc.LastError()
		if err == nil {
			log.Debugf("Connected to %s", nc.ConnectedUrl())
		}
	}

	opts := []nats.Option{
		nats.MaxReconnects(100),
		nats.NoEcho(),
		nats.ErrorHandler(errh),
		nats.ClosedHandler(closedh),
		nats.ReconnectHandler(connh),
	}

	// Try NATS context file first, fall back to default URL
	home, err := os.UserHomeDir()
	if err == nil {
		ctxFile := home + "/.config/nats/context/" + nctx + ".json"
		if fileExist(ctxFile) {
			log.Debugf("Using NATS context %s", nctx)
			// nats contexts store credentials, let nats.go handle it
		}
		credsFile := home + "/.piper.creds"
		if fileExist(credsFile) {
			log.Debugf("Using credentials in %s", credsFile)
			opts = append(opts, nats.UserCredentials(credsFile))
		}
	}

	nc, err := nats.Connect(nats.DefaultURL, opts...)
	if err != nil {
		return nil, err
	}

	log.Debugf("Connected to %s", nc.ConnectedUrl())

	return nc, err
}

func fileExist(f string) bool {
	_, err := os.Stat(f)
	return !os.IsNotExist(err)
}

func decompress(data []byte) (string, error) {
	b := bytes.NewBuffer(data)
	zr, err := gzip.NewReader(b)
	if err != nil {
		return "", err
	}

	d, err := io.ReadAll(zr)
	if err != nil {
		return "", err
	}

	return string(d), nil
}

func compress(data string) ([]byte, error) {
	var b bytes.Buffer

	gz := gzip.NewWriter(&b)

	_, err := gz.Write([]byte(data))
	if err != nil {
		return nil, err
	}

	if err := gz.Flush(); err != nil {
		return nil, err
	}

	if err := gz.Close(); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func createStream(js nats.JetStreamContext) error {
	_, err := js.StreamInfo("PIPER")
	if err == nil {
		return nil // stream already exists
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:      "PIPER",
		Subjects:  []string{"piper.ASYNC.>"},
		Retention: nats.WorkQueuePolicy,
		MaxAge:    24 * 7 * time.Hour,
		Storage:   nats.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("could not create stream: %w", err)
	}

	return nil
}

func createConsumer(name string, js nats.JetStreamContext) error {
	_, err := js.ConsumerInfo("PIPER", name)
	if err == nil {
		log.Debugf("Consumer %s already exists", name)
		return nil
	}

	_, err = js.AddConsumer("PIPER", &nats.ConsumerConfig{
		Durable:       name,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		FilterSubject: asyncName(name),
	})
	if err != nil {
		return fmt.Errorf("could not create consumer: %w", err)
	}

	return nil
}

func asyncName(s string) string {
	return "piper.ASYNC." + s
}

func syncName(s string) string {
	return "piper." + s
}

func parseDuration(s string) time.Duration {
	if s == "" {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}
