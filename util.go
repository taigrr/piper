package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

type natsContextConfig struct {
	URL      string `json:"url"`
	Token    string `json:"token"`
	User     string `json:"user"`
	Password string `json:"password"`
	Creds    string `json:"creds"`
	Nkey     string `json:"nkey"`
	Cert     string `json:"cert"`
	Key      string `json:"key"`
	CA       string `json:"ca"`
}

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

	url := nats.DefaultURL
	home, err := os.UserHomeDir()
	if err == nil {
		ctxFile := filepath.Join(home, ".config", "nats", "context", nctx+".json")
		ctxConfig, ctxErr := loadContextConfig(ctxFile)
		if ctxErr != nil {
			return nil, ctxErr
		}
		if ctxConfig != nil {
			log.Debugf("Using NATS context %s", nctx)
			url = contextURL(ctxConfig.URL)
			ctxOpts, optErr := contextOptions(ctxConfig)
			if optErr != nil {
				return nil, optErr
			}
			opts = append(opts, ctxOpts...)
		}

		credsFile := filepath.Join(home, ".piper.creds")
		if ctxConfig == nil || strings.TrimSpace(ctxConfig.Creds) == "" {
			if fileExist(credsFile) {
				log.Debugf("Using credentials in %s", credsFile)
				opts = append(opts, nats.UserCredentials(credsFile))
			}
		}
	}

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, err
	}

	log.Debugf("Connected to %s", nc.ConnectedUrl())

	return nc, err
}

func loadContextConfig(path string) (*natsContextConfig, error) {
	if !fileExist(path) {
		return nil, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not read NATS context %s: %w", path, err)
	}

	var cfg natsContextConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("could not parse NATS context %s: %w", path, err)
	}

	cfg.resolvePaths(filepath.Dir(path))

	return &cfg, nil
}

func (cfg *natsContextConfig) resolvePaths(baseDir string) {
	if cfg == nil {
		return
	}

	cfg.Creds = resolvePath(baseDir, cfg.Creds)
	cfg.Nkey = resolvePath(baseDir, cfg.Nkey)
	cfg.Cert = resolvePath(baseDir, cfg.Cert)
	cfg.Key = resolvePath(baseDir, cfg.Key)
	cfg.CA = resolvePath(baseDir, cfg.CA)
}

func resolvePath(baseDir, path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}

	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err == nil {
			return filepath.Join(home, strings.TrimPrefix(path, "~/"))
		}
	}

	if filepath.IsAbs(path) {
		return path
	}

	if baseDir == "" {
		return path
	}

	return filepath.Join(baseDir, path)
}

func contextURL(url string) string {
	url = strings.TrimSpace(url)
	if url == "" {
		return nats.DefaultURL
	}
	return url
}

func contextOptions(cfg *natsContextConfig) ([]nats.Option, error) {
	var opts []nats.Option
	if cfg == nil {
		return opts, nil
	}

	if creds := strings.TrimSpace(cfg.Creds); creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}
	if token := strings.TrimSpace(cfg.Token); token != "" {
		opts = append(opts, nats.Token(token))
	}
	if user := strings.TrimSpace(cfg.User); user != "" {
		opts = append(opts, nats.UserInfo(user, cfg.Password))
	}
	if nkey := strings.TrimSpace(cfg.Nkey); nkey != "" {
		opt, err := nats.NkeyOptionFromSeed(nkey)
		if err != nil {
			return nil, fmt.Errorf("could not configure NKey from %s: %w", nkey, err)
		}
		opts = append(opts, opt)
	}
	if cert := strings.TrimSpace(cfg.Cert); cert != "" {
		key := strings.TrimSpace(cfg.Key)
		ca := strings.TrimSpace(cfg.CA)
		if key != "" {
			opts = append(opts, nats.ClientCert(cert, key))
		}
		if ca != "" {
			opts = append(opts, nats.RootCAs(ca))
		}
	} else if ca := strings.TrimSpace(cfg.CA); ca != "" {
		opts = append(opts, nats.RootCAs(ca))
	}

	return opts, nil
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
	defer zr.Close()

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
	shouldCreate, err := shouldCreateStream(err)
	if err != nil {
		return err
	}
	if !shouldCreate {
		return nil
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
	shouldCreate, err := shouldCreateConsumer(err)
	if err != nil {
		return err
	}
	if !shouldCreate {
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

func shouldCreateStream(err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	if errors.Is(err, nats.ErrStreamNotFound) {
		return true, nil
	}
	return false, fmt.Errorf("could not check stream: %w", err)
}

func shouldCreateConsumer(err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	if errors.Is(err, nats.ErrConsumerNotFound) {
		return true, nil
	}
	return false, fmt.Errorf("could not check consumer: %w", err)
}

func asyncName(s string) string {
	return "piper.ASYNC." + s
}

func syncName(s string) string {
	return "piper." + s
}
