package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestCompressDecompress(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"short string", "hello world"},
		{"unicode", "こんにちは世界 🌍"},
		{"multiline", "line1\nline2\nline3"},
		{"large", string(make([]byte, 10000))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := compress(tt.input)
			if err != nil {
				t.Fatalf("compress(%q): unexpected error: %v", tt.input, err)
			}

			got, err := decompress(compressed)
			if err != nil {
				t.Fatalf("decompress: unexpected error: %v", err)
			}

			if got != tt.input {
				t.Errorf("roundtrip mismatch: got %q, want %q", got, tt.input)
			}
		})
	}
}

func TestDecompressInvalid(t *testing.T) {
	_, err := decompress([]byte("not gzip data"))
	if err == nil {
		t.Error("decompress(invalid): expected error, got nil")
	}
}

func TestFileExist(t *testing.T) {
	// Existing file
	tmpFile := filepath.Join(t.TempDir(), "testfile")
	if err := os.WriteFile(tmpFile, []byte("test"), 0o644); err != nil {
		t.Fatal(err)
	}

	if !fileExist(tmpFile) {
		t.Errorf("fileExist(%q): expected true for existing file", tmpFile)
	}

	// Non-existent file
	if fileExist(filepath.Join(t.TempDir(), "nonexistent")) {
		t.Error("fileExist(nonexistent): expected false")
	}
}

func TestAsyncName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"test", "piper.ASYNC.test"},
		{"my-pipe", "piper.ASYNC.my-pipe"},
		{"", "piper.ASYNC."},
	}

	for _, tt := range tests {
		got := asyncName(tt.input)
		if got != tt.want {
			t.Errorf("asyncName(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestSyncName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"test", "piper.test"},
		{"my-pipe", "piper.my-pipe"},
		{"", "piper."},
	}

	for _, tt := range tests {
		got := syncName(tt.input)
		if got != tt.want {
			t.Errorf("syncName(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
	}{
		{"", 0},
		{"invalid", 0},
		{"30s", 30 * time.Second},
		{"5m", 5 * time.Minute},
		{"1h30m", time.Hour + 30*time.Minute},
		{"100ms", 100 * time.Millisecond},
	}

	for _, tt := range tests {
		got := parseDuration(tt.input)
		if got != tt.want {
			t.Errorf("parseDuration(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestGetVersion(t *testing.T) {
	// With default "dev" version, should return build info or "development"
	v := getVersion()
	if v == "" {
		t.Error("getVersion() returned empty string")
	}
}

func TestGetVersionCustom(t *testing.T) {
	original := version
	defer func() { version = original }()

	version = "1.2.3"
	v := getVersion()
	if v != "1.2.3" {
		t.Errorf("getVersion() = %q, want %q", v, "1.2.3")
	}
}

func TestCompressLargePayload(t *testing.T) {
	// Verify compression actually reduces size for compressible data
	input := ""
	for range 1000 {
		input += "the quick brown fox jumps over the lazy dog "
	}

	compressed, err := compress(input)
	if err != nil {
		t.Fatalf("compress: unexpected error: %v", err)
	}

	if len(compressed) >= len(input) {
		t.Errorf("compressed size (%d) should be smaller than input (%d)", len(compressed), len(input))
	}

	got, err := decompress(compressed)
	if err != nil {
		t.Fatalf("decompress: unexpected error: %v", err)
	}

	if got != input {
		t.Error("roundtrip mismatch for large payload")
	}
}

func TestDecompressTruncated(t *testing.T) {
	// Compress valid data, then truncate
	compressed, err := compress("hello world")
	if err != nil {
		t.Fatal(err)
	}

	_, err = decompress(compressed[:len(compressed)/2])
	if err == nil {
		t.Error("decompress(truncated): expected error, got nil")
	}
}

func TestDecompressEmpty(t *testing.T) {
	_, err := decompress([]byte{})
	if err == nil {
		t.Error("decompress(empty): expected error, got nil")
	}
}

func TestFileExistDirectory(t *testing.T) {
	dir := t.TempDir()
	if !fileExist(dir) {
		t.Errorf("fileExist(%q): expected true for existing directory", dir)
	}
}

func TestParseDurationEdgeCases(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
	}{
		{"0s", 0},
		{"0m", 0},
		{"24h", 24 * time.Hour},
		{"1h1m1s", time.Hour + time.Minute + time.Second},
		{"-5s", -5 * time.Second},
	}

	for _, tt := range tests {
		got := parseDuration(tt.input)
		if got != tt.want {
			t.Errorf("parseDuration(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestLoadContextConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "piper.json")
	content := `{
		"url": "nats://demo.nats.io:4222",
		"creds": "/tmp/piper.creds",
		"user": "demo",
		"password": "secret",
		"token": "tok",
		"nkey": "/tmp/piper.nk",
		"cert": "/tmp/client.crt",
		"key": "/tmp/client.key",
		"ca": "/tmp/ca.pem"
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := loadContextConfig(path)
	if err != nil {
		t.Fatalf("loadContextConfig() unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("loadContextConfig() returned nil config")
	}
	if cfg.URL != "nats://demo.nats.io:4222" || cfg.Creds != "/tmp/piper.creds" || cfg.User != "demo" || cfg.Password != "secret" || cfg.Token != "tok" || cfg.Nkey != "/tmp/piper.nk" || cfg.Cert != "/tmp/client.crt" || cfg.Key != "/tmp/client.key" || cfg.CA != "/tmp/ca.pem" {
		t.Fatalf("loadContextConfig() returned unexpected config: %#v", cfg)
	}
}

func TestLoadContextConfigResolvesRelativePaths(t *testing.T) {
	ctxDir := filepath.Join(t.TempDir(), "context")
	if err := os.MkdirAll(ctxDir, 0o755); err != nil {
		t.Fatal(err)
	}

	home := t.TempDir()
	t.Setenv("HOME", home)

	path := filepath.Join(ctxDir, "piper.json")
	content := `{
		"creds": "creds/piper.creds",
		"nkey": "./keys/piper.nk",
		"cert": "~/certs/client.crt",
		"key": "tls/client.key",
		"ca": "tls/ca.pem"
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := loadContextConfig(path)
	if err != nil {
		t.Fatalf("loadContextConfig() unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("loadContextConfig() returned nil config")
	}

	if got, want := cfg.Creds, filepath.Join(ctxDir, "creds", "piper.creds"); got != want {
		t.Fatalf("cfg.Creds = %q, want %q", got, want)
	}
	if got, want := cfg.Nkey, filepath.Join(ctxDir, "keys", "piper.nk"); got != want {
		t.Fatalf("cfg.Nkey = %q, want %q", got, want)
	}
	if got, want := cfg.Cert, filepath.Join(home, "certs", "client.crt"); got != want {
		t.Fatalf("cfg.Cert = %q, want %q", got, want)
	}
	if got, want := cfg.Key, filepath.Join(ctxDir, "tls", "client.key"); got != want {
		t.Fatalf("cfg.Key = %q, want %q", got, want)
	}
	if got, want := cfg.CA, filepath.Join(ctxDir, "tls", "ca.pem"); got != want {
		t.Fatalf("cfg.CA = %q, want %q", got, want)
	}
}

func TestLoadContextConfigMissing(t *testing.T) {
	cfg, err := loadContextConfig(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatalf("loadContextConfig() missing returned error: %v", err)
	}
	if cfg != nil {
		t.Fatalf("loadContextConfig() missing = %#v, want nil", cfg)
	}
}

func TestLoadContextConfigInvalid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broken.json")
	if err := os.WriteFile(path, []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := loadContextConfig(path)
	if err == nil {
		t.Fatal("loadContextConfig() expected error for invalid JSON")
	}
	if cfg != nil {
		t.Fatalf("loadContextConfig() invalid = %#v, want nil", cfg)
	}
}

func TestResolvePath(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "context")
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		t.Fatal(err)
	}

	home := t.TempDir()
	t.Setenv("HOME", home)

	if got, want := resolvePath(baseDir, "relative/file.txt"), filepath.Join(baseDir, "relative", "file.txt"); got != want {
		t.Fatalf("resolvePath(relative) = %q, want %q", got, want)
	}
	if got, want := resolvePath(baseDir, "~/config/file.txt"), filepath.Join(home, "config", "file.txt"); got != want {
		t.Fatalf("resolvePath(home) = %q, want %q", got, want)
	}
	if got, want := resolvePath(baseDir, "/tmp/file.txt"), "/tmp/file.txt"; got != want {
		t.Fatalf("resolvePath(abs) = %q, want %q", got, want)
	}
}

func TestContextURL(t *testing.T) {
	if got := contextURL(""); got != nats.DefaultURL {
		t.Fatalf("contextURL(empty) = %q, want %q", got, nats.DefaultURL)
	}
	if got := contextURL("  nats://example:4222  "); got != "nats://example:4222" {
		t.Fatalf("contextURL(trimmed) = %q", got)
	}
}

func TestContextOptions(t *testing.T) {
	cfg := &natsContextConfig{
		Creds: "/tmp/piper.creds",
		Token: "tok",
		User:  "demo",
		Cert:  "/tmp/client.crt",
		Key:   "/tmp/client.key",
		CA:    "/tmp/ca.pem",
	}

	opts, err := contextOptions(cfg)
	if err != nil {
		t.Fatalf("contextOptions() unexpected error: %v", err)
	}
	if len(opts) != 5 {
		t.Fatalf("contextOptions() len = %d, want 5", len(opts))
	}

	nilOpts, err := contextOptions(nil)
	if err != nil {
		t.Fatalf("contextOptions(nil) unexpected error: %v", err)
	}
	if len(nilOpts) != 0 {
		t.Fatalf("contextOptions(nil) len = %d, want 0", len(nilOpts))
	}
}

func TestContextOptionsInvalidNkey(t *testing.T) {
	_, err := contextOptions(&natsContextConfig{Nkey: filepath.Join(t.TempDir(), "missing.nk")})
	if err == nil {
		t.Fatal("contextOptions() expected error for invalid nkey seed")
	}
}

func TestAsyncSyncNameConsistency(t *testing.T) {
	// Async names should have ASYNC prefix, sync should not
	name := "test-pipe"
	asyncN := asyncName(name)
	syncN := syncName(name)

	if asyncN == syncN {
		t.Error("async and sync names should differ")
	}

	if asyncN != "piper.ASYNC.test-pipe" {
		t.Errorf("asyncName unexpected: %s", asyncN)
	}

	if syncN != "piper.test-pipe" {
		t.Errorf("syncName unexpected: %s", syncN)
	}
}

type errReader struct{}

func (errReader) Read(_ []byte) (int, error) {
	return 0, errors.New("boom")
}

func TestReadMessage(t *testing.T) {
	t.Run("reads full input", func(t *testing.T) {
		input := strings.Repeat("abcdefghijklmnopqrstuvwxyz", 400)

		got, err := readMessage(bytes.NewBufferString(input))
		if err != nil {
			t.Fatalf("readMessage: unexpected error: %v", err)
		}

		if got != input {
			t.Fatalf("readMessage mismatch: got %d bytes, want %d", len(got), len(input))
		}
	})

	t.Run("propagates reader errors", func(t *testing.T) {
		_, err := readMessage(errReader{})
		if err == nil {
			t.Fatal("readMessage: expected error, got nil")
		}
	})
}
