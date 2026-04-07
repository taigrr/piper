package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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
