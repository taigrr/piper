package main

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

func TestTimeoutFlagParsesValidDuration(t *testing.T) {
	var parsed time.Duration
	flags := pflag.NewFlagSet("piper", pflag.ContinueOnError)
	flags.DurationVar(&parsed, "timeout", 0, "")

	if err := flags.Parse([]string{"--timeout", "5m"}); err != nil {
		t.Fatalf("Parse(valid): unexpected error: %v", err)
	}
	if parsed != 5*time.Minute {
		t.Fatalf("parsed timeout = %v, want %v", parsed, 5*time.Minute)
	}
}

func TestTimeoutFlagRejectsInvalidDuration(t *testing.T) {
	var parsed time.Duration
	flags := pflag.NewFlagSet("piper", pflag.ContinueOnError)
	flags.DurationVar(&parsed, "timeout", 0, "")

	err := flags.Parse([]string{"--timeout", "invalid"})
	if err == nil {
		t.Fatal("Parse(invalid): expected error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid argument \"invalid\"") {
		t.Fatalf("Parse(invalid) error = %q, want invalid argument message", err)
	}
}
