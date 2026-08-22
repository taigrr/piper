package main

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newContextCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "piper", Run: func(*cobra.Command, []string) {}}
	cmd.Flags().String("context", "piper", "")
	return cmd
}

func TestContextIsExplicit(t *testing.T) {
	t.Run("flag changed", func(t *testing.T) {
		envContextSet = false
		cmd := newContextCmd()
		if err := cmd.Flags().Parse([]string{"--context", "prod"}); err != nil {
			t.Fatal(err)
		}
		if !contextIsExplicit(cmd) {
			t.Fatal("contextIsExplicit() = false with --context set, want true")
		}
	})

	t.Run("env set", func(t *testing.T) {
		envContextSet = true
		t.Cleanup(func() { envContextSet = false })
		cmd := newContextCmd()
		if !contextIsExplicit(cmd) {
			t.Fatal("contextIsExplicit() = false with env set, want true")
		}
	})

	t.Run("default bare", func(t *testing.T) {
		envContextSet = false
		cmd := newContextCmd()
		if contextIsExplicit(cmd) {
			t.Fatal("contextIsExplicit() = true with no flag/env, want false")
		}
	})
}

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
