package main

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newContextCmdTree() (*cobra.Command, *cobra.Command) {
	root := &cobra.Command{Use: "piper"}
	root.PersistentFlags().String("context", "piper", "")
	sub := &cobra.Command{Use: "listen", Run: func(*cobra.Command, []string) {}}
	root.AddCommand(sub)
	return root, sub
}

func TestContextIsExplicit(t *testing.T) {
	t.Run("flag changed on subcommand", func(t *testing.T) {
		envContextSet = false
		t.Cleanup(func() { envContextSet = false })
		root, _ := newContextCmdTree()
		root.SetArgs([]string{"listen", "--context", "prod"})
		var got bool
		root.Commands()[0].Run = func(cmd *cobra.Command, _ []string) { got = contextIsExplicit(cmd) }
		if err := root.Execute(); err != nil {
			t.Fatal(err)
		}
		if !got {
			t.Fatal("contextIsExplicit() = false with --context set on subcommand, want true")
		}
	})

	t.Run("env set", func(t *testing.T) {
		envContextSet = true
		t.Cleanup(func() { envContextSet = false })
		_, sub := newContextCmdTree()
		if !contextIsExplicit(sub) {
			t.Fatal("contextIsExplicit() = false with env set, want true")
		}
	})

	t.Run("default bare", func(t *testing.T) {
		envContextSet = false
		t.Cleanup(func() { envContextSet = false })
		root, _ := newContextCmdTree()
		root.SetArgs([]string{"listen"})
		var got bool
		root.Commands()[0].Run = func(cmd *cobra.Command, _ []string) { got = contextIsExplicit(cmd) }
		if err := root.Execute(); err != nil {
			t.Fatal(err)
		}
		if got {
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
