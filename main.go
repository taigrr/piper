package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	rd "runtime/debug"
	"strings"
	"time"

	"github.com/charmbracelet/fang"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/taigrr/jety"
)

var (
	debug         bool
	async         bool
	nctx          string
	nctxExplicit  bool
	envContextSet bool
	timeout       time.Duration

	version = "dev"
)

func main() {
	cfg := jety.NewConfigManager().WithEnvPrefix("PIPER_")
	cfg.SetDefault("async", false)
	cfg.SetDefault("debug", false)
	cfg.SetDefault("context", "piper")
	cfg.SetDefault("timeout", "")

	async = cfg.GetBool("async")
	debug = cfg.GetBool("debug")
	nctx = cfg.GetString("context")
	if v, ok := os.LookupEnv("PIPER_CONTEXT"); ok && strings.TrimSpace(v) != "" {
		envContextSet = true
	}
	if s := cfg.GetString("timeout"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			log.Fatalf("invalid PIPER_TIMEOUT value %q: %v", s, err)
		}
		timeout = d
	}

	rootCmd := &cobra.Command{
		Use:   "piper",
		Short: "Network pipes using NATS",
		Long:  "Patchbay style distributed pipe using NATS.io. Supports synchronous request/reply and asynchronous JetStream work queues.",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			log.SetOutput(os.Stderr)
			if debug {
				log.SetLevel(log.DebugLevel)
			}
		},
		Version: getVersion(),
	}

	rootCmd.PersistentFlags().StringVar(&nctx, "context", nctx, "NATS context to use for connection")
	rootCmd.PersistentFlags().BoolVarP(&async, "async", "a", async, "Operate asynchronously using JetStream work queues")
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", debug, "Enable debug logging")
	rootCmd.PersistentFlags().DurationVar(&timeout, "timeout", timeout, "How long to wait before giving up (e.g. 30s, 5m)")

	listenCmd := &cobra.Command{
		Use:   "listen <name>",
		Short: "Listen for messages on the pipe",
		Args:  cobra.ExactArgs(1),
		RunE:  runListen,
	}
	var listenGroup bool
	listenCmd.Flags().BoolVar(&listenGroup, "group", false, "Listen on a work group")

	notifyCmd := &cobra.Command{
		Use:   "notify <name> [message]",
		Short: "Notify listeners with a message",
		Long:  "Publish a message to a named pipe. If no message argument is given, reads from STDIN.",
		Args:  cobra.RangeArgs(1, 2),
		RunE:  runNotify,
	}

	setupCmd := &cobra.Command{
		Use:   "setup",
		Short: "Create JetStream configuration for async mode",
		RunE:  runSetup,
	}

	rootCmd.AddCommand(listenCmd, notifyCmd, setupCmd)

	if err := fang.Execute(context.Background(), rootCmd); err != nil {
		os.Exit(1)
	}
}

func runListen(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	nctxExplicit = contextIsExplicit(cmd)

	group, _ := cmd.Flags().GetBool("group")

	l := &Listener{
		Name:            args[0],
		Group:           group,
		Context:         nctx,
		ContextExplicit: nctxExplicit,
		DataSubj:        "piper." + args[0],
		errc:            make(chan error, 1),
	}

	return l.Listen(ctx)
}

func runNotify(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	nctxExplicit = contextIsExplicit(cmd)

	var msg string
	if len(args) > 1 {
		msg = args[1]
	}

	var sub string
	if async {
		sub = asyncName(args[0])
	} else {
		sub = syncName(args[0])
	}

	n := &Notifier{
		Name:            args[0],
		Context:         nctx,
		ContextExplicit: nctxExplicit,
		Message:         msg,
		Timeout:         timeout,
		Subject:         sub,
	}

	return n.Notify(ctx)
}

func runSetup(cmd *cobra.Command, args []string) error {
	nctxExplicit = contextIsExplicit(cmd)

	nc, err := connect(nctx, nctxExplicit)
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("could not get JetStream context: %w", err)
	}

	if err := createStream(js); err != nil {
		return err
	}

	log.Info("Created 'PIPER' Stream")
	return nil
}

func contextIsExplicit(cmd *cobra.Command) bool {
	return envContextSet || cmd.Flags().Changed("context")
}

func getVersion() string {
	if version != "dev" {
		return version
	}
	mods, ok := rd.ReadBuildInfo()
	if !ok || mods.Main.Version == "" || mods.Main.Version == "(devel)" {
		return "development"
	}
	return mods.Main.Version
}
