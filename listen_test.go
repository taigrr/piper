package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type fakeFetcher struct {
	fetch func(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error)
}

func (f fakeFetcher) Fetch(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
	return f.fetch(batch, opts...)
}

func TestWaitForAsyncMessageReturnsFirstMessage(t *testing.T) {
	compressed, err := compress("hello")
	if err != nil {
		t.Fatalf("compress: %v", err)
	}

	calls := 0
	fetcher := fakeFetcher{fetch: func(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
		calls++
		switch calls {
		case 1:
			return nil, nats.ErrTimeout
		case 2:
			return []*nats.Msg{{Data: compressed}}, nil
		default:
			t.Fatalf("unexpected extra fetch call %d", calls)
			return nil, nil
		}
	}}

	msg, err := waitForAsyncMessage(context.Background(), fetcher)
	if err != nil {
		t.Fatalf("waitForAsyncMessage: %v", err)
	}
	if msg == nil {
		t.Fatal("waitForAsyncMessage returned nil message")
	}
	if calls != 2 {
		t.Fatalf("Fetch called %d times, want 2", calls)
	}
	if string(msg.Data) != string(compressed) {
		t.Fatal("returned message data mismatch")
	}
}

func TestWaitForAsyncMessageHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	fetcher := fakeFetcher{fetch: func(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
		cancel()
		return nil, nats.ErrTimeout
	}}

	_, err := waitForAsyncMessage(ctx, fetcher)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForAsyncMessage error = %v, want context.Canceled", err)
	}
}

func TestWaitForAsyncMessageReturnsNonTimeoutErrors(t *testing.T) {
	expected := errors.New("boom")
	fetcher := fakeFetcher{fetch: func(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
		return nil, expected
	}}

	_, err := waitForAsyncMessage(context.Background(), fetcher)
	if !errors.Is(err, expected) {
		t.Fatalf("waitForAsyncMessage error = %v, want %v", err, expected)
	}
}

func TestWaitForAsyncMessagePassesOnlyMaxWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	fetcher := fakeFetcher{fetch: func(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
		// nats.go rejects Fetch when both a context and a max-wait option are
		// set (ErrContextAndTimeout), and rejects a deadline-less context
		// (ErrNoDeadlineContext). Only nats.MaxWait must be passed here.
		if len(opts) != 1 {
			t.Fatalf("expected exactly the max wait option, got %d", len(opts))
		}
		return nil, context.Canceled
	}}

	_, err := waitForAsyncMessage(ctx, fetcher)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForAsyncMessage error = %v, want context.Canceled", err)
	}
}
