package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type fakeAsyncPublisher struct {
	publish func(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error)
}

func (f fakeAsyncPublisher) Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	return f.publish(subj, data, opts...)
}

type fakeSyncRequester struct {
	request func(ctx context.Context, subj string, data []byte) (*nats.Msg, error)
}

func (f fakeSyncRequester) RequestWithContext(ctx context.Context, subj string, data []byte) (*nats.Msg, error) {
	return f.request(ctx, subj, data)
}

func TestPublishAsyncPublishesToJetStream(t *testing.T) {
	payload := []byte("hello")
	calls := 0
	publisher := fakeAsyncPublisher{
		publish: func(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
			calls++
			if subj != "piper.ASYNC.jobs" {
				t.Fatalf("subject = %q, want %q", subj, "piper.ASYNC.jobs")
			}
			if string(data) != string(payload) {
				t.Fatalf("data = %q, want %q", data, payload)
			}
			if len(opts) == 0 {
				t.Fatal("expected context publish option")
			}
			return &nats.PubAck{Stream: "PIPER"}, nil
		},
	}

	if err := publishAsync(context.Background(), publisher, "piper.ASYNC.jobs", payload); err != nil {
		t.Fatalf("publishAsync() unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("Publish called %d times, want 1", calls)
	}
}

func TestPublishAsyncWrapsPublishError(t *testing.T) {
	expected := errors.New("no stream")
	publisher := fakeAsyncPublisher{
		publish: func(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
			return nil, expected
		},
	}

	err := publishAsync(context.Background(), publisher, "piper.ASYNC.jobs", []byte("hello"))
	if !errors.Is(err, expected) {
		t.Fatalf("publishAsync() error = %v, want wrapped %v", err, expected)
	}
}

func TestPublishSyncRetriesNonContextErrors(t *testing.T) {
	calls := 0
	requester := fakeSyncRequester{
		request: func(ctx context.Context, subj string, data []byte) (*nats.Msg, error) {
			calls++
			if subj != "piper.jobs" {
				t.Fatalf("subject = %q, want %q", subj, "piper.jobs")
			}
			if string(data) != "hello" {
				t.Fatalf("data = %q, want %q", data, "hello")
			}
			if calls == 1 {
				return nil, errors.New("temporary")
			}
			return &nats.Msg{}, nil
		},
	}

	if err := publishSync(context.Background(), requester, "piper.jobs", []byte("hello"), time.Minute); err != nil {
		t.Fatalf("publishSync() unexpected error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("RequestWithContext called %d times, want 2", calls)
	}
}

func TestPublishSyncStopsRetryWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	requester := fakeSyncRequester{
		request: func(ctx context.Context, subj string, data []byte) (*nats.Msg, error) {
			return nil, errors.New("temporary")
		},
	}

	err := publishSync(ctx, requester, "piper.jobs", []byte("hello"), time.Minute)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("publishSync() error = %v, want context.Canceled", err)
	}
}

func TestRequestContextDone(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"canceled", context.Canceled, true},
		{"deadline exceeded", context.DeadlineExceeded, true},
		{"wrapped canceled", fmt.Errorf("request failed: %w", context.Canceled), true},
		{"wrapped deadline exceeded", fmt.Errorf("request failed: %w", context.DeadlineExceeded), true},
		{"other error", fmt.Errorf("request failed"), false},
		{"nil", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := requestContextDone(tt.err); got != tt.want {
				t.Fatalf("requestContextDone(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}
