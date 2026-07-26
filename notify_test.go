package main

import (
	"context"
	"fmt"
	"testing"
)

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
