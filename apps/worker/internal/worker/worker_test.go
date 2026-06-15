package worker

import (
	"errors"
	"testing"
	"time"

	"mediaflow/apps/worker/internal/job"
)

func TestClassifyFailureSchedulesBackoffBelowMax(t *testing.T) {
	base := 30 * time.Second
	max := 3

	cases := []struct {
		attempts  int
		wantDelay time.Duration
	}{
		{attempts: 1, wantDelay: 60 * time.Second},  // 30s * 2^1
		{attempts: 2, wantDelay: 120 * time.Second}, // 30s * 2^2
	}
	for _, tc := range cases {
		retry, delay := classifyFailure(errors.New("network blip"), tc.attempts, max, base)
		if !retry {
			t.Fatalf("attempts=%d: expected retry", tc.attempts)
		}
		if delay != tc.wantDelay {
			t.Fatalf("attempts=%d: expected delay %s, got %s", tc.attempts, tc.wantDelay, delay)
		}
	}
}

func TestClassifyFailureStopsAtMaxAttempts(t *testing.T) {
	retry, _ := classifyFailure(errors.New("network blip"), 3, 3, 30*time.Second)
	if retry {
		t.Fatal("expected no retry once attempts reach max")
	}
}

func TestClassifyFailureNeverRetriesPermanent(t *testing.T) {
	retry, _ := classifyFailure(job.Permanent(errors.New("no video stream")), 1, 3, 30*time.Second)
	if retry {
		t.Fatal("permanent failures must not be retried even below max attempts")
	}
}

func TestTerminalReasonDistinguishesCause(t *testing.T) {
	perm := terminalReason(job.Permanent(errors.New("no video stream")), 1, 3)
	if got := "permanent failure: no video stream"; perm != got {
		t.Fatalf("permanent reason: want %q, got %q", got, perm)
	}

	exhausted := terminalReason(errors.New("network blip"), 3, 3)
	if want := "attempts exhausted (3/3): network blip"; exhausted != want {
		t.Fatalf("exhausted reason: want %q, got %q", want, exhausted)
	}
}
