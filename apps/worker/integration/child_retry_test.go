//go:build integration

package integration

import (
	"testing"
	"time"

	"mediaflow/apps/worker/internal/job"
)

// TestChildRetryQueuesDeadLetterBackToStage proves the per-stage retry topology
// added in M7 slice B: a message parked in a stage's retry queue with a short TTL
// is dead-lettered back to that stage's own main queue — so a retrying rendition
// re-runs as a rendition, never restarting the whole plan.
func TestChildRetryQueuesDeadLetterBackToStage(t *testing.T) {
	db := openDB(t)
	newTestWorker(t, db) // declares the retry/DLQ topology
	purgeQueues(t)

	for _, c := range []struct{ retryKey, mainKey string }{
		{job.RenditionRetryRoutingKey, job.RenditionRoutingKey},
		{job.FinalizeRetryRoutingKey, job.FinalizeRoutingKey},
	} {
		body := []byte(`{"jobId":"probe-` + c.mainKey + `"}`)
		publishRawWithTTL(t, c.retryKey, body, "800")

		// Nothing consumes the retry queue; after the TTL the broker routes it back.
		msg := getMessage(t, c.mainKey, 10*time.Second)
		if string(msg.Body) != string(body) {
			t.Fatalf("retry %s did not dead-letter to %s: got %s", c.retryKey, c.mainKey, msg.Body)
		}
	}
}
