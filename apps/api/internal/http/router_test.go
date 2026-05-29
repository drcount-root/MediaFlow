package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"mediaflow/apps/api/internal/config"
)

func TestHealthEndpointReturnsOK(t *testing.T) {
	router := NewRouter(config.Config{AppEnv: "test"})

	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}

	var body map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}

	if body["status"] != "ok" {
		t.Fatalf("expected status ok, got %q", body["status"])
	}

	if body["service"] != "mediaflow-api" {
		t.Fatalf("expected mediaflow-api service, got %q", body["service"])
	}
}
