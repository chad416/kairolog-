package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"kairolog/internal/topic"
)

func TestHealth(t *testing.T) {
	srv := newTestServer(t)

	recorder := performRequest(srv.Handler, http.MethodGet, "/health", "")

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response healthResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if response.Status != "ok" {
		t.Fatalf("expected status ok, got %q", response.Status)
	}
}

func TestCreateTopicAndListTopics(t *testing.T) {
	srv := newTestServer(t)

	recorder := createTopic(t, srv.Handler, "orders", 3)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, recorder.Code)
	}

	recorder = performRequest(srv.Handler, http.MethodGet, "/topics", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response topicsResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	expected := []string{"orders"}
	if !reflect.DeepEqual(response.Topics, expected) {
		t.Fatalf("expected %v, got %v", expected, response.Topics)
	}
}

func TestFetchReturnsRecordsFromOffset(t *testing.T) {
	srv := newTestServer(t)

	createTopic(t, srv.Handler, "orders", 1)
	produceMessage(t, srv.Handler, "orders", 0, "first")
	produceMessage(t, srv.Handler, "orders", 0, "second")
	produceMessage(t, srv.Handler, "orders", 0, "third")

	records := fetchRecords(t, srv.Handler, "/fetch?topic=orders&partition=0&offset=1")
	expected := []fetchRecord{
		{Offset: 1, Message: "second"},
		{Offset: 2, Message: "third"},
	}

	if !reflect.DeepEqual(records, expected) {
		t.Fatalf("expected %v, got %v", expected, records)
	}
}

func newTestServer(t *testing.T) *http.Server {
	t.Helper()
	chdirTemp(t)

	return newServer(topic.NewManager())
}

func createTopic(t *testing.T, handler http.Handler, name string, partitions int) *httptest.ResponseRecorder {
	t.Helper()

	body := `{"name":"` + name + `","partitions":` + strconv.Itoa(partitions) + `}`
	return performRequest(handler, http.MethodPost, "/topics", body)
}

func produceMessage(t *testing.T, handler http.Handler, topicName string, partition int, message string) *httptest.ResponseRecorder {
	t.Helper()

	body := `{"topic":"` + topicName + `","partition":` + strconv.Itoa(partition) + `,"message":"` + message + `"}`
	return performRequest(handler, http.MethodPost, "/produce", body)
}

func fetchRecords(t *testing.T, handler http.Handler, path string) []fetchRecord {
	t.Helper()

	recorder := performRequest(handler, http.MethodGet, path, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response fetchResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	return response.Records
}

func performRequest(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	return recorder
}

func chdirTemp(t *testing.T) {
	t.Helper()

	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}

	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatalf("failed to change working directory: %v", err)
	}

	t.Cleanup(func() {
		if err := os.Chdir(originalDir); err != nil {
			t.Fatalf("failed to restore working directory: %v", err)
		}
	})
}
