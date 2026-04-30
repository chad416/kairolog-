package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	"kairolog/internal/topic"
)

func TestHealth(t *testing.T) {
	srv := newTestServer(t)

	recorder := performRequest(srv.Handler, http.MethodGet, "/health", nil)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response healthResponse
	decodeJSON(t, recorder, &response)

	if response.Status != "ok" {
		t.Fatalf("expected status %q, got %q", "ok", response.Status)
	}
}

func TestCreateTopicAndListTopics(t *testing.T) {
	srv := newTestServer(t)

	recorder := createTopic(t, srv.Handler, "orders", 3)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, recorder.Code)
	}

	recorder = performRequest(srv.Handler, http.MethodGet, "/topics", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response topicsResponse
	decodeJSON(t, recorder, &response)

	expected := []string{"orders"}
	if !reflect.DeepEqual(response.Topics, expected) {
		t.Fatalf("expected %v, got %v", expected, response.Topics)
	}
}

func TestProduceStoresMessageInPartition(t *testing.T) {
	srv := newTestServer(t)

	createTopic(t, srv.Handler, "orders", 2)

	recorder := produceMessage(t, srv.Handler, "orders", 0, "hello")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response produceResponse
	decodeJSON(t, recorder, &response)

	if response.Status != "stored" {
		t.Fatalf("expected status %q, got %q", "stored", response.Status)
	}
	if response.Offset != 0 {
		t.Fatalf("expected offset 0, got %d", response.Offset)
	}

	records := fetchRecords(t, srv.Handler, "/fetch?topic=orders&partition=0&offset=0")
	expected := []fetchRecord{
		{Offset: 0, Message: "hello"},
	}

	if !reflect.DeepEqual(records, expected) {
		t.Fatalf("expected %v, got %v", expected, records)
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

	return performRequest(handler, http.MethodPost, "/topics", createTopicRequest{
		Name:       name,
		Partitions: partitions,
	})
}

func produceMessage(t *testing.T, handler http.Handler, topicName string, partition int, message string) *httptest.ResponseRecorder {
	t.Helper()

	return performRequest(handler, http.MethodPost, "/produce", produceRequest{
		Topic:     topicName,
		Partition: partition,
		Message:   message,
	})
}

func fetchRecords(t *testing.T, handler http.Handler, path string) []fetchRecord {
	t.Helper()

	recorder := performRequest(handler, http.MethodGet, path, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response fetchResponse
	decodeJSON(t, recorder, &response)

	return response.Records
}

func performRequest(handler http.Handler, method string, path string, body interface{}) *httptest.ResponseRecorder {
	requestBody := bytes.NewReader(nil)
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			panic(err)
		}
		requestBody = bytes.NewReader(data)
	}

	req := httptest.NewRequest(method, path, requestBody)
	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	return recorder
}

func decodeJSON(t *testing.T, recorder *httptest.ResponseRecorder, target interface{}) {
	t.Helper()

	if err := json.NewDecoder(recorder.Body).Decode(target); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
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
