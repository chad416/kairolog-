package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestProduceStoresMessage(t *testing.T) {
	srv := New()

	recorder := performRequest(t, srv.Handler, http.MethodPost, "/produce", `{"message":"hello"}`)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response produceResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if response.Status != "stored" {
		t.Fatalf("expected status stored, got %q", response.Status)
	}

	messages := readMessages(t, srv.Handler)
	expected := []string{"hello"}

	if !reflect.DeepEqual(messages, expected) {
		t.Fatalf("expected %v, got %v", expected, messages)
	}
}

func TestMessagesReturnsStoredMessages(t *testing.T) {
	srv := New()

	performRequest(t, srv.Handler, http.MethodPost, "/produce", `{"message":"first"}`)
	performRequest(t, srv.Handler, http.MethodPost, "/produce", `{"message":"second"}`)

	messages := readMessages(t, srv.Handler)
	expected := []string{"first", "second"}

	if !reflect.DeepEqual(messages, expected) {
		t.Fatalf("expected %v, got %v", expected, messages)
	}
}

func performRequest(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	return recorder
}

func readMessages(t *testing.T, handler http.Handler) []string {
	t.Helper()

	recorder := performRequest(t, handler, http.MethodGet, "/messages", "")

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response messagesResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	return response.Messages
}
