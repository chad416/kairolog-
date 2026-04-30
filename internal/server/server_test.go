package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
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

func TestCreateTopicRejectsInvalidInput(t *testing.T) {
	srv := newTestServer(t)

	recorder := createTopic(t, srv.Handler, "", 1)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}

	recorder = createTopic(t, srv.Handler, "orders", 0)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestCreateTopicRejectsDuplicateTopic(t *testing.T) {
	srv := newTestServer(t)

	recorder := createTopic(t, srv.Handler, "orders", 1)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, recorder.Code)
	}

	recorder = createTopic(t, srv.Handler, "orders", 1)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d", http.StatusConflict, recorder.Code)
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

func TestProduceRejectsUnknownTopicOrPartition(t *testing.T) {
	srv := newTestServer(t)

	recorder := produceMessage(t, srv.Handler, "missing", 0, "hello")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, recorder.Code)
	}

	createTopic(t, srv.Handler, "orders", 1)

	recorder = produceMessage(t, srv.Handler, "orders", 2, "hello")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, recorder.Code)
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

func TestFetchRejectsMissingOrInvalidOffset(t *testing.T) {
	srv := newTestServer(t)

	createTopic(t, srv.Handler, "orders", 1)

	recorder := performRequest(srv.Handler, http.MethodGet, "/fetch?topic=orders&partition=0", nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}

	recorder = performRequest(srv.Handler, http.MethodGet, "/fetch?topic=orders&partition=0&offset=invalid", nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestOffsetCommitAndGet(t *testing.T) {
	srv := newTestServer(t)

	recorder := commitOffset(t, srv.Handler, "analytics-workers", "orders", 0, 42)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var commitResponse offsetCommitResponse
	decodeJSON(t, recorder, &commitResponse)

	if commitResponse.Status != "committed" {
		t.Fatalf("expected status %q, got %q", "committed", commitResponse.Status)
	}

	offset := getOffset(t, srv.Handler, "/offsets?group=analytics-workers&topic=orders&partition=0")
	expected := offsetResponse{
		Group:     "analytics-workers",
		Topic:     "orders",
		Partition: 0,
		Offset:    42,
		Found:     true,
	}

	if !reflect.DeepEqual(offset, expected) {
		t.Fatalf("expected %v, got %v", expected, offset)
	}
}

func TestOffsetsReturnsNotFound(t *testing.T) {
	srv := newTestServer(t)

	offset := getOffset(t, srv.Handler, "/offsets?group=analytics-workers&topic=orders&partition=0")
	expected := offsetResponse{
		Group:     "analytics-workers",
		Topic:     "orders",
		Partition: 0,
		Offset:    0,
		Found:     false,
	}

	if !reflect.DeepEqual(offset, expected) {
		t.Fatalf("expected %v, got %v", expected, offset)
	}
}

func TestOffsetCommitRejectsInvalidInput(t *testing.T) {
	srv := newTestServer(t)

	tests := []struct {
		name string
		body interface{}
	}{
		{
			name: "missing group",
			body: map[string]interface{}{
				"topic":     "orders",
				"partition": 0,
				"offset":    42,
			},
		},
		{
			name: "empty group",
			body: offsetCommitRequest{
				Group:     "",
				Topic:     "orders",
				Partition: 0,
				Offset:    42,
			},
		},
		{
			name: "missing topic",
			body: map[string]interface{}{
				"group":     "analytics-workers",
				"partition": 0,
				"offset":    42,
			},
		},
		{
			name: "empty topic",
			body: offsetCommitRequest{
				Group:     "analytics-workers",
				Topic:     "",
				Partition: 0,
				Offset:    42,
			},
		},
		{
			name: "invalid partition",
			body: map[string]interface{}{
				"group":     "analytics-workers",
				"topic":     "orders",
				"partition": "invalid",
				"offset":    42,
			},
		},
		{
			name: "negative partition",
			body: offsetCommitRequest{
				Group:     "analytics-workers",
				Topic:     "orders",
				Partition: -1,
				Offset:    42,
			},
		},
		{
			name: "negative offset",
			body: offsetCommitRequest{
				Group:     "analytics-workers",
				Topic:     "orders",
				Partition: 0,
				Offset:    -1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := performRequest(srv.Handler, http.MethodPost, "/offsets/commit", tt.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
			}
		})
	}
}

func TestOffsetsRejectsInvalidInput(t *testing.T) {
	srv := newTestServer(t)

	tests := []struct {
		name string
		path string
	}{
		{name: "missing group", path: "/offsets?topic=orders&partition=0"},
		{name: "empty group", path: "/offsets?group=&topic=orders&partition=0"},
		{name: "missing topic", path: "/offsets?group=analytics-workers&partition=0"},
		{name: "empty topic", path: "/offsets?group=analytics-workers&topic=&partition=0"},
		{name: "missing partition", path: "/offsets?group=analytics-workers&topic=orders"},
		{name: "invalid partition", path: "/offsets?group=analytics-workers&topic=orders&partition=invalid"},
		{name: "negative partition", path: "/offsets?group=analytics-workers&topic=orders&partition=-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := performRequest(srv.Handler, http.MethodGet, tt.path, nil)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
			}
		})
	}
}

func TestGroupAssignReturnsAssignments(t *testing.T) {
	srv := newTestServer(t)

	createTopic(t, srv.Handler, "orders", 4)

	recorder := assignGroup(t, srv.Handler, "orders", "member-b", "member-a")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response groupAssignResponse
	decodeJSON(t, recorder, &response)

	expected := groupAssignResponse{
		Assignments: []groupAssignmentResponse{
			{
				MemberID: "member-a",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{0, 1}},
				},
			},
			{
				MemberID: "member-b",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{2, 3}},
				},
			},
		},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupAssignRejectsUnknownTopic(t *testing.T) {
	srv := newTestServer(t)

	recorder := assignGroup(t, srv.Handler, "missing", "member-a")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, recorder.Code)
	}
}

func TestGroupAssignRejectsWrongMethod(t *testing.T) {
	srv := newTestServer(t)

	recorder := performRequest(srv.Handler, http.MethodGet, "/groups/assign", nil)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, recorder.Code)
	}
}

func TestGroupAssignRejectsInvalidInput(t *testing.T) {
	srv := newTestServer(t)

	createTopic(t, srv.Handler, "orders", 2)

	tests := []struct {
		name string
		body interface{}
	}{
		{
			name: "missing topic",
			body: map[string]interface{}{
				"members": []map[string]string{{"id": "member-a"}},
			},
		},
		{
			name: "empty topic",
			body: groupAssignRequest{
				Topic:   "",
				Members: []groupMemberRequest{{ID: "member-a"}},
			},
		},
		{
			name: "missing members",
			body: groupAssignRequest{
				Topic: "orders",
			},
		},
		{
			name: "empty members",
			body: groupAssignRequest{
				Topic:   "orders",
				Members: []groupMemberRequest{},
			},
		},
		{
			name: "empty member ID",
			body: groupAssignRequest{
				Topic:   "orders",
				Members: []groupMemberRequest{{ID: ""}},
			},
		},
		{
			name: "duplicate member ID",
			body: groupAssignRequest{
				Topic: "orders",
				Members: []groupMemberRequest{
					{ID: "member-a"},
					{ID: "member-a"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := performRequest(srv.Handler, http.MethodPost, "/groups/assign", tt.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
			}
		})
	}
}

func TestGroupAssignRejectsInvalidJSON(t *testing.T) {
	srv := newTestServer(t)

	recorder := performRawRequest(srv.Handler, http.MethodPost, "/groups/assign", "{")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func newTestServer(t *testing.T) *http.Server {
	t.Helper()
	chdirTemp(t)

	srv, err := New()
	if err != nil {
		t.Fatalf("failed to create test server: %v", err)
	}

	return srv
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

func commitOffset(t *testing.T, handler http.Handler, group string, topicName string, partition int, offset int64) *httptest.ResponseRecorder {
	t.Helper()

	return performRequest(handler, http.MethodPost, "/offsets/commit", offsetCommitRequest{
		Group:     group,
		Topic:     topicName,
		Partition: partition,
		Offset:    offset,
	})
}

func getOffset(t *testing.T, handler http.Handler, path string) offsetResponse {
	t.Helper()

	recorder := performRequest(handler, http.MethodGet, path, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response offsetResponse
	decodeJSON(t, recorder, &response)

	return response
}

func assignGroup(t *testing.T, handler http.Handler, topicName string, memberIDs ...string) *httptest.ResponseRecorder {
	t.Helper()

	members := make([]groupMemberRequest, 0, len(memberIDs))
	for _, memberID := range memberIDs {
		members = append(members, groupMemberRequest{ID: memberID})
	}

	return performRequest(handler, http.MethodPost, "/groups/assign", groupAssignRequest{
		Topic:   topicName,
		Members: members,
	})
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

func performRawRequest(handler http.Handler, method string, path string, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
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
