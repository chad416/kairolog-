package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"kairolog/internal/consumer"
	"kairolog/internal/group"
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

func TestGroupRebalanceAssignsTopicPartitionsToRegisteredMembers(t *testing.T) {
	srv := newTestServer(t)

	createTopic(t, srv.Handler, "orders", 4)
	joinGroup(t, srv.Handler, "analytics-workers", "member-a")
	joinGroup(t, srv.Handler, "analytics-workers", "member-b")

	recorder := rebalanceGroup(t, srv.Handler, "analytics-workers", "orders")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response groupRebalanceResponse
	decodeJSON(t, recorder, &response)

	expected := groupRebalanceResponse{
		Group: "analytics-workers",
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

func TestGroupRebalanceAssignsMembersDeterministicallyByID(t *testing.T) {
	srv := newTestServer(t)

	createTopic(t, srv.Handler, "orders", 2)
	joinGroup(t, srv.Handler, "analytics-workers", "member-b")
	joinGroup(t, srv.Handler, "analytics-workers", "member-a")

	recorder := rebalanceGroup(t, srv.Handler, "analytics-workers", "orders")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response groupRebalanceResponse
	decodeJSON(t, recorder, &response)

	expected := groupRebalanceResponse{
		Group: "analytics-workers",
		Assignments: []groupAssignmentResponse{
			{
				MemberID: "member-a",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{0}},
				},
			},
			{
				MemberID: "member-b",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{1}},
				},
			},
		},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupRebalanceAssignsUnevenPartitions(t *testing.T) {
	srv := newTestServer(t)

	createTopic(t, srv.Handler, "orders", 5)
	joinGroup(t, srv.Handler, "analytics-workers", "member-a")
	joinGroup(t, srv.Handler, "analytics-workers", "member-b")

	recorder := rebalanceGroup(t, srv.Handler, "analytics-workers", "orders")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response groupRebalanceResponse
	decodeJSON(t, recorder, &response)

	expected := groupRebalanceResponse{
		Group: "analytics-workers",
		Assignments: []groupAssignmentResponse{
			{
				MemberID: "member-a",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{0, 1, 2}},
				},
			},
			{
				MemberID: "member-b",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{3, 4}},
				},
			},
		},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupRebalanceRejectsGroupWithNoMembers(t *testing.T) {
	srv := newTestServer(t)

	createTopic(t, srv.Handler, "orders", 2)

	recorder := rebalanceGroup(t, srv.Handler, "analytics-workers", "orders")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestGroupRebalanceRejectsUnknownTopic(t *testing.T) {
	srv := newTestServer(t)

	joinGroup(t, srv.Handler, "analytics-workers", "member-a")

	recorder := rebalanceGroup(t, srv.Handler, "analytics-workers", "missing")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, recorder.Code)
	}
}

func TestGroupRebalanceRejectsInvalidBody(t *testing.T) {
	srv := newTestServer(t)

	tests := []struct {
		name string
		body interface{}
	}{
		{
			name: "missing group",
			body: map[string]interface{}{
				"topic": "orders",
			},
		},
		{
			name: "empty group",
			body: groupRebalanceRequest{
				Group: "",
				Topic: "orders",
			},
		},
		{
			name: "missing topic",
			body: map[string]interface{}{
				"group": "analytics-workers",
			},
		},
		{
			name: "empty topic",
			body: groupRebalanceRequest{
				Group: "analytics-workers",
				Topic: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := performRequest(srv.Handler, http.MethodPost, "/groups/rebalance", tt.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
			}
		})
	}
}

func TestGroupRebalanceRejectsInvalidJSON(t *testing.T) {
	srv := newTestServer(t)

	recorder := performRawRequest(srv.Handler, http.MethodPost, "/groups/rebalance", "{")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestGroupRebalanceRejectsWrongMethod(t *testing.T) {
	srv := newTestServer(t)

	recorder := performRequest(srv.Handler, http.MethodGet, "/groups/rebalance", nil)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, recorder.Code)
	}
}

func TestGroupRebalanceSavesAssignments(t *testing.T) {
	srv, _, assignmentStore := newTestServerWithRegistryAndAssignmentStore(t)

	createTopic(t, srv.Handler, "orders", 4)
	joinGroup(t, srv.Handler, "analytics-workers", "member-a")
	joinGroup(t, srv.Handler, "analytics-workers", "member-b")

	recorder := rebalanceGroup(t, srv.Handler, "analytics-workers", "orders")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	assignments, found, err := assignmentStore.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get saved assignments: %v", err)
	}
	if !found {
		t.Fatal("expected saved assignments to be found")
	}

	expected := []group.Assignment{
		{
			MemberID: "member-a",
			Topics: []group.TopicAssignment{
				{Topic: "orders", Partitions: []int{0, 1}},
			},
		},
		{
			MemberID: "member-b",
			Topics: []group.TopicAssignment{
				{Topic: "orders", Partitions: []int{2, 3}},
			},
		},
	}

	if !reflect.DeepEqual(assignments, expected) {
		t.Fatalf("expected %v, got %v", expected, assignments)
	}
}

func TestGroupRebalanceWithNilAssignmentStoreReturnsInternalServerError(t *testing.T) {
	srv := newTestServerWithNilAssignmentStore(t)

	createTopic(t, srv.Handler, "orders", 2)
	joinGroup(t, srv.Handler, "analytics-workers", "member-a")

	recorder := rebalanceGroup(t, srv.Handler, "analytics-workers", "orders")
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, recorder.Code)
	}
}

func TestGroupCleanupAndRebalanceRemovesStaleMembersAndRebalancesRemainingMembers(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	createTopic(t, srv.Handler, "orders", 4)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-10*time.Minute))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", time.Now().Add(-time.Second))

	response := cleanupAndRebalanceGroup(t, srv.Handler, "analytics-workers", "orders", 300000)
	expected := groupCleanupAndRebalanceResponse{
		Group:          "analytics-workers",
		Topic:          "orders",
		TimeoutMS:      300000,
		RemovedMembers: []groupMemberResponse{{ID: "member-a"}},
		Assignments: []groupAssignmentResponse{
			{
				MemberID: "member-b",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{0, 1, 2, 3}},
				},
			},
		},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupCleanupAndRebalanceActiveMembersRemainAndReceiveAssignments(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	createTopic(t, srv.Handler, "orders", 4)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-10*time.Minute))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", time.Now().Add(-time.Second))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-c", time.Now().Add(-time.Second))

	response := cleanupAndRebalanceGroup(t, srv.Handler, "analytics-workers", "orders", 300000)
	expected := groupCleanupAndRebalanceResponse{
		Group:          "analytics-workers",
		Topic:          "orders",
		TimeoutMS:      300000,
		RemovedMembers: []groupMemberResponse{{ID: "member-a"}},
		Assignments: []groupAssignmentResponse{
			{
				MemberID: "member-b",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{0, 1}},
				},
			},
			{
				MemberID: "member-c",
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

func TestGroupCleanupAndRebalanceRemovedMembersAreReturnedSorted(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)
	staleTime := time.Now().Add(-10 * time.Minute)

	createTopic(t, srv.Handler, "orders", 1)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-c", staleTime)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", staleTime)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", staleTime)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-d", time.Now().Add(-time.Second))

	response := cleanupAndRebalanceGroup(t, srv.Handler, "analytics-workers", "orders", 300000)
	expected := groupCleanupAndRebalanceResponse{
		Group:     "analytics-workers",
		Topic:     "orders",
		TimeoutMS: 300000,
		RemovedMembers: []groupMemberResponse{
			{ID: "member-a"},
			{ID: "member-b"},
			{ID: "member-c"},
		},
		Assignments: []groupAssignmentResponse{
			{
				MemberID: "member-d",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{0}},
				},
			},
		},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupCleanupAndRebalanceAssignmentsAreDeterministicByMemberID(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	createTopic(t, srv.Handler, "orders", 2)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", time.Now().Add(-time.Second))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-time.Second))

	response := cleanupAndRebalanceGroup(t, srv.Handler, "analytics-workers", "orders", 300000)
	expected := groupCleanupAndRebalanceResponse{
		Group:          "analytics-workers",
		Topic:          "orders",
		TimeoutMS:      300000,
		RemovedMembers: []groupMemberResponse{},
		Assignments: []groupAssignmentResponse{
			{
				MemberID: "member-a",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{0}},
				},
			},
			{
				MemberID: "member-b",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{1}},
				},
			},
		},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupCleanupAndRebalanceAssignsUnevenPartitionsAfterCleanup(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	createTopic(t, srv.Handler, "orders", 5)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-time.Second))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", time.Now().Add(-time.Second))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-c", time.Now().Add(-10*time.Minute))

	response := cleanupAndRebalanceGroup(t, srv.Handler, "analytics-workers", "orders", 300000)
	expected := groupCleanupAndRebalanceResponse{
		Group:          "analytics-workers",
		Topic:          "orders",
		TimeoutMS:      300000,
		RemovedMembers: []groupMemberResponse{{ID: "member-c"}},
		Assignments: []groupAssignmentResponse{
			{
				MemberID: "member-a",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{0, 1, 2}},
				},
			},
			{
				MemberID: "member-b",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{3, 4}},
				},
			},
		},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupCleanupAndRebalanceWithNoStaleMembersRebalancesAllActiveMembers(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	createTopic(t, srv.Handler, "orders", 4)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-time.Second))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", time.Now().Add(-time.Second))

	response := cleanupAndRebalanceGroup(t, srv.Handler, "analytics-workers", "orders", 300000)
	expected := groupCleanupAndRebalanceResponse{
		Group:          "analytics-workers",
		Topic:          "orders",
		TimeoutMS:      300000,
		RemovedMembers: []groupMemberResponse{},
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

func TestGroupCleanupAndRebalanceRejectsUnknownTopic(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-time.Second))

	recorder := performRequest(srv.Handler, http.MethodPost, "/groups/cleanup-and-rebalance", groupCleanupAndRebalanceRequest{
		Group:     "analytics-workers",
		Topic:     "missing",
		TimeoutMS: 300000,
	})
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, recorder.Code)
	}
}

func TestGroupCleanupAndRebalanceRejectsGroupWithNoMembers(t *testing.T) {
	srv := newTestServer(t)

	createTopic(t, srv.Handler, "orders", 2)

	recorder := performRequest(srv.Handler, http.MethodPost, "/groups/cleanup-and-rebalance", groupCleanupAndRebalanceRequest{
		Group:     "analytics-workers",
		Topic:     "orders",
		TimeoutMS: 300000,
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestGroupCleanupAndRebalanceRejectsWhenAllMembersAreStaleAndRemoved(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)
	staleTime := time.Now().Add(-10 * time.Minute)

	createTopic(t, srv.Handler, "orders", 2)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", staleTime)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", staleTime)

	recorder := performRequest(srv.Handler, http.MethodPost, "/groups/cleanup-and-rebalance", groupCleanupAndRebalanceRequest{
		Group:     "analytics-workers",
		Topic:     "orders",
		TimeoutMS: 300000,
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestGroupCleanupAndRebalanceRejectsInvalidBody(t *testing.T) {
	srv := newTestServer(t)

	tests := []struct {
		name string
		body interface{}
	}{
		{
			name: "missing group",
			body: map[string]interface{}{
				"topic":      "orders",
				"timeout_ms": 300000,
			},
		},
		{
			name: "empty group",
			body: groupCleanupAndRebalanceRequest{
				Group:     "",
				Topic:     "orders",
				TimeoutMS: 300000,
			},
		},
		{
			name: "missing topic",
			body: map[string]interface{}{
				"group":      "analytics-workers",
				"timeout_ms": 300000,
			},
		},
		{
			name: "empty topic",
			body: groupCleanupAndRebalanceRequest{
				Group:     "analytics-workers",
				Topic:     "",
				TimeoutMS: 300000,
			},
		},
		{
			name: "missing timeout",
			body: map[string]interface{}{
				"group": "analytics-workers",
				"topic": "orders",
			},
		},
		{
			name: "zero timeout",
			body: groupCleanupAndRebalanceRequest{
				Group:     "analytics-workers",
				Topic:     "orders",
				TimeoutMS: 0,
			},
		},
		{
			name: "negative timeout",
			body: groupCleanupAndRebalanceRequest{
				Group:     "analytics-workers",
				Topic:     "orders",
				TimeoutMS: -1,
			},
		},
		{
			name: "invalid timeout",
			body: map[string]interface{}{
				"group":      "analytics-workers",
				"topic":      "orders",
				"timeout_ms": "invalid",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := performRequest(srv.Handler, http.MethodPost, "/groups/cleanup-and-rebalance", tt.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
			}
		})
	}
}

func TestGroupCleanupAndRebalanceRejectsInvalidJSON(t *testing.T) {
	srv := newTestServer(t)

	recorder := performRawRequest(srv.Handler, http.MethodPost, "/groups/cleanup-and-rebalance", "{")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestGroupCleanupAndRebalanceRejectsWrongMethod(t *testing.T) {
	srv := newTestServer(t)

	recorder := performRequest(srv.Handler, http.MethodGet, "/groups/cleanup-and-rebalance", nil)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, recorder.Code)
	}
}

func TestGroupCleanupAndRebalanceSavesAssignments(t *testing.T) {
	srv, registry, assignmentStore := newTestServerWithRegistryAndAssignmentStore(t)

	createTopic(t, srv.Handler, "orders", 4)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-10*time.Minute))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", time.Now().Add(-time.Second))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-c", time.Now().Add(-time.Second))

	response := cleanupAndRebalanceGroup(t, srv.Handler, "analytics-workers", "orders", 300000)
	expectedResponse := groupCleanupAndRebalanceResponse{
		Group:          "analytics-workers",
		Topic:          "orders",
		TimeoutMS:      300000,
		RemovedMembers: []groupMemberResponse{{ID: "member-a"}},
		Assignments: []groupAssignmentResponse{
			{
				MemberID: "member-b",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{0, 1}},
				},
			},
			{
				MemberID: "member-c",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{2, 3}},
				},
			},
		},
	}

	if !reflect.DeepEqual(response, expectedResponse) {
		t.Fatalf("expected %v, got %v", expectedResponse, response)
	}

	assignments, found, err := assignmentStore.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get saved assignments: %v", err)
	}
	if !found {
		t.Fatal("expected saved assignments to be found")
	}

	expectedAssignments := []group.Assignment{
		{
			MemberID: "member-b",
			Topics: []group.TopicAssignment{
				{Topic: "orders", Partitions: []int{0, 1}},
			},
		},
		{
			MemberID: "member-c",
			Topics: []group.TopicAssignment{
				{Topic: "orders", Partitions: []int{2, 3}},
			},
		},
	}

	if !reflect.DeepEqual(assignments, expectedAssignments) {
		t.Fatalf("expected %v, got %v", expectedAssignments, assignments)
	}
}

func TestGroupCleanupAndRebalanceWithNilAssignmentStoreReturnsInternalServerError(t *testing.T) {
	srv, registry := newTestServerWithNilAssignmentStoreAndRegistry(t)

	createTopic(t, srv.Handler, "orders", 2)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-time.Second))

	recorder := performRequest(srv.Handler, http.MethodPost, "/groups/cleanup-and-rebalance", groupCleanupAndRebalanceRequest{
		Group:     "analytics-workers",
		Topic:     "orders",
		TimeoutMS: 300000,
	})
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, recorder.Code)
	}
}

func TestGroupAssignmentsReturnsSavedAssignmentsAfterRebalance(t *testing.T) {
	srv := newTestServer(t)

	createTopic(t, srv.Handler, "orders", 4)
	joinGroup(t, srv.Handler, "analytics-workers", "member-a")
	joinGroup(t, srv.Handler, "analytics-workers", "member-b")
	rebalanceGroup(t, srv.Handler, "analytics-workers", "orders")

	response := getGroupAssignments(t, srv.Handler, "analytics-workers", "orders")
	expected := groupAssignmentsResponse{
		Group: "analytics-workers",
		Topic: "orders",
		Found: true,
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

func TestGroupAssignmentsReturnsSavedAssignmentsAfterCleanupAndRebalance(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	createTopic(t, srv.Handler, "orders", 4)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-10*time.Minute))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", time.Now().Add(-time.Second))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-c", time.Now().Add(-time.Second))
	cleanupAndRebalanceGroup(t, srv.Handler, "analytics-workers", "orders", 300000)

	response := getGroupAssignments(t, srv.Handler, "analytics-workers", "orders")
	expected := groupAssignmentsResponse{
		Group: "analytics-workers",
		Topic: "orders",
		Found: true,
		Assignments: []groupAssignmentResponse{
			{
				MemberID: "member-b",
				Topics: []groupTopicAssignmentResponse{
					{Topic: "orders", Partitions: []int{0, 1}},
				},
			},
			{
				MemberID: "member-c",
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

func TestGroupAssignmentsReturnsNotFoundWhenMissing(t *testing.T) {
	srv := newTestServer(t)

	response := getGroupAssignments(t, srv.Handler, "analytics-workers", "orders")
	expected := groupAssignmentsResponse{
		Group:       "analytics-workers",
		Topic:       "orders",
		Found:       false,
		Assignments: []groupAssignmentResponse{},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupAssignmentsRejectsInvalidQuery(t *testing.T) {
	srv := newTestServer(t)

	tests := []struct {
		name string
		path string
	}{
		{name: "missing group", path: "/groups/assignments?topic=orders"},
		{name: "empty group", path: "/groups/assignments?group=&topic=orders"},
		{name: "missing topic", path: "/groups/assignments?group=analytics-workers"},
		{name: "empty topic", path: "/groups/assignments?group=analytics-workers&topic="},
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

func TestGroupAssignmentsRejectsWrongMethod(t *testing.T) {
	srv := newTestServer(t)

	recorder := performRequest(srv.Handler, http.MethodPost, "/groups/assignments?group=analytics-workers&topic=orders", nil)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, recorder.Code)
	}
}

func TestGroupAssignmentsWithNilAssignmentStoreReturnsInternalServerError(t *testing.T) {
	srv := newTestServerWithNilAssignmentStore(t)

	recorder := performRequest(srv.Handler, http.MethodGet, "/groups/assignments?group=analytics-workers&topic=orders", nil)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, recorder.Code)
	}
}

func TestGroupJoin(t *testing.T) {
	srv := newTestServer(t)

	recorder := joinGroup(t, srv.Handler, "analytics-workers", "member-a")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response groupMembershipResponse
	decodeJSON(t, recorder, &response)
	if response.Status != "joined" {
		t.Fatalf("expected status %q, got %q", "joined", response.Status)
	}

	members := getGroupMembers(t, srv.Handler, "analytics-workers")
	expected := groupMembersResponse{
		Group:   "analytics-workers",
		Members: []groupMemberResponse{{ID: "member-a"}},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestGroupJoinDuplicateIsIdempotent(t *testing.T) {
	srv := newTestServer(t)

	joinGroup(t, srv.Handler, "analytics-workers", "member-a")
	recorder := joinGroup(t, srv.Handler, "analytics-workers", "member-a")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	members := getGroupMembers(t, srv.Handler, "analytics-workers")
	expected := groupMembersResponse{
		Group:   "analytics-workers",
		Members: []groupMemberResponse{{ID: "member-a"}},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestGroupLeave(t *testing.T) {
	srv := newTestServer(t)

	joinGroup(t, srv.Handler, "analytics-workers", "member-a")

	recorder := leaveGroup(t, srv.Handler, "analytics-workers", "member-a")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response groupMembershipResponse
	decodeJSON(t, recorder, &response)
	if response.Status != "left" {
		t.Fatalf("expected status %q, got %q", "left", response.Status)
	}

	members := getGroupMembers(t, srv.Handler, "analytics-workers")
	expected := groupMembersResponse{
		Group:   "analytics-workers",
		Members: []groupMemberResponse{},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestGroupLeaveMissingMemberIsIdempotent(t *testing.T) {
	srv := newTestServer(t)

	recorder := leaveGroup(t, srv.Handler, "analytics-workers", "member-a")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	members := getGroupMembers(t, srv.Handler, "analytics-workers")
	expected := groupMembersResponse{
		Group:   "analytics-workers",
		Members: []groupMemberResponse{},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestGroupHeartbeatRecordsHeartbeat(t *testing.T) {
	srv := newTestServer(t)

	recorder := heartbeatGroup(t, srv.Handler, "analytics-workers", "member-a")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response groupMembershipResponse
	decodeJSON(t, recorder, &response)
	if response.Status != "heartbeat recorded" {
		t.Fatalf("expected status %q, got %q", "heartbeat recorded", response.Status)
	}

	members := getGroupMembers(t, srv.Handler, "analytics-workers")
	expected := groupMembersResponse{
		Group:   "analytics-workers",
		Members: []groupMemberResponse{{ID: "member-a"}},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestGroupHeartbeatAddsMissingMember(t *testing.T) {
	srv := newTestServer(t)

	recorder := heartbeatGroup(t, srv.Handler, "analytics-workers", "member-a")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	members := getGroupMembers(t, srv.Handler, "analytics-workers")
	expected := groupMembersResponse{
		Group:   "analytics-workers",
		Members: []groupMemberResponse{{ID: "member-a"}},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestGroupHeartbeatUpdatesExistingMemberWithoutDuplicate(t *testing.T) {
	srv := newTestServer(t)

	joinGroup(t, srv.Handler, "analytics-workers", "member-a")
	heartbeatGroup(t, srv.Handler, "analytics-workers", "member-a")
	heartbeatGroup(t, srv.Handler, "analytics-workers", "member-a")

	members := getGroupMembers(t, srv.Handler, "analytics-workers")
	expected := groupMembersResponse{
		Group:   "analytics-workers",
		Members: []groupMemberResponse{{ID: "member-a"}},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestGroupHeartbeatRejectsInvalidBody(t *testing.T) {
	srv := newTestServer(t)

	tests := []struct {
		name string
		body interface{}
	}{
		{
			name: "missing group",
			body: map[string]interface{}{
				"member_id": "member-a",
			},
		},
		{
			name: "empty group",
			body: groupMembershipRequest{
				Group:    "",
				MemberID: "member-a",
			},
		},
		{
			name: "missing member ID",
			body: map[string]interface{}{
				"group": "analytics-workers",
			},
		},
		{
			name: "empty member ID",
			body: groupMembershipRequest{
				Group:    "analytics-workers",
				MemberID: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := performRequest(srv.Handler, http.MethodPost, "/groups/heartbeat", tt.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
			}
		})
	}
}

func TestGroupHeartbeatRejectsInvalidJSON(t *testing.T) {
	srv := newTestServer(t)

	recorder := performRawRequest(srv.Handler, http.MethodPost, "/groups/heartbeat", "{")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestGroupStaleReturnsStaleMembers(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-10*time.Minute))

	response := getStaleGroupMembers(t, srv.Handler, "/groups/stale?group=analytics-workers&timeout_ms=300000")
	expected := groupStaleResponse{
		Group:     "analytics-workers",
		TimeoutMS: 300000,
		Members:   []groupMemberResponse{{ID: "member-a"}},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupStaleDoesNotReturnActiveMembers(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-time.Second))

	response := getStaleGroupMembers(t, srv.Handler, "/groups/stale?group=analytics-workers&timeout_ms=300000")
	expected := groupStaleResponse{
		Group:     "analytics-workers",
		TimeoutMS: 300000,
		Members:   []groupMemberResponse{},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupStaleReturnsOnlyStaleMembers(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-time.Second))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", time.Now().Add(-10*time.Minute))

	response := getStaleGroupMembers(t, srv.Handler, "/groups/stale?group=analytics-workers&timeout_ms=300000")
	expected := groupStaleResponse{
		Group:     "analytics-workers",
		TimeoutMS: 300000,
		Members:   []groupMemberResponse{{ID: "member-b"}},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupStaleMembersAreReturnedSorted(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)
	staleTime := time.Now().Add(-10 * time.Minute)

	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-c", staleTime)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", staleTime)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", staleTime)

	response := getStaleGroupMembers(t, srv.Handler, "/groups/stale?group=analytics-workers&timeout_ms=300000")
	expected := groupStaleResponse{
		Group:     "analytics-workers",
		TimeoutMS: 300000,
		Members: []groupMemberResponse{
			{ID: "member-a"},
			{ID: "member-b"},
			{ID: "member-c"},
		},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupStaleMissingGroupReturnsEmptyMembers(t *testing.T) {
	srv, _ := newTestServerWithRegistry(t)

	response := getStaleGroupMembers(t, srv.Handler, "/groups/stale?group=missing-workers&timeout_ms=300000")
	expected := groupStaleResponse{
		Group:     "missing-workers",
		TimeoutMS: 300000,
		Members:   []groupMemberResponse{},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupStaleRejectsInvalidQuery(t *testing.T) {
	srv := newTestServer(t)

	tests := []struct {
		name string
		path string
	}{
		{name: "missing group", path: "/groups/stale?timeout_ms=300000"},
		{name: "empty group", path: "/groups/stale?group=&timeout_ms=300000"},
		{name: "missing timeout", path: "/groups/stale?group=analytics-workers"},
		{name: "invalid timeout", path: "/groups/stale?group=analytics-workers&timeout_ms=invalid"},
		{name: "zero timeout", path: "/groups/stale?group=analytics-workers&timeout_ms=0"},
		{name: "negative timeout", path: "/groups/stale?group=analytics-workers&timeout_ms=-1"},
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

func TestGroupStaleRejectsWrongMethod(t *testing.T) {
	srv := newTestServer(t)

	recorder := performRequest(srv.Handler, http.MethodPost, "/groups/stale?group=analytics-workers&timeout_ms=300000", nil)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, recorder.Code)
	}
}

func TestGroupRemoveStaleRemovesStaleMembers(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-10*time.Minute))

	response := removeStaleGroupMembers(t, srv.Handler, "analytics-workers", 300000)
	expected := groupRemoveStaleResponse{
		Group:          "analytics-workers",
		TimeoutMS:      300000,
		RemovedMembers: []groupMemberResponse{{ID: "member-a"}},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupRemoveStaleDoesNotRemoveActiveMembers(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-time.Second))

	response := removeStaleGroupMembers(t, srv.Handler, "analytics-workers", 300000)
	expected := groupRemoveStaleResponse{
		Group:          "analytics-workers",
		TimeoutMS:      300000,
		RemovedMembers: []groupMemberResponse{},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}

	members := getGroupMembers(t, srv.Handler, "analytics-workers")
	expectedMembers := groupMembersResponse{
		Group:   "analytics-workers",
		Members: []groupMemberResponse{{ID: "member-a"}},
	}

	if !reflect.DeepEqual(members, expectedMembers) {
		t.Fatalf("expected %v, got %v", expectedMembers, members)
	}
}

func TestGroupRemoveStaleRemovesOnlyStaleMembers(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-time.Second))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", time.Now().Add(-10*time.Minute))

	response := removeStaleGroupMembers(t, srv.Handler, "analytics-workers", 300000)
	expected := groupRemoveStaleResponse{
		Group:          "analytics-workers",
		TimeoutMS:      300000,
		RemovedMembers: []groupMemberResponse{{ID: "member-b"}},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupRemoveStaleRemovedMembersAreReturnedSorted(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)
	staleTime := time.Now().Add(-10 * time.Minute)

	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-c", staleTime)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", staleTime)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", staleTime)

	response := removeStaleGroupMembers(t, srv.Handler, "analytics-workers", 300000)
	expected := groupRemoveStaleResponse{
		Group:     "analytics-workers",
		TimeoutMS: 300000,
		RemovedMembers: []groupMemberResponse{
			{ID: "member-a"},
			{ID: "member-b"},
			{ID: "member-c"},
		},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupRemoveStaleRemovedMembersNoLongerAppear(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-10*time.Minute))

	removeStaleGroupMembers(t, srv.Handler, "analytics-workers", 300000)

	members := getGroupMembers(t, srv.Handler, "analytics-workers")
	expected := groupMembersResponse{
		Group:   "analytics-workers",
		Members: []groupMemberResponse{},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestGroupRemoveStaleActiveMembersRemain(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-10*time.Minute))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", time.Now().Add(-time.Second))

	removeStaleGroupMembers(t, srv.Handler, "analytics-workers", 300000)

	members := getGroupMembers(t, srv.Handler, "analytics-workers")
	expected := groupMembersResponse{
		Group:   "analytics-workers",
		Members: []groupMemberResponse{{ID: "member-b"}},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestGroupRemoveStaleMissingGroupReturnsEmptyRemovedMembers(t *testing.T) {
	srv, _ := newTestServerWithRegistry(t)

	response := removeStaleGroupMembers(t, srv.Handler, "missing-workers", 300000)
	expected := groupRemoveStaleResponse{
		Group:          "missing-workers",
		TimeoutMS:      300000,
		RemovedMembers: []groupMemberResponse{},
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupRemoveStaleRejectsInvalidBody(t *testing.T) {
	srv := newTestServer(t)

	tests := []struct {
		name string
		body interface{}
	}{
		{
			name: "missing group",
			body: map[string]interface{}{
				"timeout_ms": 300000,
			},
		},
		{
			name: "empty group",
			body: groupRemoveStaleRequest{
				Group:     "",
				TimeoutMS: 300000,
			},
		},
		{
			name: "missing timeout",
			body: map[string]interface{}{
				"group": "analytics-workers",
			},
		},
		{
			name: "zero timeout",
			body: groupRemoveStaleRequest{
				Group:     "analytics-workers",
				TimeoutMS: 0,
			},
		},
		{
			name: "negative timeout",
			body: groupRemoveStaleRequest{
				Group:     "analytics-workers",
				TimeoutMS: -1,
			},
		},
		{
			name: "invalid timeout",
			body: map[string]interface{}{
				"group":      "analytics-workers",
				"timeout_ms": "invalid",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := performRequest(srv.Handler, http.MethodPost, "/groups/remove-stale", tt.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
			}
		})
	}
}

func TestGroupRemoveStaleRejectsInvalidJSON(t *testing.T) {
	srv := newTestServer(t)

	recorder := performRawRequest(srv.Handler, http.MethodPost, "/groups/remove-stale", "{")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestGroupRemoveStaleRejectsWrongMethod(t *testing.T) {
	srv := newTestServer(t)

	recorder := performRequest(srv.Handler, http.MethodGet, "/groups/remove-stale", nil)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, recorder.Code)
	}
}

func TestGroupMembersAreReturnedSorted(t *testing.T) {
	srv := newTestServer(t)

	joinGroup(t, srv.Handler, "analytics-workers", "member-c")
	joinGroup(t, srv.Handler, "analytics-workers", "member-a")
	joinGroup(t, srv.Handler, "analytics-workers", "member-b")

	members := getGroupMembers(t, srv.Handler, "analytics-workers")
	expected := groupMembersResponse{
		Group: "analytics-workers",
		Members: []groupMemberResponse{
			{ID: "member-a"},
			{ID: "member-b"},
			{ID: "member-c"},
		},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestGroupMembersAreIsolatedByGroup(t *testing.T) {
	srv := newTestServer(t)

	joinGroup(t, srv.Handler, "analytics-workers", "member-a")
	joinGroup(t, srv.Handler, "billing-workers", "member-b")

	members := getGroupMembers(t, srv.Handler, "analytics-workers")
	expected := groupMembersResponse{
		Group:   "analytics-workers",
		Members: []groupMemberResponse{{ID: "member-a"}},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestGroupJoinRejectsInvalidBody(t *testing.T) {
	srv := newTestServer(t)

	tests := []struct {
		name string
		body interface{}
	}{
		{
			name: "missing group",
			body: map[string]interface{}{
				"member_id": "member-a",
			},
		},
		{
			name: "empty group",
			body: groupMembershipRequest{
				Group:    "",
				MemberID: "member-a",
			},
		},
		{
			name: "missing member ID",
			body: map[string]interface{}{
				"group": "analytics-workers",
			},
		},
		{
			name: "empty member ID",
			body: groupMembershipRequest{
				Group:    "analytics-workers",
				MemberID: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := performRequest(srv.Handler, http.MethodPost, "/groups/join", tt.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
			}
		})
	}

	recorder := performRawRequest(srv.Handler, http.MethodPost, "/groups/join", "{")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestGroupLeaveRejectsInvalidBody(t *testing.T) {
	srv := newTestServer(t)

	tests := []struct {
		name string
		body interface{}
	}{
		{
			name: "missing group",
			body: map[string]interface{}{
				"member_id": "member-a",
			},
		},
		{
			name: "empty group",
			body: groupMembershipRequest{
				Group:    "",
				MemberID: "member-a",
			},
		},
		{
			name: "missing member ID",
			body: map[string]interface{}{
				"group": "analytics-workers",
			},
		},
		{
			name: "empty member ID",
			body: groupMembershipRequest{
				Group:    "analytics-workers",
				MemberID: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := performRequest(srv.Handler, http.MethodPost, "/groups/leave", tt.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
			}
		})
	}

	recorder := performRawRequest(srv.Handler, http.MethodPost, "/groups/leave", "{")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestGroupMembersRejectsMissingGroup(t *testing.T) {
	srv := newTestServer(t)

	recorder := performRequest(srv.Handler, http.MethodGet, "/groups/members", nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}

	recorder = performRequest(srv.Handler, http.MethodGet, "/groups/members?group=", nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestGroupMembershipRejectsWrongMethods(t *testing.T) {
	srv := newTestServer(t)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "join", method: http.MethodGet, path: "/groups/join"},
		{name: "leave", method: http.MethodGet, path: "/groups/leave"},
		{name: "heartbeat", method: http.MethodGet, path: "/groups/heartbeat"},
		{name: "members", method: http.MethodPost, path: "/groups/members?group=analytics-workers"},
		{name: "stale", method: http.MethodPost, path: "/groups/stale?group=analytics-workers&timeout_ms=300000"},
		{name: "remove stale", method: http.MethodGet, path: "/groups/remove-stale"},
		{name: "rebalance", method: http.MethodGet, path: "/groups/rebalance"},
		{name: "cleanup and rebalance", method: http.MethodGet, path: "/groups/cleanup-and-rebalance"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := performRequest(srv.Handler, tt.method, tt.path, nil)
			if recorder.Code != http.StatusMethodNotAllowed {
				t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, recorder.Code)
			}
		})
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

func newTestServerWithRegistry(t *testing.T) (*http.Server, *group.Registry) {
	t.Helper()

	srv, registry, _ := newTestServerWithRegistryAndAssignmentStore(t)
	return srv, registry
}

func newTestServerWithRegistryAndAssignmentStore(t *testing.T) (*http.Server, *group.Registry, *group.AssignmentStore) {
	t.Helper()
	chdirTemp(t)

	offsetStore, err := consumer.NewOffsetStore(defaultOffsetStorePath)
	if err != nil {
		t.Fatalf("failed to create offset store: %v", err)
	}

	registry := group.NewRegistry()
	assignmentStore := group.NewAssignmentStore()
	server := &Server{
		topicManager:    topic.NewManager(),
		offsetStore:     offsetStore,
		assigner:        group.NewAssigner(),
		registry:        registry,
		assignmentStore: assignmentStore,
	}

	return &http.Server{
		Addr:    defaultAddr,
		Handler: server.routes(),
	}, registry, assignmentStore
}

func newTestServerWithNilAssignmentStore(t *testing.T) *http.Server {
	t.Helper()

	srv, _ := newTestServerWithNilAssignmentStoreAndRegistry(t)
	return srv
}

func newTestServerWithNilAssignmentStoreAndRegistry(t *testing.T) (*http.Server, *group.Registry) {
	t.Helper()
	chdirTemp(t)

	offsetStore, err := consumer.NewOffsetStore(defaultOffsetStorePath)
	if err != nil {
		t.Fatalf("failed to create offset store: %v", err)
	}

	registry := group.NewRegistry()
	server := &Server{
		topicManager: topic.NewManager(),
		offsetStore:  offsetStore,
		assigner:     group.NewAssigner(),
		registry:     registry,
	}

	return &http.Server{
		Addr:    defaultAddr,
		Handler: server.routes(),
	}, registry
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

func rebalanceGroup(t *testing.T, handler http.Handler, groupName string, topicName string) *httptest.ResponseRecorder {
	t.Helper()

	return performRequest(handler, http.MethodPost, "/groups/rebalance", groupRebalanceRequest{
		Group: groupName,
		Topic: topicName,
	})
}

func cleanupAndRebalanceGroup(t *testing.T, handler http.Handler, groupName string, topicName string, timeoutMS int64) groupCleanupAndRebalanceResponse {
	t.Helper()

	recorder := performRequest(handler, http.MethodPost, "/groups/cleanup-and-rebalance", groupCleanupAndRebalanceRequest{
		Group:     groupName,
		Topic:     topicName,
		TimeoutMS: timeoutMS,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response groupCleanupAndRebalanceResponse
	decodeJSON(t, recorder, &response)

	return response
}

func joinGroup(t *testing.T, handler http.Handler, groupName string, memberID string) *httptest.ResponseRecorder {
	t.Helper()

	return performRequest(handler, http.MethodPost, "/groups/join", groupMembershipRequest{
		Group:    groupName,
		MemberID: memberID,
	})
}

func leaveGroup(t *testing.T, handler http.Handler, groupName string, memberID string) *httptest.ResponseRecorder {
	t.Helper()

	return performRequest(handler, http.MethodPost, "/groups/leave", groupMembershipRequest{
		Group:    groupName,
		MemberID: memberID,
	})
}

func heartbeatGroup(t *testing.T, handler http.Handler, groupName string, memberID string) *httptest.ResponseRecorder {
	t.Helper()

	return performRequest(handler, http.MethodPost, "/groups/heartbeat", groupMembershipRequest{
		Group:    groupName,
		MemberID: memberID,
	})
}

func getGroupMembers(t *testing.T, handler http.Handler, groupName string) groupMembersResponse {
	t.Helper()

	recorder := performRequest(handler, http.MethodGet, "/groups/members?group="+groupName, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response groupMembersResponse
	decodeJSON(t, recorder, &response)

	return response
}

func getStaleGroupMembers(t *testing.T, handler http.Handler, path string) groupStaleResponse {
	t.Helper()

	recorder := performRequest(handler, http.MethodGet, path, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response groupStaleResponse
	decodeJSON(t, recorder, &response)

	return response
}

func getGroupAssignments(t *testing.T, handler http.Handler, groupName string, topicName string) groupAssignmentsResponse {
	t.Helper()

	recorder := performRequest(handler, http.MethodGet, "/groups/assignments?group="+groupName+"&topic="+topicName, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response groupAssignmentsResponse
	decodeJSON(t, recorder, &response)

	return response
}

func removeStaleGroupMembers(t *testing.T, handler http.Handler, groupName string, timeoutMS int64) groupRemoveStaleResponse {
	t.Helper()

	recorder := performRequest(handler, http.MethodPost, "/groups/remove-stale", groupRemoveStaleRequest{
		Group:     groupName,
		TimeoutMS: timeoutMS,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response groupRemoveStaleResponse
	decodeJSON(t, recorder, &response)

	return response
}

func recordRegistryHeartbeat(t *testing.T, registry *group.Registry, groupName string, memberID string, lastSeen time.Time) {
	t.Helper()

	if err := registry.Heartbeat(groupName, memberID, lastSeen); err != nil {
		t.Fatalf("failed to record registry heartbeat: %v", err)
	}
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
