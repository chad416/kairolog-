package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
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

func TestGroupAssignmentsDeleteRemovesSavedAssignmentsAfterRebalance(t *testing.T) {
	srv := newTestServer(t)

	createTopic(t, srv.Handler, "orders", 4)
	joinGroup(t, srv.Handler, "analytics-workers", "member-a")
	joinGroup(t, srv.Handler, "analytics-workers", "member-b")
	rebalanceGroup(t, srv.Handler, "analytics-workers", "orders")

	response := deleteGroupAssignments(t, srv.Handler, "analytics-workers", "orders")
	expected := groupAssignmentDeleteResponse{
		Status: "deleted",
		Group:  "analytics-workers",
		Topic:  "orders",
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}

	assignments := getGroupAssignments(t, srv.Handler, "analytics-workers", "orders")
	expectedAssignments := groupAssignmentsResponse{
		Group:       "analytics-workers",
		Topic:       "orders",
		Found:       false,
		Assignments: []groupAssignmentResponse{},
	}

	if !reflect.DeepEqual(assignments, expectedAssignments) {
		t.Fatalf("expected %v, got %v", expectedAssignments, assignments)
	}
}

func TestGroupAssignmentsDeleteRemovesSavedAssignmentsAfterCleanupAndRebalance(t *testing.T) {
	srv, registry := newTestServerWithRegistry(t)

	createTopic(t, srv.Handler, "orders", 4)
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-10*time.Minute))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", time.Now().Add(-time.Second))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-c", time.Now().Add(-time.Second))
	cleanupAndRebalanceGroup(t, srv.Handler, "analytics-workers", "orders", 300000)

	response := deleteGroupAssignments(t, srv.Handler, "analytics-workers", "orders")
	expected := groupAssignmentDeleteResponse{
		Status: "deleted",
		Group:  "analytics-workers",
		Topic:  "orders",
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}

	assignments := getGroupAssignments(t, srv.Handler, "analytics-workers", "orders")
	expectedAssignments := groupAssignmentsResponse{
		Group:       "analytics-workers",
		Topic:       "orders",
		Found:       false,
		Assignments: []groupAssignmentResponse{},
	}

	if !reflect.DeepEqual(assignments, expectedAssignments) {
		t.Fatalf("expected %v, got %v", expectedAssignments, assignments)
	}
}

func TestGroupAssignmentsDeleteMissingAssignmentReturnsOK(t *testing.T) {
	srv := newTestServer(t)

	response := deleteGroupAssignments(t, srv.Handler, "analytics-workers", "orders")
	expected := groupAssignmentDeleteResponse{
		Status: "deleted",
		Group:  "analytics-workers",
		Topic:  "orders",
	}

	if !reflect.DeepEqual(response, expected) {
		t.Fatalf("expected %v, got %v", expected, response)
	}
}

func TestGroupAssignmentsDeleteRejectsInvalidQuery(t *testing.T) {
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
			recorder := performRequest(srv.Handler, http.MethodDelete, tt.path, nil)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
			}
		})
	}
}

func TestGroupAssignmentsDeleteWithNilAssignmentStoreReturnsInternalServerError(t *testing.T) {
	srv := newTestServerWithNilAssignmentStore(t)

	recorder := performRequest(srv.Handler, http.MethodDelete, "/groups/assignments?group=analytics-workers&topic=orders", nil)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, recorder.Code)
	}
}

func TestNewWiresFileBackedAssignmentStore(t *testing.T) {
	chdirTemp(t)

	srv := newTestServerInCurrentDir(t)
	if srv == nil {
		t.Fatal("expected server")
	}

	if _, err := os.Stat(defaultAssignmentStorePath); err != nil {
		t.Fatalf("expected assignment store file to exist: %v", err)
	}
}

func TestNewWiresFileBackedGroupRegistry(t *testing.T) {
	chdirTemp(t)

	srv := newTestServerInCurrentDir(t)
	if srv == nil {
		t.Fatal("expected server")
	}

	if _, err := os.Stat(defaultRegistryStorePath); err != nil {
		t.Fatalf("expected registry store file to exist: %v", err)
	}
}

func TestGroupAssignmentsSavedThroughRebalanceSurviveServerRestart(t *testing.T) {
	chdirTemp(t)

	srv := newTestServerInCurrentDir(t)
	createTopic(t, srv.Handler, "orders", 4)
	joinGroup(t, srv.Handler, "analytics-workers", "member-a")
	joinGroup(t, srv.Handler, "analytics-workers", "member-b")

	recorder := rebalanceGroup(t, srv.Handler, "analytics-workers", "orders")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	restarted := newTestServerInCurrentDir(t)
	response := getGroupAssignments(t, restarted.Handler, "analytics-workers", "orders")
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

func TestGroupAssignmentsSavedThroughCleanupAndRebalanceSurviveServerRestart(t *testing.T) {
	chdirTemp(t)

	srv := newTestServerInCurrentDir(t)
	createTopic(t, srv.Handler, "orders", 4)
	joinGroup(t, srv.Handler, "analytics-workers", "member-a")
	joinGroup(t, srv.Handler, "analytics-workers", "member-b")

	cleanupAndRebalanceGroup(t, srv.Handler, "analytics-workers", "orders", 300000)

	restarted := newTestServerInCurrentDir(t)
	response := getGroupAssignments(t, restarted.Handler, "analytics-workers", "orders")
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

func TestGroupAssignmentsDeletePersistsDeletionToDisk(t *testing.T) {
	chdirTemp(t)

	srv := newTestServerInCurrentDir(t)
	createTopic(t, srv.Handler, "orders", 4)
	joinGroup(t, srv.Handler, "analytics-workers", "member-a")
	joinGroup(t, srv.Handler, "analytics-workers", "member-b")

	recorder := rebalanceGroup(t, srv.Handler, "analytics-workers", "orders")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	deleteGroupAssignments(t, srv.Handler, "analytics-workers", "orders")

	restarted := newTestServerInCurrentDir(t)
	response := getGroupAssignments(t, restarted.Handler, "analytics-workers", "orders")
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

func TestGroupJoinSurvivesServerRestart(t *testing.T) {
	chdirTemp(t)

	srv := newTestServerInCurrentDir(t)
	joinGroup(t, srv.Handler, "analytics-workers", "member-a")

	restarted := newTestServerInCurrentDir(t)
	members := getGroupMembers(t, restarted.Handler, "analytics-workers")
	expected := groupMembersResponse{
		Group:   "analytics-workers",
		Members: []groupMemberResponse{{ID: "member-a"}},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestGroupHeartbeatSurvivesServerRestart(t *testing.T) {
	chdirTemp(t)

	srv := newTestServerInCurrentDir(t)
	heartbeatGroup(t, srv.Handler, "analytics-workers", "member-a")

	restarted := newTestServerInCurrentDir(t)
	members := getGroupMembers(t, restarted.Handler, "analytics-workers")
	expected := groupMembersResponse{
		Group:   "analytics-workers",
		Members: []groupMemberResponse{{ID: "member-a"}},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}

	registryStore := loadDefaultRegistryFileStore(t)
	storedMembers, err := registryStore.Members("analytics-workers")
	if err != nil {
		t.Fatalf("failed to get stored members: %v", err)
	}
	if len(storedMembers) != 1 {
		t.Fatalf("expected 1 stored member, got %d", len(storedMembers))
	}
	if storedMembers[0].LastSeen.IsZero() {
		t.Fatal("expected persisted LastSeen to be set")
	}
}

func TestGroupLeavePersistsDeletionToDisk(t *testing.T) {
	chdirTemp(t)

	srv := newTestServerInCurrentDir(t)
	joinGroup(t, srv.Handler, "analytics-workers", "member-a")
	leaveGroup(t, srv.Handler, "analytics-workers", "member-a")

	restarted := newTestServerInCurrentDir(t)
	members := getGroupMembers(t, restarted.Handler, "analytics-workers")
	expected := groupMembersResponse{
		Group:   "analytics-workers",
		Members: []groupMemberResponse{},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestGroupRemoveStalePersistsDeletionToDisk(t *testing.T) {
	chdirTemp(t)
	now := time.Now()

	seedDefaultRegistryFileStore(t, "analytics-workers", map[string]time.Time{
		"member-a": now.Add(-10 * time.Minute),
		"member-b": now.Add(-time.Second),
	})

	srv := newTestServerInCurrentDir(t)
	response := removeStaleGroupMembers(t, srv.Handler, "analytics-workers", 300000)
	expectedResponse := groupRemoveStaleResponse{
		Group:          "analytics-workers",
		TimeoutMS:      300000,
		RemovedMembers: []groupMemberResponse{{ID: "member-a"}},
	}

	if !reflect.DeepEqual(response, expectedResponse) {
		t.Fatalf("expected %v, got %v", expectedResponse, response)
	}

	restarted := newTestServerInCurrentDir(t)
	members := getGroupMembers(t, restarted.Handler, "analytics-workers")
	expectedMembers := groupMembersResponse{
		Group:   "analytics-workers",
		Members: []groupMemberResponse{{ID: "member-b"}},
	}

	if !reflect.DeepEqual(members, expectedMembers) {
		t.Fatalf("expected %v, got %v", expectedMembers, members)
	}
}

func TestGroupRebalanceAfterRestartUsesPersistedGroupMembers(t *testing.T) {
	chdirTemp(t)

	srv := newTestServerInCurrentDir(t)
	joinGroup(t, srv.Handler, "analytics-workers", "member-b")
	joinGroup(t, srv.Handler, "analytics-workers", "member-a")

	restarted := newTestServerInCurrentDir(t)
	createTopic(t, restarted.Handler, "orders", 4)

	recorder := rebalanceGroup(t, restarted.Handler, "analytics-workers", "orders")
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

func TestGroupCleanupAndRebalanceAfterRestartUsesPersistedMembersAndLastSeen(t *testing.T) {
	chdirTemp(t)
	now := time.Now()

	seedDefaultRegistryFileStore(t, "analytics-workers", map[string]time.Time{
		"member-a": now.Add(-10 * time.Minute),
		"member-b": now.Add(-time.Second),
		"member-c": now.Add(-time.Second),
	})

	srv := newTestServerInCurrentDir(t)
	createTopic(t, srv.Handler, "orders", 4)

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

func TestCleanupStaleMembersAndRebalanceOnceRemovesStaleMembersAndRebalancesAssignments(t *testing.T) {
	chdirTemp(t)
	registry := group.NewRegistry()
	assignmentStore := group.NewAssignmentStore()
	topicManager := topic.NewManager()
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)

	if err := topicManager.CreateTopic("orders", 4); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", now.Add(-10*time.Minute))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", now.Add(-time.Minute))
	saveAssignments(t, assignmentStore, "analytics-workers", "orders", []group.Assignment{
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
	})

	if err := cleanupStaleMembersAndRebalanceOnce(registry, assignmentStore, topicManager, group.NewAssigner(), now, 5*time.Minute); err != nil {
		t.Fatalf("failed to clean stale members and rebalance: %v", err)
	}

	members, err := registry.Members("analytics-workers")
	if err != nil {
		t.Fatalf("failed to get group members: %v", err)
	}
	expectedMembers := []group.GroupMember{
		{ID: "member-b", LastSeen: now.Add(-time.Minute)},
	}
	if !reflect.DeepEqual(members, expectedMembers) {
		t.Fatalf("expected %v, got %v", expectedMembers, members)
	}

	assignments, found, err := assignmentStore.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get assignments: %v", err)
	}
	if !found {
		t.Fatal("expected assignments to be found")
	}
	expectedAssignments := []group.Assignment{
		{
			MemberID: "member-b",
			Topics: []group.TopicAssignment{
				{Topic: "orders", Partitions: []int{0, 1, 2, 3}},
			},
		},
	}
	if !reflect.DeepEqual(assignments, expectedAssignments) {
		t.Fatalf("expected %v, got %v", expectedAssignments, assignments)
	}
}

func TestCleanupStaleMembersAndRebalanceOnceDoesNotRebalanceWhenNoStaleMembersAreRemoved(t *testing.T) {
	expectedErr := errors.New("should not be called")
	registry := &fakeCleanupRegistry{
		groups: []string{"analytics-workers"},
	}
	assignmentStore := &fakeCleanupAssignmentStore{
		topicsErr: expectedErr,
		saveErr:   expectedErr,
	}
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)

	if err := cleanupStaleMembersAndRebalanceOnce(registry, assignmentStore, topic.NewManager(), group.NewAssigner(), now, 5*time.Minute); err != nil {
		t.Fatalf("expected cleanup to succeed, got %v", err)
	}

	if got := assignmentStore.topicsCallCount(); got != 0 {
		t.Fatalf("expected no topic calls, got %d", got)
	}
	if got := assignmentStore.saveCallCount(); got != 0 {
		t.Fatalf("expected no save calls, got %d", got)
	}
}

func TestCleanupStaleMembersAndRebalanceOnceHandlesGroupWithNoSavedAssignmentTopics(t *testing.T) {
	registry := group.NewRegistry()
	assignmentStore := group.NewAssignmentStore()
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)

	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", now.Add(-10*time.Minute))

	if err := cleanupStaleMembersAndRebalanceOnce(registry, assignmentStore, topic.NewManager(), group.NewAssigner(), now, 5*time.Minute); err != nil {
		t.Fatalf("failed to clean stale members and rebalance: %v", err)
	}

	members, err := registry.Members("analytics-workers")
	if err != nil {
		t.Fatalf("failed to get group members: %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("expected no members, got %v", members)
	}
}

func TestCleanupStaleMembersAndRebalanceOnceRebalancesMultipleSavedTopicsForGroup(t *testing.T) {
	chdirTemp(t)
	registry := group.NewRegistry()
	assignmentStore := group.NewAssignmentStore()
	topicManager := topic.NewManager()
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)

	if err := topicManager.CreateTopic("orders", 4); err != nil {
		t.Fatalf("failed to create orders topic: %v", err)
	}
	if err := topicManager.CreateTopic("payments", 2); err != nil {
		t.Fatalf("failed to create payments topic: %v", err)
	}
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", now.Add(-10*time.Minute))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", now.Add(-time.Minute))
	saveAssignments(t, assignmentStore, "analytics-workers", "orders", []group.Assignment{
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
	})
	saveAssignments(t, assignmentStore, "analytics-workers", "payments", []group.Assignment{
		{
			MemberID: "member-a",
			Topics: []group.TopicAssignment{
				{Topic: "payments", Partitions: []int{0}},
			},
		},
		{
			MemberID: "member-b",
			Topics: []group.TopicAssignment{
				{Topic: "payments", Partitions: []int{1}},
			},
		},
	})

	if err := cleanupStaleMembersAndRebalanceOnce(registry, assignmentStore, topicManager, group.NewAssigner(), now, 5*time.Minute); err != nil {
		t.Fatalf("failed to clean stale members and rebalance: %v", err)
	}

	ordersAssignments, found, err := assignmentStore.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get orders assignments: %v", err)
	}
	if !found {
		t.Fatal("expected orders assignments to be found")
	}
	expectedOrdersAssignments := []group.Assignment{
		{
			MemberID: "member-b",
			Topics: []group.TopicAssignment{
				{Topic: "orders", Partitions: []int{0, 1, 2, 3}},
			},
		},
	}
	if !reflect.DeepEqual(ordersAssignments, expectedOrdersAssignments) {
		t.Fatalf("expected %v, got %v", expectedOrdersAssignments, ordersAssignments)
	}

	paymentsAssignments, found, err := assignmentStore.Get("analytics-workers", "payments")
	if err != nil {
		t.Fatalf("failed to get payments assignments: %v", err)
	}
	if !found {
		t.Fatal("expected payments assignments to be found")
	}
	expectedPaymentsAssignments := []group.Assignment{
		{
			MemberID: "member-b",
			Topics: []group.TopicAssignment{
				{Topic: "payments", Partitions: []int{0, 1}},
			},
		},
	}
	if !reflect.DeepEqual(paymentsAssignments, expectedPaymentsAssignments) {
		t.Fatalf("expected %v, got %v", expectedPaymentsAssignments, paymentsAssignments)
	}
}

func TestCleanupStaleMembersAndRebalanceOnceHandlesMultipleGroups(t *testing.T) {
	chdirTemp(t)
	registry := group.NewRegistry()
	assignmentStore := group.NewAssignmentStore()
	topicManager := topic.NewManager()
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)

	if err := topicManager.CreateTopic("orders", 2); err != nil {
		t.Fatalf("failed to create orders topic: %v", err)
	}
	if err := topicManager.CreateTopic("payments", 2); err != nil {
		t.Fatalf("failed to create payments topic: %v", err)
	}
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", now.Add(-10*time.Minute))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", now.Add(-time.Minute))
	recordRegistryHeartbeat(t, registry, "billing-workers", "member-c", now.Add(-10*time.Minute))
	recordRegistryHeartbeat(t, registry, "billing-workers", "member-d", now.Add(-time.Minute))
	saveAssignments(t, assignmentStore, "analytics-workers", "orders", []group.Assignment{
		{
			MemberID: "member-a",
			Topics: []group.TopicAssignment{
				{Topic: "orders", Partitions: []int{0}},
			},
		},
		{
			MemberID: "member-b",
			Topics: []group.TopicAssignment{
				{Topic: "orders", Partitions: []int{1}},
			},
		},
	})
	saveAssignments(t, assignmentStore, "billing-workers", "payments", []group.Assignment{
		{
			MemberID: "member-c",
			Topics: []group.TopicAssignment{
				{Topic: "payments", Partitions: []int{0}},
			},
		},
		{
			MemberID: "member-d",
			Topics: []group.TopicAssignment{
				{Topic: "payments", Partitions: []int{1}},
			},
		},
	})

	if err := cleanupStaleMembersAndRebalanceOnce(registry, assignmentStore, topicManager, group.NewAssigner(), now, 5*time.Minute); err != nil {
		t.Fatalf("failed to clean stale members and rebalance: %v", err)
	}

	analyticsAssignments, found, err := assignmentStore.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get analytics assignments: %v", err)
	}
	if !found {
		t.Fatal("expected analytics assignments to be found")
	}
	expectedAnalyticsAssignments := []group.Assignment{
		{
			MemberID: "member-b",
			Topics: []group.TopicAssignment{
				{Topic: "orders", Partitions: []int{0, 1}},
			},
		},
	}
	if !reflect.DeepEqual(analyticsAssignments, expectedAnalyticsAssignments) {
		t.Fatalf("expected %v, got %v", expectedAnalyticsAssignments, analyticsAssignments)
	}

	billingAssignments, found, err := assignmentStore.Get("billing-workers", "payments")
	if err != nil {
		t.Fatalf("failed to get billing assignments: %v", err)
	}
	if !found {
		t.Fatal("expected billing assignments to be found")
	}
	expectedBillingAssignments := []group.Assignment{
		{
			MemberID: "member-d",
			Topics: []group.TopicAssignment{
				{Topic: "payments", Partitions: []int{0, 1}},
			},
		},
	}
	if !reflect.DeepEqual(billingAssignments, expectedBillingAssignments) {
		t.Fatalf("expected %v, got %v", expectedBillingAssignments, billingAssignments)
	}
}

func TestCleanupStaleMembersAndRebalanceOnceDeletesAssignmentsWhenAllMembersAreRemoved(t *testing.T) {
	chdirTemp(t)
	registry := group.NewRegistry()
	assignmentStore := group.NewAssignmentStore()
	topicManager := topic.NewManager()
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)

	if err := topicManager.CreateTopic("orders", 2); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", now.Add(-10*time.Minute))
	saveAssignments(t, assignmentStore, "analytics-workers", "orders", []group.Assignment{
		{
			MemberID: "member-a",
			Topics: []group.TopicAssignment{
				{Topic: "orders", Partitions: []int{0, 1}},
			},
		},
	})

	if err := cleanupStaleMembersAndRebalanceOnce(registry, assignmentStore, topicManager, group.NewAssigner(), now, 5*time.Minute); err != nil {
		t.Fatalf("failed to clean stale members and rebalance: %v", err)
	}

	members, err := registry.Members("analytics-workers")
	if err != nil {
		t.Fatalf("failed to get members: %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("expected no members, got %v", members)
	}

	assignments, found, err := assignmentStore.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get assignments: %v", err)
	}
	if found {
		t.Fatalf("expected assignments to be deleted, got %v", assignments)
	}
}

func TestCleanupStaleMembersAndRebalanceOnceSkipsMissingSavedAssignmentTopic(t *testing.T) {
	registry := group.NewRegistry()
	assignmentStore := group.NewAssignmentStore()
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)

	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", now.Add(-10*time.Minute))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", now.Add(-time.Minute))
	saveAssignments(t, assignmentStore, "analytics-workers", "missing-topic", []group.Assignment{
		{
			MemberID: "member-a",
			Topics: []group.TopicAssignment{
				{Topic: "missing-topic", Partitions: []int{0}},
			},
		},
	})

	if err := cleanupStaleMembersAndRebalanceOnce(registry, assignmentStore, topic.NewManager(), group.NewAssigner(), now, 5*time.Minute); err != nil {
		t.Fatalf("failed to clean stale members and rebalance: %v", err)
	}

	assignments, found, err := assignmentStore.Get("analytics-workers", "missing-topic")
	if err != nil {
		t.Fatalf("failed to get assignments: %v", err)
	}
	if !found {
		t.Fatal("expected missing topic assignment state to be preserved")
	}
	expectedAssignments := []group.Assignment{
		{
			MemberID: "member-a",
			Topics: []group.TopicAssignment{
				{Topic: "missing-topic", Partitions: []int{0}},
			},
		},
	}
	if !reflect.DeepEqual(assignments, expectedAssignments) {
		t.Fatalf("expected %v, got %v", expectedAssignments, assignments)
	}
}

func TestCleanupStaleMembersAndRebalanceOnceRejectsInvalidInput(t *testing.T) {
	registry := group.NewRegistry()
	store := group.NewAssignmentStore()
	topicManager := topic.NewManager()
	assigner := group.NewAssigner()
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name            string
		registry        groupRegistry
		assignmentStore assignmentStore
		topicManager    *topic.Manager
		assigner        *group.Assigner
		now             time.Time
		timeout         time.Duration
	}{
		{name: "nil registry", registry: nil, assignmentStore: store, topicManager: topicManager, assigner: assigner, now: now, timeout: 5 * time.Minute},
		{name: "nil assignment store", registry: registry, assignmentStore: nil, topicManager: topicManager, assigner: assigner, now: now, timeout: 5 * time.Minute},
		{name: "nil topic manager", registry: registry, assignmentStore: store, topicManager: nil, assigner: assigner, now: now, timeout: 5 * time.Minute},
		{name: "nil assigner", registry: registry, assignmentStore: store, topicManager: topicManager, assigner: nil, now: now, timeout: 5 * time.Minute},
		{name: "zero now", registry: registry, assignmentStore: store, topicManager: topicManager, assigner: assigner, now: time.Time{}, timeout: 5 * time.Minute},
		{name: "zero timeout", registry: registry, assignmentStore: store, topicManager: topicManager, assigner: assigner, now: now, timeout: 0},
		{name: "negative timeout", registry: registry, assignmentStore: store, topicManager: topicManager, assigner: assigner, now: now, timeout: -time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := cleanupStaleMembersAndRebalanceOnce(tt.registry, tt.assignmentStore, tt.topicManager, tt.assigner, tt.now, tt.timeout); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestCleanupStaleMembersAndRebalanceOnceReturnsDependencyErrors(t *testing.T) {
	chdirTemp(t)
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name            string
		registry        groupRegistry
		assignmentStore assignmentStore
		topicManager    *topic.Manager
		expectedErr     error
	}{
		{
			name:            "groups fails",
			registry:        &fakeCleanupRegistry{groupsErr: errors.New("groups failed")},
			assignmentStore: group.NewAssignmentStore(),
			topicManager:    topic.NewManager(),
			expectedErr:     errors.New("groups failed"),
		},
		{
			name: "remove stale members fails",
			registry: &fakeCleanupRegistry{
				groups:    []string{"analytics-workers"},
				removeErr: errors.New("remove failed"),
			},
			assignmentStore: group.NewAssignmentStore(),
			topicManager:    topic.NewManager(),
			expectedErr:     errors.New("remove failed"),
		},
		{
			name: "assignment topics fails",
			registry: &fakeCleanupRegistry{
				groups: []string{"analytics-workers"},
				removedMembers: map[string][]group.GroupMember{
					"analytics-workers": {{ID: "member-a", LastSeen: now.Add(-10 * time.Minute)}},
				},
			},
			assignmentStore: &fakeCleanupAssignmentStore{topicsErr: errors.New("topics failed")},
			topicManager:    topic.NewManager(),
			expectedErr:     errors.New("topics failed"),
		},
		{
			name: "members fails after stale removal",
			registry: &fakeCleanupRegistry{
				groups: []string{"analytics-workers"},
				removedMembers: map[string][]group.GroupMember{
					"analytics-workers": {{ID: "member-a", LastSeen: now.Add(-10 * time.Minute)}},
				},
				membersErr: errors.New("members failed"),
			},
			assignmentStore: &fakeCleanupAssignmentStore{
				topics: map[string][]string{"analytics-workers": []string{"orders"}},
			},
			topicManager: topic.NewManager(),
			expectedErr:  errors.New("members failed"),
		},
		{
			name: "save fails",
			registry: &fakeCleanupRegistry{
				groups: []string{"analytics-workers"},
				removedMembers: map[string][]group.GroupMember{
					"analytics-workers": {{ID: "member-a", LastSeen: now.Add(-10 * time.Minute)}},
				},
				members: map[string][]group.GroupMember{
					"analytics-workers": {{ID: "member-b", LastSeen: now.Add(-time.Minute)}},
				},
			},
			assignmentStore: &fakeCleanupAssignmentStore{
				topics:  map[string][]string{"analytics-workers": []string{"orders"}},
				saveErr: errors.New("save failed"),
			},
			topicManager: topicManagerWithTopic(t, "orders", 2),
			expectedErr:  errors.New("save failed"),
		},
		{
			name: "delete fails when no active members remain",
			registry: &fakeCleanupRegistry{
				groups: []string{"analytics-workers"},
				removedMembers: map[string][]group.GroupMember{
					"analytics-workers": {{ID: "member-a", LastSeen: now.Add(-10 * time.Minute)}},
				},
			},
			assignmentStore: &fakeCleanupAssignmentStore{
				topics:    map[string][]string{"analytics-workers": []string{"orders"}},
				deleteErr: errors.New("delete failed"),
			},
			topicManager: topic.NewManager(),
			expectedErr:  errors.New("delete failed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cleanupStaleMembersAndRebalanceOnce(tt.registry, tt.assignmentStore, tt.topicManager, group.NewAssigner(), now, 5*time.Minute)
			if err == nil {
				t.Fatal("expected error")
			}
			if err.Error() == "" || tt.expectedErr.Error() == "" || !errors.Is(err, tt.expectedErr) && !strings.Contains(err.Error(), tt.expectedErr.Error()) {
				t.Fatalf("expected error %v, got %v", tt.expectedErr, err)
			}
		})
	}
}

func TestBackgroundStaleMemberCleanupRemovesStaleMembersAndUpdatesAssignmentsAfterTick(t *testing.T) {
	chdirTemp(t)
	registry := group.NewRegistry()
	assignmentStore := group.NewAssignmentStore()
	topicManager := topic.NewManager()

	if err := topicManager.CreateTopic("orders", 4); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-a", time.Now().Add(-10*time.Minute))
	recordRegistryHeartbeat(t, registry, "analytics-workers", "member-b", time.Now().Add(-time.Second))
	saveAssignments(t, assignmentStore, "analytics-workers", "orders", []group.Assignment{
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
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startStaleMemberCleanup(ctx, registry, assignmentStore, topicManager, group.NewAssigner(), 5*time.Millisecond, 5*time.Minute)

	expectedAssignments := []group.Assignment{
		{
			MemberID: "member-b",
			Topics: []group.TopicAssignment{
				{Topic: "orders", Partitions: []int{0, 1, 2, 3}},
			},
		},
	}
	waitForCondition(t, time.Second, func() bool {
		assignments, found, err := assignmentStore.Get("analytics-workers", "orders")
		if err != nil || !found {
			return false
		}

		return reflect.DeepEqual(assignments, expectedAssignments)
	})

	members, err := registry.Members("analytics-workers")
	if err != nil {
		t.Fatalf("failed to get members: %v", err)
	}
	if len(members) != 1 || members[0].ID != "member-b" {
		t.Fatalf("expected only active member-b to remain, got %v", members)
	}
}

func TestBackgroundStaleMemberCleanupStopsWhenContextIsCancelled(t *testing.T) {
	registry := &fakeCleanupRegistry{groups: []string{"analytics-workers"}}
	assignmentStore := &fakeCleanupAssignmentStore{}
	ctx, cancel := context.WithCancel(context.Background())

	startStaleMemberCleanup(ctx, registry, assignmentStore, topic.NewManager(), group.NewAssigner(), 5*time.Millisecond, 5*time.Minute)
	waitForCondition(t, time.Second, func() bool {
		return registry.removeCallCount() > 0
	})

	cancel()
	time.Sleep(25 * time.Millisecond)
	callsAfterCancel := registry.removeCallCount()
	time.Sleep(25 * time.Millisecond)

	if got := registry.removeCallCount(); got != callsAfterCancel {
		t.Fatalf("expected cleanup to stop at %d calls, got %d", callsAfterCancel, got)
	}
}

func TestBackgroundStaleMemberCleanupKeepsRunningAfterCleanupError(t *testing.T) {
	registry := &fakeCleanupRegistry{
		groups:    []string{"analytics-workers"},
		groupsErr: errors.New("first cleanup failed"),
	}
	assignmentStore := &fakeCleanupAssignmentStore{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startStaleMemberCleanup(ctx, registry, assignmentStore, topic.NewManager(), group.NewAssigner(), 5*time.Millisecond, 5*time.Minute)
	waitForCondition(t, time.Second, func() bool {
		return registry.removeCallCount() > 0
	})
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

	return newTestServerInCurrentDir(t)
}

func newTestServerInCurrentDir(t *testing.T) *http.Server {
	t.Helper()

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

func deleteGroupAssignments(t *testing.T, handler http.Handler, groupName string, topicName string) groupAssignmentDeleteResponse {
	t.Helper()

	recorder := performRequest(handler, http.MethodDelete, "/groups/assignments?group="+groupName+"&topic="+topicName, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var response groupAssignmentDeleteResponse
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

func seedDefaultRegistryFileStore(t *testing.T, groupName string, members map[string]time.Time) {
	t.Helper()

	registryStore, err := group.NewRegistryFileStore(defaultRegistryStorePath)
	if err != nil {
		t.Fatalf("failed to create registry store: %v", err)
	}
	if err := registryStore.Load(); err != nil {
		t.Fatalf("failed to load registry store: %v", err)
	}

	for memberID, lastSeen := range members {
		if err := registryStore.Heartbeat(groupName, memberID, lastSeen); err != nil {
			t.Fatalf("failed to seed registry member %q: %v", memberID, err)
		}
	}
}

func loadDefaultRegistryFileStore(t *testing.T) *group.RegistryFileStore {
	t.Helper()

	registryStore, err := group.NewRegistryFileStore(defaultRegistryStorePath)
	if err != nil {
		t.Fatalf("failed to create registry store: %v", err)
	}
	if err := registryStore.Load(); err != nil {
		t.Fatalf("failed to load registry store: %v", err)
	}

	return registryStore
}

func saveAssignments(t *testing.T, store assignmentStore, groupName string, topicName string, assignments []group.Assignment) {
	t.Helper()

	if err := store.Save(groupName, topicName, assignments); err != nil {
		t.Fatalf("failed to save assignments: %v", err)
	}
}

func topicManagerWithTopic(t *testing.T, topicName string, partitionCount int) *topic.Manager {
	t.Helper()
	chdirTemp(t)

	topicManager := topic.NewManager()
	if err := topicManager.CreateTopic(topicName, partitionCount); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	return topicManager
}

type fakeCleanupRegistry struct {
	mu             sync.Mutex
	groups         []string
	groupsErr      error
	removeErr      error
	membersErr     error
	removedMembers map[string][]group.GroupMember
	members        map[string][]group.GroupMember
	removeCalls    int
}

func (r *fakeCleanupRegistry) Groups() ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.groupsErr != nil {
		err := r.groupsErr
		r.groupsErr = nil
		return nil, err
	}

	return append([]string(nil), r.groups...), nil
}

func (r *fakeCleanupRegistry) Join(_ string, _ string) error {
	return nil
}

func (r *fakeCleanupRegistry) Heartbeat(_ string, _ string, _ time.Time) error {
	return nil
}

func (r *fakeCleanupRegistry) Leave(_ string, _ string) error {
	return nil
}

func (r *fakeCleanupRegistry) Members(groupName string) ([]group.GroupMember, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.membersErr != nil {
		return nil, r.membersErr
	}

	return cloneTestGroupMembers(r.members[groupName]), nil
}

func (r *fakeCleanupRegistry) StaleMembers(_ string, _ time.Time, _ time.Duration) ([]group.GroupMember, error) {
	return nil, nil
}

func (r *fakeCleanupRegistry) RemoveStaleMembers(groupName string, _ time.Time, _ time.Duration) ([]group.GroupMember, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.removeCalls++
	if r.removeErr != nil {
		return nil, r.removeErr
	}

	return cloneTestGroupMembers(r.removedMembers[groupName]), nil
}

func (r *fakeCleanupRegistry) removeCallCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.removeCalls
}

type fakeCleanupAssignmentStore struct {
	mu          sync.Mutex
	assignments map[string]map[string][]group.Assignment
	topics      map[string][]string
	topicsErr   error
	saveErr     error
	deleteErr   error
	topicsCalls int
	saveCalls   int
	deleteCalls int
}

func (s *fakeCleanupAssignmentStore) Save(groupName string, topicName string, assignments []group.Assignment) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.saveCalls++
	if s.saveErr != nil {
		return s.saveErr
	}

	if s.assignments == nil {
		s.assignments = make(map[string]map[string][]group.Assignment)
	}
	if _, exists := s.assignments[groupName]; !exists {
		s.assignments[groupName] = make(map[string][]group.Assignment)
	}

	s.assignments[groupName][topicName] = cloneTestAssignments(assignments)
	return nil
}

func (s *fakeCleanupAssignmentStore) Get(groupName string, topicName string) ([]group.Assignment, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	groupAssignments, exists := s.assignments[groupName]
	if !exists {
		return nil, false, nil
	}

	assignments, exists := groupAssignments[topicName]
	if !exists {
		return nil, false, nil
	}

	return cloneTestAssignments(assignments), true, nil
}

func (s *fakeCleanupAssignmentStore) Topics(groupName string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.topicsCalls++
	if s.topicsErr != nil {
		return nil, s.topicsErr
	}

	if s.topics != nil {
		return append([]string(nil), s.topics[groupName]...), nil
	}

	groupAssignments := s.assignments[groupName]
	topics := make([]string, 0, len(groupAssignments))
	for topicName := range groupAssignments {
		topics = append(topics, topicName)
	}

	return topics, nil
}

func (s *fakeCleanupAssignmentStore) Delete(groupName string, topicName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.deleteCalls++
	if s.deleteErr != nil {
		return s.deleteErr
	}

	groupAssignments, exists := s.assignments[groupName]
	if !exists {
		return nil
	}

	delete(groupAssignments, topicName)
	if len(groupAssignments) == 0 {
		delete(s.assignments, groupName)
	}

	return nil
}

func (s *fakeCleanupAssignmentStore) topicsCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.topicsCalls
}

func (s *fakeCleanupAssignmentStore) saveCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.saveCalls
}

func cloneTestAssignments(assignments []group.Assignment) []group.Assignment {
	copiedAssignments := make([]group.Assignment, len(assignments))
	for i, assignment := range assignments {
		copiedAssignments[i] = group.Assignment{
			MemberID: assignment.MemberID,
			Topics:   cloneTestTopicAssignments(assignment.Topics),
		}
	}

	return copiedAssignments
}

func cloneTestTopicAssignments(topicAssignments []group.TopicAssignment) []group.TopicAssignment {
	copiedTopicAssignments := make([]group.TopicAssignment, len(topicAssignments))
	for i, topicAssignment := range topicAssignments {
		copiedTopicAssignments[i] = group.TopicAssignment{
			Topic:      topicAssignment.Topic,
			Partitions: append([]int(nil), topicAssignment.Partitions...),
		}
	}

	return copiedTopicAssignments
}

func cloneTestGroupMembers(members []group.GroupMember) []group.GroupMember {
	return append([]group.GroupMember(nil), members...)
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

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatalf("condition was not met within %s", timeout)
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
