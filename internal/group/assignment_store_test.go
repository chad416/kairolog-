package group

import (
	"reflect"
	"testing"
)

func TestAssignmentStoreSaveAndGetAssignment(t *testing.T) {
	store := NewAssignmentStore()
	assignments := sampleAssignments()

	if err := store.Save("analytics-workers", "orders", assignments); err != nil {
		t.Fatalf("failed to save assignments: %v", err)
	}

	got, found, err := store.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get assignments: %v", err)
	}
	if !found {
		t.Fatal("expected assignment to be found")
	}
	if !reflect.DeepEqual(got, assignments) {
		t.Fatalf("expected %v, got %v", assignments, got)
	}
}

func TestAssignmentStoreGetMissingAssignmentReturnsNotFound(t *testing.T) {
	store := NewAssignmentStore()

	assignments, found, err := store.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get assignments: %v", err)
	}
	if found {
		t.Fatal("expected assignment to be missing")
	}
	if len(assignments) != 0 {
		t.Fatalf("expected no assignments, got %v", assignments)
	}
}

func TestAssignmentStoreSaveReplacesPreviousAssignment(t *testing.T) {
	store := NewAssignmentStore()
	original := sampleAssignments()
	replacement := []Assignment{
		{
			MemberID: "member-c",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{0, 1, 2}},
			},
		},
	}

	if err := store.Save("analytics-workers", "orders", original); err != nil {
		t.Fatalf("failed to save original assignments: %v", err)
	}
	if err := store.Save("analytics-workers", "orders", replacement); err != nil {
		t.Fatalf("failed to save replacement assignments: %v", err)
	}

	got, found, err := store.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get assignments: %v", err)
	}
	if !found {
		t.Fatal("expected assignment to be found")
	}
	if !reflect.DeepEqual(got, replacement) {
		t.Fatalf("expected %v, got %v", replacement, got)
	}
}

func TestAssignmentStoreSeparateGroupsAreIsolated(t *testing.T) {
	store := NewAssignmentStore()
	analyticsAssignments := sampleAssignments()
	billingAssignments := []Assignment{
		{
			MemberID: "billing-member-a",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{0}},
			},
		},
	}

	if err := store.Save("analytics-workers", "orders", analyticsAssignments); err != nil {
		t.Fatalf("failed to save analytics assignments: %v", err)
	}
	if err := store.Save("billing-workers", "orders", billingAssignments); err != nil {
		t.Fatalf("failed to save billing assignments: %v", err)
	}

	got, found, err := store.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get assignments: %v", err)
	}
	if !found {
		t.Fatal("expected assignment to be found")
	}
	if !reflect.DeepEqual(got, analyticsAssignments) {
		t.Fatalf("expected %v, got %v", analyticsAssignments, got)
	}
}

func TestAssignmentStoreSeparateTopicsInSameGroupAreIsolated(t *testing.T) {
	store := NewAssignmentStore()
	orderAssignments := sampleAssignments()
	paymentAssignments := []Assignment{
		{
			MemberID: "member-c",
			Topics: []TopicAssignment{
				{Topic: "payments", Partitions: []int{0}},
			},
		},
	}

	if err := store.Save("analytics-workers", "orders", orderAssignments); err != nil {
		t.Fatalf("failed to save order assignments: %v", err)
	}
	if err := store.Save("analytics-workers", "payments", paymentAssignments); err != nil {
		t.Fatalf("failed to save payment assignments: %v", err)
	}

	got, found, err := store.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get assignments: %v", err)
	}
	if !found {
		t.Fatal("expected assignment to be found")
	}
	if !reflect.DeepEqual(got, orderAssignments) {
		t.Fatalf("expected %v, got %v", orderAssignments, got)
	}
}

func TestAssignmentStoreTopicsReturnsEmptySliceWhenStoreIsEmpty(t *testing.T) {
	store := NewAssignmentStore()

	topics := mustAssignmentStoreTopics(t, store, "analytics-workers")
	if len(topics) != 0 {
		t.Fatalf("expected no topics, got %v", topics)
	}
}

func TestAssignmentStoreTopicsReturnsSortedTopicNamesForGroup(t *testing.T) {
	store := NewAssignmentStore()

	if err := store.Save("analytics-workers", "payments", samplePaymentAssignments()); err != nil {
		t.Fatalf("failed to save payment assignments: %v", err)
	}
	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save order assignments: %v", err)
	}

	topics := mustAssignmentStoreTopics(t, store, "analytics-workers")
	expected := []string{"orders", "payments"}
	if !reflect.DeepEqual(topics, expected) {
		t.Fatalf("expected %v, got %v", expected, topics)
	}
}

func TestAssignmentStoreTopicsReturnsOnlyTopicsForRequestedGroup(t *testing.T) {
	store := NewAssignmentStore()

	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save analytics assignments: %v", err)
	}
	if err := store.Save("billing-workers", "payments", samplePaymentAssignments()); err != nil {
		t.Fatalf("failed to save billing assignments: %v", err)
	}

	topics := mustAssignmentStoreTopics(t, store, "analytics-workers")
	expected := []string{"orders"}
	if !reflect.DeepEqual(topics, expected) {
		t.Fatalf("expected %v, got %v", expected, topics)
	}
}

func TestAssignmentStoreTopicsExcludesDeletedGroupTopicAssignment(t *testing.T) {
	store := NewAssignmentStore()

	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save order assignments: %v", err)
	}
	if err := store.Save("analytics-workers", "payments", samplePaymentAssignments()); err != nil {
		t.Fatalf("failed to save payment assignments: %v", err)
	}
	if err := store.Delete("analytics-workers", "orders"); err != nil {
		t.Fatalf("failed to delete order assignments: %v", err)
	}

	topics := mustAssignmentStoreTopics(t, store, "analytics-workers")
	expected := []string{"payments"}
	if !reflect.DeepEqual(topics, expected) {
		t.Fatalf("expected %v, got %v", expected, topics)
	}
}

func TestAssignmentStoreTopicsReturnsEmptyAfterDeleteGroup(t *testing.T) {
	store := NewAssignmentStore()

	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save order assignments: %v", err)
	}
	if err := store.Save("analytics-workers", "payments", samplePaymentAssignments()); err != nil {
		t.Fatalf("failed to save payment assignments: %v", err)
	}
	if err := store.DeleteGroup("analytics-workers"); err != nil {
		t.Fatalf("failed to delete group assignments: %v", err)
	}

	topics := mustAssignmentStoreTopics(t, store, "analytics-workers")
	if len(topics) != 0 {
		t.Fatalf("expected no topics, got %v", topics)
	}
}

func TestAssignmentStoreTopicsRejectsEmptyGroup(t *testing.T) {
	store := NewAssignmentStore()

	if _, err := store.Topics(""); err == nil {
		t.Fatal("expected error")
	}
}

func TestAssignmentStoreDeleteRemovesOneGroupTopicAssignment(t *testing.T) {
	store := NewAssignmentStore()

	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save order assignments: %v", err)
	}
	if err := store.Save("analytics-workers", "payments", samplePaymentAssignments()); err != nil {
		t.Fatalf("failed to save payment assignments: %v", err)
	}

	if err := store.Delete("analytics-workers", "orders"); err != nil {
		t.Fatalf("failed to delete assignments: %v", err)
	}

	assignments, found, err := store.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get deleted assignments: %v", err)
	}
	if found {
		t.Fatalf("expected order assignments to be deleted, got %v", assignments)
	}

	paymentAssignments, found, err := store.Get("analytics-workers", "payments")
	if err != nil {
		t.Fatalf("failed to get payment assignments: %v", err)
	}
	if !found {
		t.Fatal("expected payment assignments to remain")
	}
	if !reflect.DeepEqual(paymentAssignments, samplePaymentAssignments()) {
		t.Fatalf("expected %v, got %v", samplePaymentAssignments(), paymentAssignments)
	}
}

func TestAssignmentStoreDeleteMissingAssignmentIsIdempotent(t *testing.T) {
	store := NewAssignmentStore()

	if err := store.Delete("analytics-workers", "orders"); err != nil {
		t.Fatalf("expected missing delete to succeed, got %v", err)
	}
}

func TestAssignmentStoreDeleteRemovesGroupEntryWhenLastTopicIsDeleted(t *testing.T) {
	store := NewAssignmentStore()

	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save assignments: %v", err)
	}
	if err := store.Delete("analytics-workers", "orders"); err != nil {
		t.Fatalf("failed to delete assignments: %v", err)
	}

	if _, exists := store.assignments["analytics-workers"]; exists {
		t.Fatal("expected group entry to be removed")
	}
}

func TestAssignmentStoreDeleteGroupRemovesAllAssignmentsForGroup(t *testing.T) {
	store := NewAssignmentStore()

	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save order assignments: %v", err)
	}
	if err := store.Save("analytics-workers", "payments", samplePaymentAssignments()); err != nil {
		t.Fatalf("failed to save payment assignments: %v", err)
	}
	if err := store.Save("billing-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save billing assignments: %v", err)
	}

	if err := store.DeleteGroup("analytics-workers"); err != nil {
		t.Fatalf("failed to delete group: %v", err)
	}

	_, found, err := store.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get order assignments: %v", err)
	}
	if found {
		t.Fatal("expected order assignments to be deleted")
	}

	_, found, err = store.Get("analytics-workers", "payments")
	if err != nil {
		t.Fatalf("failed to get payment assignments: %v", err)
	}
	if found {
		t.Fatal("expected payment assignments to be deleted")
	}

	_, found, err = store.Get("billing-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get billing assignments: %v", err)
	}
	if !found {
		t.Fatal("expected billing assignments to remain")
	}
}

func TestAssignmentStoreDeleteGroupMissingGroupIsIdempotent(t *testing.T) {
	store := NewAssignmentStore()

	if err := store.DeleteGroup("analytics-workers"); err != nil {
		t.Fatalf("expected missing group delete to succeed, got %v", err)
	}
}

func TestAssignmentStoreSaveRejectsInvalidInput(t *testing.T) {
	store := NewAssignmentStore()

	tests := []struct {
		name        string
		group       string
		topic       string
		assignments []Assignment
	}{
		{name: "empty group", group: "", topic: "orders", assignments: sampleAssignments()},
		{name: "empty topic", group: "analytics-workers", topic: "", assignments: sampleAssignments()},
		{name: "nil assignments", group: "analytics-workers", topic: "orders", assignments: nil},
		{name: "empty assignments", group: "analytics-workers", topic: "orders", assignments: []Assignment{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Save(tt.group, tt.topic, tt.assignments)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestAssignmentStoreGetRejectsInvalidInput(t *testing.T) {
	store := NewAssignmentStore()

	tests := []struct {
		name  string
		group string
		topic string
	}{
		{name: "empty group", group: "", topic: "orders"},
		{name: "empty topic", group: "analytics-workers", topic: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := store.Get(tt.group, tt.topic)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestAssignmentStoreDeleteRejectsInvalidInput(t *testing.T) {
	store := NewAssignmentStore()

	tests := []struct {
		name  string
		group string
		topic string
	}{
		{name: "empty group", group: "", topic: "orders"},
		{name: "empty topic", group: "analytics-workers", topic: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Delete(tt.group, tt.topic)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestAssignmentStoreDeleteGroupRejectsEmptyGroup(t *testing.T) {
	store := NewAssignmentStore()

	if err := store.DeleteGroup(""); err == nil {
		t.Fatal("expected error")
	}
}

func TestAssignmentStoreSaveDeepCopiesInputAssignments(t *testing.T) {
	store := NewAssignmentStore()
	assignments := sampleAssignments()

	if err := store.Save("analytics-workers", "orders", assignments); err != nil {
		t.Fatalf("failed to save assignments: %v", err)
	}

	assignments[0].MemberID = "mutated-member"
	assignments[0].Topics[0].Topic = "mutated-topic"
	assignments[0].Topics[0].Partitions[0] = 99

	got, found, err := store.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get assignments: %v", err)
	}
	if !found {
		t.Fatal("expected assignment to be found")
	}
	if !reflect.DeepEqual(got, sampleAssignments()) {
		t.Fatalf("expected %v, got %v", sampleAssignments(), got)
	}
}

func TestAssignmentStoreGetDeepCopiesReturnedAssignments(t *testing.T) {
	store := NewAssignmentStore()

	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save assignments: %v", err)
	}

	got, found, err := store.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get assignments: %v", err)
	}
	if !found {
		t.Fatal("expected assignment to be found")
	}

	got[0].MemberID = "mutated-member"
	got[0].Topics[0].Topic = "mutated-topic"
	got[0].Topics[0].Partitions[0] = 99

	gotAgain, found, err := store.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get assignments again: %v", err)
	}
	if !found {
		t.Fatal("expected assignment to be found")
	}
	if !reflect.DeepEqual(gotAgain, sampleAssignments()) {
		t.Fatalf("expected %v, got %v", sampleAssignments(), gotAgain)
	}
}

func TestAssignmentStorePreservesAssignmentOrder(t *testing.T) {
	store := NewAssignmentStore()
	assignments := []Assignment{
		{
			MemberID: "member-b",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{1}},
			},
		},
		{
			MemberID: "member-a",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{0}},
			},
		},
	}

	if err := store.Save("analytics-workers", "orders", assignments); err != nil {
		t.Fatalf("failed to save assignments: %v", err)
	}

	got, found, err := store.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get assignments: %v", err)
	}
	if !found {
		t.Fatal("expected assignment to be found")
	}
	if !reflect.DeepEqual(got, assignments) {
		t.Fatalf("expected %v, got %v", assignments, got)
	}
}

func sampleAssignments() []Assignment {
	return []Assignment{
		{
			MemberID: "member-a",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{0, 1}},
			},
		},
		{
			MemberID: "member-b",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{2, 3}},
			},
		},
	}
}

func samplePaymentAssignments() []Assignment {
	return []Assignment{
		{
			MemberID: "member-c",
			Topics: []TopicAssignment{
				{Topic: "payments", Partitions: []int{0}},
			},
		},
	}
}

func mustAssignmentStoreTopics(t *testing.T, store *AssignmentStore, group string) []string {
	t.Helper()

	topics, err := store.Topics(group)
	if err != nil {
		t.Fatalf("failed to get assignment topics: %v", err)
	}

	return topics
}
