package group

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestAssignmentFileStoreRejectsEmptyPath(t *testing.T) {
	if _, err := NewAssignmentFileStore(""); err == nil {
		t.Fatal("expected error")
	}
}

func TestAssignmentFileStoreSaveAndGetAssignment(t *testing.T) {
	store := newTestAssignmentFileStore(t)
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

func TestAssignmentFileStoreGetMissingAssignmentReturnsNotFound(t *testing.T) {
	store := newTestAssignmentFileStore(t)

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

func TestAssignmentFileStoreSaveReplacesPreviousAssignment(t *testing.T) {
	store := newTestAssignmentFileStore(t)
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

func TestAssignmentFileStoreSeparateGroupsAreIsolated(t *testing.T) {
	store := newTestAssignmentFileStore(t)
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
		t.Fatalf("failed to get analytics assignments: %v", err)
	}
	if !found {
		t.Fatal("expected analytics assignment to be found")
	}
	if !reflect.DeepEqual(got, analyticsAssignments) {
		t.Fatalf("expected %v, got %v", analyticsAssignments, got)
	}

	got, found, err = store.Get("billing-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get billing assignments: %v", err)
	}
	if !found {
		t.Fatal("expected billing assignment to be found")
	}
	if !reflect.DeepEqual(got, billingAssignments) {
		t.Fatalf("expected %v, got %v", billingAssignments, got)
	}
}

func TestAssignmentFileStoreSeparateTopicsInSameGroupAreIsolated(t *testing.T) {
	store := newTestAssignmentFileStore(t)
	orderAssignments := sampleAssignments()
	paymentAssignments := samplePaymentAssignments()

	if err := store.Save("analytics-workers", "orders", orderAssignments); err != nil {
		t.Fatalf("failed to save order assignments: %v", err)
	}
	if err := store.Save("analytics-workers", "payments", paymentAssignments); err != nil {
		t.Fatalf("failed to save payment assignments: %v", err)
	}

	got, found, err := store.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get order assignments: %v", err)
	}
	if !found {
		t.Fatal("expected order assignment to be found")
	}
	if !reflect.DeepEqual(got, orderAssignments) {
		t.Fatalf("expected %v, got %v", orderAssignments, got)
	}

	got, found, err = store.Get("analytics-workers", "payments")
	if err != nil {
		t.Fatalf("failed to get payment assignments: %v", err)
	}
	if !found {
		t.Fatal("expected payment assignment to be found")
	}
	if !reflect.DeepEqual(got, paymentAssignments) {
		t.Fatalf("expected %v, got %v", paymentAssignments, got)
	}
}

func TestAssignmentFileStoreTopicsReturnsEmptySliceWhenStoreIsEmpty(t *testing.T) {
	store := newTestAssignmentFileStore(t)

	topics := mustAssignmentFileStoreTopics(t, store, "analytics-workers")
	if len(topics) != 0 {
		t.Fatalf("expected no topics, got %v", topics)
	}
}

func TestAssignmentFileStoreTopicsReturnsSortedTopicNamesForGroup(t *testing.T) {
	store := newTestAssignmentFileStore(t)

	if err := store.Save("analytics-workers", "payments", samplePaymentAssignments()); err != nil {
		t.Fatalf("failed to save payment assignments: %v", err)
	}
	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save order assignments: %v", err)
	}

	topics := mustAssignmentFileStoreTopics(t, store, "analytics-workers")
	expected := []string{"orders", "payments"}
	if !reflect.DeepEqual(topics, expected) {
		t.Fatalf("expected %v, got %v", expected, topics)
	}
}

func TestAssignmentFileStoreTopicsReturnsOnlyTopicsForRequestedGroup(t *testing.T) {
	store := newTestAssignmentFileStore(t)

	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save analytics assignments: %v", err)
	}
	if err := store.Save("billing-workers", "payments", samplePaymentAssignments()); err != nil {
		t.Fatalf("failed to save billing assignments: %v", err)
	}

	topics := mustAssignmentFileStoreTopics(t, store, "analytics-workers")
	expected := []string{"orders"}
	if !reflect.DeepEqual(topics, expected) {
		t.Fatalf("expected %v, got %v", expected, topics)
	}
}

func TestAssignmentFileStoreTopicsExcludesDeletedGroupTopicAssignment(t *testing.T) {
	store := newTestAssignmentFileStore(t)

	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save order assignments: %v", err)
	}
	if err := store.Save("analytics-workers", "payments", samplePaymentAssignments()); err != nil {
		t.Fatalf("failed to save payment assignments: %v", err)
	}
	if err := store.Delete("analytics-workers", "orders"); err != nil {
		t.Fatalf("failed to delete order assignments: %v", err)
	}

	topics := mustAssignmentFileStoreTopics(t, store, "analytics-workers")
	expected := []string{"payments"}
	if !reflect.DeepEqual(topics, expected) {
		t.Fatalf("expected %v, got %v", expected, topics)
	}
}

func TestAssignmentFileStoreTopicsReturnsEmptyAfterDeleteGroup(t *testing.T) {
	store := newTestAssignmentFileStore(t)

	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save order assignments: %v", err)
	}
	if err := store.Save("analytics-workers", "payments", samplePaymentAssignments()); err != nil {
		t.Fatalf("failed to save payment assignments: %v", err)
	}
	if err := store.DeleteGroup("analytics-workers"); err != nil {
		t.Fatalf("failed to delete group assignments: %v", err)
	}

	topics := mustAssignmentFileStoreTopics(t, store, "analytics-workers")
	if len(topics) != 0 {
		t.Fatalf("expected no topics, got %v", topics)
	}
}

func TestAssignmentFileStoreTopicsPersistAcrossLoad(t *testing.T) {
	path := assignmentFileStorePath(t)
	store := newTestAssignmentFileStoreAt(t, path)

	if err := store.Save("analytics-workers", "payments", samplePaymentAssignments()); err != nil {
		t.Fatalf("failed to save payment assignments: %v", err)
	}
	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save order assignments: %v", err)
	}

	reopened := newTestAssignmentFileStoreAt(t, path)
	if err := reopened.Load(); err != nil {
		t.Fatalf("failed to load assignments: %v", err)
	}

	topics := mustAssignmentFileStoreTopics(t, reopened, "analytics-workers")
	expected := []string{"orders", "payments"}
	if !reflect.DeepEqual(topics, expected) {
		t.Fatalf("expected %v, got %v", expected, topics)
	}
}

func TestAssignmentFileStoreTopicsReflectDeleteAfterLoad(t *testing.T) {
	path := assignmentFileStorePath(t)
	store := newTestAssignmentFileStoreAt(t, path)

	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save order assignments: %v", err)
	}
	if err := store.Save("analytics-workers", "payments", samplePaymentAssignments()); err != nil {
		t.Fatalf("failed to save payment assignments: %v", err)
	}
	if err := store.Delete("analytics-workers", "orders"); err != nil {
		t.Fatalf("failed to delete order assignments: %v", err)
	}

	reopened := newTestAssignmentFileStoreAt(t, path)
	if err := reopened.Load(); err != nil {
		t.Fatalf("failed to load assignments: %v", err)
	}

	topics := mustAssignmentFileStoreTopics(t, reopened, "analytics-workers")
	expected := []string{"payments"}
	if !reflect.DeepEqual(topics, expected) {
		t.Fatalf("expected %v, got %v", expected, topics)
	}
}

func TestAssignmentFileStoreTopicsReflectDeleteGroupAfterLoad(t *testing.T) {
	path := assignmentFileStorePath(t)
	store := newTestAssignmentFileStoreAt(t, path)

	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save order assignments: %v", err)
	}
	if err := store.Save("analytics-workers", "payments", samplePaymentAssignments()); err != nil {
		t.Fatalf("failed to save payment assignments: %v", err)
	}
	if err := store.DeleteGroup("analytics-workers"); err != nil {
		t.Fatalf("failed to delete group assignments: %v", err)
	}

	reopened := newTestAssignmentFileStoreAt(t, path)
	if err := reopened.Load(); err != nil {
		t.Fatalf("failed to load assignments: %v", err)
	}

	topics := mustAssignmentFileStoreTopics(t, reopened, "analytics-workers")
	if len(topics) != 0 {
		t.Fatalf("expected no topics, got %v", topics)
	}
}

func TestAssignmentFileStoreTopicsRejectsEmptyGroup(t *testing.T) {
	store := newTestAssignmentFileStore(t)

	if _, err := store.Topics(""); err == nil {
		t.Fatal("expected error")
	}
}

func TestAssignmentFileStoreDeleteRemovesOneGroupTopicAssignment(t *testing.T) {
	store := newTestAssignmentFileStore(t)

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

func TestAssignmentFileStoreDeleteMissingAssignmentIsIdempotent(t *testing.T) {
	store := newTestAssignmentFileStore(t)

	if err := store.Delete("analytics-workers", "orders"); err != nil {
		t.Fatalf("expected missing delete to succeed, got %v", err)
	}
}

func TestAssignmentFileStoreDeleteGroupRemovesAllAssignmentsForGroup(t *testing.T) {
	store := newTestAssignmentFileStore(t)

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

func TestAssignmentFileStoreDeleteGroupMissingGroupIsIdempotent(t *testing.T) {
	store := newTestAssignmentFileStore(t)

	if err := store.DeleteGroup("analytics-workers"); err != nil {
		t.Fatalf("expected missing group delete to succeed, got %v", err)
	}
}

func TestAssignmentFileStoreSaveRejectsInvalidInput(t *testing.T) {
	store := newTestAssignmentFileStore(t)

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

func TestAssignmentFileStoreGetRejectsInvalidInput(t *testing.T) {
	store := newTestAssignmentFileStore(t)

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

func TestAssignmentFileStoreDeleteRejectsInvalidInput(t *testing.T) {
	store := newTestAssignmentFileStore(t)

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

func TestAssignmentFileStoreDeleteGroupRejectsEmptyGroup(t *testing.T) {
	store := newTestAssignmentFileStore(t)

	if err := store.DeleteGroup(""); err == nil {
		t.Fatal("expected error")
	}
}

func TestAssignmentFileStoreSaveDeepCopiesInputAssignments(t *testing.T) {
	store := newTestAssignmentFileStore(t)
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

func TestAssignmentFileStoreGetDeepCopiesReturnedAssignments(t *testing.T) {
	store := newTestAssignmentFileStore(t)

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

func TestAssignmentFileStorePreservesAssignmentOrder(t *testing.T) {
	store := newTestAssignmentFileStore(t)
	assignments := orderedAssignments()

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

func TestAssignmentFileStoreSavePersistsAssignmentToDisk(t *testing.T) {
	path := assignmentFileStorePath(t)
	store := newTestAssignmentFileStoreAt(t, path)

	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save assignments: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read assignment file: %v", err)
	}

	contents := string(data)
	if !strings.Contains(contents, `"group":"analytics-workers"`) {
		t.Fatalf("expected assignment file to contain group, got %q", contents)
	}
	if !strings.Contains(contents, `"topic":"orders"`) {
		t.Fatalf("expected assignment file to contain topic, got %q", contents)
	}
}

func TestAssignmentFileStoreLoadRestoresSavedAssignmentFromDisk(t *testing.T) {
	path := assignmentFileStorePath(t)
	store := newTestAssignmentFileStoreAt(t, path)

	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save assignments: %v", err)
	}

	reopened := newTestAssignmentFileStoreAt(t, path)
	if err := reopened.Load(); err != nil {
		t.Fatalf("failed to load assignments: %v", err)
	}

	got, found, err := reopened.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get loaded assignments: %v", err)
	}
	if !found {
		t.Fatal("expected assignment to be found")
	}
	if !reflect.DeepEqual(got, sampleAssignments()) {
		t.Fatalf("expected %v, got %v", sampleAssignments(), got)
	}
}

func TestAssignmentFileStoreDeletePersistsDeletionToDisk(t *testing.T) {
	path := assignmentFileStorePath(t)
	store := newTestAssignmentFileStoreAt(t, path)

	if err := store.Save("analytics-workers", "orders", sampleAssignments()); err != nil {
		t.Fatalf("failed to save assignments: %v", err)
	}
	if err := store.Delete("analytics-workers", "orders"); err != nil {
		t.Fatalf("failed to delete assignments: %v", err)
	}

	reopened := newTestAssignmentFileStoreAt(t, path)
	if err := reopened.Load(); err != nil {
		t.Fatalf("failed to load assignments: %v", err)
	}

	assignments, found, err := reopened.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get deleted assignments: %v", err)
	}
	if found {
		t.Fatalf("expected assignment to be deleted, got %v", assignments)
	}
}

func TestAssignmentFileStoreDeleteGroupPersistsDeletionToDisk(t *testing.T) {
	path := assignmentFileStorePath(t)
	store := newTestAssignmentFileStoreAt(t, path)

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
		t.Fatalf("failed to delete group assignments: %v", err)
	}

	reopened := newTestAssignmentFileStoreAt(t, path)
	if err := reopened.Load(); err != nil {
		t.Fatalf("failed to load assignments: %v", err)
	}

	_, found, err := reopened.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get order assignments: %v", err)
	}
	if found {
		t.Fatal("expected order assignments to be deleted")
	}

	_, found, err = reopened.Get("analytics-workers", "payments")
	if err != nil {
		t.Fatalf("failed to get payment assignments: %v", err)
	}
	if found {
		t.Fatal("expected payment assignments to be deleted")
	}

	_, found, err = reopened.Get("billing-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get billing assignments: %v", err)
	}
	if !found {
		t.Fatal("expected billing assignments to remain")
	}
}

func TestAssignmentFileStoreLoadOnMissingFileSucceedsWithEmptyState(t *testing.T) {
	path := assignmentFileStorePath(t)
	store := newTestAssignmentFileStoreAt(t, path)

	if err := os.Remove(path); err != nil {
		t.Fatalf("failed to remove assignment file: %v", err)
	}

	if err := store.Load(); err != nil {
		t.Fatalf("expected load on missing file to succeed, got %v", err)
	}

	assignments, found, err := store.Get("analytics-workers", "orders")
	if err != nil {
		t.Fatalf("failed to get assignments: %v", err)
	}
	if found {
		t.Fatalf("expected assignment to be missing, got %v", assignments)
	}
	if len(assignments) != 0 {
		t.Fatalf("expected empty assignments, got %v", assignments)
	}
}

func TestAssignmentFileStoreCreatesParentDirectoryAutomatically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "assignments", "assignments.log")

	if _, err := NewAssignmentFileStore(path); err != nil {
		t.Fatalf("failed to create assignment file store: %v", err)
	}

	if _, err := os.Stat(filepath.Dir(path)); err != nil {
		t.Fatalf("expected parent directory to exist: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected assignment file to exist: %v", err)
	}
}

func newTestAssignmentFileStore(t *testing.T) *AssignmentFileStore {
	t.Helper()
	return newTestAssignmentFileStoreAt(t, assignmentFileStorePath(t))
}

func newTestAssignmentFileStoreAt(t *testing.T, path string) *AssignmentFileStore {
	t.Helper()

	store, err := NewAssignmentFileStore(path)
	if err != nil {
		t.Fatalf("failed to create assignment file store: %v", err)
	}

	return store
}

func assignmentFileStorePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "data", "assignments.log")
}

func orderedAssignments() []Assignment {
	return []Assignment{
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
}

func mustAssignmentFileStoreTopics(t *testing.T, store *AssignmentFileStore, group string) []string {
	t.Helper()

	topics, err := store.Topics(group)
	if err != nil {
		t.Fatalf("failed to get assignment topics: %v", err)
	}

	return topics
}
