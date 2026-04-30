package consumer

import (
	"bufio"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNewOffsetStoreCreatesFileAndParentDirectories(t *testing.T) {
	path := filepath.Join(t.TempDir(), "consumer", "offsets.log")

	if _, err := NewOffsetStore(path); err != nil {
		t.Fatalf("failed to create offset store: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected offset store file to exist: %v", err)
	}
}

func TestCommitAndGet(t *testing.T) {
	store := newTestOffsetStore(t)

	if err := store.Commit("group-a", "orders", 0, 42); err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}

	offset, ok, err := store.Get("group-a", "orders", 0)
	if err != nil {
		t.Fatalf("failed to get committed offset: %v", err)
	}
	if !ok {
		t.Fatalf("expected committed offset to exist")
	}
	if offset != 42 {
		t.Fatalf("expected offset 42, got %d", offset)
	}
}

func TestGetReturnsFalseForMissingCommit(t *testing.T) {
	store := newTestOffsetStore(t)

	_, ok, err := store.Get("group-a", "orders", 0)
	if err != nil {
		t.Fatalf("failed to get missing committed offset: %v", err)
	}
	if ok {
		t.Fatalf("expected missing committed offset")
	}
}

func TestListReturnsCommittedOffsets(t *testing.T) {
	store := newTestOffsetStore(t)

	commitOffset(t, store, "group-b", "payments", 1, 20)
	commitOffset(t, store, "group-a", "orders", 0, 10)

	commits, err := store.List()
	if err != nil {
		t.Fatalf("failed to list commits: %v", err)
	}

	expected := []OffsetCommit{
		{Group: "group-a", Topic: "orders", Partition: 0, Offset: 10},
		{Group: "group-b", Topic: "payments", Partition: 1, Offset: 20},
	}

	if !reflect.DeepEqual(commits, expected) {
		t.Fatalf("expected %v, got %v", expected, commits)
	}
}

func TestCommitReplacesExistingOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offsets.log")
	store, err := NewOffsetStore(path)
	if err != nil {
		t.Fatalf("failed to create offset store: %v", err)
	}

	commitOffset(t, store, "group-a", "orders", 0, 10)
	commitOffset(t, store, "group-a", "orders", 0, 25)

	offset, ok, err := store.Get("group-a", "orders", 0)
	if err != nil {
		t.Fatalf("failed to get committed offset: %v", err)
	}
	if !ok {
		t.Fatalf("expected committed offset to exist")
	}
	if offset != 25 {
		t.Fatalf("expected latest offset 25, got %d", offset)
	}

	if lineCount := countLines(t, path); lineCount != 1 {
		t.Fatalf("expected 1 persisted commit line, got %d", lineCount)
	}
}

func TestReopenReadsExistingCommits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offsets.log")

	store, err := NewOffsetStore(path)
	if err != nil {
		t.Fatalf("failed to create offset store: %v", err)
	}

	commitOffset(t, store, "group-a", "orders", 0, 10)
	commitOffset(t, store, "group-a", "orders", 1, 15)

	reopenedStore, err := NewOffsetStore(path)
	if err != nil {
		t.Fatalf("failed to reopen offset store: %v", err)
	}

	offset, ok, err := reopenedStore.Get("group-a", "orders", 1)
	if err != nil {
		t.Fatalf("failed to get committed offset after reopen: %v", err)
	}
	if !ok {
		t.Fatalf("expected committed offset to exist after reopen")
	}
	if offset != 15 {
		t.Fatalf("expected offset 15 after reopen, got %d", offset)
	}
}

func TestCommitRejectsInvalidInput(t *testing.T) {
	store := newTestOffsetStore(t)

	tests := []struct {
		name      string
		group     string
		topic     string
		partition int
		offset    int64
	}{
		{name: "empty group", group: "", topic: "orders", partition: 0, offset: 1},
		{name: "empty topic", group: "group-a", topic: "", partition: 0, offset: 1},
		{name: "negative partition", group: "group-a", topic: "orders", partition: -1, offset: 1},
		{name: "negative offset", group: "group-a", topic: "orders", partition: 0, offset: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Commit(tt.group, tt.topic, tt.partition, tt.offset)
			if err == nil {
				t.Fatalf("expected commit to reject invalid input")
			}
		})
	}
}

func TestGetRejectsInvalidInput(t *testing.T) {
	store := newTestOffsetStore(t)

	tests := []struct {
		name      string
		group     string
		topic     string
		partition int
	}{
		{name: "empty group", group: "", topic: "orders", partition: 0},
		{name: "empty topic", group: "group-a", topic: "", partition: 0},
		{name: "negative partition", group: "group-a", topic: "orders", partition: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := store.Get(tt.group, tt.topic, tt.partition)
			if err == nil {
				t.Fatalf("expected get to reject invalid input")
			}
		})
	}
}

func newTestOffsetStore(t *testing.T) *OffsetStore {
	t.Helper()

	store, err := NewOffsetStore(filepath.Join(t.TempDir(), "offsets.log"))
	if err != nil {
		t.Fatalf("failed to create offset store: %v", err)
	}

	return store
}

func commitOffset(t *testing.T, store *OffsetStore, group string, topic string, partition int, offset int64) {
	t.Helper()

	if err := store.Commit(group, topic, partition, offset); err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}
}

func countLines(t *testing.T, path string) int {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	count := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		count++
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	return count
}
