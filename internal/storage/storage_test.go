package storage

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestAppendWritesMessage(t *testing.T) {
	store, path := newTestStore(t)

	if err := store.Append("hello"); err != nil {
		t.Fatalf("failed to append message: %v", err)
	}

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read messages file: %v", err)
	}

	expected := "hello\n"
	if string(contents) != expected {
		t.Fatalf("expected file contents %q, got %q", expected, string(contents))
	}
}

func TestReadAllReturnsAllStoredMessages(t *testing.T) {
	store, _ := newTestStore(t)

	if err := store.Append("first"); err != nil {
		t.Fatalf("failed to append first message: %v", err)
	}
	if err := store.Append("second"); err != nil {
		t.Fatalf("failed to append second message: %v", err)
	}

	messages, err := store.ReadAll()
	if err != nil {
		t.Fatalf("failed to read messages: %v", err)
	}

	expected := []string{"first", "second"}
	if !reflect.DeepEqual(messages, expected) {
		t.Fatalf("expected %v, got %v", expected, messages)
	}
}

func TestMultipleAppends(t *testing.T) {
	store, _ := newTestStore(t)

	expected := []string{"one", "two", "three"}
	for _, message := range expected {
		if err := store.Append(message); err != nil {
			t.Fatalf("failed to append message %q: %v", message, err)
		}
	}

	messages, err := store.ReadAll()
	if err != nil {
		t.Fatalf("failed to read messages: %v", err)
	}

	if !reflect.DeepEqual(messages, expected) {
		t.Fatalf("expected %v, got %v", expected, messages)
	}
}

func TestAppendRecordReturnsOffsets(t *testing.T) {
	store, _ := newTestStore(t)

	firstOffset, err := store.AppendRecord("first")
	if err != nil {
		t.Fatalf("failed to append first record: %v", err)
	}

	secondOffset, err := store.AppendRecord("second")
	if err != nil {
		t.Fatalf("failed to append second record: %v", err)
	}

	if firstOffset != 0 {
		t.Fatalf("expected first offset 0, got %d", firstOffset)
	}
	if secondOffset != 1 {
		t.Fatalf("expected second offset 1, got %d", secondOffset)
	}
}

func TestReadAllRecordsReturnsStoredRecords(t *testing.T) {
	store, _ := newTestStore(t)

	if err := store.Append("first"); err != nil {
		t.Fatalf("failed to append first message: %v", err)
	}
	if err := store.Append("second"); err != nil {
		t.Fatalf("failed to append second message: %v", err)
	}

	records, err := store.ReadAllRecords()
	if err != nil {
		t.Fatalf("failed to read records: %v", err)
	}

	expected := []Record{
		{Offset: 0, Message: "first"},
		{Offset: 1, Message: "second"},
	}

	if !reflect.DeepEqual(records, expected) {
		t.Fatalf("expected %v, got %v", expected, records)
	}
}

func TestNextOffsetIsRecoveredFromFile(t *testing.T) {
	store, path := newTestStore(t)

	if err := store.Append("existing"); err != nil {
		t.Fatalf("failed to append existing message: %v", err)
	}

	restartedStore, err := NewFileStoreAt(path)
	if err != nil {
		t.Fatalf("failed to restart file store: %v", err)
	}

	offset, err := restartedStore.AppendRecord("new")
	if err != nil {
		t.Fatalf("failed to append new record: %v", err)
	}

	if offset != 1 {
		t.Fatalf("expected recovered offset 1, got %d", offset)
	}
}

func TestFileIsCreatedAutomatically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data", "messages.log")

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected messages file to not exist before store creation")
	}

	if _, err := NewFileStoreAt(path); err != nil {
		t.Fatalf("failed to create file store: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("expected messages file to be created: %v", err)
	}
	if info.IsDir() {
		t.Fatalf("expected messages path to be a file")
	}
}

func TestReturnedMessagesMatchFileContents(t *testing.T) {
	store, path := newTestStore(t)

	expected := []string{"alpha", "beta", "gamma"}
	for _, message := range expected {
		if err := store.Append(message); err != nil {
			t.Fatalf("failed to append message %q: %v", message, err)
		}
	}

	messages, err := store.ReadAll()
	if err != nil {
		t.Fatalf("failed to read messages: %v", err)
	}

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read messages file: %v", err)
	}

	fileMessages := strings.Split(strings.TrimSuffix(string(contents), "\n"), "\n")
	if !reflect.DeepEqual(messages, fileMessages) {
		t.Fatalf("expected returned messages %v to match file contents %v", messages, fileMessages)
	}
}

func newTestStore(t *testing.T) (*FileStore, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "data", "messages.log")
	store, err := NewFileStoreAt(path)
	if err != nil {
		t.Fatalf("failed to create file store: %v", err)
	}

	return store, path
}
