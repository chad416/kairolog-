package index

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNewIndexCreatesPaddedIndexFile(t *testing.T) {
	dir := t.TempDir()

	index, err := NewIndex(dir, 0)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	expectedPath := filepath.Join(dir, "00000000000000000000.index")
	if index.Path() != expectedPath {
		t.Fatalf("expected path %q, got %q", expectedPath, index.Path())
	}

	if _, err := os.Stat(expectedPath); err != nil {
		t.Fatalf("expected index file to exist: %v", err)
	}
}

func TestAppendAndReadAllReturnsEntries(t *testing.T) {
	index, err := NewIndex(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	if err := index.Append(10, 0); err != nil {
		t.Fatalf("failed to append first entry: %v", err)
	}
	if err := index.Append(11, 42); err != nil {
		t.Fatalf("failed to append second entry: %v", err)
	}

	entries, err := index.ReadAll()
	if err != nil {
		t.Fatalf("failed to read entries: %v", err)
	}

	expected := []Entry{
		{Offset: 10, Position: 0},
		{Offset: 11, Position: 42},
	}

	if !reflect.DeepEqual(entries, expected) {
		t.Fatalf("expected %v, got %v", expected, entries)
	}
}

func TestReadAllReturnsEntriesAfterReopen(t *testing.T) {
	dir := t.TempDir()

	index, err := NewIndex(dir, 100)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	if err := index.Append(100, 0); err != nil {
		t.Fatalf("failed to append first entry: %v", err)
	}
	if err := index.Append(101, 24); err != nil {
		t.Fatalf("failed to append second entry: %v", err)
	}

	reopenedIndex, err := NewIndex(dir, 100)
	if err != nil {
		t.Fatalf("failed to reopen index: %v", err)
	}

	entries, err := reopenedIndex.ReadAll()
	if err != nil {
		t.Fatalf("failed to read entries: %v", err)
	}

	expected := []Entry{
		{Offset: 100, Position: 0},
		{Offset: 101, Position: 24},
	}

	if !reflect.DeepEqual(entries, expected) {
		t.Fatalf("expected %v, got %v", expected, entries)
	}
}

func TestAppendRejectsOffsetSmallerThanBaseOffset(t *testing.T) {
	index, err := NewIndex(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	if err := index.Append(9, 0); err == nil {
		t.Fatalf("expected offset smaller than base offset to be rejected")
	}
}

func TestAppendRejectsNegativePosition(t *testing.T) {
	index, err := NewIndex(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	if err := index.Append(0, -1); err == nil {
		t.Fatalf("expected negative position to be rejected")
	}
}

func TestIndexAccessors(t *testing.T) {
	dir := t.TempDir()

	index, err := NewIndex(dir, 42)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	expectedPath := filepath.Join(dir, "00000000000000000042.index")
	if index.BaseOffset() != 42 {
		t.Fatalf("expected base offset 42, got %d", index.BaseOffset())
	}
	if index.Path() != expectedPath {
		t.Fatalf("expected path %q, got %q", expectedPath, index.Path())
	}
}
