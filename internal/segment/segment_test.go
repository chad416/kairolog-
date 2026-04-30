package segment

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNewSegmentCreatesPaddedSegmentFile(t *testing.T) {
	dir := t.TempDir()

	segment, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	expectedPath := filepath.Join(dir, "00000000000000000000.log")
	if segment.Path() != expectedPath {
		t.Fatalf("expected path %q, got %q", expectedPath, segment.Path())
	}

	if _, err := os.Stat(expectedPath); err != nil {
		t.Fatalf("expected segment file to exist: %v", err)
	}
}

func TestSegmentOffsetsStartAtBaseOffset(t *testing.T) {
	segment, err := NewSegment(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	firstOffset, err := segment.Append("first")
	if err != nil {
		t.Fatalf("failed to append first record: %v", err)
	}

	secondOffset, err := segment.Append("second")
	if err != nil {
		t.Fatalf("failed to append second record: %v", err)
	}

	if firstOffset != 10 {
		t.Fatalf("expected first offset 10, got %d", firstOffset)
	}
	if secondOffset != 11 {
		t.Fatalf("expected second offset 11, got %d", secondOffset)
	}
	if segment.NextOffset() != 12 {
		t.Fatalf("expected next offset 12, got %d", segment.NextOffset())
	}
}

func TestReadAllReturnsRecords(t *testing.T) {
	segment, err := NewSegment(t.TempDir(), 5)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	if _, err := segment.Append("first"); err != nil {
		t.Fatalf("failed to append first record: %v", err)
	}
	if _, err := segment.Append("second"); err != nil {
		t.Fatalf("failed to append second record: %v", err)
	}

	records, err := segment.ReadAll()
	if err != nil {
		t.Fatalf("failed to read records: %v", err)
	}

	expected := []Record{
		{Offset: 5, Message: "first"},
		{Offset: 6, Message: "second"},
	}

	if !reflect.DeepEqual(records, expected) {
		t.Fatalf("expected %v, got %v", expected, records)
	}
}

func TestSegmentRecoversNextOffsetOnReopen(t *testing.T) {
	dir := t.TempDir()

	segment, err := NewSegment(dir, 100)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	if _, err := segment.Append("first"); err != nil {
		t.Fatalf("failed to append first record: %v", err)
	}
	if _, err := segment.Append("second"); err != nil {
		t.Fatalf("failed to append second record: %v", err)
	}

	reopenedSegment, err := NewSegment(dir, 100)
	if err != nil {
		t.Fatalf("failed to reopen segment: %v", err)
	}

	if reopenedSegment.NextOffset() != 102 {
		t.Fatalf("expected recovered next offset 102, got %d", reopenedSegment.NextOffset())
	}

	offset, err := reopenedSegment.Append("third")
	if err != nil {
		t.Fatalf("failed to append third record: %v", err)
	}

	if offset != 102 {
		t.Fatalf("expected appended offset 102, got %d", offset)
	}
}

func TestAppendRejectsNewlineCharacters(t *testing.T) {
	segment, err := NewSegment(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	if _, err := segment.Append("hello\nworld"); err == nil {
		t.Fatalf("expected newline message to be rejected")
	}

	if _, err := segment.Append("hello\rworld"); err == nil {
		t.Fatalf("expected carriage return message to be rejected")
	}
}

func TestSegmentAccessors(t *testing.T) {
	dir := t.TempDir()

	segment, err := NewSegment(dir, 42)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	expectedPath := filepath.Join(dir, "00000000000000000042.log")
	if segment.BaseOffset() != 42 {
		t.Fatalf("expected base offset 42, got %d", segment.BaseOffset())
	}
	if segment.NextOffset() != 42 {
		t.Fatalf("expected next offset 42, got %d", segment.NextOffset())
	}
	if segment.Path() != expectedPath {
		t.Fatalf("expected path %q, got %q", expectedPath, segment.Path())
	}
}
