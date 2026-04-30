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

func TestAppendWithPositionReturnsBytePositions(t *testing.T) {
	segment, err := NewSegment(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	firstOffset, firstPosition, err := segment.AppendWithPosition("first")
	if err != nil {
		t.Fatalf("failed to append first record: %v", err)
	}

	secondOffset, secondPosition, err := segment.AppendWithPosition("second")
	if err != nil {
		t.Fatalf("failed to append second record: %v", err)
	}

	if firstOffset != 10 || firstPosition != 0 {
		t.Fatalf("expected first offset/position 10/0, got %d/%d", firstOffset, firstPosition)
	}

	expectedSecondPosition := int64(len("first\n"))
	if secondOffset != 11 || secondPosition != expectedSecondPosition {
		t.Fatalf("expected second offset/position 11/%d, got %d/%d", expectedSecondPosition, secondOffset, secondPosition)
	}
}

func TestReadFromPositionReturnsRecordsFromBytePosition(t *testing.T) {
	segment, err := NewSegment(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	if _, _, err := segment.AppendWithPosition("first"); err != nil {
		t.Fatalf("failed to append first record: %v", err)
	}

	secondOffset, secondPosition, err := segment.AppendWithPosition("second")
	if err != nil {
		t.Fatalf("failed to append second record: %v", err)
	}

	if _, _, err := segment.AppendWithPosition("third"); err != nil {
		t.Fatalf("failed to append third record: %v", err)
	}

	records, err := segment.ReadFromPosition(secondOffset, secondPosition)
	if err != nil {
		t.Fatalf("failed to read records from position: %v", err)
	}

	expected := []Record{
		{Offset: 11, Message: "second"},
		{Offset: 12, Message: "third"},
	}

	if !reflect.DeepEqual(records, expected) {
		t.Fatalf("expected %v, got %v", expected, records)
	}
}
