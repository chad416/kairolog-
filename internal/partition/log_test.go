package partition

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNewLogCreatesSegmentAndIndex(t *testing.T) {
	dir := t.TempDir()

	if _, err := NewLog(dir); err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	segmentPath := filepath.Join(dir, "00000000000000000000.log")
	if _, err := os.Stat(segmentPath); err != nil {
		t.Fatalf("expected segment file to exist: %v", err)
	}

	indexPath := filepath.Join(dir, "00000000000000000000.index")
	if _, err := os.Stat(indexPath); err != nil {
		t.Fatalf("expected index file to exist: %v", err)
	}
}

func TestAppendReturnsOffsetsAndWritesIndexEntries(t *testing.T) {
	log, err := NewLog(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	firstOffset, err := log.Append("first")
	if err != nil {
		t.Fatalf("failed to append first record: %v", err)
	}

	secondOffset, err := log.Append("second")
	if err != nil {
		t.Fatalf("failed to append second record: %v", err)
	}

	if firstOffset != 0 {
		t.Fatalf("expected first offset 0, got %d", firstOffset)
	}
	if secondOffset != 1 {
		t.Fatalf("expected second offset 1, got %d", secondOffset)
	}

	entries, err := log.index.ReadAll()
	if err != nil {
		t.Fatalf("failed to read index entries: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 index entries, got %d", len(entries))
	}
	if entries[0].Offset != 0 || entries[0].Position != 0 {
		t.Fatalf("expected first index entry offset 0 position 0, got %+v", entries[0])
	}

	expectedSecondPosition := int64(len("first\n"))
	if entries[1].Offset != 1 || entries[1].Position != expectedSecondPosition {
		t.Fatalf("expected second index entry offset 1 position %d, got %+v", expectedSecondPosition, entries[1])
	}
}

func TestReadAllReturnsAllRecords(t *testing.T) {
	log, err := NewLog(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	if _, err := log.Append("first"); err != nil {
		t.Fatalf("failed to append first record: %v", err)
	}
	if _, err := log.Append("second"); err != nil {
		t.Fatalf("failed to append second record: %v", err)
	}

	records, err := log.ReadAll()
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

func TestReadFromReturnsRecordsAtOrAfterOffset(t *testing.T) {
	log, err := NewLog(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	if _, err := log.Append("first"); err != nil {
		t.Fatalf("failed to append first record: %v", err)
	}
	if _, err := log.Append("second"); err != nil {
		t.Fatalf("failed to append second record: %v", err)
	}
	if _, err := log.Append("third"); err != nil {
		t.Fatalf("failed to append third record: %v", err)
	}

	records, err := log.ReadFrom(1)
	if err != nil {
		t.Fatalf("failed to read records from offset: %v", err)
	}

	expected := []Record{
		{Offset: 1, Message: "second"},
		{Offset: 2, Message: "third"},
	}

	if !reflect.DeepEqual(records, expected) {
		t.Fatalf("expected %v, got %v", expected, records)
	}
}

func TestReadFromRejectsNegativeOffset(t *testing.T) {
	log, err := NewLog(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	if _, err := log.ReadFrom(-1); err == nil {
		t.Fatalf("expected negative offset to be rejected")
	}
}

func TestLogRecoversRecordsOnReopen(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir)
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	if _, err := log.Append("first"); err != nil {
		t.Fatalf("failed to append first record: %v", err)
	}
	if _, err := log.Append("second"); err != nil {
		t.Fatalf("failed to append second record: %v", err)
	}

	reopenedLog, err := NewLog(dir)
	if err != nil {
		t.Fatalf("failed to reopen partition log: %v", err)
	}

	offset, err := reopenedLog.Append("third")
	if err != nil {
		t.Fatalf("failed to append third record: %v", err)
	}
	if offset != 2 {
		t.Fatalf("expected recovered next offset 2, got %d", offset)
	}

	records, err := reopenedLog.ReadAll()
	if err != nil {
		t.Fatalf("failed to read records: %v", err)
	}

	expected := []Record{
		{Offset: 0, Message: "first"},
		{Offset: 1, Message: "second"},
		{Offset: 2, Message: "third"},
	}

	if !reflect.DeepEqual(records, expected) {
		t.Fatalf("expected %v, got %v", expected, records)
	}
}
