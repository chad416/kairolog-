package partition

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	indexpkg "kairolog/internal/index"
	segmentpkg "kairolog/internal/segment"
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

func TestAppendReturnsOffsetsAndWritesRealBytePositionIndexEntries(t *testing.T) {
	log, err := NewLogWithMaxSegmentSize(t.TempDir(), 1024)
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

	entries, err := log.segments[0].index.ReadAll()
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
	log, err := NewLogWithMaxSegmentSize(t.TempDir(), 1024)
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	appendMessages(t, log, "first", "second")

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
	log, err := NewLogWithMaxSegmentSize(t.TempDir(), 1024)
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	appendMessages(t, log, "first", "second", "third")

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

	log, err := NewLogWithMaxSegmentSize(dir, 1024)
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	appendMessages(t, log, "first", "second")

	reopenedLog, err := NewLogWithMaxSegmentSize(dir, 1024)
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

func TestReadFromUsesIndexBackedPositionedReads(t *testing.T) {
	dir := t.TempDir()

	segment, err := segmentpkg.NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	index, err := indexpkg.NewIndex(dir, 0)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	if _, _, err := segment.AppendWithPosition("ignored"); err != nil {
		t.Fatalf("failed to append ignored record: %v", err)
	}

	_, targetPosition, err := segment.AppendWithPosition("target")
	if err != nil {
		t.Fatalf("failed to append target record: %v", err)
	}

	if err := index.Append(10, targetPosition); err != nil {
		t.Fatalf("failed to append index entry: %v", err)
	}

	log := &Log{
		dir:            dir,
		maxSegmentSize: 1024,
		segments: []segmentPair{
			{segment: segment, index: index},
		},
	}

	records, err := log.ReadFrom(10)
	if err != nil {
		t.Fatalf("failed to read from offset: %v", err)
	}

	expected := []Record{
		{Offset: 10, Message: "target"},
	}

	if !reflect.DeepEqual(records, expected) {
		t.Fatalf("expected %v, got %v", expected, records)
	}
}

func TestSegmentRotationCreatesMultipleSegmentAndIndexFiles(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLogWithMaxSegmentSize(dir, int64(len("aaa\n")))
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	appendMessages(t, log, "aaa", "bbb")

	if countFilesWithSuffix(t, dir, ".log") != 2 {
		t.Fatalf("expected 2 segment files")
	}
	if countFilesWithSuffix(t, dir, ".index") != 2 {
		t.Fatalf("expected 2 index files")
	}
}

func TestOffsetsContinueAcrossRotatedSegments(t *testing.T) {
	log, err := NewLogWithMaxSegmentSize(t.TempDir(), int64(len("aaa\n")))
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	firstOffset, err := log.Append("aaa")
	if err != nil {
		t.Fatalf("failed to append first record: %v", err)
	}

	secondOffset, err := log.Append("bbb")
	if err != nil {
		t.Fatalf("failed to append second record: %v", err)
	}

	thirdOffset, err := log.Append("ccc")
	if err != nil {
		t.Fatalf("failed to append third record: %v", err)
	}

	expected := []int64{0, 1, 2}
	actual := []int64{firstOffset, secondOffset, thirdOffset}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("expected offsets %v, got %v", expected, actual)
	}
}

func TestReadAllReadsAcrossSegments(t *testing.T) {
	log, err := NewLogWithMaxSegmentSize(t.TempDir(), int64(len("aaa\n")))
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	appendMessages(t, log, "aaa", "bbb", "ccc")

	records, err := log.ReadAll()
	if err != nil {
		t.Fatalf("failed to read records: %v", err)
	}

	expected := []Record{
		{Offset: 0, Message: "aaa"},
		{Offset: 1, Message: "bbb"},
		{Offset: 2, Message: "ccc"},
	}

	if !reflect.DeepEqual(records, expected) {
		t.Fatalf("expected %v, got %v", expected, records)
	}
}

func TestReadFromReadsAcrossSegments(t *testing.T) {
	log, err := NewLogWithMaxSegmentSize(t.TempDir(), int64(len("aaa\n")+len("bbb\n")))
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	appendMessages(t, log, "aaa", "bbb", "ccc")

	records, err := log.ReadFrom(1)
	if err != nil {
		t.Fatalf("failed to read records from offset: %v", err)
	}

	expected := []Record{
		{Offset: 1, Message: "bbb"},
		{Offset: 2, Message: "ccc"},
	}

	if !reflect.DeepEqual(records, expected) {
		t.Fatalf("expected %v, got %v", expected, records)
	}
}

func TestReopeningRecoversRotatedSegmentsAndContinuesAtCorrectOffset(t *testing.T) {
	dir := t.TempDir()
	maxSegmentSize := int64(len("aaa\n") + len("bbb\n"))

	log, err := NewLogWithMaxSegmentSize(dir, maxSegmentSize)
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	appendMessages(t, log, "aaa", "bbb", "ccc")

	reopenedLog, err := NewLogWithMaxSegmentSize(dir, maxSegmentSize)
	if err != nil {
		t.Fatalf("failed to reopen partition log: %v", err)
	}

	offset, err := reopenedLog.Append("ddd")
	if err != nil {
		t.Fatalf("failed to append after reopen: %v", err)
	}
	if offset != 3 {
		t.Fatalf("expected offset 3 after reopen, got %d", offset)
	}

	records, err := reopenedLog.ReadAll()
	if err != nil {
		t.Fatalf("failed to read records: %v", err)
	}

	expected := []Record{
		{Offset: 0, Message: "aaa"},
		{Offset: 1, Message: "bbb"},
		{Offset: 2, Message: "ccc"},
		{Offset: 3, Message: "ddd"},
	}

	if !reflect.DeepEqual(records, expected) {
		t.Fatalf("expected %v, got %v", expected, records)
	}
}

func TestMissingIndexIsRebuiltOnReopen(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLogWithMaxSegmentSize(dir, 1024)
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	appendMessages(t, log, "first", "second")
	removeIndexFile(t, dir, 0)

	if _, err := NewLogWithMaxSegmentSize(dir, 1024); err != nil {
		t.Fatalf("failed to reopen partition log: %v", err)
	}

	indexPath := filepath.Join(dir, "00000000000000000000.index")
	if _, err := os.Stat(indexPath); err != nil {
		t.Fatalf("expected rebuilt index file to exist: %v", err)
	}
}

func TestRebuiltIndexContainsRealBytePositions(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLogWithMaxSegmentSize(dir, 1024)
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	appendMessages(t, log, "a", "bbb", "cc")
	removeIndexFile(t, dir, 0)

	if _, err := NewLogWithMaxSegmentSize(dir, 1024); err != nil {
		t.Fatalf("failed to reopen partition log: %v", err)
	}

	entries := readIndexEntries(t, dir, 0)
	expected := []indexpkg.Entry{
		{Offset: 0, Position: 0},
		{Offset: 1, Position: int64(len("a\n"))},
		{Offset: 2, Position: int64(len("a\n") + len("bbb\n"))},
	}

	if !reflect.DeepEqual(entries, expected) {
		t.Fatalf("expected %v, got %v", expected, entries)
	}
}

func TestReadFromWorksAfterIndexRebuild(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLogWithMaxSegmentSize(dir, 1024)
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	appendMessages(t, log, "first", "second", "third")
	removeIndexFile(t, dir, 0)

	recoveredLog, err := NewLogWithMaxSegmentSize(dir, 1024)
	if err != nil {
		t.Fatalf("failed to reopen partition log: %v", err)
	}

	records, err := recoveredLog.ReadFrom(1)
	if err != nil {
		t.Fatalf("failed to read records after index rebuild: %v", err)
	}

	expected := []Record{
		{Offset: 1, Message: "second"},
		{Offset: 2, Message: "third"},
	}

	if !reflect.DeepEqual(records, expected) {
		t.Fatalf("expected %v, got %v", expected, records)
	}
}

func TestAppendAfterIndexRebuildContinuesAtCorrectOffset(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLogWithMaxSegmentSize(dir, 1024)
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	appendMessages(t, log, "first", "second")
	removeIndexFile(t, dir, 0)

	recoveredLog, err := NewLogWithMaxSegmentSize(dir, 1024)
	if err != nil {
		t.Fatalf("failed to reopen partition log: %v", err)
	}

	offset, err := recoveredLog.Append("third")
	if err != nil {
		t.Fatalf("failed to append after index rebuild: %v", err)
	}
	if offset != 2 {
		t.Fatalf("expected offset 2 after index rebuild, got %d", offset)
	}

	records, err := recoveredLog.ReadAll()
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

func TestMultipleRotatedSegmentsRecoverMissingIndexes(t *testing.T) {
	dir := t.TempDir()
	maxSegmentSize := int64(len("aaa\n"))

	log, err := NewLogWithMaxSegmentSize(dir, maxSegmentSize)
	if err != nil {
		t.Fatalf("failed to create partition log: %v", err)
	}

	appendMessages(t, log, "aaa", "bbb", "ccc")
	removeIndexFile(t, dir, 0)
	removeIndexFile(t, dir, 2)

	recoveredLog, err := NewLogWithMaxSegmentSize(dir, maxSegmentSize)
	if err != nil {
		t.Fatalf("failed to reopen partition log: %v", err)
	}

	if countFilesWithSuffix(t, dir, ".index") != 3 {
		t.Fatalf("expected 3 index files after recovery")
	}

	records, err := recoveredLog.ReadFrom(1)
	if err != nil {
		t.Fatalf("failed to read records after recovery: %v", err)
	}

	expected := []Record{
		{Offset: 1, Message: "bbb"},
		{Offset: 2, Message: "ccc"},
	}

	if !reflect.DeepEqual(records, expected) {
		t.Fatalf("expected %v, got %v", expected, records)
	}

	offset, err := recoveredLog.Append("ddd")
	if err != nil {
		t.Fatalf("failed to append after recovery: %v", err)
	}
	if offset != 3 {
		t.Fatalf("expected offset 3 after recovery, got %d", offset)
	}
}

func appendMessages(t *testing.T, log *Log, messages ...string) {
	t.Helper()

	for _, message := range messages {
		if _, err := log.Append(message); err != nil {
			t.Fatalf("failed to append message %q: %v", message, err)
		}
	}
}

func countFilesWithSuffix(t *testing.T, dir string, suffix string) int {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}

	count := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == suffix {
			count++
		}
	}

	return count
}

func removeIndexFile(t *testing.T, dir string, baseOffset int64) {
	t.Helper()

	indexPath := filepath.Join(dir, segmentFileName(baseOffset, ".index"))
	if err := os.Remove(indexPath); err != nil {
		t.Fatalf("failed to remove index file %q: %v", indexPath, err)
	}
}

func readIndexEntries(t *testing.T, dir string, baseOffset int64) []indexpkg.Entry {
	t.Helper()

	idx, err := indexpkg.NewIndex(dir, baseOffset)
	if err != nil {
		t.Fatalf("failed to open index: %v", err)
	}

	entries, err := idx.ReadAll()
	if err != nil {
		t.Fatalf("failed to read index entries: %v", err)
	}

	return entries
}
