package partition

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"kairolog/internal/index"
	"kairolog/internal/segment"
)

const defaultMaxSegmentSize int64 = 64 * 1024 * 1024

type Record struct {
	Offset  int64
	Message string
}

type Log struct {
	mu             sync.Mutex
	dir            string
	maxSegmentSize int64
	segments       []segmentPair
}

type segmentPair struct {
	segment *segment.Segment
	index   *index.Index
}

func NewLog(dir string) (*Log, error) {
	return NewLogWithMaxSegmentSize(dir, defaultMaxSegmentSize)
}

func NewLogWithMaxSegmentSize(dir string, maxSegmentSize int64) (*Log, error) {
	if maxSegmentSize <= 0 {
		return nil, fmt.Errorf("max segment size must be greater than zero")
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create partition log directory: %w", err)
	}

	segments, err := loadSegmentPairs(dir)
	if err != nil {
		return nil, err
	}

	if len(segments) == 0 {
		pair, err := newSegmentPair(dir, 0)
		if err != nil {
			return nil, err
		}
		segments = append(segments, pair)
	}

	return &Log{
		dir:            dir,
		maxSegmentSize: maxSegmentSize,
		segments:       segments,
	}, nil
}

func (l *Log) Append(message string) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.rotateIfNeeded(); err != nil {
		return 0, err
	}

	active := &l.segments[len(l.segments)-1]
	offset, position, err := active.segment.AppendWithPosition(message)
	if err != nil {
		return 0, fmt.Errorf("append to segment: %w", err)
	}

	if err := active.index.Append(offset, position); err != nil {
		return 0, fmt.Errorf("append to index: %w", err)
	}

	return offset, nil
}

func (l *Log) ReadAll() ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	records := make([]Record, 0)
	for _, pair := range l.segments {
		segmentRecords, err := pair.segment.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("read segment %d: %w", pair.segment.BaseOffset(), err)
		}

		records = append(records, convertRecords(segmentRecords)...)
	}

	return records, nil
}

func (l *Log) ReadFrom(offset int64) ([]Record, error) {
	if offset < 0 {
		return nil, fmt.Errorf("offset cannot be negative")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	startSegment := l.segmentIndexForOffset(offset)
	records := make([]Record, 0)

	for i := startSegment; i < len(l.segments); i++ {
		pair := l.segments[i]

		var segmentRecords []segment.Record
		var err error
		if i == startSegment {
			startOffset, position, err := l.indexedReadStart(pair, offset)
			if err != nil {
				return nil, err
			}

			segmentRecords, err = pair.segment.ReadFromPosition(startOffset, position)
			if err != nil {
				return nil, fmt.Errorf("read segment %d from index position: %w", pair.segment.BaseOffset(), err)
			}
		} else {
			segmentRecords, err = pair.segment.ReadAll()
			if err != nil {
				return nil, fmt.Errorf("read segment %d: %w", pair.segment.BaseOffset(), err)
			}
		}

		for _, record := range segmentRecords {
			if record.Offset >= offset {
				records = append(records, Record{
					Offset:  record.Offset,
					Message: record.Message,
				})
			}
		}
	}

	return records, nil
}

func (l *Log) rotateIfNeeded() error {
	active := l.segments[len(l.segments)-1].segment

	size, err := fileSize(active.Path())
	if err != nil {
		return err
	}

	if size < l.maxSegmentSize {
		return nil
	}

	pair, err := newSegmentPair(l.dir, active.NextOffset())
	if err != nil {
		return err
	}

	l.segments = append(l.segments, pair)
	return nil
}

func (l *Log) segmentIndexForOffset(offset int64) int {
	segmentIndex := 0
	for i, pair := range l.segments {
		if pair.segment.BaseOffset() <= offset {
			segmentIndex = i
		}
	}

	return segmentIndex
}

func (l *Log) indexedReadStart(pair segmentPair, offset int64) (int64, int64, error) {
	entries, err := pair.index.ReadAll()
	if err != nil {
		return 0, 0, fmt.Errorf("read index %d: %w", pair.index.BaseOffset(), err)
	}

	startOffset := pair.segment.BaseOffset()
	position := int64(0)
	for _, entry := range entries {
		if entry.Offset <= offset && entry.Offset >= startOffset {
			startOffset = entry.Offset
			position = entry.Position
		}
	}

	return startOffset, position, nil
}

func loadSegmentPairs(dir string) ([]segmentPair, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read partition log directory: %w", err)
	}

	baseOffsets := make([]int64, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		baseOffset, ok := parseSegmentLogFileName(entry.Name())
		if !ok {
			continue
		}

		baseOffsets = append(baseOffsets, baseOffset)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	segments := make([]segmentPair, 0, len(baseOffsets))
	for _, baseOffset := range baseOffsets {
		if err := ensureIndexForSegment(dir, baseOffset); err != nil {
			return nil, err
		}

		pair, err := newSegmentPair(dir, baseOffset)
		if err != nil {
			return nil, err
		}

		segments = append(segments, pair)
	}

	return segments, nil
}

func ensureIndexForSegment(dir string, baseOffset int64) error {
	indexPath := filepath.Join(dir, segmentFileName(baseOffset, ".index"))
	if _, err := os.Stat(indexPath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat index file %q: %w", indexPath, err)
	}

	if err := rebuildIndexFromSegmentFile(dir, baseOffset); err != nil {
		return fmt.Errorf("rebuild index %d: %w", baseOffset, err)
	}

	return nil
}

func rebuildIndexFromSegmentFile(dir string, baseOffset int64) error {
	segmentPath := filepath.Join(dir, segmentFileName(baseOffset, ".log"))
	file, err := os.Open(segmentPath)
	if err != nil {
		return fmt.Errorf("open segment for index rebuild: %w", err)
	}
	defer file.Close()

	idx, err := index.NewIndex(dir, baseOffset)
	if err != nil {
		return fmt.Errorf("create rebuilt index: %w", err)
	}

	reader := bufio.NewReader(file)
	offset := baseOffset
	position := int64(0)

	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			if err := idx.Append(offset, position); err != nil {
				return fmt.Errorf("append rebuilt index entry: %w", err)
			}

			position += int64(len(line))
			offset++
		}

		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("read segment for index rebuild: %w", err)
		}
	}

	return nil
}

func newSegmentPair(dir string, baseOffset int64) (segmentPair, error) {
	seg, err := segment.NewSegment(dir, baseOffset)
	if err != nil {
		return segmentPair{}, fmt.Errorf("create segment %d: %w", baseOffset, err)
	}

	idx, err := index.NewIndex(dir, baseOffset)
	if err != nil {
		return segmentPair{}, fmt.Errorf("create index %d: %w", baseOffset, err)
	}

	return segmentPair{
		segment: seg,
		index:   idx,
	}, nil
}

func convertRecords(segmentRecords []segment.Record) []Record {
	records := make([]Record, 0, len(segmentRecords))
	for _, record := range segmentRecords {
		records = append(records, Record{
			Offset:  record.Offset,
			Message: record.Message,
		})
	}

	return records
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("stat segment file %q: %w", path, err)
	}

	return info.Size(), nil
}

func parseSegmentLogFileName(name string) (int64, bool) {
	if !strings.HasSuffix(name, ".log") {
		return 0, false
	}

	base := strings.TrimSuffix(name, ".log")
	if len(base) != 20 {
		return 0, false
	}

	baseOffset, err := strconv.ParseInt(base, 10, 64)
	if err != nil {
		return 0, false
	}

	if segmentFileName(baseOffset, ".log") != name {
		return 0, false
	}

	return baseOffset, true
}

func segmentFileName(baseOffset int64, suffix string) string {
	return fmt.Sprintf("%020d%s", baseOffset, suffix)
}
