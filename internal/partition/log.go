package partition

import (
	"fmt"
	"sync"

	"kairolog/internal/index"
	"kairolog/internal/segment"
)

type Record struct {
	Offset  int64
	Message string
}

type Log struct {
	mu      sync.Mutex
	segment *segment.Segment
	index   *index.Index
}

func NewLog(dir string) (*Log, error) {
	segment, err := segment.NewSegment(dir, 0)
	if err != nil {
		return nil, fmt.Errorf("create segment: %w", err)
	}

	index, err := index.NewIndex(dir, 0)
	if err != nil {
		return nil, fmt.Errorf("create index: %w", err)
	}

	return &Log{
		segment: segment,
		index:   index,
	}, nil
}

func (l *Log) Append(message string) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	offset, position, err := l.segment.AppendWithPosition(message)
	if err != nil {
		return 0, fmt.Errorf("append to segment: %w", err)
	}

	if err := l.index.Append(offset, position); err != nil {
		return 0, fmt.Errorf("append to index: %w", err)
	}

	return offset, nil
}

func (l *Log) ReadAll() ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.readAll()
}

func (l *Log) ReadFrom(offset int64) ([]Record, error) {
	if offset < 0 {
		return nil, fmt.Errorf("offset cannot be negative")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	entries, err := l.index.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read index: %w", err)
	}

	startOffset := l.segment.BaseOffset()
	position := int64(0)
	for _, entry := range entries {
		if entry.Offset <= offset && entry.Offset >= startOffset {
			startOffset = entry.Offset
			position = entry.Position
		}
	}

	segmentRecords, err := l.segment.ReadFromPosition(startOffset, position)
	if err != nil {
		return nil, fmt.Errorf("read segment from index position: %w", err)
	}

	filtered := make([]Record, 0, len(segmentRecords))
	for _, record := range segmentRecords {
		if record.Offset >= offset {
			filtered = append(filtered, Record{
				Offset:  record.Offset,
				Message: record.Message,
			})
		}
	}

	return filtered, nil
}

func (l *Log) readAll() ([]Record, error) {
	segmentRecords, err := l.segment.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read segment: %w", err)
	}

	records := make([]Record, 0, len(segmentRecords))
	for _, record := range segmentRecords {
		records = append(records, Record{
			Offset:  record.Offset,
			Message: record.Message,
		})
	}

	return records, nil
}
