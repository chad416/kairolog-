package segment

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Record struct {
	Offset  int64
	Message string
}

type Segment struct {
	mu         sync.Mutex
	baseOffset int64
	nextOffset int64
	path       string
}

func NewSegment(dir string, baseOffset int64) (*Segment, error) {
	if baseOffset < 0 {
		return nil, fmt.Errorf("base offset cannot be negative")
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create segment directory: %w", err)
	}

	segment := &Segment{
		baseOffset: baseOffset,
		path:       filepath.Join(dir, segmentFileName(baseOffset)),
	}

	if err := segment.ensureFile(); err != nil {
		return nil, err
	}

	nextOffset, err := segment.recoverNextOffset()
	if err != nil {
		return nil, err
	}
	segment.nextOffset = nextOffset

	return segment, nil
}

func (s *Segment) Append(message string) (int64, error) {
	if strings.ContainsAny(message, "\r\n") {
		return 0, fmt.Errorf("message cannot contain newline characters")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.OpenFile(s.path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("open segment for append: %w", err)
	}

	offset := s.nextOffset
	if _, err := fmt.Fprintln(file, message); err != nil {
		_ = file.Close()
		return 0, fmt.Errorf("append record: %w", err)
	}

	if err := file.Close(); err != nil {
		return 0, fmt.Errorf("close segment after append: %w", err)
	}

	s.nextOffset++
	return offset, nil
}

func (s *Segment) ReadAll() ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Open(s.path)
	if err != nil {
		return nil, fmt.Errorf("open segment for read: %w", err)
	}
	defer file.Close()

	records := make([]Record, 0)
	offset := s.baseOffset
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		records = append(records, Record{
			Offset:  offset,
			Message: scanner.Text(),
		})
		offset++
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read segment: %w", err)
	}

	return records, nil
}

func (s *Segment) BaseOffset() int64 {
	return s.baseOffset
}

func (s *Segment) NextOffset() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.nextOffset
}

func (s *Segment) Path() string {
	return s.path
}

func (s *Segment) ensureFile() error {
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("create segment file: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("close segment file: %w", err)
	}

	return nil
}

func (s *Segment) recoverNextOffset() (int64, error) {
	file, err := os.Open(s.path)
	if err != nil {
		return 0, fmt.Errorf("open segment for offset recovery: %w", err)
	}
	defer file.Close()

	nextOffset := s.baseOffset
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		nextOffset++
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("recover segment offset: %w", err)
	}

	return nextOffset, nil
}

func segmentFileName(baseOffset int64) string {
	return fmt.Sprintf("%020d.log", baseOffset)
}
