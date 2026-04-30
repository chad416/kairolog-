package index

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type Entry struct {
	Offset   int64
	Position int64
}

type Index struct {
	mu         sync.Mutex
	baseOffset int64
	path       string
}

func NewIndex(dir string, baseOffset int64) (*Index, error) {
	if baseOffset < 0 {
		return nil, fmt.Errorf("base offset cannot be negative")
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create index directory: %w", err)
	}

	index := &Index{
		baseOffset: baseOffset,
		path:       filepath.Join(dir, indexFileName(baseOffset)),
	}

	if err := index.ensureFile(); err != nil {
		return nil, err
	}

	return index, nil
}

func (i *Index) Append(offset int64, position int64) error {
	if offset < i.baseOffset {
		return fmt.Errorf("offset cannot be smaller than base offset")
	}
	if position < 0 {
		return fmt.Errorf("position cannot be negative")
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	file, err := os.OpenFile(i.path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open index for append: %w", err)
	}

	if _, err := fmt.Fprintf(file, "%d %d\n", offset, position); err != nil {
		_ = file.Close()
		return fmt.Errorf("append index entry: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("close index after append: %w", err)
	}

	return nil
}

func (i *Index) ReadAll() ([]Entry, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	file, err := os.Open(i.path)
	if err != nil {
		return nil, fmt.Errorf("open index for read: %w", err)
	}
	defer file.Close()

	entries := make([]Entry, 0)
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++

		entry, err := parseEntry(scanner.Text())
		if err != nil {
			return nil, fmt.Errorf("parse index line %d: %w", lineNumber, err)
		}

		entries = append(entries, entry)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read index: %w", err)
	}

	return entries, nil
}

func (i *Index) BaseOffset() int64 {
	return i.baseOffset
}

func (i *Index) Path() string {
	return i.path
}

func (i *Index) ensureFile() error {
	file, err := os.OpenFile(i.path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("create index file: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("close index file: %w", err)
	}

	return nil
}

func parseEntry(line string) (Entry, error) {
	fields := strings.Fields(line)
	if len(fields) != 2 {
		return Entry{}, fmt.Errorf("expected offset and position")
	}

	offset, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return Entry{}, fmt.Errorf("parse offset: %w", err)
	}

	position, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return Entry{}, fmt.Errorf("parse position: %w", err)
	}

	return Entry{
		Offset:   offset,
		Position: position,
	}, nil
}

func indexFileName(baseOffset int64) string {
	return fmt.Sprintf("%020d.index", baseOffset)
}
