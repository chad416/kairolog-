package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const defaultMessagesPath = "data/messages.log"

type Record struct {
	Offset  int64
	Message string
}

type FileStore struct {
	mu         sync.Mutex
	path       string
	nextOffset int64
}

func NewFileStore() (*FileStore, error) {
	return NewFileStoreAt(defaultMessagesPath)
}

func NewFileStoreAt(path string) (*FileStore, error) {
	store := &FileStore{
		path: path,
	}

	if err := store.ensureFile(); err != nil {
		return nil, err
	}

	nextOffset, err := store.readNextOffset()
	if err != nil {
		return nil, err
	}
	store.nextOffset = nextOffset

	return store, nil
}

func (s *FileStore) Append(message string) error {
	_, err := s.AppendRecord(message)
	return err
}

func (s *FileStore) AppendRecord(message string) (int64, error) {
	if strings.ContainsAny(message, "\r\n") {
		return 0, fmt.Errorf("message cannot contain newline characters")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureFile(); err != nil {
		return 0, err
	}

	file, err := os.OpenFile(s.path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("open messages file for append: %w", err)
	}
	defer file.Close()

	if _, err := fmt.Fprintln(file, message); err != nil {
		return 0, fmt.Errorf("append message: %w", err)
	}

	offset := s.nextOffset
	s.nextOffset++

	return offset, nil
}

func (s *FileStore) ReadAll() ([]string, error) {
	records, err := s.ReadAllRecords()
	if err != nil {
		return nil, err
	}

	messages := make([]string, 0, len(records))
	for _, record := range records {
		messages = append(messages, record.Message)
	}

	return messages, nil
}

func (s *FileStore) ReadAllRecords() ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureFile(); err != nil {
		return nil, err
	}

	file, err := os.Open(s.path)
	if err != nil {
		return nil, fmt.Errorf("open messages file for read: %w", err)
	}
	defer file.Close()

	records := make([]Record, 0)
	scanner := bufio.NewScanner(file)
	var offset int64
	for scanner.Scan() {
		records = append(records, Record{
			Offset:  offset,
			Message: scanner.Text(),
		})
		offset++
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read messages file: %w", err)
	}

	return records, nil
}

func (s *FileStore) ensureFile() error {
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}

	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("create messages file: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("close messages file: %w", err)
	}

	return nil
}

func (s *FileStore) readNextOffset() (int64, error) {
	file, err := os.Open(s.path)
	if err != nil {
		return 0, fmt.Errorf("open messages file for offset recovery: %w", err)
	}
	defer file.Close()

	var nextOffset int64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		nextOffset++
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("read messages file for offset recovery: %w", err)
	}

	return nextOffset, nil
}
