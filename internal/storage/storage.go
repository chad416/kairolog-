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

type FileStore struct {
	mu   sync.Mutex
	path string
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

	return store, nil
}

func (s *FileStore) Append(message string) error {
	if strings.ContainsAny(message, "\r\n") {
		return fmt.Errorf("message cannot contain newline characters")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureFile(); err != nil {
		return err
	}

	file, err := os.OpenFile(s.path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open messages file for append: %w", err)
	}
	defer file.Close()

	if _, err := fmt.Fprintln(file, message); err != nil {
		return fmt.Errorf("append message: %w", err)
	}

	return nil
}

func (s *FileStore) ReadAll() ([]string, error) {
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

	messages := make([]string, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		messages = append(messages, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read messages file: %w", err)
	}

	return messages, nil
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
