package consumer

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type OffsetKey struct {
	Group     string
	Topic     string
	Partition int
}

type OffsetCommit struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

type OffsetStore struct {
	mu      sync.Mutex
	path    string
	commits map[OffsetKey]int64
}

func NewOffsetStore(path string) (*OffsetStore, error) {
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("create offset store directory: %w", err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("create offset store file: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close offset store file: %w", err)
	}

	store := &OffsetStore{
		path:    path,
		commits: make(map[OffsetKey]int64),
	}

	if err := store.load(); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *OffsetStore) Commit(group string, topic string, partition int, offset int64) error {
	if err := validateCommit(group, topic, partition, offset); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := OffsetKey{
		Group:     group,
		Topic:     topic,
		Partition: partition,
	}
	s.commits[key] = offset

	if err := s.persistLocked(); err != nil {
		return fmt.Errorf("persist offset commit: %w", err)
	}

	return nil
}

func (s *OffsetStore) Get(group string, topic string, partition int) (int64, bool, error) {
	if err := validateKey(group, topic, partition); err != nil {
		return 0, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	offset, ok := s.commits[OffsetKey{
		Group:     group,
		Topic:     topic,
		Partition: partition,
	}]

	return offset, ok, nil
}

func (s *OffsetStore) List() ([]OffsetCommit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.sortedCommitsLocked(), nil
}

func (s *OffsetStore) load() error {
	file, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("open offset store for read: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var commit OffsetCommit
		if err := json.Unmarshal(line, &commit); err != nil {
			return fmt.Errorf("parse offset commit line %d: %w", lineNumber, err)
		}
		if err := validateCommit(commit.Group, commit.Topic, commit.Partition, commit.Offset); err != nil {
			return fmt.Errorf("invalid offset commit line %d: %w", lineNumber, err)
		}

		key := OffsetKey{
			Group:     commit.Group,
			Topic:     commit.Topic,
			Partition: commit.Partition,
		}
		s.commits[key] = commit.Offset
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read offset store: %w", err)
	}

	return nil
}

func (s *OffsetStore) persistLocked() error {
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open offset store for write: %w", err)
	}

	encoder := json.NewEncoder(file)
	for _, commit := range s.sortedCommitsLocked() {
		if err := encoder.Encode(commit); err != nil {
			_ = file.Close()
			return fmt.Errorf("write offset commit: %w", err)
		}
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("close offset store after write: %w", err)
	}

	return nil
}

func (s *OffsetStore) sortedCommitsLocked() []OffsetCommit {
	commits := make([]OffsetCommit, 0, len(s.commits))
	for key, offset := range s.commits {
		commits = append(commits, OffsetCommit{
			Group:     key.Group,
			Topic:     key.Topic,
			Partition: key.Partition,
			Offset:    offset,
		})
	}

	sort.Slice(commits, func(i, j int) bool {
		if commits[i].Group != commits[j].Group {
			return commits[i].Group < commits[j].Group
		}
		if commits[i].Topic != commits[j].Topic {
			return commits[i].Topic < commits[j].Topic
		}
		return commits[i].Partition < commits[j].Partition
	})

	return commits
}

func validateCommit(group string, topic string, partition int, offset int64) error {
	if err := validateKey(group, topic, partition); err != nil {
		return err
	}
	if offset < 0 {
		return fmt.Errorf("offset cannot be negative")
	}

	return nil
}

func validateKey(group string, topic string, partition int) error {
	if group == "" {
		return fmt.Errorf("group cannot be empty")
	}
	if topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	if partition < 0 {
		return fmt.Errorf("partition cannot be negative")
	}

	return nil
}
