package group

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type AssignmentFileStore struct {
	mu          sync.RWMutex
	path        string
	assignments map[string]map[string][]Assignment
}

type assignmentFileRecord struct {
	Group       string       `json:"group"`
	Topic       string       `json:"topic"`
	Assignments []Assignment `json:"assignments"`
}

func NewAssignmentFileStore(path string) (*AssignmentFileStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	if err := ensureAssignmentFile(path); err != nil {
		return nil, err
	}

	return &AssignmentFileStore{
		path:        path,
		assignments: make(map[string]map[string][]Assignment),
	}, nil
}

func (s *AssignmentFileStore) Save(group string, topic string, assignments []Assignment) error {
	group, topic, err := validateAssignmentKey(group, topic)
	if err != nil {
		return err
	}
	if assignments == nil {
		return fmt.Errorf("assignments cannot be nil")
	}
	if len(assignments) == 0 {
		return fmt.Errorf("assignments cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.assignments == nil {
		s.assignments = make(map[string]map[string][]Assignment)
	}
	if _, exists := s.assignments[group]; !exists {
		s.assignments[group] = make(map[string][]Assignment)
	}

	s.assignments[group][topic] = copyAssignments(assignments)
	return s.persistLocked()
}

func (s *AssignmentFileStore) Get(group string, topic string) ([]Assignment, bool, error) {
	group, topic, err := validateAssignmentKey(group, topic)
	if err != nil {
		return nil, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	groupAssignments, exists := s.assignments[group]
	if !exists {
		return nil, false, nil
	}

	assignments, exists := groupAssignments[topic]
	if !exists {
		return nil, false, nil
	}

	return copyAssignments(assignments), true, nil
}

func (s *AssignmentFileStore) Topics(group string) ([]string, error) {
	group, err := validateGroup(group)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return sortedAssignmentTopics(s.assignments[group]), nil
}

func (s *AssignmentFileStore) Delete(group string, topic string) error {
	group, topic, err := validateAssignmentKey(group, topic)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	groupAssignments, exists := s.assignments[group]
	if !exists {
		return s.persistLocked()
	}

	delete(groupAssignments, topic)
	if len(groupAssignments) == 0 {
		delete(s.assignments, group)
	}

	return s.persistLocked()
}

func (s *AssignmentFileStore) DeleteGroup(group string) error {
	group, err := validateGroup(group)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.assignments, group)
	return s.persistLocked()
}

func (s *AssignmentFileStore) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ensureAssignmentFile(s.path); err != nil {
		return err
	}

	file, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("open assignment file: %w", err)
	}
	defer file.Close()

	loadedAssignments := make(map[string]map[string][]Assignment)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var record assignmentFileRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return fmt.Errorf("decode assignment record: %w", err)
		}

		group, topic, err := validateAssignmentKey(record.Group, record.Topic)
		if err != nil {
			return err
		}
		if record.Assignments == nil {
			return fmt.Errorf("assignments cannot be nil")
		}
		if len(record.Assignments) == 0 {
			return fmt.Errorf("assignments cannot be empty")
		}

		if _, exists := loadedAssignments[group]; !exists {
			loadedAssignments[group] = make(map[string][]Assignment)
		}
		loadedAssignments[group][topic] = copyAssignments(record.Assignments)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan assignment file: %w", err)
	}

	s.assignments = loadedAssignments
	return nil
}

func (s *AssignmentFileStore) persistLocked() error {
	if err := ensureAssignmentFile(s.path); err != nil {
		return err
	}

	records := assignmentRecordsFromMap(s.assignments)
	var builder strings.Builder
	for _, record := range records {
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode assignment record: %w", err)
		}

		builder.Write(data)
		builder.WriteByte('\n')
	}

	if err := os.WriteFile(s.path, []byte(builder.String()), 0o600); err != nil {
		return fmt.Errorf("write assignment file: %w", err)
	}

	return nil
}

func assignmentRecordsFromMap(assignments map[string]map[string][]Assignment) []assignmentFileRecord {
	groups := make([]string, 0, len(assignments))
	for group := range assignments {
		groups = append(groups, group)
	}
	sort.Strings(groups)

	records := make([]assignmentFileRecord, 0)
	for _, group := range groups {
		topics := make([]string, 0, len(assignments[group]))
		for topic := range assignments[group] {
			topics = append(topics, topic)
		}
		sort.Strings(topics)

		for _, topic := range topics {
			records = append(records, assignmentFileRecord{
				Group:       group,
				Topic:       topic,
				Assignments: copyAssignments(assignments[group][topic]),
			})
		}
	}

	return records
}

func ensureAssignmentFile(path string) error {
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create assignment directory: %w", err)
		}
	}

	file, err := os.OpenFile(path, os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("create assignment file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close assignment file: %w", err)
	}

	return nil
}
