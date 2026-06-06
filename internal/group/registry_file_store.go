package group

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type RegistryFileStore struct {
	mu     sync.RWMutex
	path   string
	groups map[string]map[string]time.Time
}

type registryFileRecord struct {
	Group    string    `json:"group"`
	MemberID string    `json:"member_id"`
	LastSeen time.Time `json:"last_seen"`
}

func NewRegistryFileStore(path string) (*RegistryFileStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	if err := ensureRegistryFile(path); err != nil {
		return nil, err
	}

	return &RegistryFileStore{
		path:   path,
		groups: make(map[string]map[string]time.Time),
	}, nil
}

func (s *RegistryFileStore) Join(group string, memberID string) error {
	group, memberID, err := validateGroupMember(group, memberID)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups == nil {
		s.groups = make(map[string]map[string]time.Time)
	}
	if _, exists := s.groups[group]; !exists {
		s.groups[group] = make(map[string]time.Time)
	}

	if _, exists := s.groups[group][memberID]; !exists {
		s.groups[group][memberID] = time.Now()
	}

	return s.persistLocked()
}

func (s *RegistryFileStore) Heartbeat(group string, memberID string, now time.Time) error {
	group, memberID, err := validateGroupMember(group, memberID)
	if err != nil {
		return err
	}
	if now.IsZero() {
		return fmt.Errorf("heartbeat time cannot be zero")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups == nil {
		s.groups = make(map[string]map[string]time.Time)
	}
	if _, exists := s.groups[group]; !exists {
		s.groups[group] = make(map[string]time.Time)
	}

	s.groups[group][memberID] = now
	return s.persistLocked()
}

func (s *RegistryFileStore) Leave(group string, memberID string) error {
	group, memberID, err := validateGroupMember(group, memberID)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	members, exists := s.groups[group]
	if !exists {
		return s.persistLocked()
	}

	delete(members, memberID)
	if len(members) == 0 {
		delete(s.groups, group)
	}

	return s.persistLocked()
}

func (s *RegistryFileStore) Members(group string) ([]GroupMember, error) {
	group, err := validateGroup(group)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return sortedGroupMembers(s.groups[group]), nil
}

func (s *RegistryFileStore) Groups() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return sortedGroupNames(s.groups), nil
}

func (s *RegistryFileStore) State(group string) (GroupState, error) {
	group, err := validateGroup(group)
	if err != nil {
		return GroupState{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return GroupState{
		Group:   group,
		Members: sortedGroupMembers(s.groups[group]),
	}, nil
}

func (s *RegistryFileStore) StaleMembers(group string, now time.Time, timeout time.Duration) ([]GroupMember, error) {
	group, err := validateGroup(group)
	if err != nil {
		return nil, err
	}
	if now.IsZero() {
		return nil, fmt.Errorf("now time cannot be zero")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("timeout must be positive")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	members := sortedGroupMembers(s.groups[group])
	staleMembers := make([]GroupMember, 0, len(members))
	for _, member := range members {
		if now.Sub(member.LastSeen) > timeout {
			staleMembers = append(staleMembers, member)
		}
	}

	return staleMembers, nil
}

func (s *RegistryFileStore) RemoveStaleMembers(group string, now time.Time, timeout time.Duration) ([]GroupMember, error) {
	group, err := validateGroup(group)
	if err != nil {
		return nil, err
	}
	if now.IsZero() {
		return nil, fmt.Errorf("now time cannot be zero")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("timeout must be positive")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	members := s.groups[group]
	removedMembers := make([]GroupMember, 0, len(members))
	for _, member := range sortedGroupMembers(members) {
		if now.Sub(member.LastSeen) > timeout {
			removedMembers = append(removedMembers, member)
			delete(members, member.ID)
		}
	}

	if len(members) == 0 {
		delete(s.groups, group)
	}

	if err := s.persistLocked(); err != nil {
		return nil, err
	}

	return removedMembers, nil
}

func (s *RegistryFileStore) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ensureRegistryFile(s.path); err != nil {
		return err
	}

	file, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("open registry file: %w", err)
	}
	defer file.Close()

	loadedGroups := make(map[string]map[string]time.Time)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var record registryFileRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return fmt.Errorf("decode registry record: %w", err)
		}

		group, memberID, err := validateGroupMember(record.Group, record.MemberID)
		if err != nil {
			return err
		}
		if record.LastSeen.IsZero() {
			return fmt.Errorf("last seen time cannot be zero")
		}

		if _, exists := loadedGroups[group]; !exists {
			loadedGroups[group] = make(map[string]time.Time)
		}
		loadedGroups[group][memberID] = record.LastSeen
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan registry file: %w", err)
	}

	s.groups = loadedGroups
	return nil
}

func (s *RegistryFileStore) persistLocked() error {
	if err := ensureRegistryFile(s.path); err != nil {
		return err
	}

	records := registryRecordsFromMap(s.groups)
	var builder strings.Builder
	for _, record := range records {
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode registry record: %w", err)
		}

		builder.Write(data)
		builder.WriteByte('\n')
	}

	if err := os.WriteFile(s.path, []byte(builder.String()), 0o600); err != nil {
		return fmt.Errorf("write registry file: %w", err)
	}

	return nil
}

func registryRecordsFromMap(groups map[string]map[string]time.Time) []registryFileRecord {
	groupNames := sortedGroupNames(groups)

	records := make([]registryFileRecord, 0)
	for _, group := range groupNames {
		members := sortedGroupMembers(groups[group])
		for _, member := range members {
			records = append(records, registryFileRecord{
				Group:    group,
				MemberID: member.ID,
				LastSeen: member.LastSeen,
			})
		}
	}

	return records
}

func ensureRegistryFile(path string) error {
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create registry directory: %w", err)
		}
	}

	file, err := os.OpenFile(path, os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("create registry file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close registry file: %w", err)
	}

	return nil
}
