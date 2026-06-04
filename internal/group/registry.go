package group

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type GroupMember struct {
	ID       string
	LastSeen time.Time
}

type GroupState struct {
	Group   string
	Members []GroupMember
}

type Registry struct {
	mu     sync.RWMutex
	groups map[string]map[string]time.Time
}

func NewRegistry() *Registry {
	return &Registry{
		groups: make(map[string]map[string]time.Time),
	}
}

func (r *Registry) Join(group string, memberID string) error {
	group, memberID, err := validateGroupMember(group, memberID)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.groups == nil {
		r.groups = make(map[string]map[string]time.Time)
	}
	if _, exists := r.groups[group]; !exists {
		r.groups[group] = make(map[string]time.Time)
	}

	if _, exists := r.groups[group][memberID]; !exists {
		r.groups[group][memberID] = time.Now()
	}
	return nil
}

func (r *Registry) Heartbeat(group string, memberID string, now time.Time) error {
	group, memberID, err := validateGroupMember(group, memberID)
	if err != nil {
		return err
	}
	if now.IsZero() {
		return fmt.Errorf("heartbeat time cannot be zero")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.groups == nil {
		r.groups = make(map[string]map[string]time.Time)
	}
	if _, exists := r.groups[group]; !exists {
		r.groups[group] = make(map[string]time.Time)
	}

	r.groups[group][memberID] = now
	return nil
}

func (r *Registry) Leave(group string, memberID string) error {
	group, memberID, err := validateGroupMember(group, memberID)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	members, exists := r.groups[group]
	if !exists {
		return nil
	}

	delete(members, memberID)
	if len(members) == 0 {
		delete(r.groups, group)
	}

	return nil
}

func (r *Registry) Members(group string) ([]GroupMember, error) {
	group, err := validateGroup(group)
	if err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	return sortedGroupMembers(r.groups[group]), nil
}

func (r *Registry) State(group string) (GroupState, error) {
	group, err := validateGroup(group)
	if err != nil {
		return GroupState{}, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	return GroupState{
		Group:   group,
		Members: sortedGroupMembers(r.groups[group]),
	}, nil
}

func (r *Registry) StaleMembers(group string, now time.Time, timeout time.Duration) ([]GroupMember, error) {
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

	r.mu.RLock()
	defer r.mu.RUnlock()

	members := sortedGroupMembers(r.groups[group])
	staleMembers := make([]GroupMember, 0, len(members))
	for _, member := range members {
		if now.Sub(member.LastSeen) > timeout {
			staleMembers = append(staleMembers, member)
		}
	}

	return staleMembers, nil
}

func (r *Registry) RemoveStaleMembers(group string, now time.Time, timeout time.Duration) ([]GroupMember, error) {
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

	r.mu.Lock()
	defer r.mu.Unlock()

	members := r.groups[group]
	removedMembers := make([]GroupMember, 0, len(members))
	for _, member := range sortedGroupMembers(members) {
		if now.Sub(member.LastSeen) > timeout {
			removedMembers = append(removedMembers, member)
			delete(members, member.ID)
		}
	}

	if len(members) == 0 {
		delete(r.groups, group)
	}

	return removedMembers, nil
}

func sortedGroupMembers(memberSet map[string]time.Time) []GroupMember {
	members := make([]GroupMember, 0, len(memberSet))
	for id, lastSeen := range memberSet {
		members = append(members, GroupMember{
			ID:       id,
			LastSeen: lastSeen,
		})
	}

	sort.Slice(members, func(i, j int) bool {
		return members[i].ID < members[j].ID
	})

	return members
}

func validateGroupMember(group string, memberID string) (string, string, error) {
	group, err := validateGroup(group)
	if err != nil {
		return "", "", err
	}

	memberID = strings.TrimSpace(memberID)
	if memberID == "" {
		return "", "", fmt.Errorf("member ID cannot be empty")
	}

	return group, memberID, nil
}

func validateGroup(group string) (string, error) {
	group = strings.TrimSpace(group)
	if group == "" {
		return "", fmt.Errorf("group cannot be empty")
	}

	return group, nil
}
