package group

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type GroupMember struct {
	ID string
}

type GroupState struct {
	Group   string
	Members []GroupMember
}

type Registry struct {
	mu     sync.RWMutex
	groups map[string]map[string]struct{}
}

func NewRegistry() *Registry {
	return &Registry{
		groups: make(map[string]map[string]struct{}),
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
		r.groups = make(map[string]map[string]struct{})
	}
	if _, exists := r.groups[group]; !exists {
		r.groups[group] = make(map[string]struct{})
	}

	r.groups[group][memberID] = struct{}{}
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

func sortedGroupMembers(memberSet map[string]struct{}) []GroupMember {
	members := make([]GroupMember, 0, len(memberSet))
	for id := range memberSet {
		members = append(members, GroupMember{ID: id})
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
