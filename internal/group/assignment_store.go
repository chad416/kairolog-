package group

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type AssignmentStore struct {
	mu          sync.RWMutex
	assignments map[string]map[string][]Assignment
}

func NewAssignmentStore() *AssignmentStore {
	return &AssignmentStore{
		assignments: make(map[string]map[string][]Assignment),
	}
}

func (s *AssignmentStore) Save(group string, topic string, assignments []Assignment) error {
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
	return nil
}

func (s *AssignmentStore) Get(group string, topic string) ([]Assignment, bool, error) {
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

func (s *AssignmentStore) Topics(group string) ([]string, error) {
	group, err := validateGroup(group)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return sortedAssignmentTopics(s.assignments[group]), nil
}

func (s *AssignmentStore) Delete(group string, topic string) error {
	group, topic, err := validateAssignmentKey(group, topic)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	groupAssignments, exists := s.assignments[group]
	if !exists {
		return nil
	}

	delete(groupAssignments, topic)
	if len(groupAssignments) == 0 {
		delete(s.assignments, group)
	}

	return nil
}

func (s *AssignmentStore) DeleteGroup(group string) error {
	group, err := validateGroup(group)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.assignments, group)
	return nil
}

func validateAssignmentKey(group string, topic string) (string, string, error) {
	group, err := validateGroup(group)
	if err != nil {
		return "", "", err
	}

	topic = strings.TrimSpace(topic)
	if topic == "" {
		return "", "", fmt.Errorf("topic cannot be empty")
	}

	return group, topic, nil
}

func sortedAssignmentTopics(groupAssignments map[string][]Assignment) []string {
	topics := make([]string, 0, len(groupAssignments))
	for topic, assignments := range groupAssignments {
		if len(assignments) == 0 {
			continue
		}

		topics = append(topics, topic)
	}

	sort.Strings(topics)
	return topics
}

func copyAssignments(assignments []Assignment) []Assignment {
	copiedAssignments := make([]Assignment, len(assignments))
	for i, assignment := range assignments {
		copiedAssignments[i] = Assignment{
			MemberID: assignment.MemberID,
			Topics:   copyTopicAssignments(assignment.Topics),
		}
	}

	return copiedAssignments
}

func copyTopicAssignments(topicAssignments []TopicAssignment) []TopicAssignment {
	copiedTopicAssignments := make([]TopicAssignment, len(topicAssignments))
	for i, topicAssignment := range topicAssignments {
		copiedTopicAssignments[i] = TopicAssignment{
			Topic:      topicAssignment.Topic,
			Partitions: append([]int(nil), topicAssignment.Partitions...),
		}
	}

	return copiedTopicAssignments
}
