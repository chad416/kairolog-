package topic

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	partitionlog "kairolog/internal/partition"
)

type Topic struct {
	Name       string
	Partitions []Partition
}

type Partition struct {
	ID  int
	Dir string

	log *partitionlog.Log
}

type Manager struct {
	mu     sync.RWMutex
	topics map[string]*Topic
}

func NewManager() *Manager {
	return &Manager{
		topics: make(map[string]*Topic),
	}
}

func (m *Manager) CreateTopic(name string, partitionCount int) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("topic name cannot be empty")
	}
	if partitionCount <= 0 {
		return fmt.Errorf("partition count must be greater than zero")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.topics == nil {
		m.topics = make(map[string]*Topic)
	}

	if _, exists := m.topics[name]; exists {
		return fmt.Errorf("topic %q already exists", name)
	}

	partitions := make([]Partition, 0, partitionCount)
	for id := 0; id < partitionCount; id++ {
		dir := partitionDir(name, id)
		log, err := partitionlog.NewLog(dir)
		if err != nil {
			return fmt.Errorf("create log for topic %q partition %d: %w", name, id, err)
		}

		partitions = append(partitions, Partition{
			ID:  id,
			Dir: dir,
			log: log,
		})
	}

	m.topics[name] = &Topic{
		Name:       name,
		Partitions: partitions,
	}

	return nil
}

func (m *Manager) GetTopic(name string) (*Topic, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topic, exists := m.topics[name]
	if !exists {
		return nil, false
	}

	return cloneTopic(topic), true
}

func (m *Manager) ListTopics() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topics := make([]string, 0, len(m.topics))
	for name := range m.topics {
		topics = append(topics, name)
	}

	sort.Strings(topics)
	return topics
}

func (p Partition) Append(message string) (int64, error) {
	if p.log == nil {
		return 0, fmt.Errorf("partition log is not initialized")
	}

	return p.log.Append(message)
}

func (p Partition) ReadFrom(offset int64) ([]partitionlog.Record, error) {
	if p.log == nil {
		return nil, fmt.Errorf("partition log is not initialized")
	}

	return p.log.ReadFrom(offset)
}

func partitionDir(topicName string, partitionID int) string {
	return fmt.Sprintf("data/%s/partition-%d", topicName, partitionID)
}

func cloneTopic(topic *Topic) *Topic {
	partitions := make([]Partition, len(topic.Partitions))
	copy(partitions, topic.Partitions)

	return &Topic{
		Name:       topic.Name,
		Partitions: partitions,
	}
}
