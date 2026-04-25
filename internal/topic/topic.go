package topic

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"kairolog/internal/storage"
)

type Topic struct {
	Name       string
	Partitions []Partition
}

type Partition struct {
	ID          int
	StoragePath string

	store *storage.FileStore
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
		storagePath := partitionStoragePath(name, id)
		store, err := storage.NewFileStoreAt(storagePath)
		if err != nil {
			return fmt.Errorf("create storage for topic %q partition %d: %w", name, id, err)
		}

		partitions = append(partitions, Partition{
			ID:          id,
			StoragePath: storagePath,
			store:       store,
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

func partitionStoragePath(topicName string, partitionID int) string {
	return fmt.Sprintf("data/%s/partition-%d/messages.log", topicName, partitionID)
}

func cloneTopic(topic *Topic) *Topic {
	partitions := make([]Partition, len(topic.Partitions))
	copy(partitions, topic.Partitions)

	return &Topic{
		Name:       topic.Name,
		Partitions: partitions,
	}
}
