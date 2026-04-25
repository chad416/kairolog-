package topic

import (
	"os"
	"reflect"
	"testing"
)

func TestCreateTopicCreatesPartitions(t *testing.T) {
	chdirTemp(t)

	manager := NewManager()

	if err := manager.CreateTopic("orders", 3); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	topic, exists := manager.GetTopic("orders")
	if !exists {
		t.Fatalf("expected topic to exist")
	}

	if topic.Name != "orders" {
		t.Fatalf("expected topic name %q, got %q", "orders", topic.Name)
	}
	if len(topic.Partitions) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(topic.Partitions))
	}

	for id, partition := range topic.Partitions {
		expectedPath := partitionStoragePath("orders", id)
		if partition.ID != id {
			t.Fatalf("expected partition ID %d, got %d", id, partition.ID)
		}
		if partition.StoragePath != expectedPath {
			t.Fatalf("expected storage path %q, got %q", expectedPath, partition.StoragePath)
		}
		if _, err := os.Stat(expectedPath); err != nil {
			t.Fatalf("expected storage file %q to exist: %v", expectedPath, err)
		}
	}
}

func TestGetTopicReturnsFalseForMissingTopic(t *testing.T) {
	manager := NewManager()

	if _, exists := manager.GetTopic("missing"); exists {
		t.Fatalf("expected missing topic to not exist")
	}
}

func TestListTopicsReturnsTopicNames(t *testing.T) {
	chdirTemp(t)

	manager := NewManager()

	if err := manager.CreateTopic("payments", 1); err != nil {
		t.Fatalf("failed to create payments topic: %v", err)
	}
	if err := manager.CreateTopic("orders", 1); err != nil {
		t.Fatalf("failed to create orders topic: %v", err)
	}

	topics := manager.ListTopics()
	expected := []string{"orders", "payments"}

	if !reflect.DeepEqual(topics, expected) {
		t.Fatalf("expected %v, got %v", expected, topics)
	}
}

func TestCreateTopicRejectsEmptyName(t *testing.T) {
	manager := NewManager()

	if err := manager.CreateTopic("", 1); err == nil {
		t.Fatalf("expected empty topic name to be rejected")
	}
}

func TestCreateTopicRejectsDuplicateName(t *testing.T) {
	chdirTemp(t)

	manager := NewManager()

	if err := manager.CreateTopic("orders", 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	if err := manager.CreateTopic("orders", 1); err == nil {
		t.Fatalf("expected duplicate topic name to be rejected")
	}
}

func TestCreateTopicRejectsNonPositivePartitionCount(t *testing.T) {
	manager := NewManager()

	if err := manager.CreateTopic("orders", 0); err == nil {
		t.Fatalf("expected zero partition count to be rejected")
	}
	if err := manager.CreateTopic("payments", -1); err == nil {
		t.Fatalf("expected negative partition count to be rejected")
	}
}

func chdirTemp(t *testing.T) {
	t.Helper()

	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}

	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatalf("failed to change working directory: %v", err)
	}

	t.Cleanup(func() {
		if err := os.Chdir(originalDir); err != nil {
			t.Fatalf("failed to restore working directory: %v", err)
		}
	})
}
