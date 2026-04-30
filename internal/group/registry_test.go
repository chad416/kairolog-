package group

import (
	"reflect"
	"testing"
)

func TestRegistryMemberCanJoinGroup(t *testing.T) {
	registry := NewRegistry()

	if err := registry.Join("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to join group: %v", err)
	}

	members, err := registry.Members("analytics-workers")
	if err != nil {
		t.Fatalf("failed to get members: %v", err)
	}

	expected := []GroupMember{{ID: "member-a"}}
	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryDuplicateJoinIsIdempotent(t *testing.T) {
	registry := NewRegistry()

	if err := registry.Join("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to join group: %v", err)
	}
	if err := registry.Join("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to join group again: %v", err)
	}

	members, err := registry.Members("analytics-workers")
	if err != nil {
		t.Fatalf("failed to get members: %v", err)
	}

	expected := []GroupMember{{ID: "member-a"}}
	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryMemberCanLeaveGroup(t *testing.T) {
	registry := NewRegistry()

	if err := registry.Join("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to join group: %v", err)
	}
	if err := registry.Leave("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to leave group: %v", err)
	}

	members, err := registry.Members("analytics-workers")
	if err != nil {
		t.Fatalf("failed to get members: %v", err)
	}

	if len(members) != 0 {
		t.Fatalf("expected no members, got %v", members)
	}
}

func TestRegistryLeavingMissingMemberIsIdempotent(t *testing.T) {
	registry := NewRegistry()

	if err := registry.Leave("analytics-workers", "member-a"); err != nil {
		t.Fatalf("expected leaving missing member to succeed: %v", err)
	}

	if err := registry.Join("analytics-workers", "member-b"); err != nil {
		t.Fatalf("failed to join group: %v", err)
	}
	if err := registry.Leave("analytics-workers", "member-a"); err != nil {
		t.Fatalf("expected leaving absent member to succeed: %v", err)
	}

	members, err := registry.Members("analytics-workers")
	if err != nil {
		t.Fatalf("failed to get members: %v", err)
	}

	expected := []GroupMember{{ID: "member-b"}}
	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryMembersAreReturnedSorted(t *testing.T) {
	registry := NewRegistry()

	joinMembers(t, registry, "analytics-workers", "member-c", "member-a", "member-b")

	members, err := registry.Members("analytics-workers")
	if err != nil {
		t.Fatalf("failed to get members: %v", err)
	}

	expected := []GroupMember{
		{ID: "member-a"},
		{ID: "member-b"},
		{ID: "member-c"},
	}
	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistrySeparateGroupsAreIsolated(t *testing.T) {
	registry := NewRegistry()

	if err := registry.Join("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to join analytics group: %v", err)
	}
	if err := registry.Join("billing-workers", "member-b"); err != nil {
		t.Fatalf("failed to join billing group: %v", err)
	}

	members, err := registry.Members("analytics-workers")
	if err != nil {
		t.Fatalf("failed to get analytics members: %v", err)
	}

	expected := []GroupMember{{ID: "member-a"}}
	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryStateReturnsGroupAndMembers(t *testing.T) {
	registry := NewRegistry()

	joinMembers(t, registry, "analytics-workers", "member-b", "member-a")

	state, err := registry.State("analytics-workers")
	if err != nil {
		t.Fatalf("failed to get group state: %v", err)
	}

	expected := GroupState{
		Group: "analytics-workers",
		Members: []GroupMember{
			{ID: "member-a"},
			{ID: "member-b"},
		},
	}
	if !reflect.DeepEqual(state, expected) {
		t.Fatalf("expected %v, got %v", expected, state)
	}
}

func TestRegistryRejectsEmptyGroup(t *testing.T) {
	registry := NewRegistry()

	if err := registry.Join("", "member-a"); err == nil {
		t.Fatalf("expected join to reject empty group")
	}
	if err := registry.Leave("", "member-a"); err == nil {
		t.Fatalf("expected leave to reject empty group")
	}
	if _, err := registry.Members(""); err == nil {
		t.Fatalf("expected members to reject empty group")
	}
	if _, err := registry.State(""); err == nil {
		t.Fatalf("expected state to reject empty group")
	}
}

func TestRegistryRejectsEmptyMemberID(t *testing.T) {
	registry := NewRegistry()

	if err := registry.Join("analytics-workers", ""); err == nil {
		t.Fatalf("expected join to reject empty member ID")
	}
	if err := registry.Leave("analytics-workers", ""); err == nil {
		t.Fatalf("expected leave to reject empty member ID")
	}
}

func joinMembers(t *testing.T, registry *Registry, group string, memberIDs ...string) {
	t.Helper()

	for _, memberID := range memberIDs {
		if err := registry.Join(group, memberID); err != nil {
			t.Fatalf("failed to join member %q: %v", memberID, err)
		}
	}
}
