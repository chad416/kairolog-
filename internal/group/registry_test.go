package group

import (
	"reflect"
	"testing"
	"time"
)

func TestRegistryJoinSetsLastSeen(t *testing.T) {
	registry := NewRegistry()

	if err := registry.Join("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to join group: %v", err)
	}

	members, err := registry.Members("analytics-workers")
	if err != nil {
		t.Fatalf("failed to get members: %v", err)
	}

	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}
	if members[0].ID != "member-a" {
		t.Fatalf("expected member ID %q, got %q", "member-a", members[0].ID)
	}
	if members[0].LastSeen.IsZero() {
		t.Fatalf("expected LastSeen to be set")
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

	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}
	if members[0].ID != "member-a" {
		t.Fatalf("expected member ID %q, got %q", "member-a", members[0].ID)
	}
}

func TestRegistryHeartbeatUpdatesLastSeen(t *testing.T) {
	registry := NewRegistry()

	if err := registry.Join("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to join group: %v", err)
	}

	heartbeatTime := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	if err := registry.Heartbeat("analytics-workers", "member-a", heartbeatTime); err != nil {
		t.Fatalf("failed to heartbeat member: %v", err)
	}

	members := mustMembers(t, registry, "analytics-workers")
	expected := []GroupMember{
		{ID: "member-a", LastSeen: heartbeatTime},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryHeartbeatAddsMissingMember(t *testing.T) {
	registry := NewRegistry()
	heartbeatTime := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

	if err := registry.Heartbeat("analytics-workers", "member-a", heartbeatTime); err != nil {
		t.Fatalf("failed to heartbeat missing member: %v", err)
	}

	members := mustMembers(t, registry, "analytics-workers")
	expected := []GroupMember{
		{ID: "member-a", LastSeen: heartbeatTime},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryLeaveRemovesMemberAndLastSeen(t *testing.T) {
	registry := NewRegistry()
	heartbeatTime := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

	if err := registry.Heartbeat("analytics-workers", "member-a", heartbeatTime); err != nil {
		t.Fatalf("failed to heartbeat member: %v", err)
	}
	if err := registry.Leave("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to leave group: %v", err)
	}

	members := mustMembers(t, registry, "analytics-workers")
	if len(members) != 0 {
		t.Fatalf("expected no members, got %v", members)
	}
}

func TestRegistryLeavingMissingMemberIsIdempotent(t *testing.T) {
	registry := NewRegistry()
	heartbeatTime := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

	if err := registry.Leave("analytics-workers", "member-a"); err != nil {
		t.Fatalf("expected leaving missing member to succeed: %v", err)
	}
	if err := registry.Heartbeat("analytics-workers", "member-b", heartbeatTime); err != nil {
		t.Fatalf("failed to heartbeat member: %v", err)
	}
	if err := registry.Leave("analytics-workers", "member-a"); err != nil {
		t.Fatalf("expected leaving absent member to succeed: %v", err)
	}

	members := mustMembers(t, registry, "analytics-workers")
	expected := []GroupMember{
		{ID: "member-b", LastSeen: heartbeatTime},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryMembersAreReturnedSorted(t *testing.T) {
	registry := NewRegistry()
	t1 := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Minute)
	t3 := t2.Add(time.Minute)

	heartbeatMembers(t, registry, "analytics-workers", map[string]time.Time{
		"member-c": t3,
		"member-a": t1,
		"member-b": t2,
	})

	members := mustMembers(t, registry, "analytics-workers")
	expected := []GroupMember{
		{ID: "member-a", LastSeen: t1},
		{ID: "member-b", LastSeen: t2},
		{ID: "member-c", LastSeen: t3},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistrySeparateGroupsAreIsolated(t *testing.T) {
	registry := NewRegistry()
	analyticsSeen := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	billingSeen := analyticsSeen.Add(time.Minute)

	if err := registry.Heartbeat("analytics-workers", "member-a", analyticsSeen); err != nil {
		t.Fatalf("failed to heartbeat analytics group: %v", err)
	}
	if err := registry.Heartbeat("billing-workers", "member-b", billingSeen); err != nil {
		t.Fatalf("failed to heartbeat billing group: %v", err)
	}

	members := mustMembers(t, registry, "analytics-workers")
	expected := []GroupMember{
		{ID: "member-a", LastSeen: analyticsSeen},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryStateIncludesLastSeen(t *testing.T) {
	registry := NewRegistry()
	t1 := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Minute)

	heartbeatMembers(t, registry, "analytics-workers", map[string]time.Time{
		"member-b": t2,
		"member-a": t1,
	})

	state, err := registry.State("analytics-workers")
	if err != nil {
		t.Fatalf("failed to get group state: %v", err)
	}

	expected := GroupState{
		Group: "analytics-workers",
		Members: []GroupMember{
			{ID: "member-a", LastSeen: t1},
			{ID: "member-b", LastSeen: t2},
		},
	}

	if !reflect.DeepEqual(state, expected) {
		t.Fatalf("expected %v, got %v", expected, state)
	}
}

func TestRegistryRejectsEmptyGroup(t *testing.T) {
	registry := NewRegistry()
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

	if err := registry.Join("", "member-a"); err == nil {
		t.Fatalf("expected join to reject empty group")
	}
	if err := registry.Heartbeat("", "member-a", now); err == nil {
		t.Fatalf("expected heartbeat to reject empty group")
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
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

	if err := registry.Join("analytics-workers", ""); err == nil {
		t.Fatalf("expected join to reject empty member ID")
	}
	if err := registry.Heartbeat("analytics-workers", "", now); err == nil {
		t.Fatalf("expected heartbeat to reject empty member ID")
	}
	if err := registry.Leave("analytics-workers", ""); err == nil {
		t.Fatalf("expected leave to reject empty member ID")
	}
}

func TestRegistryRejectsZeroHeartbeatTime(t *testing.T) {
	registry := NewRegistry()

	if err := registry.Heartbeat("analytics-workers", "member-a", time.Time{}); err == nil {
		t.Fatalf("expected zero heartbeat time to be rejected")
	}
}

func mustMembers(t *testing.T, registry *Registry, group string) []GroupMember {
	t.Helper()

	members, err := registry.Members(group)
	if err != nil {
		t.Fatalf("failed to get members: %v", err)
	}

	return members
}

func heartbeatMembers(t *testing.T, registry *Registry, group string, members map[string]time.Time) {
	t.Helper()

	for memberID, lastSeen := range members {
		if err := registry.Heartbeat(group, memberID, lastSeen); err != nil {
			t.Fatalf("failed to heartbeat member %q: %v", memberID, err)
		}
	}
}
