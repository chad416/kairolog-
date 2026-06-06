package group

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRegistryFileStoreRejectsEmptyPath(t *testing.T) {
	if _, err := NewRegistryFileStore(""); err == nil {
		t.Fatal("expected error")
	}
}

func TestRegistryFileStoreJoinAddsMember(t *testing.T) {
	store := newTestRegistryFileStore(t)

	if err := store.Join("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to join group: %v", err)
	}

	members := mustFileStoreMembers(t, store, "analytics-workers")
	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}
	if members[0].ID != "member-a" {
		t.Fatalf("expected member ID %q, got %q", "member-a", members[0].ID)
	}
	if members[0].LastSeen.IsZero() {
		t.Fatal("expected LastSeen to be set")
	}
}

func TestRegistryFileStoreJoinTrimsGroupAndMemberID(t *testing.T) {
	store := newTestRegistryFileStore(t)

	if err := store.Join(" analytics-workers ", " member-a "); err != nil {
		t.Fatalf("failed to join group: %v", err)
	}

	members := mustFileStoreMembers(t, store, "analytics-workers")
	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}
	if members[0].ID != "member-a" {
		t.Fatalf("expected member ID %q, got %q", "member-a", members[0].ID)
	}
}

func TestRegistryFileStoreDuplicateJoinIsIdempotent(t *testing.T) {
	store := newTestRegistryFileStore(t)

	if err := store.Join("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to join group: %v", err)
	}
	firstMembers := mustFileStoreMembers(t, store, "analytics-workers")

	if err := store.Join("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to join group again: %v", err)
	}
	secondMembers := mustFileStoreMembers(t, store, "analytics-workers")

	if len(secondMembers) != 1 {
		t.Fatalf("expected 1 member, got %d", len(secondMembers))
	}
	if !secondMembers[0].LastSeen.Equal(firstMembers[0].LastSeen) {
		t.Fatalf("expected duplicate join to keep LastSeen %v, got %v", firstMembers[0].LastSeen, secondMembers[0].LastSeen)
	}
}

func TestRegistryFileStoreHeartbeatUpdatesLastSeen(t *testing.T) {
	store := newTestRegistryFileStore(t)
	heartbeatTime := registryFileStoreBaseTime()

	if err := store.Join("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to join group: %v", err)
	}
	if err := store.Heartbeat("analytics-workers", "member-a", heartbeatTime); err != nil {
		t.Fatalf("failed to heartbeat member: %v", err)
	}

	members := mustFileStoreMembers(t, store, "analytics-workers")
	expected := []GroupMember{
		{ID: "member-a", LastSeen: heartbeatTime},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryFileStoreHeartbeatCreatesMissingMember(t *testing.T) {
	store := newTestRegistryFileStore(t)
	heartbeatTime := registryFileStoreBaseTime()

	if err := store.Heartbeat("analytics-workers", "member-a", heartbeatTime); err != nil {
		t.Fatalf("failed to heartbeat missing member: %v", err)
	}

	members := mustFileStoreMembers(t, store, "analytics-workers")
	expected := []GroupMember{
		{ID: "member-a", LastSeen: heartbeatTime},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryFileStoreHeartbeatRejectsZeroTime(t *testing.T) {
	store := newTestRegistryFileStore(t)

	if err := store.Heartbeat("analytics-workers", "member-a", time.Time{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestRegistryFileStoreLeaveRemovesMember(t *testing.T) {
	store := newTestRegistryFileStore(t)

	if err := store.Heartbeat("analytics-workers", "member-a", registryFileStoreBaseTime()); err != nil {
		t.Fatalf("failed to heartbeat member: %v", err)
	}
	if err := store.Leave("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to leave group: %v", err)
	}

	members := mustFileStoreMembers(t, store, "analytics-workers")
	if len(members) != 0 {
		t.Fatalf("expected no members, got %v", members)
	}
}

func TestRegistryFileStoreLeaveMissingMemberIsIdempotent(t *testing.T) {
	store := newTestRegistryFileStore(t)
	heartbeatTime := registryFileStoreBaseTime()

	if err := store.Leave("analytics-workers", "member-a"); err != nil {
		t.Fatalf("expected leaving missing member to succeed: %v", err)
	}
	if err := store.Heartbeat("analytics-workers", "member-b", heartbeatTime); err != nil {
		t.Fatalf("failed to heartbeat member: %v", err)
	}
	if err := store.Leave("analytics-workers", "member-a"); err != nil {
		t.Fatalf("expected leaving absent member to succeed: %v", err)
	}

	members := mustFileStoreMembers(t, store, "analytics-workers")
	expected := []GroupMember{
		{ID: "member-b", LastSeen: heartbeatTime},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryFileStoreMembersReturnsSortedMembers(t *testing.T) {
	store := newTestRegistryFileStore(t)
	t1 := registryFileStoreBaseTime()
	t2 := t1.Add(time.Minute)
	t3 := t2.Add(time.Minute)

	heartbeatFileStoreMembers(t, store, "analytics-workers", map[string]time.Time{
		"member-c": t3,
		"member-a": t1,
		"member-b": t2,
	})

	members := mustFileStoreMembers(t, store, "analytics-workers")
	expected := []GroupMember{
		{ID: "member-a", LastSeen: t1},
		{ID: "member-b", LastSeen: t2},
		{ID: "member-c", LastSeen: t3},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryFileStoreMembersForMissingGroupReturnsEmptySlice(t *testing.T) {
	store := newTestRegistryFileStore(t)

	members := mustFileStoreMembers(t, store, "missing-workers")
	if len(members) != 0 {
		t.Fatalf("expected no members, got %v", members)
	}
}

func TestRegistryFileStoreStateReturnsGroupAndSortedMembers(t *testing.T) {
	store := newTestRegistryFileStore(t)
	t1 := registryFileStoreBaseTime()
	t2 := t1.Add(time.Minute)

	heartbeatFileStoreMembers(t, store, "analytics-workers", map[string]time.Time{
		"member-b": t2,
		"member-a": t1,
	})

	state, err := store.State("analytics-workers")
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

func TestRegistryFileStoreSeparateGroupsAreIsolated(t *testing.T) {
	store := newTestRegistryFileStore(t)
	analyticsSeen := registryFileStoreBaseTime()
	billingSeen := analyticsSeen.Add(time.Minute)

	if err := store.Heartbeat("analytics-workers", "member-a", analyticsSeen); err != nil {
		t.Fatalf("failed to heartbeat analytics group: %v", err)
	}
	if err := store.Heartbeat("billing-workers", "member-b", billingSeen); err != nil {
		t.Fatalf("failed to heartbeat billing group: %v", err)
	}

	members := mustFileStoreMembers(t, store, "analytics-workers")
	expected := []GroupMember{
		{ID: "member-a", LastSeen: analyticsSeen},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryFileStoreStaleMembersReturnsStaleMembersOnly(t *testing.T) {
	store := newTestRegistryFileStore(t)
	now := registryFileStoreBaseTime()
	staleSeen := now.Add(-6 * time.Minute)
	activeSeen := now.Add(-4 * time.Minute)

	heartbeatFileStoreMembers(t, store, "analytics-workers", map[string]time.Time{
		"member-a": activeSeen,
		"member-b": staleSeen,
	})

	staleMembers := mustFileStoreStaleMembers(t, store, "analytics-workers", now, 5*time.Minute)
	expected := []GroupMember{
		{ID: "member-b", LastSeen: staleSeen},
	}

	if !reflect.DeepEqual(staleMembers, expected) {
		t.Fatalf("expected %v, got %v", expected, staleMembers)
	}
}

func TestRegistryFileStoreStaleMembersReturnsSortedMembers(t *testing.T) {
	store := newTestRegistryFileStore(t)
	now := registryFileStoreBaseTime()
	t1 := now.Add(-8 * time.Minute)
	t2 := now.Add(-7 * time.Minute)
	t3 := now.Add(-6 * time.Minute)

	heartbeatFileStoreMembers(t, store, "analytics-workers", map[string]time.Time{
		"member-c": t3,
		"member-a": t1,
		"member-b": t2,
	})

	staleMembers := mustFileStoreStaleMembers(t, store, "analytics-workers", now, 5*time.Minute)
	expected := []GroupMember{
		{ID: "member-a", LastSeen: t1},
		{ID: "member-b", LastSeen: t2},
		{ID: "member-c", LastSeen: t3},
	}

	if !reflect.DeepEqual(staleMembers, expected) {
		t.Fatalf("expected %v, got %v", expected, staleMembers)
	}
}

func TestRegistryFileStoreStaleMembersDoesNotRemoveMembers(t *testing.T) {
	store := newTestRegistryFileStore(t)
	now := registryFileStoreBaseTime()
	staleSeen := now.Add(-6 * time.Minute)

	if err := store.Heartbeat("analytics-workers", "member-a", staleSeen); err != nil {
		t.Fatalf("failed to heartbeat member: %v", err)
	}

	mustFileStoreStaleMembers(t, store, "analytics-workers", now, 5*time.Minute)

	members := mustFileStoreMembers(t, store, "analytics-workers")
	expected := []GroupMember{
		{ID: "member-a", LastSeen: staleSeen},
	}
	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryFileStoreStaleMembersRejectsZeroNow(t *testing.T) {
	store := newTestRegistryFileStore(t)

	if _, err := store.StaleMembers("analytics-workers", time.Time{}, 5*time.Minute); err == nil {
		t.Fatal("expected error")
	}
}

func TestRegistryFileStoreStaleMembersRejectsNonPositiveTimeout(t *testing.T) {
	store := newTestRegistryFileStore(t)
	now := registryFileStoreBaseTime()

	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{name: "zero timeout", timeout: 0},
		{name: "negative timeout", timeout: -time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := store.StaleMembers("analytics-workers", now, tt.timeout); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestRegistryFileStoreRemoveStaleMembersRemovesStaleMembers(t *testing.T) {
	store := newTestRegistryFileStore(t)
	now := registryFileStoreBaseTime()
	staleSeen := now.Add(-6 * time.Minute)

	if err := store.Heartbeat("analytics-workers", "member-a", staleSeen); err != nil {
		t.Fatalf("failed to heartbeat member: %v", err)
	}

	removedMembers := mustFileStoreRemoveStaleMembers(t, store, "analytics-workers", now, 5*time.Minute)
	expected := []GroupMember{
		{ID: "member-a", LastSeen: staleSeen},
	}

	if !reflect.DeepEqual(removedMembers, expected) {
		t.Fatalf("expected %v, got %v", expected, removedMembers)
	}

	members := mustFileStoreMembers(t, store, "analytics-workers")
	if len(members) != 0 {
		t.Fatalf("expected no members, got %v", members)
	}
}

func TestRegistryFileStoreRemoveStaleMembersKeepsActiveMembers(t *testing.T) {
	store := newTestRegistryFileStore(t)
	now := registryFileStoreBaseTime()
	activeSeen := now.Add(-5 * time.Minute)

	if err := store.Heartbeat("analytics-workers", "member-a", activeSeen); err != nil {
		t.Fatalf("failed to heartbeat member: %v", err)
	}

	removedMembers := mustFileStoreRemoveStaleMembers(t, store, "analytics-workers", now, 5*time.Minute)
	if len(removedMembers) != 0 {
		t.Fatalf("expected no removed members, got %v", removedMembers)
	}

	members := mustFileStoreMembers(t, store, "analytics-workers")
	expected := []GroupMember{
		{ID: "member-a", LastSeen: activeSeen},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryFileStoreRemoveStaleMembersReturnsRemovedMembersSorted(t *testing.T) {
	store := newTestRegistryFileStore(t)
	now := registryFileStoreBaseTime()
	t1 := now.Add(-8 * time.Minute)
	t2 := now.Add(-7 * time.Minute)
	t3 := now.Add(-6 * time.Minute)

	heartbeatFileStoreMembers(t, store, "analytics-workers", map[string]time.Time{
		"member-c": t3,
		"member-a": t1,
		"member-b": t2,
	})

	removedMembers := mustFileStoreRemoveStaleMembers(t, store, "analytics-workers", now, 5*time.Minute)
	expected := []GroupMember{
		{ID: "member-a", LastSeen: t1},
		{ID: "member-b", LastSeen: t2},
		{ID: "member-c", LastSeen: t3},
	}

	if !reflect.DeepEqual(removedMembers, expected) {
		t.Fatalf("expected %v, got %v", expected, removedMembers)
	}
}

func TestRegistryFileStoreRemoveStaleMembersRemovesGroupWhenAllMembersAreRemoved(t *testing.T) {
	store := newTestRegistryFileStore(t)
	now := registryFileStoreBaseTime()

	heartbeatFileStoreMembers(t, store, "analytics-workers", map[string]time.Time{
		"member-a": now.Add(-6 * time.Minute),
		"member-b": now.Add(-7 * time.Minute),
	})

	mustFileStoreRemoveStaleMembers(t, store, "analytics-workers", now, 5*time.Minute)

	if _, exists := store.groups["analytics-workers"]; exists {
		t.Fatal("expected group entry to be removed")
	}
}

func TestRegistryFileStoreRemoveStaleMembersRejectsZeroNow(t *testing.T) {
	store := newTestRegistryFileStore(t)

	if _, err := store.RemoveStaleMembers("analytics-workers", time.Time{}, 5*time.Minute); err == nil {
		t.Fatal("expected error")
	}
}

func TestRegistryFileStoreRemoveStaleMembersRejectsNonPositiveTimeout(t *testing.T) {
	store := newTestRegistryFileStore(t)
	now := registryFileStoreBaseTime()

	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{name: "zero timeout", timeout: 0},
		{name: "negative timeout", timeout: -time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := store.RemoveStaleMembers("analytics-workers", now, tt.timeout); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestRegistryFileStoreJoinPersistsStateToDisk(t *testing.T) {
	path := registryFileStorePath(t)
	store := newTestRegistryFileStoreAt(t, path)

	if err := store.Join("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to join group: %v", err)
	}
	originalMembers := mustFileStoreMembers(t, store, "analytics-workers")

	reopened := newTestRegistryFileStoreAt(t, path)
	if err := reopened.Load(); err != nil {
		t.Fatalf("failed to load registry: %v", err)
	}

	loadedMembers := mustFileStoreMembers(t, reopened, "analytics-workers")
	if len(loadedMembers) != 1 {
		t.Fatalf("expected 1 loaded member, got %d", len(loadedMembers))
	}
	if loadedMembers[0].ID != originalMembers[0].ID {
		t.Fatalf("expected member ID %q, got %q", originalMembers[0].ID, loadedMembers[0].ID)
	}
	if !loadedMembers[0].LastSeen.Equal(originalMembers[0].LastSeen) {
		t.Fatalf("expected LastSeen %v, got %v", originalMembers[0].LastSeen, loadedMembers[0].LastSeen)
	}
}

func TestRegistryFileStoreHeartbeatPersistsUpdatedLastSeenToDisk(t *testing.T) {
	path := registryFileStorePath(t)
	store := newTestRegistryFileStoreAt(t, path)
	heartbeatTime := registryFileStoreBaseTime()

	if err := store.Join("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to join group: %v", err)
	}
	if err := store.Heartbeat("analytics-workers", "member-a", heartbeatTime); err != nil {
		t.Fatalf("failed to heartbeat member: %v", err)
	}

	reopened := newTestRegistryFileStoreAt(t, path)
	if err := reopened.Load(); err != nil {
		t.Fatalf("failed to load registry: %v", err)
	}

	members := mustFileStoreMembers(t, reopened, "analytics-workers")
	expected := []GroupMember{
		{ID: "member-a", LastSeen: heartbeatTime},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryFileStoreLeavePersistsDeletionToDisk(t *testing.T) {
	path := registryFileStorePath(t)
	store := newTestRegistryFileStoreAt(t, path)

	if err := store.Heartbeat("analytics-workers", "member-a", registryFileStoreBaseTime()); err != nil {
		t.Fatalf("failed to heartbeat member: %v", err)
	}
	if err := store.Leave("analytics-workers", "member-a"); err != nil {
		t.Fatalf("failed to leave group: %v", err)
	}

	reopened := newTestRegistryFileStoreAt(t, path)
	if err := reopened.Load(); err != nil {
		t.Fatalf("failed to load registry: %v", err)
	}

	members := mustFileStoreMembers(t, reopened, "analytics-workers")
	if len(members) != 0 {
		t.Fatalf("expected no members, got %v", members)
	}
}

func TestRegistryFileStoreRemoveStaleMembersPersistsDeletionToDisk(t *testing.T) {
	path := registryFileStorePath(t)
	store := newTestRegistryFileStoreAt(t, path)
	now := registryFileStoreBaseTime()
	staleSeen := now.Add(-6 * time.Minute)
	activeSeen := now.Add(-4 * time.Minute)

	heartbeatFileStoreMembers(t, store, "analytics-workers", map[string]time.Time{
		"member-a": staleSeen,
		"member-b": activeSeen,
	})

	mustFileStoreRemoveStaleMembers(t, store, "analytics-workers", now, 5*time.Minute)

	reopened := newTestRegistryFileStoreAt(t, path)
	if err := reopened.Load(); err != nil {
		t.Fatalf("failed to load registry: %v", err)
	}

	members := mustFileStoreMembers(t, reopened, "analytics-workers")
	expected := []GroupMember{
		{ID: "member-b", LastSeen: activeSeen},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryFileStoreLoadRestoresMembersAndLastSeenFromDisk(t *testing.T) {
	path := registryFileStorePath(t)
	store := newTestRegistryFileStoreAt(t, path)
	t1 := registryFileStoreBaseTime()
	t2 := t1.Add(time.Minute)

	heartbeatFileStoreMembers(t, store, "analytics-workers", map[string]time.Time{
		"member-b": t2,
		"member-a": t1,
	})

	reopened := newTestRegistryFileStoreAt(t, path)
	if err := reopened.Load(); err != nil {
		t.Fatalf("failed to load registry: %v", err)
	}

	members := mustFileStoreMembers(t, reopened, "analytics-workers")
	expected := []GroupMember{
		{ID: "member-a", LastSeen: t1},
		{ID: "member-b", LastSeen: t2},
	}

	if !reflect.DeepEqual(members, expected) {
		t.Fatalf("expected %v, got %v", expected, members)
	}
}

func TestRegistryFileStoreLoadOnMissingFileSucceedsWithEmptyState(t *testing.T) {
	path := registryFileStorePath(t)
	store := newTestRegistryFileStoreAt(t, path)

	if err := os.Remove(path); err != nil {
		t.Fatalf("failed to remove registry file: %v", err)
	}

	if err := store.Load(); err != nil {
		t.Fatalf("expected load on missing file to succeed, got %v", err)
	}

	members := mustFileStoreMembers(t, store, "analytics-workers")
	if len(members) != 0 {
		t.Fatalf("expected no members, got %v", members)
	}
}

func TestRegistryFileStoreCreatesParentDirectoryAutomatically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "registry", "groups.log")

	if _, err := NewRegistryFileStore(path); err != nil {
		t.Fatalf("failed to create registry file store: %v", err)
	}

	if _, err := os.Stat(filepath.Dir(path)); err != nil {
		t.Fatalf("expected parent directory to exist: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected registry file to exist: %v", err)
	}
}

func TestRegistryFileStoreInvalidGroupAndMemberInputReturnsErrors(t *testing.T) {
	store := newTestRegistryFileStore(t)
	now := registryFileStoreBaseTime()

	tests := []struct {
		name string
		fn   func() error
	}{
		{name: "join empty group", fn: func() error { return store.Join("", "member-a") }},
		{name: "join empty member ID", fn: func() error { return store.Join("analytics-workers", "") }},
		{name: "heartbeat empty group", fn: func() error { return store.Heartbeat("", "member-a", now) }},
		{name: "heartbeat empty member ID", fn: func() error { return store.Heartbeat("analytics-workers", "", now) }},
		{name: "leave empty group", fn: func() error { return store.Leave("", "member-a") }},
		{name: "leave empty member ID", fn: func() error { return store.Leave("analytics-workers", "") }},
		{name: "members empty group", fn: func() error {
			_, err := store.Members("")
			return err
		}},
		{name: "state empty group", fn: func() error {
			_, err := store.State("")
			return err
		}},
		{name: "stale members empty group", fn: func() error {
			_, err := store.StaleMembers("", now, time.Minute)
			return err
		}},
		{name: "remove stale members empty group", fn: func() error {
			_, err := store.RemoveStaleMembers("", now, time.Minute)
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fn(); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestRegistryFileStorePersistsReadableSortedRecords(t *testing.T) {
	path := registryFileStorePath(t)
	store := newTestRegistryFileStoreAt(t, path)
	t1 := registryFileStoreBaseTime()
	t2 := t1.Add(time.Minute)

	if err := store.Heartbeat("billing-workers", "member-b", t2); err != nil {
		t.Fatalf("failed to heartbeat billing group: %v", err)
	}
	if err := store.Heartbeat("analytics-workers", "member-a", t1); err != nil {
		t.Fatalf("failed to heartbeat analytics group: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read registry file: %v", err)
	}
	contents := string(data)
	if !strings.Contains(contents, `"group":"analytics-workers"`) {
		t.Fatalf("expected registry file to contain analytics group, got %q", contents)
	}
	if !strings.Contains(contents, `"member_id":"member-a"`) {
		t.Fatalf("expected registry file to contain member ID, got %q", contents)
	}
	if strings.Index(contents, `"group":"analytics-workers"`) > strings.Index(contents, `"group":"billing-workers"`) {
		t.Fatalf("expected records to be sorted by group, got %q", contents)
	}
}

func newTestRegistryFileStore(t *testing.T) *RegistryFileStore {
	t.Helper()
	return newTestRegistryFileStoreAt(t, registryFileStorePath(t))
}

func newTestRegistryFileStoreAt(t *testing.T, path string) *RegistryFileStore {
	t.Helper()

	store, err := NewRegistryFileStore(path)
	if err != nil {
		t.Fatalf("failed to create registry file store: %v", err)
	}

	return store
}

func registryFileStorePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "data", "groups.log")
}

func registryFileStoreBaseTime() time.Time {
	return time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
}

func mustFileStoreMembers(t *testing.T, store *RegistryFileStore, group string) []GroupMember {
	t.Helper()

	members, err := store.Members(group)
	if err != nil {
		t.Fatalf("failed to get members: %v", err)
	}

	return members
}

func mustFileStoreStaleMembers(t *testing.T, store *RegistryFileStore, group string, now time.Time, timeout time.Duration) []GroupMember {
	t.Helper()

	members, err := store.StaleMembers(group, now, timeout)
	if err != nil {
		t.Fatalf("failed to get stale members: %v", err)
	}

	return members
}

func mustFileStoreRemoveStaleMembers(t *testing.T, store *RegistryFileStore, group string, now time.Time, timeout time.Duration) []GroupMember {
	t.Helper()

	members, err := store.RemoveStaleMembers(group, now, timeout)
	if err != nil {
		t.Fatalf("failed to remove stale members: %v", err)
	}

	return members
}

func heartbeatFileStoreMembers(t *testing.T, store *RegistryFileStore, group string, members map[string]time.Time) {
	t.Helper()

	for memberID, lastSeen := range members {
		if err := store.Heartbeat(group, memberID, lastSeen); err != nil {
			t.Fatalf("failed to heartbeat member %q: %v", memberID, err)
		}
	}
}
