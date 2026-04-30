package group

import (
	"reflect"
	"testing"
)

func TestAssignEvenly(t *testing.T) {
	assigner := NewAssigner()

	assignments, err := assigner.Assign("orders", 4, []Member{
		{ID: "member-a"},
		{ID: "member-b"},
	})
	if err != nil {
		t.Fatalf("failed to assign partitions: %v", err)
	}

	expected := []Assignment{
		{
			MemberID: "member-a",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{0, 1}},
			},
		},
		{
			MemberID: "member-b",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{2, 3}},
			},
		},
	}

	if !reflect.DeepEqual(assignments, expected) {
		t.Fatalf("expected %v, got %v", expected, assignments)
	}
}

func TestAssignUnevenly(t *testing.T) {
	assigner := NewAssigner()

	assignments, err := assigner.Assign("orders", 5, []Member{
		{ID: "member-a"},
		{ID: "member-b"},
	})
	if err != nil {
		t.Fatalf("failed to assign partitions: %v", err)
	}

	expected := []Assignment{
		{
			MemberID: "member-a",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{0, 1, 2}},
			},
		},
		{
			MemberID: "member-b",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{3, 4}},
			},
		},
	}

	if !reflect.DeepEqual(assignments, expected) {
		t.Fatalf("expected %v, got %v", expected, assignments)
	}
}

func TestAssignMoreMembersThanPartitions(t *testing.T) {
	assigner := NewAssigner()

	assignments, err := assigner.Assign("orders", 2, []Member{
		{ID: "member-a"},
		{ID: "member-b"},
		{ID: "member-c"},
	})
	if err != nil {
		t.Fatalf("failed to assign partitions: %v", err)
	}

	expected := []Assignment{
		{
			MemberID: "member-a",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{0}},
			},
		},
		{
			MemberID: "member-b",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{1}},
			},
		},
		{
			MemberID: "member-c",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{}},
			},
		},
	}

	if !reflect.DeepEqual(assignments, expected) {
		t.Fatalf("expected %v, got %v", expected, assignments)
	}
}

func TestAssignSortsMembersDeterministically(t *testing.T) {
	assigner := NewAssigner()

	assignments, err := assigner.Assign("orders", 3, []Member{
		{ID: "member-c"},
		{ID: "member-a"},
		{ID: "member-b"},
	})
	if err != nil {
		t.Fatalf("failed to assign partitions: %v", err)
	}

	expectedMemberIDs := []string{"member-a", "member-b", "member-c"}
	actualMemberIDs := make([]string, 0, len(assignments))
	for _, assignment := range assignments {
		actualMemberIDs = append(actualMemberIDs, assignment.MemberID)
	}

	if !reflect.DeepEqual(actualMemberIDs, expectedMemberIDs) {
		t.Fatalf("expected member IDs %v, got %v", expectedMemberIDs, actualMemberIDs)
	}

	expected := []Assignment{
		{
			MemberID: "member-a",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{0}},
			},
		},
		{
			MemberID: "member-b",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{1}},
			},
		},
		{
			MemberID: "member-c",
			Topics: []TopicAssignment{
				{Topic: "orders", Partitions: []int{2}},
			},
		},
	}

	if !reflect.DeepEqual(assignments, expected) {
		t.Fatalf("expected %v, got %v", expected, assignments)
	}
}

func TestAssignReturnsAssignmentForEveryMember(t *testing.T) {
	assigner := NewAssigner()
	members := []Member{
		{ID: "member-a"},
		{ID: "member-b"},
		{ID: "member-c"},
	}

	assignments, err := assigner.Assign("orders", 1, members)
	if err != nil {
		t.Fatalf("failed to assign partitions: %v", err)
	}

	if len(assignments) != len(members) {
		t.Fatalf("expected %d assignments, got %d", len(members), len(assignments))
	}
}

func TestAssignRejectsInvalidTopic(t *testing.T) {
	assigner := NewAssigner()

	_, err := assigner.Assign("", 1, []Member{{ID: "member-a"}})
	if err == nil {
		t.Fatalf("expected empty topic to be rejected")
	}
}

func TestAssignRejectsInvalidPartitionCount(t *testing.T) {
	assigner := NewAssigner()

	_, err := assigner.Assign("orders", 0, []Member{{ID: "member-a"}})
	if err == nil {
		t.Fatalf("expected invalid partition count to be rejected")
	}
}

func TestAssignRejectsEmptyMembers(t *testing.T) {
	assigner := NewAssigner()

	_, err := assigner.Assign("orders", 1, nil)
	if err == nil {
		t.Fatalf("expected empty members to be rejected")
	}
}

func TestAssignRejectsEmptyMemberID(t *testing.T) {
	assigner := NewAssigner()

	_, err := assigner.Assign("orders", 1, []Member{{ID: ""}})
	if err == nil {
		t.Fatalf("expected empty member ID to be rejected")
	}
}

func TestAssignRejectsDuplicateMemberIDs(t *testing.T) {
	assigner := NewAssigner()

	_, err := assigner.Assign("orders", 1, []Member{
		{ID: "member-a"},
		{ID: "member-a"},
	})
	if err == nil {
		t.Fatalf("expected duplicate member IDs to be rejected")
	}
}
