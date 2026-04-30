package group

import (
	"fmt"
	"sort"
	"strings"
)

type Member struct {
	ID string
}

type TopicAssignment struct {
	Topic      string
	Partitions []int
}

type Assignment struct {
	MemberID string
	Topics   []TopicAssignment
}

type Assigner struct{}

func NewAssigner() *Assigner {
	return &Assigner{}
}

func (a *Assigner) Assign(topic string, partitionCount int, members []Member) ([]Assignment, error) {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return nil, fmt.Errorf("topic cannot be empty")
	}
	if partitionCount <= 0 {
		return nil, fmt.Errorf("partition count must be greater than zero")
	}
	if len(members) == 0 {
		return nil, fmt.Errorf("members cannot be empty")
	}

	sortedMembers, err := validateAndSortMembers(members)
	if err != nil {
		return nil, err
	}

	assignments := make([]Assignment, 0, len(sortedMembers))
	nextPartition := 0
	basePartitionsPerMember := partitionCount / len(sortedMembers)
	extraPartitions := partitionCount % len(sortedMembers)

	for i, member := range sortedMembers {
		partitionTotal := basePartitionsPerMember
		if i < extraPartitions {
			partitionTotal++
		}

		partitions := make([]int, 0, partitionTotal)
		for j := 0; j < partitionTotal; j++ {
			partitions = append(partitions, nextPartition)
			nextPartition++
		}

		assignments = append(assignments, Assignment{
			MemberID: member.ID,
			Topics: []TopicAssignment{
				{
					Topic:      topic,
					Partitions: partitions,
				},
			},
		})
	}

	return assignments, nil
}

func validateAndSortMembers(members []Member) ([]Member, error) {
	sortedMembers := make([]Member, 0, len(members))
	seen := make(map[string]struct{}, len(members))

	for _, member := range members {
		id := strings.TrimSpace(member.ID)
		if id == "" {
			return nil, fmt.Errorf("member ID cannot be empty")
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("duplicate member ID %q", id)
		}

		seen[id] = struct{}{}
		sortedMembers = append(sortedMembers, Member{ID: id})
	}

	sort.Slice(sortedMembers, func(i, j int) bool {
		return sortedMembers[i].ID < sortedMembers[j].ID
	})

	return sortedMembers, nil
}
