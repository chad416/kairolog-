package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"kairolog/internal/consumer"
	"kairolog/internal/group"
	"kairolog/internal/topic"
)

const defaultAddr = ":8080"
const defaultOffsetStorePath = "data/consumer_offsets.log"
const defaultAssignmentStorePath = "data/group_assignments.log"
const defaultRegistryStorePath = "data/group_registry.log"
const defaultStaleCleanupInterval = 1 * time.Minute
const defaultStaleMemberTimeout = 5 * time.Minute

type assignmentStore interface {
	Save(group string, topic string, assignments []group.Assignment) error
	Get(group string, topic string) ([]group.Assignment, bool, error)
	Topics(group string) ([]string, error)
	Delete(group string, topic string) error
}

type groupRegistry interface {
	Groups() ([]string, error)
	Join(group string, memberID string) error
	Heartbeat(group string, memberID string, now time.Time) error
	Leave(group string, memberID string) error
	Members(group string) ([]group.GroupMember, error)
	StaleMembers(group string, now time.Time, timeout time.Duration) ([]group.GroupMember, error)
	RemoveStaleMembers(group string, now time.Time, timeout time.Duration) ([]group.GroupMember, error)
}

type Server struct {
	topicManager    *topic.Manager
	offsetStore     *consumer.OffsetStore
	assigner        *group.Assigner
	registry        groupRegistry
	assignmentStore assignmentStore
}

type healthResponse struct {
	Status string `json:"status"`
}

type createTopicRequest struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

type topicsResponse struct {
	Topics []string `json:"topics"`
}

type produceRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Message   string `json:"message"`
}

type produceResponse struct {
	Status string `json:"status"`
	Offset int64  `json:"offset"`
}

type fetchRecord struct {
	Offset  int64  `json:"offset"`
	Message string `json:"message"`
}

type fetchResponse struct {
	Records []fetchRecord `json:"records"`
}

type offsetCommitRequest struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

type offsetCommitResponse struct {
	Status string `json:"status"`
}

type offsetResponse struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Found     bool   `json:"found"`
}

type groupAssignRequest struct {
	Topic   string               `json:"topic"`
	Members []groupMemberRequest `json:"members"`
}

type groupMemberRequest struct {
	ID string `json:"id"`
}

type groupAssignResponse struct {
	Assignments []groupAssignmentResponse `json:"assignments"`
}

type groupRebalanceRequest struct {
	Group string `json:"group"`
	Topic string `json:"topic"`
}

type groupRebalanceResponse struct {
	Group       string                    `json:"group"`
	Assignments []groupAssignmentResponse `json:"assignments"`
}

type groupCleanupAndRebalanceRequest struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	TimeoutMS int64  `json:"timeout_ms"`
}

type groupCleanupAndRebalanceResponse struct {
	Group          string                    `json:"group"`
	Topic          string                    `json:"topic"`
	TimeoutMS      int64                     `json:"timeout_ms"`
	RemovedMembers []groupMemberResponse     `json:"removed_members"`
	Assignments    []groupAssignmentResponse `json:"assignments"`
}

type groupAssignmentsResponse struct {
	Group       string                    `json:"group"`
	Topic       string                    `json:"topic"`
	Found       bool                      `json:"found"`
	Assignments []groupAssignmentResponse `json:"assignments"`
}

type groupAssignmentDeleteResponse struct {
	Status string `json:"status"`
	Group  string `json:"group"`
	Topic  string `json:"topic"`
}

type groupAssignmentResponse struct {
	MemberID string                         `json:"member_id"`
	Topics   []groupTopicAssignmentResponse `json:"topics"`
}

type groupTopicAssignmentResponse struct {
	Topic      string `json:"topic"`
	Partitions []int  `json:"partitions"`
}

type groupMembershipRequest struct {
	Group    string `json:"group"`
	MemberID string `json:"member_id"`
}

type groupMembershipResponse struct {
	Status string `json:"status"`
}

type groupMembersResponse struct {
	Group   string                `json:"group"`
	Members []groupMemberResponse `json:"members"`
}

type groupStaleResponse struct {
	Group     string                `json:"group"`
	TimeoutMS int64                 `json:"timeout_ms"`
	Members   []groupMemberResponse `json:"members"`
}

type groupRemoveStaleRequest struct {
	Group     string `json:"group"`
	TimeoutMS int64  `json:"timeout_ms"`
}

type groupRemoveStaleResponse struct {
	Group          string                `json:"group"`
	TimeoutMS      int64                 `json:"timeout_ms"`
	RemovedMembers []groupMemberResponse `json:"removed_members"`
}

type groupMemberResponse struct {
	ID string `json:"id"`
}

func New() (*http.Server, error) {
	srv, _, err := newConfiguredServer()
	return srv, err
}

func newConfiguredServer() (*http.Server, *Server, error) {
	offsetStore, err := consumer.NewOffsetStore(defaultOffsetStorePath)
	if err != nil {
		return nil, nil, fmt.Errorf("create offset store: %w", err)
	}

	assignmentFileStore, err := group.NewAssignmentFileStore(defaultAssignmentStorePath)
	if err != nil {
		return nil, nil, fmt.Errorf("create assignment store: %w", err)
	}
	if err := assignmentFileStore.Load(); err != nil {
		return nil, nil, fmt.Errorf("load assignment store: %w", err)
	}

	registryStore, err := group.NewRegistryFileStore(defaultRegistryStorePath)
	if err != nil {
		return nil, nil, fmt.Errorf("create registry store: %w", err)
	}
	if err := registryStore.Load(); err != nil {
		return nil, nil, fmt.Errorf("load registry store: %w", err)
	}

	server := &Server{
		topicManager:    topic.NewManager(),
		offsetStore:     offsetStore,
		assigner:        group.NewAssigner(),
		registry:        registryStore,
		assignmentStore: assignmentFileStore,
	}

	return &http.Server{
		Addr:    defaultAddr,
		Handler: server.routes(),
	}, server, nil
}

func newServer(topicManager *topic.Manager, offsetStore *consumer.OffsetStore, assignmentStore assignmentStore, registry groupRegistry) *http.Server {
	server := &Server{
		topicManager:    topicManager,
		offsetStore:     offsetStore,
		assigner:        group.NewAssigner(),
		registry:        registry,
		assignmentStore: assignmentStore,
	}

	return &http.Server{
		Addr:    defaultAddr,
		Handler: server.routes(),
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.healthHandler)
	mux.HandleFunc("/topics", s.topicsHandler)
	mux.HandleFunc("/produce", s.produceHandler)
	mux.HandleFunc("/fetch", s.fetchHandler)
	mux.HandleFunc("/offsets/commit", s.offsetCommitHandler)
	mux.HandleFunc("/offsets", s.offsetsHandler)
	mux.HandleFunc("/groups/assign", s.groupAssignHandler)
	mux.HandleFunc("/groups/join", s.groupJoinHandler)
	mux.HandleFunc("/groups/leave", s.groupLeaveHandler)
	mux.HandleFunc("/groups/heartbeat", s.groupHeartbeatHandler)
	mux.HandleFunc("/groups/members", s.groupMembersHandler)
	mux.HandleFunc("/groups/stale", s.groupStaleHandler)
	mux.HandleFunc("/groups/remove-stale", s.groupRemoveStaleHandler)
	mux.HandleFunc("/groups/rebalance", s.groupRebalanceHandler)
	mux.HandleFunc("/groups/cleanup-and-rebalance", s.groupCleanupAndRebalanceHandler)
	mux.HandleFunc("/groups/assignments", s.groupAssignmentsHandler)

	return mux
}

func Start() error {
	srv, server, err := newConfiguredServer()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv.RegisterOnShutdown(cancel)
	startStaleMemberCleanup(
		ctx,
		server.registry,
		server.assignmentStore,
		server.topicManager,
		server.assigner,
		defaultStaleCleanupInterval,
		defaultStaleMemberTimeout,
	)

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("start broker HTTP server: %w", err)
	}

	return nil
}

func cleanupStaleMembersOnce(registry groupRegistry, now time.Time, timeout time.Duration) error {
	if registry == nil {
		return fmt.Errorf("group registry cannot be nil")
	}
	if now.IsZero() {
		return fmt.Errorf("now time cannot be zero")
	}
	if timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}

	groups, err := registry.Groups()
	if err != nil {
		return fmt.Errorf("list groups: %w", err)
	}

	for _, groupName := range groups {
		if _, err := registry.RemoveStaleMembers(groupName, now, timeout); err != nil {
			return fmt.Errorf("remove stale members for group %q: %w", groupName, err)
		}
	}

	return nil
}

func cleanupStaleMembersAndRebalanceOnce(registry groupRegistry, assignmentStore assignmentStore, topicManager *topic.Manager, assigner *group.Assigner, now time.Time, timeout time.Duration) error {
	if registry == nil {
		return fmt.Errorf("group registry cannot be nil")
	}
	if assignmentStore == nil {
		return fmt.Errorf("assignment store cannot be nil")
	}
	if topicManager == nil {
		return fmt.Errorf("topic manager cannot be nil")
	}
	if assigner == nil {
		return fmt.Errorf("assigner cannot be nil")
	}
	if now.IsZero() {
		return fmt.Errorf("now time cannot be zero")
	}
	if timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}

	groups, err := registry.Groups()
	if err != nil {
		return fmt.Errorf("list groups: %w", err)
	}

	for _, groupName := range groups {
		removedMembers, err := registry.RemoveStaleMembers(groupName, now, timeout)
		if err != nil {
			return fmt.Errorf("remove stale members for group %q: %w", groupName, err)
		}
		if len(removedMembers) == 0 {
			continue
		}

		topicNames, err := assignmentStore.Topics(groupName)
		if err != nil {
			return fmt.Errorf("list assignment topics for group %q: %w", groupName, err)
		}
		if len(topicNames) == 0 {
			continue
		}

		activeMembers, err := registry.Members(groupName)
		if err != nil {
			return fmt.Errorf("list active members for group %q: %w", groupName, err)
		}

		if len(activeMembers) == 0 {
			for _, topicName := range topicNames {
				if err := assignmentStore.Delete(groupName, topicName); err != nil {
					return fmt.Errorf("delete assignments for group %q topic %q: %w", groupName, topicName, err)
				}
			}
			continue
		}

		assignerMembers := convertRegistryMembers(activeMembers)
		for _, topicName := range topicNames {
			topicInfo, exists := topicManager.GetTopic(topicName)
			if !exists {
				continue
			}

			assignments, err := assigner.Assign(topicName, len(topicInfo.Partitions), assignerMembers)
			if err != nil {
				return fmt.Errorf("assign partitions for group %q topic %q: %w", groupName, topicName, err)
			}

			if err := assignmentStore.Save(groupName, topicName, assignments); err != nil {
				return fmt.Errorf("save assignments for group %q topic %q: %w", groupName, topicName, err)
			}
		}
	}

	return nil
}

func startStaleMemberCleanup(ctx context.Context, registry groupRegistry, assignmentStore assignmentStore, topicManager *topic.Manager, assigner *group.Assigner, interval time.Duration, timeout time.Duration) {
	if ctx == nil || registry == nil || assignmentStore == nil || topicManager == nil || assigner == nil || interval <= 0 || timeout <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				_ = cleanupStaleMembersAndRebalanceOnce(registry, assignmentStore, topicManager, assigner, now, timeout)
			}
		}
	}()
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

func (s *Server) topicsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.createTopicHandler(w, r)
	case http.MethodGet:
		s.listTopicsHandler(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) createTopicHandler(w http.ResponseWriter, r *http.Request) {
	var req createTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" || req.Partitions <= 0 {
		http.Error(w, "invalid topic request", http.StatusBadRequest)
		return
	}

	if _, exists := s.topicManager.GetTopic(name); exists {
		http.Error(w, "topic already exists", http.StatusConflict)
		return
	}

	if err := s.topicManager.CreateTopic(name, req.Partitions); err != nil {
		if _, exists := s.topicManager.GetTopic(name); exists {
			http.Error(w, "topic already exists", http.StatusConflict)
			return
		}

		http.Error(w, "failed to create topic", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (s *Server) listTopicsHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, topicsResponse{Topics: s.topicManager.ListTopics()})
}

func (s *Server) produceHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req produceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	topicName := strings.TrimSpace(req.Topic)
	if topicName == "" || req.Partition < 0 {
		http.Error(w, "invalid produce request", http.StatusBadRequest)
		return
	}

	partition, ok := s.getPartition(topicName, req.Partition)
	if !ok {
		http.Error(w, "topic or partition not found", http.StatusNotFound)
		return
	}

	offset, err := partition.Append(req.Message)
	if err != nil {
		http.Error(w, "failed to store message", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, produceResponse{
		Status: "stored",
		Offset: offset,
	})
}

func (s *Server) fetchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topicName := strings.TrimSpace(r.URL.Query().Get("topic"))
	if topicName == "" {
		http.Error(w, "missing topic", http.StatusBadRequest)
		return
	}

	partitionValue := r.URL.Query().Get("partition")
	if partitionValue == "" {
		http.Error(w, "missing partition", http.StatusBadRequest)
		return
	}

	partitionID, err := strconv.Atoi(partitionValue)
	if err != nil || partitionID < 0 {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}

	offsetValue := r.URL.Query().Get("offset")
	if offsetValue == "" {
		http.Error(w, "missing offset", http.StatusBadRequest)
		return
	}

	offset, err := strconv.ParseInt(offsetValue, 10, 64)
	if err != nil || offset < 0 {
		http.Error(w, "invalid offset", http.StatusBadRequest)
		return
	}

	partition, ok := s.getPartition(topicName, partitionID)
	if !ok {
		http.Error(w, "topic or partition not found", http.StatusNotFound)
		return
	}

	records, err := partition.ReadFrom(offset)
	if err != nil {
		http.Error(w, "failed to read records", http.StatusInternalServerError)
		return
	}

	responseRecords := make([]fetchRecord, 0, len(records))
	for _, record := range records {
		responseRecords = append(responseRecords, fetchRecord{
			Offset:  record.Offset,
			Message: record.Message,
		})
	}

	writeJSON(w, http.StatusOK, fetchResponse{Records: responseRecords})
}

func (s *Server) offsetCommitHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req offsetCommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	group := strings.TrimSpace(req.Group)
	topicName := strings.TrimSpace(req.Topic)
	if group == "" || topicName == "" || req.Partition < 0 || req.Offset < 0 {
		http.Error(w, "invalid offset commit request", http.StatusBadRequest)
		return
	}

	if s.offsetStore == nil {
		http.Error(w, "offset store is not initialized", http.StatusInternalServerError)
		return
	}

	if err := s.offsetStore.Commit(group, topicName, req.Partition, req.Offset); err != nil {
		http.Error(w, "failed to commit offset", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, offsetCommitResponse{Status: "committed"})
}

func (s *Server) offsetsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	group := strings.TrimSpace(r.URL.Query().Get("group"))
	if group == "" {
		http.Error(w, "missing group", http.StatusBadRequest)
		return
	}

	topicName := strings.TrimSpace(r.URL.Query().Get("topic"))
	if topicName == "" {
		http.Error(w, "missing topic", http.StatusBadRequest)
		return
	}

	partitionValue := r.URL.Query().Get("partition")
	if partitionValue == "" {
		http.Error(w, "missing partition", http.StatusBadRequest)
		return
	}

	partitionID, err := strconv.Atoi(partitionValue)
	if err != nil || partitionID < 0 {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}

	if s.offsetStore == nil {
		http.Error(w, "offset store is not initialized", http.StatusInternalServerError)
		return
	}

	offset, found, err := s.offsetStore.Get(group, topicName, partitionID)
	if err != nil {
		http.Error(w, "failed to get offset", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, offsetResponse{
		Group:     group,
		Topic:     topicName,
		Partition: partitionID,
		Offset:    offset,
		Found:     found,
	})
}

func (s *Server) groupAssignHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req groupAssignRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	topicName := strings.TrimSpace(req.Topic)
	if topicName == "" {
		http.Error(w, "missing topic", http.StatusBadRequest)
		return
	}
	if len(req.Members) == 0 {
		http.Error(w, "missing members", http.StatusBadRequest)
		return
	}

	members, err := parseGroupMembers(req.Members)
	if err != nil {
		http.Error(w, "invalid members", http.StatusBadRequest)
		return
	}

	topicInfo, exists := s.topicManager.GetTopic(topicName)
	if !exists {
		http.Error(w, "topic not found", http.StatusNotFound)
		return
	}

	if s.assigner == nil {
		http.Error(w, "assigner is not initialized", http.StatusInternalServerError)
		return
	}

	assignments, err := s.assigner.Assign(topicName, len(topicInfo.Partitions), members)
	if err != nil {
		http.Error(w, "failed to assign partitions", http.StatusBadRequest)
		return
	}

	writeJSON(w, http.StatusOK, groupAssignResponse{
		Assignments: convertGroupAssignments(assignments),
	})
}

func (s *Server) groupRebalanceHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req groupRebalanceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	groupName := strings.TrimSpace(req.Group)
	if groupName == "" {
		http.Error(w, "missing group", http.StatusBadRequest)
		return
	}

	topicName := strings.TrimSpace(req.Topic)
	if topicName == "" {
		http.Error(w, "missing topic", http.StatusBadRequest)
		return
	}

	if s.registry == nil {
		http.Error(w, "group registry is not initialized", http.StatusInternalServerError)
		return
	}

	registeredMembers, err := s.registry.Members(groupName)
	if err != nil {
		http.Error(w, "failed to get group members", http.StatusBadRequest)
		return
	}
	if len(registeredMembers) == 0 {
		http.Error(w, "group has no members", http.StatusBadRequest)
		return
	}

	topicInfo, exists := s.topicManager.GetTopic(topicName)
	if !exists {
		http.Error(w, "topic not found", http.StatusNotFound)
		return
	}

	if s.assigner == nil {
		http.Error(w, "assigner is not initialized", http.StatusInternalServerError)
		return
	}

	members := make([]group.Member, 0, len(registeredMembers))
	for _, member := range registeredMembers {
		members = append(members, group.Member{ID: member.ID})
	}

	assignments, err := s.assigner.Assign(topicName, len(topicInfo.Partitions), members)
	if err != nil {
		http.Error(w, "failed to assign partitions", http.StatusBadRequest)
		return
	}

	if s.assignmentStore == nil {
		http.Error(w, "assignment store is not initialized", http.StatusInternalServerError)
		return
	}
	if err := s.assignmentStore.Save(groupName, topicName, assignments); err != nil {
		http.Error(w, "failed to save assignments", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, groupRebalanceResponse{
		Group:       groupName,
		Assignments: convertGroupAssignments(assignments),
	})
}

func (s *Server) groupCleanupAndRebalanceHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req groupCleanupAndRebalanceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	groupName := strings.TrimSpace(req.Group)
	if groupName == "" {
		http.Error(w, "missing group", http.StatusBadRequest)
		return
	}

	topicName := strings.TrimSpace(req.Topic)
	if topicName == "" {
		http.Error(w, "missing topic", http.StatusBadRequest)
		return
	}

	if req.TimeoutMS <= 0 {
		http.Error(w, "invalid timeout", http.StatusBadRequest)
		return
	}

	topicInfo, exists := s.topicManager.GetTopic(topicName)
	if !exists {
		http.Error(w, "topic not found", http.StatusNotFound)
		return
	}

	if s.registry == nil {
		http.Error(w, "group registry is not initialized", http.StatusInternalServerError)
		return
	}

	timeout := time.Duration(req.TimeoutMS) * time.Millisecond
	removedMembers, err := s.registry.RemoveStaleMembers(groupName, time.Now(), timeout)
	if err != nil {
		http.Error(w, "failed to remove stale group members", http.StatusBadRequest)
		return
	}

	remainingMembers, err := s.registry.Members(groupName)
	if err != nil {
		http.Error(w, "failed to get group members", http.StatusBadRequest)
		return
	}
	if len(remainingMembers) == 0 {
		http.Error(w, "group has no members", http.StatusBadRequest)
		return
	}

	if s.assigner == nil {
		http.Error(w, "assigner is not initialized", http.StatusInternalServerError)
		return
	}

	members := make([]group.Member, 0, len(remainingMembers))
	for _, member := range remainingMembers {
		members = append(members, group.Member{ID: member.ID})
	}

	assignments, err := s.assigner.Assign(topicName, len(topicInfo.Partitions), members)
	if err != nil {
		http.Error(w, "failed to assign partitions", http.StatusBadRequest)
		return
	}

	if s.assignmentStore == nil {
		http.Error(w, "assignment store is not initialized", http.StatusInternalServerError)
		return
	}
	if err := s.assignmentStore.Save(groupName, topicName, assignments); err != nil {
		http.Error(w, "failed to save assignments", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, groupCleanupAndRebalanceResponse{
		Group:          groupName,
		Topic:          topicName,
		TimeoutMS:      req.TimeoutMS,
		RemovedMembers: convertGroupMembers(removedMembers),
		Assignments:    convertGroupAssignments(assignments),
	})
}

func (s *Server) groupAssignmentsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.getGroupAssignmentsHandler(w, r)
	case http.MethodDelete:
		s.deleteGroupAssignmentsHandler(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) getGroupAssignmentsHandler(w http.ResponseWriter, r *http.Request) {
	groupName := strings.TrimSpace(r.URL.Query().Get("group"))
	if groupName == "" {
		http.Error(w, "missing group", http.StatusBadRequest)
		return
	}

	topicName := strings.TrimSpace(r.URL.Query().Get("topic"))
	if topicName == "" {
		http.Error(w, "missing topic", http.StatusBadRequest)
		return
	}

	if s.assignmentStore == nil {
		http.Error(w, "assignment store is not initialized", http.StatusInternalServerError)
		return
	}

	assignments, found, err := s.assignmentStore.Get(groupName, topicName)
	if err != nil {
		http.Error(w, "failed to get assignments", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, groupAssignmentsResponse{
		Group:       groupName,
		Topic:       topicName,
		Found:       found,
		Assignments: convertGroupAssignments(assignments),
	})
}

func (s *Server) deleteGroupAssignmentsHandler(w http.ResponseWriter, r *http.Request) {
	groupName := strings.TrimSpace(r.URL.Query().Get("group"))
	if groupName == "" {
		http.Error(w, "missing group", http.StatusBadRequest)
		return
	}

	topicName := strings.TrimSpace(r.URL.Query().Get("topic"))
	if topicName == "" {
		http.Error(w, "missing topic", http.StatusBadRequest)
		return
	}

	if s.assignmentStore == nil {
		http.Error(w, "assignment store is not initialized", http.StatusInternalServerError)
		return
	}

	if err := s.assignmentStore.Delete(groupName, topicName); err != nil {
		http.Error(w, "failed to delete assignments", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, groupAssignmentDeleteResponse{
		Status: "deleted",
		Group:  groupName,
		Topic:  topicName,
	})
}

func (s *Server) groupJoinHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req groupMembershipRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	groupName, memberID, ok := parseMembershipRequest(req)
	if !ok {
		http.Error(w, "invalid group membership request", http.StatusBadRequest)
		return
	}

	if s.registry == nil {
		http.Error(w, "group registry is not initialized", http.StatusInternalServerError)
		return
	}

	if err := s.registry.Join(groupName, memberID); err != nil {
		http.Error(w, "failed to join group", http.StatusBadRequest)
		return
	}

	writeJSON(w, http.StatusOK, groupMembershipResponse{Status: "joined"})
}

func (s *Server) groupLeaveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req groupMembershipRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	groupName, memberID, ok := parseMembershipRequest(req)
	if !ok {
		http.Error(w, "invalid group membership request", http.StatusBadRequest)
		return
	}

	if s.registry == nil {
		http.Error(w, "group registry is not initialized", http.StatusInternalServerError)
		return
	}

	if err := s.registry.Leave(groupName, memberID); err != nil {
		http.Error(w, "failed to leave group", http.StatusBadRequest)
		return
	}

	writeJSON(w, http.StatusOK, groupMembershipResponse{Status: "left"})
}

func (s *Server) groupHeartbeatHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req groupMembershipRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	groupName, memberID, ok := parseMembershipRequest(req)
	if !ok {
		http.Error(w, "invalid group membership request", http.StatusBadRequest)
		return
	}

	if s.registry == nil {
		http.Error(w, "group registry is not initialized", http.StatusInternalServerError)
		return
	}

	if err := s.registry.Heartbeat(groupName, memberID, time.Now()); err != nil {
		http.Error(w, "failed to record heartbeat", http.StatusBadRequest)
		return
	}

	writeJSON(w, http.StatusOK, groupMembershipResponse{Status: "heartbeat recorded"})
}

func (s *Server) groupMembersHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	groupName := strings.TrimSpace(r.URL.Query().Get("group"))
	if groupName == "" {
		http.Error(w, "missing group", http.StatusBadRequest)
		return
	}

	if s.registry == nil {
		http.Error(w, "group registry is not initialized", http.StatusInternalServerError)
		return
	}

	members, err := s.registry.Members(groupName)
	if err != nil {
		http.Error(w, "failed to get group members", http.StatusBadRequest)
		return
	}

	writeJSON(w, http.StatusOK, groupMembersResponse{
		Group:   groupName,
		Members: convertGroupMembers(members),
	})
}

func (s *Server) groupStaleHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	groupName := strings.TrimSpace(r.URL.Query().Get("group"))
	if groupName == "" {
		http.Error(w, "missing group", http.StatusBadRequest)
		return
	}

	timeoutValue := r.URL.Query().Get("timeout_ms")
	if timeoutValue == "" {
		http.Error(w, "missing timeout", http.StatusBadRequest)
		return
	}

	timeoutMS, err := strconv.ParseInt(timeoutValue, 10, 64)
	if err != nil || timeoutMS <= 0 {
		http.Error(w, "invalid timeout", http.StatusBadRequest)
		return
	}

	if s.registry == nil {
		http.Error(w, "group registry is not initialized", http.StatusInternalServerError)
		return
	}

	timeout := time.Duration(timeoutMS) * time.Millisecond
	members, err := s.registry.StaleMembers(groupName, time.Now(), timeout)
	if err != nil {
		http.Error(w, "failed to get stale group members", http.StatusBadRequest)
		return
	}

	writeJSON(w, http.StatusOK, groupStaleResponse{
		Group:     groupName,
		TimeoutMS: timeoutMS,
		Members:   convertGroupMembers(members),
	})
}

func (s *Server) groupRemoveStaleHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req groupRemoveStaleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	groupName := strings.TrimSpace(req.Group)
	if groupName == "" {
		http.Error(w, "missing group", http.StatusBadRequest)
		return
	}
	if req.TimeoutMS <= 0 {
		http.Error(w, "invalid timeout", http.StatusBadRequest)
		return
	}

	if s.registry == nil {
		http.Error(w, "group registry is not initialized", http.StatusInternalServerError)
		return
	}

	timeout := time.Duration(req.TimeoutMS) * time.Millisecond
	removedMembers, err := s.registry.RemoveStaleMembers(groupName, time.Now(), timeout)
	if err != nil {
		http.Error(w, "failed to remove stale group members", http.StatusBadRequest)
		return
	}

	writeJSON(w, http.StatusOK, groupRemoveStaleResponse{
		Group:          groupName,
		TimeoutMS:      req.TimeoutMS,
		RemovedMembers: convertGroupMembers(removedMembers),
	})
}

func (s *Server) getPartition(topicName string, partitionID int) (topic.Partition, bool) {
	topicInfo, exists := s.topicManager.GetTopic(topicName)
	if !exists {
		return topic.Partition{}, false
	}

	for _, partition := range topicInfo.Partitions {
		if partition.ID == partitionID {
			return partition, true
		}
	}

	return topic.Partition{}, false
}

func parseGroupMembers(reqMembers []groupMemberRequest) ([]group.Member, error) {
	members := make([]group.Member, 0, len(reqMembers))
	seen := make(map[string]struct{}, len(reqMembers))

	for _, reqMember := range reqMembers {
		id := strings.TrimSpace(reqMember.ID)
		if id == "" {
			return nil, fmt.Errorf("member ID cannot be empty")
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("duplicate member ID %q", id)
		}

		seen[id] = struct{}{}
		members = append(members, group.Member{ID: id})
	}

	return members, nil
}

func parseMembershipRequest(req groupMembershipRequest) (string, string, bool) {
	groupName := strings.TrimSpace(req.Group)
	memberID := strings.TrimSpace(req.MemberID)

	return groupName, memberID, groupName != "" && memberID != ""
}

func convertGroupAssignments(assignments []group.Assignment) []groupAssignmentResponse {
	responseAssignments := make([]groupAssignmentResponse, 0, len(assignments))
	for _, assignment := range assignments {
		topics := make([]groupTopicAssignmentResponse, 0, len(assignment.Topics))
		for _, topicAssignment := range assignment.Topics {
			topics = append(topics, groupTopicAssignmentResponse{
				Topic:      topicAssignment.Topic,
				Partitions: topicAssignment.Partitions,
			})
		}

		responseAssignments = append(responseAssignments, groupAssignmentResponse{
			MemberID: assignment.MemberID,
			Topics:   topics,
		})
	}

	return responseAssignments
}

func convertGroupMembers(members []group.GroupMember) []groupMemberResponse {
	responseMembers := make([]groupMemberResponse, 0, len(members))
	for _, member := range members {
		responseMembers = append(responseMembers, groupMemberResponse{
			ID: member.ID,
		})
	}

	return responseMembers
}

func convertRegistryMembers(members []group.GroupMember) []group.Member {
	convertedMembers := make([]group.Member, 0, len(members))
	for _, member := range members {
		convertedMembers = append(convertedMembers, group.Member{ID: member.ID})
	}

	return convertedMembers
}

func writeJSON(w http.ResponseWriter, statusCode int, response interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}
