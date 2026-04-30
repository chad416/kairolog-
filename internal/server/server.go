package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"kairolog/internal/consumer"
	"kairolog/internal/group"
	"kairolog/internal/topic"
)

const defaultAddr = ":8080"
const defaultOffsetStorePath = "data/consumer_offsets.log"

type Server struct {
	topicManager *topic.Manager
	offsetStore  *consumer.OffsetStore
	assigner     *group.Assigner
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

type groupAssignmentResponse struct {
	MemberID string                         `json:"member_id"`
	Topics   []groupTopicAssignmentResponse `json:"topics"`
}

type groupTopicAssignmentResponse struct {
	Topic      string `json:"topic"`
	Partitions []int  `json:"partitions"`
}

func New() (*http.Server, error) {
	offsetStore, err := consumer.NewOffsetStore(defaultOffsetStorePath)
	if err != nil {
		return nil, fmt.Errorf("create offset store: %w", err)
	}

	return newServer(topic.NewManager(), offsetStore), nil
}

func newServer(topicManager *topic.Manager, offsetStores ...*consumer.OffsetStore) *http.Server {
	var offsetStore *consumer.OffsetStore
	if len(offsetStores) > 0 {
		offsetStore = offsetStores[0]
	} else {
		offsetStore, _ = consumer.NewOffsetStore(defaultOffsetStorePath)
	}

	server := &Server{
		topicManager: topicManager,
		offsetStore:  offsetStore,
		assigner:     group.NewAssigner(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.healthHandler)
	mux.HandleFunc("/topics", server.topicsHandler)
	mux.HandleFunc("/produce", server.produceHandler)
	mux.HandleFunc("/fetch", server.fetchHandler)
	mux.HandleFunc("/offsets/commit", server.offsetCommitHandler)
	mux.HandleFunc("/offsets", server.offsetsHandler)
	mux.HandleFunc("/groups/assign", server.groupAssignHandler)

	return &http.Server{
		Addr:    defaultAddr,
		Handler: mux,
	}
}

func Start() error {
	srv, err := New()
	if err != nil {
		return err
	}

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("start broker HTTP server: %w", err)
	}

	return nil
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

func writeJSON(w http.ResponseWriter, statusCode int, response interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}
