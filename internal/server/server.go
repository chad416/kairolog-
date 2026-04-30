package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"kairolog/internal/topic"
)

const defaultAddr = ":8080"

type Server struct {
	topicManager *topic.Manager
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

func New() (*http.Server, error) {
	return newServer(topic.NewManager()), nil
}

func newServer(topicManager *topic.Manager) *http.Server {
	server := &Server{
		topicManager: topicManager,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.healthHandler)
	mux.HandleFunc("/topics", server.topicsHandler)
	mux.HandleFunc("/produce", server.produceHandler)
	mux.HandleFunc("/fetch", server.fetchHandler)

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

func (s *Server) listTopicsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
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

func writeJSON(w http.ResponseWriter, statusCode int, response interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}
