package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"kairolog/internal/storage"
)

const defaultAddr = ":8080"

type messageStore interface {
	Append(message string) error
	ReadAll() ([]string, error)
	ReadAllRecords() ([]storage.Record, error)
}

type Server struct {
	store messageStore
}

type healthResponse struct {
	Status string `json:"status"`
}

type produceRequest struct {
	Message string `json:"message"`
}

type produceResponse struct {
	Status string `json:"status"`
}

type messagesResponse struct {
	Messages []string `json:"messages"`
}

type fetchRecord struct {
	Offset  int64  `json:"offset"`
	Message string `json:"message"`
}

type fetchResponse struct {
	Records []fetchRecord `json:"records"`
}

func New() (*http.Server, error) {
	store, err := storage.NewFileStore()
	if err != nil {
		return nil, fmt.Errorf("create file store: %w", err)
	}

	return newServer(store), nil
}

func newServer(store messageStore) *http.Server {
	server := &Server{
		store: store,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.healthHandler)
	mux.HandleFunc("/produce", server.produceHandler)
	mux.HandleFunc("/messages", server.messagesHandler)
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
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
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

	if err := s.store.Append(req.Message); err != nil {
		http.Error(w, "failed to store message", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, produceResponse{Status: "stored"})
}

func (s *Server) messagesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	messages, err := s.store.ReadAll()
	if err != nil {
		http.Error(w, "failed to read messages", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, messagesResponse{Messages: messages})
}

func (s *Server) fetchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
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

	records, err := s.store.ReadAllRecords()
	if err != nil {
		http.Error(w, "failed to read records", http.StatusInternalServerError)
		return
	}

	responseRecords := make([]fetchRecord, 0, len(records))
	for _, record := range records {
		if record.Offset >= offset {
			responseRecords = append(responseRecords, fetchRecord{
				Offset:  record.Offset,
				Message: record.Message,
			})
		}
	}

	writeJSON(w, http.StatusOK, fetchResponse{Records: responseRecords})
}

func writeJSON(w http.ResponseWriter, statusCode int, response interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}
