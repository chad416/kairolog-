package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	commitlog "kairolog/internal/log"
)

const defaultAddr = ":8080"

type Server struct {
	log *commitlog.Log
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

func New() *http.Server {
	server := &Server{
		log: commitlog.New(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.healthHandler)
	mux.HandleFunc("/produce", server.produceHandler)
	mux.HandleFunc("/messages", server.messagesHandler)

	return &http.Server{
		Addr:    defaultAddr,
		Handler: mux,
	}
}

func Start() error {
	srv := New()

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

	s.log.Append(req.Message)
	writeJSON(w, http.StatusOK, produceResponse{Status: "stored"})
}

func (s *Server) messagesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	writeJSON(w, http.StatusOK, messagesResponse{Messages: s.log.ReadAll()})
}

func writeJSON(w http.ResponseWriter, statusCode int, response interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}
