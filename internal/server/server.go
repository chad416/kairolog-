package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

const defaultAddr = ":8080"

type healthResponse struct {
	Status string `json:"status"`
}

func New() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)

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

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(healthResponse{Status: "ok"}); err != nil {
		http.Error(w, "failed to encode health response", http.StatusInternalServerError)
	}
}
