package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/nextflow/whatsmeow-gateway/internal/config"
	"github.com/nextflow/whatsmeow-gateway/internal/store"
)

func main() {
	cfg := config.Load()

	st, err := store.Open(context.Background(), cfg.PGDSN)
	if err != nil {
		log.Fatalf("store open error: %v", err)
	}
	_ = st

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	addr := ":" + cfg.Port
	log.Printf("gateway listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
