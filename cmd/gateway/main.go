package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/nextflow/whatsmeow-gateway/internal/api"
	"github.com/nextflow/whatsmeow-gateway/internal/config"
	"github.com/nextflow/whatsmeow-gateway/internal/session"
	"github.com/nextflow/whatsmeow-gateway/internal/store"
	"github.com/nextflow/whatsmeow-gateway/internal/webhook"
)

func main() {
	cfg := config.Load()

	st, err := store.Open(context.Background(), cfg.PGDSN)
	if err != nil {
		log.Fatalf("store open error: %v", err)
	}
	mgr := session.NewManager(st, webhook.New())

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	restAPI := &api.API{Mgr: mgr, Store: st}
	restAPI.Register(mux)

	addr := ":" + cfg.Port
	log.Printf("gateway listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
