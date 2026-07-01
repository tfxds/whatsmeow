package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/nextflow/whatsmeow-gateway/internal/api"
	"github.com/nextflow/whatsmeow-gateway/internal/call"
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
	disp := webhook.New()
	mgr := session.NewManager(st, disp)
	calls := call.NewManager()

	// Registra o cliente de chamadas em cada sessão conectada e dispara o webhook
	// IncomingCall — wirado ANTES do RestoreAll pra cobrir sessões restauradas.
	mgr.SetOnConnected(calls.EnsureClient)
	calls.SetOnIncoming(func(connID, callID, from string) {
		conn := mgr.LookupConn(connID)
		if conn == nil || conn.WebhookURL == "" {
			return
		}
		disp.Send(conn.WebhookURL, map[string]any{
			"type": "IncomingCall", "connectionId": connID, "tenantId": conn.TenantID,
			"callId": callID, "from": from,
		})
	})

	// Chamada recebida encerrou (chamador cancelou/desligou) — avisa o NextFlow pra parar
	// de tocar na UI.
	calls.SetOnCallEnded(func(connID, callID string) {
		conn := mgr.LookupConn(connID)
		if conn == nil || conn.WebhookURL == "" {
			return
		}
		disp.Send(conn.WebhookURL, map[string]any{
			"type": "CallEnded", "connectionId": connID, "tenantId": conn.TenantID,
			"callId": callID,
		})
	})

	// Reconnect previously-paired sessions on boot (non-fatal on failure).
	if err := mgr.RestoreAll(context.Background()); err != nil {
		log.Printf("restore sessions: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	restAPI := &api.API{Mgr: mgr, Store: st, Calls: calls, AdminToken: cfg.AdminToken}
	restAPI.Register(mux)

	addr := ":" + cfg.Port
	log.Printf("gateway listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
