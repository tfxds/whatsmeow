package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/nextflow/whatsmeow-gateway/internal/api"
	"github.com/nextflow/whatsmeow-gateway/internal/asterisk"
	"github.com/nextflow/whatsmeow-gateway/internal/call"
	"github.com/nextflow/whatsmeow-gateway/internal/config"
	"github.com/nextflow/whatsmeow-gateway/internal/session"
	"github.com/nextflow/whatsmeow-gateway/internal/store"
	"github.com/nextflow/whatsmeow-gateway/internal/webhook"
	waLog "go.mau.fi/whatsmeow/util/log"
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
	calls.SetOnIncoming(func(connID, callID, from string, isVideo bool) {
		conn := mgr.LookupConn(connID)
		if conn == nil || conn.WebhookURL == "" {
			return
		}
		disp.Send(conn.WebhookURL, map[string]any{
			"type": "IncomingCall", "connectionId": connID, "tenantId": conn.TenantID,
			"callId": callID, "from": from, "isVideo": isVideo,
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

	// Ponte AudioSocket (migração NexCall→whatsmeow) — GATEADO por env: só sobe na instância
	// do NexCall (que seta AUDIOSOCKET_ADDR, ex ":9092"). O gateway de PRODUÇÃO do NextFlow (225)
	// NÃO seta a env → nunca abre o listener. Mantém os dois 100% separados.
	if asAddr := os.Getenv("AUDIOSOCKET_ADDR"); asAddr != "" {
		asSrv := asterisk.NewServer(asAddr, waLog.Stdout("AudioSocket", "INFO", true))
		asSrv.OnCall(restAPI.BridgeAudioSocket)
		go func() {
			if err := asSrv.Start(); err != nil {
				log.Printf("audiosocket server: %v", err)
			}
		}()
	}

	addr := ":" + cfg.Port
	log.Printf("gateway listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
