package api

import (
	"encoding/json"
	"net/http"

	"github.com/nextflow/whatsmeow-gateway/internal/store"
)

type connectRequest struct {
	ConnectionID string `json:"connectionId"`
	TenantID     string `json:"tenantId"`
	WebhookURL   string `json:"webhookUrl"`
	Token        string `json:"token"`
}

// handleConnect persists the connection and starts (or returns) its session.
func (a *API) handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req connectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.ConnectionID == "" || req.TenantID == "" || req.Token == "" {
		writeError(w, http.StatusBadRequest, "connectionId, tenantId and token are required")
		return
	}

	ctx := r.Context()
	if err := a.Store.UpsertConn(ctx, store.Conn{
		ConnectionID: req.ConnectionID,
		TenantID:     req.TenantID,
		WebhookURL:   req.WebhookURL,
		Token:        req.Token,
	}); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if _, err := a.Mgr.Connect(ctx, req.ConnectionID, req.TenantID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	qr, connected, _ := a.Mgr.Status(req.ConnectionID)
	status := "qr"
	if connected {
		status = "connected"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"status":  status,
		"qr":      qr,
	})
}

type pairCodeRequest struct {
	ConnectionID string `json:"connectionId"`
	TenantID     string `json:"tenantId"`
	WebhookURL   string `json:"webhookUrl"`
	Token        string `json:"token"`
	Phone        string `json:"phone"`
}

// handlePairCode inicia o pareamento por CÓDIGO (alternativa ao QR). Persiste a conexão e
// retorna o código de 8 chars que o usuário digita em "Conectar com número de telefone".
func (a *API) handlePairCode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req pairCodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.ConnectionID == "" || req.TenantID == "" || req.Token == "" || req.Phone == "" {
		writeError(w, http.StatusBadRequest, "connectionId, tenantId, token and phone are required")
		return
	}
	if err := a.Store.UpsertConn(r.Context(), store.Conn{
		ConnectionID: req.ConnectionID,
		TenantID:     req.TenantID,
		WebhookURL:   req.WebhookURL,
		Token:        req.Token,
	}); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	code, err := a.Mgr.PairCode(req.ConnectionID, req.TenantID, req.Phone)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "code": code})
}

// handleQR returns the latest QR code and connection state for a connection.
func (a *API) handleQR(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	connectionID := r.URL.Query().Get("connectionId")
	if _, ok := a.authConn(w, r, connectionID); !ok {
		return
	}
	qr, connected, _ := a.Mgr.Status(connectionID)
	writeJSON(w, http.StatusOK, map[string]any{
		"qr":        qr,
		"connected": connected,
	})
}

// handleStatus reports whether a connection is live and known to the manager.
func (a *API) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	connectionID := r.URL.Query().Get("connectionId")
	if _, ok := a.authConn(w, r, connectionID); !ok {
		return
	}
	_, connected, found := a.Mgr.Status(connectionID)
	writeJSON(w, http.StatusOK, map[string]any{
		"connected": connected,
		"found":     found,
	})
}
