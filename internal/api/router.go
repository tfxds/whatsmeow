package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/nextflow/whatsmeow-gateway/internal/session"
	"github.com/nextflow/whatsmeow-gateway/internal/store"
)

// API holds the dependencies shared by all HTTP handlers.
type API struct {
	Mgr   *session.Manager
	Store *store.Store
}

// Register wires the REST endpoints onto the given mux.
func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("/session/connect", a.handleConnect) // POST {connectionId, tenantId, webhookUrl, token}
	mux.HandleFunc("/session/qr", a.handleQR)           // GET ?connectionId=
	mux.HandleFunc("/session/status", a.handleStatus)   // GET ?connectionId=
	mux.HandleFunc("/chat/send/text", a.handleSendText) // POST {connectionId, Phone, Body}

	// Media (TASK 6): outbound send + inbound download.
	mux.HandleFunc("/chat/send/image", a.handleSendMedia(kindImage))       // POST {connectionId, Phone, Image, Caption}
	mux.HandleFunc("/chat/send/video", a.handleSendMedia(kindVideo))       // POST {connectionId, Phone, Video, Caption}
	mux.HandleFunc("/chat/send/document", a.handleSendMedia(kindDocument)) // POST {connectionId, Phone, Document, Caption, FileName}
	mux.HandleFunc("/chat/send/audio", a.handleSendMedia(kindAudio))       // POST {connectionId, Phone, Audio}
	mux.HandleFunc("/chat/download", a.handleDownload)                     // POST {connectionId, kind, directPath, mediaKey, ...}

	// Utilities (TASK 8).
	mux.HandleFunc("/user/check", a.handleUserCheck)   // POST {connectionId, Phone:[...]}
	mux.HandleFunc("/chat/markread", a.handleMarkRead) // POST {connectionId, Phone, MessageID}
	mux.HandleFunc("/chat/presence", a.handlePresence) // POST {connectionId, Phone, State}
}

// writeJSON encodes v as a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError sends a JSON error body with the given status code.
func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"success": false, "error": msg})
}

// extractToken reads the auth token from the "token" header, falling back to a
// bare or "Bearer "-prefixed Authorization header.
func extractToken(r *http.Request) string {
	if t := r.Header.Get("token"); t != "" {
		return t
	}
	auth := r.Header.Get("Authorization")
	const bearer = "Bearer "
	if len(auth) > len(bearer) && auth[:len(bearer)] == bearer {
		return auth[len(bearer):]
	}
	return auth
}

// authConn validates the request token against the stored token for the given
// connection. It returns the connection on success; otherwise it writes the
// appropriate error response and returns nil.
func (a *API) authConn(w http.ResponseWriter, r *http.Request, connectionID string) (*store.Conn, bool) {
	if connectionID == "" {
		writeError(w, http.StatusBadRequest, "connectionId is required")
		return nil, false
	}
	conn, err := a.findConn(r.Context(), connectionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return nil, false
	}
	if conn == nil {
		writeError(w, http.StatusNotFound, "connection not found")
		return nil, false
	}
	if extractToken(r) != conn.Token {
		writeError(w, http.StatusUnauthorized, "invalid token")
		return nil, false
	}
	return conn, true
}

// findConn looks up a stored connection by ID, returning nil if absent.
func (a *API) findConn(ctx context.Context, connectionID string) (*store.Conn, error) {
	conns, err := a.Store.ListConns(ctx)
	if err != nil {
		return nil, err
	}
	for i := range conns {
		if conns[i].ConnectionID == connectionID {
			return &conns[i], nil
		}
	}
	return nil, nil
}
