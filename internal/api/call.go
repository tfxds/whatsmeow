package api

import (
	"encoding/json"
	"net/http"
	"strings"
)

type callStartRequest struct {
	ConnectionID string `json:"connectionId"`
	Phone        string `json:"Phone"`
}

// handleCallStart coloca uma chamada outbound de áudio (PoC) tocando o tom de teste.
func (a *API) handleCallStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req callStartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	phone := callDigitsOnly(req.Phone)
	if phone == "" {
		writeError(w, http.StatusBadRequest, "Phone is required")
		return
	}
	if _, ok := a.authConn(w, r, req.ConnectionID); !ok {
		return
	}
	sess, ok := a.Mgr.Get(req.ConnectionID)
	if !ok {
		writeError(w, http.StatusNotFound, "session not found")
		return
	}
	callID, err := a.Calls.Start(r.Context(), req.ConnectionID, sess.Client, phone)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "callId": callID})
}

type callHangupRequest struct {
	ConnectionID string `json:"connectionId"`
}

// handleCallHangup encerra a chamada ativa da conexão.
func (a *API) handleCallHangup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req callHangupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if _, ok := a.authConn(w, r, req.ConnectionID); !ok {
		return
	}
	if err := a.Calls.Hangup(req.ConnectionID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

// callDigitsOnly remove tudo que não é dígito.
func callDigitsOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}
