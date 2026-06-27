package api

import (
	"encoding/json"
	"net/http"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

type sendTextRequest struct {
	ConnectionID string `json:"connectionId"`
	Phone        string `json:"Phone"`
	Body         string `json:"Body"`
}

// handleSendText sends a plain text WhatsApp message via the named connection.
func (a *API) handleSendText(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req sendTextRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.Phone == "" || req.Body == "" {
		writeError(w, http.StatusBadRequest, "Phone and Body are required")
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

	jid, err := types.ParseJID(req.Phone + "@s.whatsapp.net")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid phone: "+err.Error())
		return
	}

	msg := &waE2E.Message{Conversation: proto.String(req.Body)}
	if _, err := sess.Client.SendMessage(r.Context(), jid, msg); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}
