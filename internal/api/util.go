package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// userAvatarRequest mirrors WuzAPI's /user/avatar body.
type userAvatarRequest struct {
	ConnectionID string `json:"connectionId"`
	Phone        string `json:"Phone"`
	Preview      bool   `json:"Preview"`
}

// handleUserAvatar retorna a URL da foto de perfil de um número/grupo (ou "" se não
// tiver/privado). NextFlow baixa e cacheia. GetProfilePictureInfo(Preview=thumbnail).
func (a *API) handleUserAvatar(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req userAvatarRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.Phone == "" {
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
	target := req.Phone
	if !strings.Contains(target, "@") {
		target += "@s.whatsapp.net"
	}
	jid, err := types.ParseJID(target)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid phone: "+err.Error())
		return
	}
	info, err := sess.Client.GetProfilePictureInfo(r.Context(), jid, &whatsmeow.GetProfilePictureParams{Preview: req.Preview})
	if err != nil || info == nil {
		// sem foto / privacidade / erro → vazio (não é falha fatal)
		writeJSON(w, http.StatusOK, map[string]any{"success": true, "URL": ""})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "URL": info.URL})
}

// userCheckRequest mirrors WuzAPI's /user/check body.
type userCheckRequest struct {
	ConnectionID string   `json:"connectionId"`
	Phone        []string `json:"Phone"`
}

// handleUserCheck resolves which of the given phone numbers are on WhatsApp,
// returning the WuzAPI-compatible {data:{Users:[{JID, IsInWhatsapp, Query}]}}.
func (a *API) handleUserCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req userCheckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if len(req.Phone) == 0 {
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

	// IsOnWhatsApp expects international format with a leading "+".
	phones := make([]string, len(req.Phone))
	for i, p := range req.Phone {
		p = strings.TrimSpace(p)
		if p != "" && !strings.HasPrefix(p, "+") {
			p = "+" + p
		}
		phones[i] = p
	}

	res, err := sess.Client.IsOnWhatsApp(r.Context(), phones)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	users := make([]map[string]any, 0, len(res))
	for _, u := range res {
		users = append(users, map[string]any{
			"Query":        u.Query,
			"JID":          u.JID.String(),
			"IsInWhatsapp": u.IsIn,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    map[string]any{"Users": users},
	})
}

// markReadRequest mirrors the fields needed to send a read receipt.
type markReadRequest struct {
	ConnectionID string   `json:"connectionId"`
	Phone        string   `json:"Phone"`
	MessageID    string   `json:"MessageID"`
	Id           []string `json:"Id"` // optional WuzAPI-style multi-id field
}

// handleMarkRead sends a read receipt for the given message(s).
func (a *API) handleMarkRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req markReadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.Phone == "" {
		writeError(w, http.StatusBadRequest, "Phone is required")
		return
	}
	ids := req.Id
	if len(ids) == 0 && req.MessageID != "" {
		ids = []string{req.MessageID}
	}
	if len(ids) == 0 {
		writeError(w, http.StatusBadRequest, "MessageID is required")
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

	chat, err := types.ParseJID(req.Phone + "@s.whatsapp.net")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid phone: "+err.Error())
		return
	}

	msgIDs := make([]types.MessageID, len(ids))
	for i, id := range ids {
		msgIDs[i] = types.MessageID(id)
	}

	// In a DM the sender equals the chat JID; the empty JID is also accepted.
	if err := sess.Client.MarkRead(r.Context(), msgIDs, time.Now(), chat, chat); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

// presenceRequest mirrors WuzAPI's /chat/presence body.
type presenceRequest struct {
	ConnectionID string `json:"connectionId"`
	Phone        string `json:"Phone"`
	State        string `json:"State"` // composing | paused
	Media        string `json:"Media"` // optional: "audio"
}

// handlePresence sends a typing/recording presence update to a chat.
func (a *API) handlePresence(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req presenceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.Phone == "" {
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

	jid, err := types.ParseJID(req.Phone + "@s.whatsapp.net")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid phone: "+err.Error())
		return
	}

	state := types.ChatPresenceComposing
	if strings.EqualFold(req.State, "paused") {
		state = types.ChatPresencePaused
	}
	media := types.ChatPresenceMediaText
	if strings.EqualFold(req.Media, "audio") {
		media = types.ChatPresenceMediaAudio
	}

	if err := sess.Client.SendChatPresence(r.Context(), jid, state, media); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}
