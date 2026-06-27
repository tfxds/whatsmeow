package api

import (
	"encoding/json"
	"net/http"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

type editRequest struct {
	ConnectionID string `json:"connectionId"`
	Phone        string `json:"Phone"`
	MessageID    string `json:"MessageID"`
	Body         string `json:"Body"`
}

// handleEdit edita uma mensagem já enviada (BuildEdit + SendMessage).
func (a *API) handleEdit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req editRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.Phone == "" || req.MessageID == "" || req.Body == "" {
		writeError(w, http.StatusBadRequest, "Phone, MessageID and Body are required")
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
	newMsg := &waE2E.Message{Conversation: proto.String(req.Body)}
	edited := sess.Client.BuildEdit(jid, types.MessageID(req.MessageID), newMsg)
	if _, err := sess.Client.SendMessage(r.Context(), jid, edited); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

type deleteRequest struct {
	ConnectionID string `json:"connectionId"`
	Phone        string `json:"Phone"`
	MessageID    string `json:"MessageID"`
}

// handleDelete apaga ("apagar para todos") uma mensagem própria (BuildRevoke + SendMessage).
func (a *API) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req deleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.Phone == "" || req.MessageID == "" {
		writeError(w, http.StatusBadRequest, "Phone and MessageID are required")
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
	// sender vazio = mensagem própria (apagar para todos).
	revoke := sess.Client.BuildRevoke(jid, types.EmptyJID, types.MessageID(req.MessageID))
	if _, err := sess.Client.SendMessage(r.Context(), jid, revoke); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

type reactRequest struct {
	ConnectionID string `json:"connectionId"`
	Phone        string `json:"Phone"`
	MessageID    string `json:"MessageID"`
	Reaction     string `json:"Reaction"` // emoji; "" remove a reação
	FromMe       bool   `json:"FromMe"`   // a mensagem reagida é minha?
}

// handleReact reage a uma mensagem (BuildReaction + SendMessage). Para reagir à própria
// mensagem o sender é o JID da conexão; senão é o JID do contato.
func (a *API) handleReact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req reactRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.Phone == "" || req.MessageID == "" {
		writeError(w, http.StatusBadRequest, "Phone and MessageID are required")
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
	sender := jid
	if req.FromMe && sess.Client.Store.ID != nil {
		sender = sess.Client.Store.ID.ToNonAD()
	}
	react := sess.Client.BuildReaction(jid, sender, types.MessageID(req.MessageID), req.Reaction)
	if _, err := sess.Client.SendMessage(r.Context(), jid, react); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}
