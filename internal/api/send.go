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
	QuotedID     string `json:"QuotedID"`     // id da msg citada (responder)
	QuotedFromMe bool   `json:"QuotedFromMe"` // a msg citada é minha?
	QuotedText   string `json:"QuotedText"`   // texto da msg citada (pra renderizar o balão)
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
	// Responder/citar: ContextInfo precisa de StanzaID + Participant + a msg citada.
	// Participant = meu JID se a citada é minha, senão o JID do contato. Usa
	// ExtendedTextMessage porque Conversation não carrega ContextInfo.
	if req.QuotedID != "" {
		participant := jid
		if req.QuotedFromMe && sess.Client.Store.ID != nil {
			participant = sess.Client.Store.ID.ToNonAD()
		}
		msg = &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text: proto.String(req.Body),
			ContextInfo: &waE2E.ContextInfo{
				StanzaID:      proto.String(req.QuotedID),
				Participant:   proto.String(participant.String()),
				QuotedMessage: &waE2E.Message{Conversation: proto.String(req.QuotedText)},
			},
		}}
	}
	resp, err := sess.Client.SendMessage(r.Context(), jid, msg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Retorna o ID do WhatsApp pra o NextFlow salvar a msg do agente com o id real
	// (senão reação/recibo na msg do agente não casam — ficam com o UUID interno).
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "id": resp.ID})
}
