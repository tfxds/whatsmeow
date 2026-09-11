package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/nextflow/whatsmeow-gateway/internal/session"
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
	// GRUPO: responde JÁ e manda em segundo plano.
	//
	// Mandar pra grupo custa 13-15s medidos: antes da mensagem sair, o whatsmeow estabelece
	// sessão de sinal com CADA dispositivo de CADA participante. Conversa individual leva
	// 400ms. Segurar a requisição todo esse tempo trava a tela do atendente sem necessidade.
	//
	// O ID é gerado AQUI e devolvido na hora, então o NextFlow grava a mensagem com o id
	// real do WhatsApp — recibo e reação continuam casando. Se o envio falhar depois, o
	// webhook avisa (Receipt/failed) e a mensagem é marcada como falha na tela.
	if jid.Server == types.GroupServer {
		id := sess.Client.GenerateMessageID()
		go a.enviarGrupoEmSegundoPlano(sess, jid, msg, id)
		writeJSON(w, http.StatusOK, map[string]any{"success": true, "id": id, "queued": true})
		return
	}

	ctx, cancelEnvio := ctxEnvio(r)
	defer cancelEnvio()
	resp, err := sess.Client.SendMessage(ctx, jid, msg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Retorna o ID do WhatsApp pra o NextFlow salvar a msg do agente com o id real
	// (senão reação/recibo na msg do agente não casam — ficam com o UUID interno).
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "id": resp.ID})
}

// enviarGrupoEmSegundoPlano termina o envio depois que a resposta HTTP já foi entregue.
//
// Contexto PRÓPRIO (não o da requisição, que já morreu) com 5 minutos — folga de sobra
// sobre os 15s típicos e sobre o timeout interno de 75s do whatsmeow pra resposta do servidor.
// Em caso de falha avisa o NextFlow por webhook, no mesmo formato de Receipt que o painel
// já entende, com Type "failed" — que tem precedência sobre qualquer outro status.
func (a *API) enviarGrupoEmSegundoPlano(sess *session.Session, jid types.JID, msg *waE2E.Message, id types.MessageID) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	inicio := time.Now()
	_, err := sess.Client.SendMessage(ctx, jid, msg, whatsmeow.SendRequestExtra{ID: id})
	decorrido := time.Since(inicio).Round(time.Millisecond)

	if err != nil {
		fmt.Printf("[SEND-GRUPO] %s id=%s FALHOU apos %s: %v\n", jid, id, decorrido, err)
		a.Mgr.Notify(sess.ConnectionID, map[string]any{
			"type":         "Receipt",
			"Type":         "failed",
			"connectionId": sess.ConnectionID,
			"tenantId":     sess.TenantID,
			"MessageIDs":   []string{string(id)},
			"Chat":         jid.String(),
		})
		return
	}
	fmt.Printf("[SEND-GRUPO] %s id=%s entregue em %s\n", jid, id, decorrido)
}
