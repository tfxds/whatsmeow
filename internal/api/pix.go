package api

import (
	"encoding/json"
	"net/http"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// pixRequest é o payload do /chat/send/pix — botão PIX copia-e-cola.
// Reusa a máquina de interativo (botão cta_copy) que o WhatsApp já renderiza.
type pixRequest struct {
	ConnectionID string `json:"connectionId"`
	Phone        string `json:"Phone"`
	PixCode      string `json:"PixCode"`    // código copia-e-cola (BR Code / EMV)
	Body         string `json:"Body"`       // texto da mensagem (ex: "Pague R$ 50,00 do pedido #123")
	Footer       string `json:"Footer"`     // rodapé opcional
	ButtonText   string `json:"ButtonText"` // rótulo do botão (default "Copiar código PIX")
}

// handleSendPix envia uma mensagem interativa com UM botão "copiar" contendo o código
// PIX copia-e-cola. Não usa WhatsApp Pay nativo (que é gated) — o cliente copia o código
// e paga no app do banco dele. Espelha o "Enviar botão PIX" do UAZAPI.
func (a *API) handleSendPix(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req pixRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.Phone == "" || req.PixCode == "" {
		writeError(w, http.StatusBadRequest, "Phone e PixCode são obrigatórios")
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

	buttonText := req.ButtonText
	if buttonText == "" {
		buttonText = "Copiar código PIX"
	}
	body := req.Body
	if body == "" {
		body = "Toque no botão abaixo para copiar o código PIX e concluir o pagamento."
	}

	im := &waE2E.InteractiveMessage{
		Header: &waE2E.InteractiveMessage_Header{},
		Body:   &waE2E.InteractiveMessage_Body{Text: proto.String(body)},
		InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{
			NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
				Buttons: buildNativeButtons([]interactiveButton{
					{Type: "cta_copy", DisplayText: buttonText, Copy: req.PixCode},
				}),
				MessageVersion: proto.Int32(1),
			},
		},
	}
	if req.Footer != "" {
		im.Footer = &waE2E.InteractiveMessage_Footer{Text: proto.String(req.Footer)}
	}

	finalMsg := &waE2E.Message{InteractiveMessage: im}
	resp, err := sess.Client.SendMessage(r.Context(), jid, finalMsg, whatsmeow.SendRequestExtra{AdditionalNodes: nativeFlowNodes()})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "id": resp.ID})
}
