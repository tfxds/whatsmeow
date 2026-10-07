package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/nextflow/whatsmeow-gateway/internal/media"
)

// sendStatusRequest — payload do POST /chat/send/status.
// Envia um STATUS (story) pra status@broadcast; o whatsmeow resolve os
// destinatários sozinho (respeita a privacidade de status/todos os contatos).
type sendStatusRequest struct {
	ConnectionID    string `json:"connectionId"`
	Type            string `json:"Type"` // text|image|video|audio
	Text            string `json:"Text"`
	Caption         string `json:"Caption"`
	File            string `json:"File"` // URL da mídia
	Mimetype        string `json:"Mimetype"`
	BackgroundColor int    `json:"BackgroundColor"` // índice 1-19 (status de texto)
	Font            int    `json:"Font"`            // 0,1,2,6,7,8,9,10
	// Audiência explícita (igual "recipients"/"max_recipients" da uazapi). Vazio = contatos
	// salvos do aparelho (regra da lib). Números são conferidos no WhatsApp; os que não
	// existem voltam em "discarded".
	Recipients    []string `json:"Recipients"`
	MaxRecipients int      `json:"MaxRecipients"`
}

// Índice de cor UAZAPI (1-19) → ARGB (alpha 0xFF). Mantém paridade visual com BTZap/Evolution.
var statusBgArgb = map[int]uint32{
	1: 0xFFDCE775, 2: 0xFFFFF59D, 3: 0xFFFFB74D, 4: 0xFF66BB6A, 5: 0xFF7CB342, 6: 0xFFAED581,
	7: 0xFF26C6DA, 8: 0xFF1E88E5, 9: 0xFF90A4AE, 10: 0xFFB39DDB, 11: 0xFFBA68C8, 12: 0xFF7E57C2,
	13: 0xFFC2185B, 14: 0xFFF48FB1, 15: 0xFFFF8A65, 16: 0xFFA1887F, 17: 0xFFB0BEC5, 18: 0xFF78909C, 19: 0xFF455A64,
}

func (a *API) handleSendStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req sendStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
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

	ctx, cancelEnvio := ctxEnvio(r)
	defer cancelEnvio()

	var discarded []map[string]string
	audiencia := 0
	if len(req.Recipients) > 0 {
		vistos := map[string]bool{}
		var phones []string
		for _, n := range req.Recipients {
			d := strings.Map(func(c rune) rune {
				if c >= '0' && c <= '9' {
					return c
				}
				return -1
			}, strings.Split(n, "@")[0])
			if len(d) < 8 {
				discarded = append(discarded, map[string]string{"number": n, "reason": "invalid_format"})
				continue
			}
			if vistos[d] {
				discarded = append(discarded, map[string]string{"number": n, "reason": "duplicate"})
				continue
			}
			vistos[d] = true
			phones = append(phones, "+"+d)
		}
		var jids []types.JID
		for i := 0; i < len(phones); i += 50 { // lote: uma consulta por 50 números
			fim := i + 50
			if fim > len(phones) {
				fim = len(phones)
			}
			res, err := sess.Client.IsOnWhatsApp(ctx, phones[i:fim])
			if err != nil {
				writeError(w, http.StatusBadGateway, "falha ao conferir números: "+err.Error())
				return
			}
			for _, x := range res {
				if x.IsIn {
					jids = append(jids, x.JID)
				} else {
					discarded = append(discarded, map[string]string{"number": x.Query, "reason": "not_on_whatsapp"})
				}
			}
		}
		if req.MaxRecipients > 0 && len(jids) > req.MaxRecipients {
			jids = jids[:req.MaxRecipients]
		}
		if len(jids) == 0 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "nenhum destinatário válido", "discarded": discarded})
			return
		}
		audiencia = len(jids)
		ctx = whatsmeow.WithStatusRecipients(ctx, jids)
	}
	to := types.StatusBroadcastJID // status@broadcast — whatsmeow resolve a audiência
	caption := req.Caption
	if caption == "" {
		caption = req.Text
	}

	var id string
	var err error

	switch strings.ToLower(req.Type) {
	case "text":
		if req.Text == "" {
			writeError(w, http.StatusBadRequest, "Text required for text status")
			return
		}
		et := &waE2E.ExtendedTextMessage{Text: proto.String(req.Text)}
		if argb, has := statusBgArgb[req.BackgroundColor]; has {
			et.BackgroundArgb = proto.Uint32(argb)
			et.TextArgb = proto.Uint32(0xFFFFFFFF)
		}
		font := waE2E.ExtendedTextMessage_FontType(req.Font)
		et.Font = font.Enum()
		resp, e := sess.Client.SendMessage(ctx, to, &waE2E.Message{ExtendedTextMessage: et})
		id, err = resp.ID, e
	case "image":
		if req.File == "" {
			writeError(w, http.StatusBadRequest, "File required for media status")
			return
		}
		id, err = media.SendImage(ctx, sess.Client, to, req.File, nil, req.Mimetype, caption)
	case "video":
		if req.File == "" {
			writeError(w, http.StatusBadRequest, "File required for media status")
			return
		}
		id, err = media.SendVideo(ctx, sess.Client, to, req.File, nil, req.Mimetype, caption)
	case "audio", "myaudio", "ptt":
		if req.File == "" {
			writeError(w, http.StatusBadRequest, "File required for media status")
			return
		}
		id, err = media.SendAudio(ctx, sess.Client, to, req.File, nil, req.Mimetype)
	default:
		writeError(w, http.StatusBadRequest, "unsupported status type: "+req.Type)
		return
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "id": id, "recipients": audiencia, "discarded": discarded})
}
