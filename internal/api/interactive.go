package api

import (
	"encoding/json"
	"net/http"

	"github.com/nextflow/whatsmeow-gateway/internal/media"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// Estruturas do payload normalizado que o NextFlow envia (mapeadas do :::INTERACTIVE:::).
type interactiveButton struct {
	Type        string `json:"type"` // quick_reply | cta_url | cta_copy | cta_call
	DisplayText string `json:"displayText"`
	ID          string `json:"id"`
	URL         string `json:"url"`
	Copy        string `json:"copy"`
	Phone       string `json:"phone"`
}

type interactiveRow struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

type interactiveSection struct {
	Title string           `json:"title"`
	Rows  []interactiveRow `json:"rows"`
}

type interactiveCard struct {
	Body     string              `json:"body"`
	Footer   string              `json:"footer"`
	ImageURL string              `json:"imageUrl"`
	Buttons  []interactiveButton `json:"buttons"`
}

type interactiveRequest struct {
	ConnectionID string               `json:"connectionId"`
	Phone        string               `json:"Phone"`
	Type         string               `json:"Type"` // buttons | list | carousel
	Body         string               `json:"Body"`
	Footer       string               `json:"Footer"`
	Header       string               `json:"Header"`
	ImageURL     string               `json:"ImageUrl"`   // header image (buttons)
	ButtonText   string               `json:"ButtonText"` // rótulo do botão "abrir lista"
	Buttons      []interactiveButton  `json:"Buttons"`
	Sections     []interactiveSection `json:"Sections"`
	Cards        []interactiveCard    `json:"Cards"`
}

// buildNativeButtons converte os botões normalizados em NativeFlowButtons (formato
// ButtonParamsJSON exato do WhatsApp, copiado do WuzAPI/asternic).
func buildNativeButtons(btns []interactiveButton) []*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton {
	out := make([]*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton, 0, len(btns))
	for _, b := range btns {
		var name string
		var pm map[string]string
		switch b.Type {
		case "cta_url":
			name = "cta_url"
			pm = map[string]string{"display_text": b.DisplayText, "url": b.URL, "merchant_url": b.URL}
		case "cta_call":
			name = "cta_call"
			pm = map[string]string{"display_text": b.DisplayText, "phone_number": b.Phone}
		case "cta_copy", "copy":
			name = "cta_copy"
			pm = map[string]string{"display_text": b.DisplayText, "copy_code": b.Copy}
		default: // quick_reply
			name = "quick_reply"
			id := b.ID
			if id == "" {
				id = b.DisplayText
			}
			pm = map[string]string{"display_text": b.DisplayText, "id": id}
		}
		paramsJSON, _ := json.Marshal(pm)
		out = append(out, &waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
			Name:             proto.String(name),
			ButtonParamsJSON: proto.String(string(paramsJSON)),
		})
	}
	return out
}

// nativeFlowNodes é o nó "biz" obrigatório pra renderizar botões/carrossel.
func nativeFlowNodes() *[]waBinary.Node {
	return &[]waBinary.Node{{
		Tag: "biz",
		Content: []waBinary.Node{{
			Tag:   "interactive",
			Attrs: waBinary.Attrs{"type": "native_flow", "v": "1"},
			Content: []waBinary.Node{{
				Tag:   "native_flow",
				Attrs: waBinary.Attrs{"v": "9", "name": "mixed"},
			}},
		}},
	}}
}

// handleSendInteractive envia botões, lista ou carrossel.
func (a *API) handleSendInteractive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req interactiveRequest
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
	cli := sess.Client
	ctx := r.Context()

	var finalMsg *waE2E.Message
	var nodes *[]waBinary.Node

	switch req.Type {
	case "list":
		var protoSections []*waE2E.ListMessage_Section
		for _, sec := range req.Sections {
			var rows []*waE2E.ListMessage_Row
			for _, it := range sec.Rows {
				if it.Title == "" {
					continue
				}
				rowID := it.ID
				if rowID == "" {
					rowID = it.Title
				}
				row := &waE2E.ListMessage_Row{RowID: proto.String(rowID), Title: proto.String(it.Title)}
				if it.Description != "" {
					row.Description = proto.String(it.Description)
				}
				rows = append(rows, row)
			}
			if len(rows) == 0 {
				continue
			}
			s := &waE2E.ListMessage_Section{Rows: rows}
			if sec.Title != "" {
				s.Title = proto.String(sec.Title)
			}
			protoSections = append(protoSections, s)
		}
		if len(protoSections) == 0 {
			writeError(w, http.StatusBadRequest, "list sem rows válidas")
			return
		}
		buttonText := req.ButtonText
		if buttonText == "" {
			buttonText = "Ver opções"
		}
		listMsg := &waE2E.ListMessage{
			Description: proto.String(req.Body),
			ButtonText:  proto.String(buttonText),
			ListType:    waE2E.ListMessage_SINGLE_SELECT.Enum(),
			Sections:    protoSections,
		}
		if req.Header != "" {
			listMsg.Title = proto.String(req.Header)
		}
		if req.Footer != "" {
			listMsg.FooterText = proto.String(req.Footer)
		}
		// Wrap correto: DocumentWithCaptionMessage > FutureProofMessage > ListMessage.
		finalMsg = &waE2E.Message{DocumentWithCaptionMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{ListMessage: listMsg},
		}}
		nodes = &[]waBinary.Node{{
			Tag: "biz",
			Content: []waBinary.Node{{
				Tag:   "list",
				Attrs: waBinary.Attrs{"type": "product_list", "v": "2"},
			}},
		}}

	case "carousel":
		cards := make([]*waE2E.InteractiveMessage, 0, len(req.Cards))
		for _, c := range req.Cards {
			card := &waE2E.InteractiveMessage{
				Body: &waE2E.InteractiveMessage_Body{Text: proto.String(c.Body)},
				Header: &waE2E.InteractiveMessage_Header{},
				InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{
					NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
						Buttons:        buildNativeButtons(c.Buttons),
						MessageVersion: proto.Int32(1),
					},
				},
			}
			if c.Footer != "" {
				card.Footer = &waE2E.InteractiveMessage_Footer{Text: proto.String(c.Footer)}
			}
			if c.ImageURL != "" {
				if img, e := media.BuildImageMessage(ctx, cli, c.ImageURL, nil, ""); e == nil {
					card.Header.HasMediaAttachment = proto.Bool(true)
					card.Header.Media = &waE2E.InteractiveMessage_Header_ImageMessage{ImageMessage: img}
				}
			}
			cards = append(cards, card)
		}
		if len(cards) == 0 {
			writeError(w, http.StatusBadRequest, "carousel sem cards")
			return
		}
		im := &waE2E.InteractiveMessage{
			Body: &waE2E.InteractiveMessage_Body{Text: proto.String(req.Body)},
			InteractiveMessage: &waE2E.InteractiveMessage_CarouselMessage_{
				CarouselMessage: &waE2E.InteractiveMessage_CarouselMessage{
					Cards:          cards,
					MessageVersion: proto.Int32(1),
				},
			},
		}
		if req.Footer != "" {
			im.Footer = &waE2E.InteractiveMessage_Footer{Text: proto.String(req.Footer)}
		}
		finalMsg = &waE2E.Message{InteractiveMessage: im}
		nodes = nativeFlowNodes()

	default: // buttons
		if len(req.Buttons) == 0 {
			writeError(w, http.StatusBadRequest, "buttons sem botões")
			return
		}
		im := &waE2E.InteractiveMessage{
			Header: &waE2E.InteractiveMessage_Header{},
			Body:   &waE2E.InteractiveMessage_Body{Text: proto.String(req.Body)},
			InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{
				NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
					Buttons:        buildNativeButtons(req.Buttons),
					MessageVersion: proto.Int32(1),
				},
			},
		}
		if req.Footer != "" {
			im.Footer = &waE2E.InteractiveMessage_Footer{Text: proto.String(req.Footer)}
		}
		if req.ImageURL != "" {
			if img, e := media.BuildImageMessage(ctx, cli, req.ImageURL, nil, ""); e == nil {
				im.Header.HasMediaAttachment = proto.Bool(true)
				im.Header.Media = &waE2E.InteractiveMessage_Header_ImageMessage{ImageMessage: img}
			} else if req.Header != "" {
				im.Header.Title = proto.String(req.Header)
			}
		} else if req.Header != "" {
			im.Header.Title = proto.String(req.Header)
		}
		finalMsg = &waE2E.Message{InteractiveMessage: im}
		nodes = nativeFlowNodes()
	}

	resp, err := cli.SendMessage(ctx, jid, finalMsg, whatsmeow.SendRequestExtra{AdditionalNodes: nodes})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "id": resp.ID})
}
