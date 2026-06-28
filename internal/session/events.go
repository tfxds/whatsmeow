package session

import (
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// jidStr renders a JID as its canonical string ("number@server"), or "" if empty.
func jidStr(j types.JID) string {
	if j.IsEmpty() {
		return ""
	}
	return j.String()
}

// normalizeReceipt converte um events.Receipt (entrega/leitura de msg ENVIADA) no
// payload que o webhook do NextFlow espera: {type:"Receipt", Type, MessageIDs}.
// O NextFlow casa MessageIDs com chat_messages.id e atualiza ✓✓/azul. Retorna nil
// pros tipos que não interessam (sender/retry/etc).
func normalizeReceipt(connID, tenantID string, r *events.Receipt) map[string]any {
	var status string
	switch r.Type {
	case types.ReceiptTypeDelivered: // "" → mapeado pra "delivery" no PROVIDER_STATUS_MAP
		status = "delivery"
	case types.ReceiptTypeRead, types.ReceiptTypeReadSelf:
		status = "read"
	case types.ReceiptTypePlayed, types.ReceiptTypePlayedSelf:
		status = "played"
	default:
		return nil
	}
	ids := make([]string, len(r.MessageIDs))
	for i, id := range r.MessageIDs {
		ids[i] = string(id)
	}
	return map[string]any{
		"type":         "Receipt",
		"Type":         status,
		"connectionId": connID,
		"tenantId":     tenantID,
		"MessageIDs":   ids,
		"Chat":         jidStr(r.Chat),
	}
}

// normalizeMessage converts a whatsmeow events.Message into the WuzAPI-style
// webhook payload that NextFlow's wuzapiRoutes.js parser expects. The parser
// reads `body.Info` (capitalized fields) and `body.Message` (protojson-style
// camelCase message subtypes).
func normalizeMessage(connID, tenantID string, m *events.Message) map[string]any {
	info := m.Info

	infoMap := map[string]any{
		"ID":             info.ID,
		"IsFromMe":       info.IsFromMe,
		"IsGroup":        info.IsGroup,
		"Sender":         jidStr(info.Sender),
		"SenderAlt":      jidStr(info.SenderAlt),
		"Recipient":      jidStr(info.Chat),
		"RecipientAlt":   jidStr(info.RecipientAlt),
		"Participant":    jidStr(info.Sender),
		"Chat":           jidStr(info.Chat),
		"RemoteJid":      jidStr(info.Chat),
		"PushName":       info.PushName,
		"AddressingMode": string(info.AddressingMode),
		"Timestamp":      info.Timestamp.UTC().Format(time.RFC3339),
	}

	msgMap := map[string]any{}
	if wa := m.Message; wa != nil {
		if txt := wa.GetConversation(); txt != "" {
			msgMap["conversation"] = txt
		}
		if ext := wa.GetExtendedTextMessage(); ext != nil {
			msgMap["extendedTextMessage"] = map[string]any{"text": ext.GetText()}
		}
		if react := wa.GetReactionMessage(); react != nil {
			// Reação do cliente: o webhook do NextFlow lê reactionMessage.key.id + .text
			// e atualiza chat_messages.reaction. text vazio = reação removida.
			k := react.GetKey()
			msgMap["reactionMessage"] = map[string]any{
				"text": react.GetText(),
				"key": map[string]any{
					"id":        k.GetID(),
					"ID":        k.GetID(),
					"fromMe":    k.GetFromMe(),
					"remoteJid": k.GetRemoteJID(),
				},
			}
		}
		if img := wa.GetImageMessage(); img != nil {
			msgMap["imageMessage"] = map[string]any{
				"mimetype":      img.GetMimetype(),
				"caption":       img.GetCaption(),
				"url":           img.GetURL(),
				"mediaKey":      img.GetMediaKey(),
				"directPath":    img.GetDirectPath(),
				"fileEncSHA256": img.GetFileEncSHA256(),
				"fileSHA256":    img.GetFileSHA256(),
				"fileLength":    img.GetFileLength(),
			}
		}
		if vid := wa.GetVideoMessage(); vid != nil {
			msgMap["videoMessage"] = map[string]any{
				"mimetype":      vid.GetMimetype(),
				"caption":       vid.GetCaption(),
				"url":           vid.GetURL(),
				"mediaKey":      vid.GetMediaKey(),
				"directPath":    vid.GetDirectPath(),
				"fileEncSHA256": vid.GetFileEncSHA256(),
				"fileSHA256":    vid.GetFileSHA256(),
				"fileLength":    vid.GetFileLength(),
			}
		}
		if doc := wa.GetDocumentMessage(); doc != nil {
			msgMap["documentMessage"] = map[string]any{
				"mimetype":      doc.GetMimetype(),
				"caption":       doc.GetCaption(),
				"fileName":      doc.GetFileName(),
				"url":           doc.GetURL(),
				"mediaKey":      doc.GetMediaKey(),
				"directPath":    doc.GetDirectPath(),
				"fileEncSHA256": doc.GetFileEncSHA256(),
				"fileSHA256":    doc.GetFileSHA256(),
				"fileLength":    doc.GetFileLength(),
			}
		}
		if aud := wa.GetAudioMessage(); aud != nil {
			msgMap["audioMessage"] = map[string]any{
				"mimetype":      aud.GetMimetype(),
				"url":           aud.GetURL(),
				"mediaKey":      aud.GetMediaKey(),
				"directPath":    aud.GetDirectPath(),
				"fileEncSHA256": aud.GetFileEncSHA256(),
				"fileSHA256":    aud.GetFileSHA256(),
				"fileLength":    aud.GetFileLength(),
				"ptt":           aud.GetPTT(),
			}
		}
		if stk := wa.GetStickerMessage(); stk != nil {
			msgMap["stickerMessage"] = map[string]any{
				"mimetype":      stk.GetMimetype(),
				"url":           stk.GetURL(),
				"mediaKey":      stk.GetMediaKey(),
				"directPath":    stk.GetDirectPath(),
				"fileEncSHA256": stk.GetFileEncSHA256(),
				"fileSHA256":    stk.GetFileSHA256(),
				"fileLength":    stk.GetFileLength(),
			}
		}
	}

	return map[string]any{
		"type":         "Message",
		"connectionId": connID,
		"tenantId":     tenantID,
		"Info":         infoMap,
		"Message":      msgMap,
	}
}
