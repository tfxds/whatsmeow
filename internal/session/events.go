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
		if img := wa.GetImageMessage(); img != nil {
			msgMap["imageMessage"] = map[string]any{
				"mimetype":   img.GetMimetype(),
				"caption":    img.GetCaption(),
				"url":        img.GetURL(),
				"mediaKey":   img.GetMediaKey(),
				"directPath": img.GetDirectPath(),
			}
		}
		if vid := wa.GetVideoMessage(); vid != nil {
			msgMap["videoMessage"] = map[string]any{
				"mimetype":   vid.GetMimetype(),
				"caption":    vid.GetCaption(),
				"url":        vid.GetURL(),
				"mediaKey":   vid.GetMediaKey(),
				"directPath": vid.GetDirectPath(),
			}
		}
		if doc := wa.GetDocumentMessage(); doc != nil {
			msgMap["documentMessage"] = map[string]any{
				"mimetype":   doc.GetMimetype(),
				"caption":    doc.GetCaption(),
				"fileName":   doc.GetFileName(),
				"url":        doc.GetURL(),
				"mediaKey":   doc.GetMediaKey(),
				"directPath": doc.GetDirectPath(),
			}
		}
		if aud := wa.GetAudioMessage(); aud != nil {
			msgMap["audioMessage"] = map[string]any{
				"mimetype":   aud.GetMimetype(),
				"url":        aud.GetURL(),
				"mediaKey":   aud.GetMediaKey(),
				"directPath": aud.GetDirectPath(),
			}
		}
		if stk := wa.GetStickerMessage(); stk != nil {
			msgMap["stickerMessage"] = map[string]any{
				"mimetype":   stk.GetMimetype(),
				"url":        stk.GetURL(),
				"mediaKey":   stk.GetMediaKey(),
				"directPath": stk.GetDirectPath(),
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
