package api

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/nextflow/whatsmeow-gateway/internal/asterisk"
	"github.com/nextflow/whatsmeow-gateway/internal/call"
	"github.com/purpshell/meowcaller"
)

// Ponte AudioSocket ↔ chamada (migração NexCall→whatsmeow). O control-plane (o Node do
// NexCall) registra ANTES a intenção pra um UUID (aceitar inbound OU discar outbound); quando
// o Asterisk conecta o AudioSocket com esse UUID, casamos com a chamada e ligamos o áudio.
// ⚠️ Só é usado na instância do NexCall (AudioSocket gateado por env no main) — NÃO afeta o
// gateway de produção do NextFlow (225), que nunca sobe o servidor AudioSocket.

// asIntent = o que fazer quando a conexão AudioSocket com esse UUID chegar.
type asIntent struct {
	kind   string // "accept" (inbound) | "dial" (outbound)
	callID string // accept: a chamada recebida a aceitar
	connID string // dial: a conexão whatsmeow que origina
	phone  string // dial: número destino (só dígitos)
}

var asIntents sync.Map // uuid(string) → *asIntent

// BridgeAudioSocket é o OnCall do servidor AudioSocket: casa o UUID da conexão com a intenção
// registrada e liga o áudio s16le nos dois sentidos via WSPipe (o mesmo pipe do browser).
func (a *API) BridgeAudioSocket(conn *asterisk.Conn) {
	v, ok := asIntents.LoadAndDelete(conn.UUID)
	if !ok {
		conn.Close() // ninguém registrou essa chamada → descarta
		return
	}
	intent := v.(*asIntent)
	ctx := context.Background()

	// downlink: voz do cliente WhatsApp → Asterisk (toca no ramal)
	pipe := call.NewWSPipe(func(s16 []byte) { _ = conn.WriteAudio(s16) })
	noState := func(string) {}

	var mcall *meowcaller.Call
	var err error
	if intent.kind == "accept" {
		mcall, err = a.Calls.AcceptIncoming(intent.callID, pipe, pipe, noState)
	} else {
		sess, ok := a.Mgr.Get(intent.connID)
		if !ok {
			conn.Close()
			_ = pipe.Close()
			return
		}
		mcall, _, err = a.Calls.StartWithPipe(ctx, intent.connID, sess.Client, intent.phone, pipe, pipe, noState)
	}
	if err != nil {
		conn.Close()
		_ = pipe.Close()
		return
	}
	defer func() {
		_ = mcall.Hangup()
		_ = pipe.Close()
	}()

	// uplink: áudio do ramal (Asterisk) → chamada WhatsApp
	for {
		s16 := conn.ReadAudio()
		if s16 == nil { // conexão AudioSocket fechou
			return
		}
		pipe.PushMic(s16)
	}
}

// POST /asterisk/accept {uuid, callId} — quando o AudioSocket <uuid> chegar, aceita a chamada
// recebida <callId>. (inbound: OnIncomingCall webhook → Node origina o AudioSocket com o uuid)
func (a *API) handleAsteriskAccept(w http.ResponseWriter, r *http.Request) {
	var body struct {
		UUID   string `json:"uuid"`
		CallID string `json:"callId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UUID == "" || body.CallID == "" {
		writeError(w, http.StatusBadRequest, "uuid e callId obrigatórios")
		return
	}
	asIntents.Store(body.UUID, &asIntent{kind: "accept", callID: body.CallID})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// POST /asterisk/dial {uuid, connId, phone} — quando o AudioSocket <uuid> chegar, disca <phone>
// pela conexão <connId>. (outbound: ramal disca → dialplan → Node registra + origina AudioSocket)
func (a *API) handleAsteriskDial(w http.ResponseWriter, r *http.Request) {
	var body struct {
		UUID   string `json:"uuid"`
		ConnID string `json:"connId"`
		Phone  string `json:"phone"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UUID == "" || body.ConnID == "" || body.Phone == "" {
		writeError(w, http.StatusBadRequest, "uuid, connId e phone obrigatórios")
		return
	}
	asIntents.Store(body.UUID, &asIntent{kind: "dial", connID: body.ConnID, phone: callDigitsOnly(body.Phone)})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
