package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

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
		log.Printf("[bridge] AudioSocket UUID=%s SEM intent registrada → descartando", conn.UUID)
		conn.Close() // ninguém registrou essa chamada → descarta
		return
	}
	intent := v.(*asIntent)
	log.Printf("[bridge] UUID=%s intent=%s connId=%s phone=%s", conn.UUID, intent.kind, intent.connID, intent.phone)
	ctx := context.Background()

	// downlink: voz do cliente WhatsApp (16k, frames de 60ms) → Asterisk (8k, 20ms).
	// O app_audiosocket do Asterisk LÊ frames continuamente e ERRA ("Failed to receive frame")
	// se o socket ficar sem dados — ex: durante o accept (~0,5s antes do áudio real do WhatsApp),
	// o que DERRUBAVA a perna SIP. Solução: o meowcaller enfileira chunks de 20ms e um PUMP manda
	// 20ms A CADA 20ms SEMPRE (chunk real quando tem na fila, silêncio quando não), mantendo a
	// perna SIP viva desde o instante em que o AudioSocket conecta.
	const chunk8k = 320 // 20ms de slin 8k
	downQ := make(chan []byte, 64)
	var downOnce sync.Once
	pipe := call.NewWSPipe(func(s16 []byte) {
		down := asterisk.Down16to8(s16)
		downOnce.Do(func() {
			log.Printf("[bridge] 1º frame downlink: meowcaller=%d bytes → asterisk=%d bytes (fatiado 20ms)", len(s16), len(down))
		})
		for off := 0; off < len(down); off += chunk8k {
			end := off + chunk8k
			if end > len(down) {
				end = len(down)
			}
			c := make([]byte, end-off)
			copy(c, down[off:end])
			select {
			case downQ <- c:
			default: // fila cheia → descarta (não deixa acumular atraso)
			}
		}
	})

	// PUMP do downlink: 20ms a cada 20ms (real ou silêncio), começa já — mantém o app_audiosocket
	// do Asterisk alimentado mesmo antes/durante o accept (senão ele erra e derruba o ramal).
	pumpStop := make(chan struct{})
	go func() {
		silence := make([]byte, chunk8k)
		// PRIMING: manda silêncio JÁ (o app_audiosocket do Asterisk lê o socket logo após o UUID
		// e falha "Failed to read data" se ainda não houver dado — foi o que derrubava a perna SIP).
		for i := 0; i < 5; i++ {
			if err := conn.WriteAudio(silence); err != nil {
				return
			}
		}
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-pumpStop:
				return
			case <-tick.C:
				var frame []byte
				select {
				case frame = <-downQ:
				default:
					frame = silence
				}
				if err := conn.WriteAudio(frame); err != nil {
					return
				}
			}
		}
	}()
	defer close(pumpStop)
	noState := func(string) {}

	var mcall *meowcaller.Call
	var err error
	if intent.kind == "accept" {
		mcall, err = a.Calls.AcceptIncoming(intent.callID, pipe, pipe, noState)
	} else {
		sess, ok := a.Mgr.Get(intent.connID)
		if !ok {
			log.Printf("[bridge] sessão connId=%s não encontrada → abortando", intent.connID)
			conn.Close()
			_ = pipe.Close()
			return
		}
		mcall, _, err = a.Calls.StartWithPipe(ctx, intent.connID, sess.Client, intent.phone, pipe, pipe, noState)
	}
	if err != nil {
		log.Printf("[bridge] falha ao iniciar chamada (%s): %v", intent.kind, err)
		conn.Close()
		_ = pipe.Close()
		return
	}
	log.Printf("[bridge] chamada %s iniciada — ponte de áudio ativa (uplink 8k→16k / downlink 16k→8k)", intent.kind)
	defer func() {
		_ = mcall.Hangup()
		_ = pipe.Close()
	}()

	// uplink: áudio do ramal (Asterisk 8k) → chamada WhatsApp (16k)
	first := true
	for {
		s16 := conn.ReadAudio()
		if s16 == nil { // conexão AudioSocket fechou
			log.Printf("[bridge] AudioSocket fechou (uplink) UUID=%s", conn.UUID)
			return
		}
		if first {
			log.Printf("[bridge] 1º frame do Asterisk = %d bytes (8k=320 / 16k=640)", len(s16))
			first = false
		}
		pipe.PushMic(asterisk.Up8to16(s16))
	}
}

// /asterisk/accept {uuid, callId} — quando o AudioSocket <uuid> chegar, aceita a chamada
// recebida <callId>. Aceita POST (JSON) OU GET (?uuid=&callId=) pra facilitar o CURL do dialplan.
func (a *API) handleAsteriskAccept(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	uuid, callID := q.Get("uuid"), q.Get("callId")
	if d := q.Get("d"); d != "" { // d=uuid:callId (mesmo motivo do dial: evita '&'/vírgula no CURL)
		if parts := strings.Split(d, ":"); len(parts) == 2 {
			uuid, callID = parts[0], parts[1]
		}
	}
	if r.Method == http.MethodPost && uuid == "" {
		var body struct {
			UUID   string `json:"uuid"`
			CallID string `json:"callId"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		uuid, callID = body.UUID, body.CallID
	}
	if uuid == "" || callID == "" {
		writeError(w, http.StatusBadRequest, "uuid e callId obrigatórios")
		return
	}
	asIntents.Store(uuid, &asIntent{kind: "accept", callID: callID})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// /asterisk/dial {uuid, connId, phone} — quando o AudioSocket <uuid> chegar, disca <phone>
// pela conexão <connId>. Aceita POST (JSON) OU GET (?uuid=&connId=&phone=) pro CURL do dialplan.
func (a *API) handleAsteriskDial(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	uuid, connID, phone := q.Get("uuid"), q.Get("connId"), q.Get("phone")
	// Formato single-param d=uuid:connId:phone — o Asterisk trunca a URL do CURL no 2º '&'
	// e mata vírgulas, então o dialplan manda tudo num param só separado por ':'.
	if d := q.Get("d"); d != "" {
		if parts := strings.Split(d, ":"); len(parts) == 3 {
			uuid, connID, phone = parts[0], parts[1], parts[2]
		}
	}
	if r.Method == http.MethodPost && uuid == "" {
		var body struct {
			UUID   string `json:"uuid"`
			ConnID string `json:"connId"`
			Phone  string `json:"phone"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		uuid, connID, phone = body.UUID, body.ConnID, body.Phone
	}
	if uuid == "" || connID == "" || phone == "" {
		writeError(w, http.StatusBadRequest, "uuid, connId e phone obrigatórios")
		return
	}
	asIntents.Store(uuid, &asIntent{kind: "dial", connID: connID, phone: callDigitsOnly(phone)})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
