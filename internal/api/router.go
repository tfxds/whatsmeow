package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/nextflow/whatsmeow-gateway/internal/call"
	"github.com/nextflow/whatsmeow-gateway/internal/session"
	"github.com/nextflow/whatsmeow-gateway/internal/store"
)

// API holds the dependencies shared by all HTTP handlers.
type API struct {
	Mgr        *session.Manager
	Store      *store.Store
	Calls      *call.Manager
	AdminToken string
}

// Register wires the REST endpoints onto the given mux.
func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("/session/connect", a.handleConnect) // POST {connectionId, tenantId, webhookUrl, token}
	mux.HandleFunc("/session/pair-code", a.handlePairCode)
	mux.HandleFunc("/session/qr", a.handleQR)           // GET ?connectionId=
	mux.HandleFunc("/session/status", a.handleStatus)   // GET ?connectionId=
	mux.HandleFunc("/chat/send/text", a.handleSendText) // POST {connectionId, Phone, Body}

	// Media (TASK 6): outbound send + inbound download.
	mux.HandleFunc("/chat/send/image", a.handleSendMedia(kindImage))       // POST {connectionId, Phone, Image, Caption}
	mux.HandleFunc("/chat/send/video", a.handleSendMedia(kindVideo))       // POST {connectionId, Phone, Video, Caption}
	mux.HandleFunc("/chat/send/document", a.handleSendMedia(kindDocument)) // POST {connectionId, Phone, Document, Caption, FileName}
	mux.HandleFunc("/chat/send/audio", a.handleSendMedia(kindAudio))       // POST {connectionId, Phone, Audio}
	mux.HandleFunc("/chat/send/status", a.handleSendStatus)                // POST {connectionId, Type, Text|File, BackgroundColor, Font} → status@broadcast
	mux.HandleFunc("/chat/download", a.handleDownload)                     // POST {connectionId, kind, directPath, mediaKey, ...}

	// Utilities (TASK 8).
	mux.HandleFunc("/user/check", a.handleUserCheck)   // POST {connectionId, Phone:[...]}
	mux.HandleFunc("/user/avatar", a.handleUserAvatar) // POST {connectionId, Phone, Preview} → {URL}
	mux.HandleFunc("/chat/markread", a.handleMarkRead) // POST {connectionId, Phone, MessageID}
	mux.HandleFunc("/chat/presence", a.handlePresence) // POST {connectionId, Phone, State}
	mux.HandleFunc("/chat/edit", a.handleEdit)         // POST {connectionId, Phone, MessageID, Body}
	mux.HandleFunc("/chat/delete", a.handleDelete)     // POST {connectionId, Phone, MessageID}
	mux.HandleFunc("/chat/react", a.handleReact)       // POST {connectionId, Phone, MessageID, Reaction, FromMe}
	mux.HandleFunc("/chat/send/interactive", a.handleSendInteractive) // POST {connectionId, Phone, Type, Body, Buttons|Sections|Cards}
	mux.HandleFunc("/chat/send/pix", a.handleSendPix)                 // POST {connectionId, Phone, PixCode, Body, Footer, ButtonText} — botão copia-e-cola

	// Call (PoC): outbound áudio + hangup.
	mux.HandleFunc("/call/start", a.handleCallStart)   // POST {connectionId, Phone}
	mux.HandleFunc("/call/hangup", a.handleCallHangup) // POST {connectionId}
	mux.HandleFunc("/call/hold", a.handleCallHold)     // POST {connectionId, callId} — transferência: segura a call
	mux.HandleFunc("/call/ws", a.handleCallWS)             // WS áudio {connectionId,phone,token}
	mux.HandleFunc("/call/video-ws", a.handleVideoWS)      // WS vídeo {connectionId,callId,token}
	mux.HandleFunc("/call/reject", a.handleCallReject)     // POST {connectionId, callId}
	mux.HandleFunc("/call/testpage", a.handleCallTestPage) // GET HTML de teste

	// Admin (painel de instâncias): listar/remover devices pareados. Gated por GW_ADMIN_TOKEN.
	mux.HandleFunc("/admin/sessions", a.handleAdminSessions) // GET lista | DELETE ?jid= remove

	// Sync/manutenção (equivalente aos botões dos outros providers)
	mux.HandleFunc("/session/restart", a.handleRestart)   // POST {connectionId} — reconecta
	mux.HandleFunc("/contacts/list", a.handleContacts)    // POST {connectionId} — lista contatos
	mux.HandleFunc("/groups/list", a.handleGroups)        // POST {connectionId} — lista grupos
	mux.HandleFunc("/groups/participants", a.handleGroupParticipants) // POST {connectionId, jid} — membros do grupo

	// NexCall (migração whatsmeow): ponte AudioSocket. Registra a intenção do UUID ANTES do
	// Asterisk conectar. Só útil na instância do NexCall (AudioSocket gateado por env no main);
	// no gateway do NextFlow (225) essas rotas existem mas ninguém usa (sem Asterisk/AudioSocket).
	mux.HandleFunc("/asterisk/accept", a.handleAsteriskAccept) // POST {uuid, callId} — aceita inbound
	mux.HandleFunc("/asterisk/dial", a.handleAsteriskDial)     // POST {uuid, connId, phone} — disca outbound
}

// writeJSON encodes v as a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError sends a JSON error body with the given status code.
func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"success": false, "error": msg})
}

// ctxEnvio devolve o contexto para operações que MANDAM mensagem.
//
// ⛔ NÃO usar r.Context() em envio. Ele é cancelado no instante em que o cliente HTTP
// desconecta, e aí o SendMessage aborta NO MEIO da criptografia — o que já saiu, saiu.
//
// Isso morde com força em GRUPO: antes da mensagem sair, o whatsmeow precisa estabelecer
// sessão de sinal com CADA dispositivo de CADA participante, e dispositivo que responde
// 406 no pedido de prekey faz ele tentar de novo. Um envio assim passa de 30s
// tranquilamente. Se a conexão cair nesse meio tempo, o log mostra exatamente isto
// (caso real, grupo "Elite do SaaS", 11/09/2026 11:30-11:31):
//
//	Failed to fetch prekeys ... info query returned status 406: not-acceptable
//	Failed to encrypt ... : failed to check if identity is trusted: context canceled
//	Server returned different participant list hash ... Some devices may not have
//	received the message.
//
// Ou seja: mensagem entregue PELA METADE, parte do grupo recebe e parte não, e do lado
// do NextFlow aparece só "socket hang up". Desamarrar o envio do ciclo de vida da
// requisição faz ele terminar o trabalho mesmo que ninguém esteja mais ouvindo a resposta.
//
// WithoutCancel preserva os valores do contexto e descarta só o cancelamento.
func ctxEnvio(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(r.Context()), 3*time.Minute)
}

// extractToken reads the auth token from the "token" header, falling back to a
// bare or "Bearer "-prefixed Authorization header.
func extractToken(r *http.Request) string {
	if t := r.Header.Get("token"); t != "" {
		return t
	}
	auth := r.Header.Get("Authorization")
	const bearer = "Bearer "
	if len(auth) > len(bearer) && auth[:len(bearer)] == bearer {
		return auth[len(bearer):]
	}
	return auth
}

// authConn validates the request token against the stored token for the given
// connection. It returns the connection on success; otherwise it writes the
// appropriate error response and returns nil.
func (a *API) authConn(w http.ResponseWriter, r *http.Request, connectionID string) (*store.Conn, bool) {
	if connectionID == "" {
		writeError(w, http.StatusBadRequest, "connectionId is required")
		return nil, false
	}
	conn, err := a.findConn(r.Context(), connectionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return nil, false
	}
	if conn == nil {
		writeError(w, http.StatusNotFound, "connection not found")
		return nil, false
	}
	if extractToken(r) != conn.Token {
		writeError(w, http.StatusUnauthorized, "invalid token")
		return nil, false
	}
	return conn, true
}

// findConn looks up a stored connection by ID, returning nil if absent.
func (a *API) findConn(ctx context.Context, connectionID string) (*store.Conn, error) {
	conns, err := a.Store.ListConns(ctx)
	if err != nil {
		return nil, err
	}
	for i := range conns {
		if conns[i].ConnectionID == connectionID {
			return &conns[i], nil
		}
	}
	return nil, nil
}
