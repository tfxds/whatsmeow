package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// syncReq é o corpo comum das ações de sync/restart (só precisa da conexão).
type syncReq struct {
	ConnectionID string `json:"connectionId"`
}

// decodeConn faz o boilerplate: decode do body, auth e lookup da sessão viva.
// Retorna a sessão e true se tudo ok (senão já escreveu o erro na resposta).
func (a *API) decodeConn(w http.ResponseWriter, r *http.Request) (connID string, ok bool) {
	var req syncReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ConnectionID == "" {
		writeError(w, http.StatusBadRequest, "connectionId is required")
		return "", false
	}
	if _, ok := a.authConn(w, r, req.ConnectionID); !ok {
		return "", false
	}
	return req.ConnectionID, true
}

// handleRestart — POST {connectionId}: desconecta e reconecta o cliente (equivale ao
// "Reiniciar" dos outros providers). Útil quando a sessão trava sem cair de vez.
func (a *API) handleRestart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	connID, ok := a.decodeConn(w, r)
	if !ok {
		return
	}
	sess, ok := a.Mgr.Get(connID)
	if !ok {
		writeError(w, http.StatusNotFound, "session not found")
		return
	}
	// Disconnect é síncrono; Connect reabre o websocket. Pequena pausa entre os dois
	// pra o socket fechar limpo antes de reabrir.
	sess.Client.Disconnect()
	time.Sleep(400 * time.Millisecond)
	if err := sess.Client.Connect(); err != nil {
		writeError(w, http.StatusInternalServerError, "reconnect failed: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "connected": sess.Client.IsConnected()})
}

// handleContacts — POST {connectionId}: lista todos os contatos do device (Store.Contacts).
// O backend faz o upsert em crm_contacts. Retorna {phone, name, pushName, business}.
func (a *API) handleContacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	connID, ok := a.decodeConn(w, r)
	if !ok {
		return
	}
	sess, ok := a.Mgr.Get(connID)
	if !ok {
		writeError(w, http.StatusNotFound, "session not found")
		return
	}
	all, err := sess.Client.Store.Contacts.GetAllContacts(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(all))
	for jid, c := range all {
		// Só contatos de usuário (ignora grupos/broadcast); JID.User = número.
		if jid.Server != "s.whatsapp.net" && jid.Server != "" {
			continue
		}
		name := strings.TrimSpace(c.FullName)
		if name == "" {
			name = strings.TrimSpace(c.FirstName)
		}
		if name == "" {
			name = strings.TrimSpace(c.BusinessName)
		}
		if name == "" {
			name = strings.TrimSpace(c.PushName)
		}
		out = append(out, map[string]any{
			"phone":    jid.User,
			"jid":      jid.String(),
			"name":     name,
			"pushName": c.PushName,
			"business": c.BusinessName,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "count": len(out), "contacts": out})
}

// handleGroups — POST {connectionId}: lista os grupos que o número participa (GetJoinedGroups).
func (a *API) handleGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	connID, ok := a.decodeConn(w, r)
	if !ok {
		return
	}
	sess, ok := a.Mgr.Get(connID)
	if !ok {
		writeError(w, http.StatusNotFound, "session not found")
		return
	}
	groups, err := sess.Client.GetJoinedGroups(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		out = append(out, map[string]any{
			"jid":          g.JID.String(),
			"name":         g.GroupName.Name,
			"participants": len(g.Participants),
			"owner":        g.OwnerJID.String(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "count": len(out), "groups": out})
}
