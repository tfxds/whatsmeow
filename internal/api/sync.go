package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/types"
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

// handleGroupParticipants — POST {connectionId, jid}: lista os MEMBROS de um grupo específico
// (GetGroupInfo → Participants). Usado pra mostrar os participantes na ficha do contato no NextFlow.
// Retorna [{jid, phone, lid, name, isAdmin, isSuperAdmin}] — o NextFlow resolve o nome via CRM depois.
func (a *API) handleGroupParticipants(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		ConnectionID string `json:"connectionId"`
		JID          string `json:"jid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ConnectionID == "" || req.JID == "" {
		writeError(w, http.StatusBadRequest, "connectionId and jid are required")
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
	gjid, err := types.ParseJID(req.JID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid group jid")
		return
	}
	info, err := sess.Client.GetGroupInfo(r.Context(), gjid)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Nome do participante vem do CONTACT STORE (o DisplayName do GroupParticipant é vazio, só
	// preenche p/ anônimo). Monta mapa número→nome (fullname/first/business/pushname), igual /contacts/list.
	nameByUser := map[string]string{}
	if contacts, cerr := sess.Client.Store.Contacts.GetAllContacts(r.Context()); cerr == nil {
		for cjid, c := range contacts {
			n := strings.TrimSpace(c.FullName)
			if n == "" {
				n = strings.TrimSpace(c.FirstName)
			}
			if n == "" {
				n = strings.TrimSpace(c.BusinessName)
			}
			if n == "" {
				n = strings.TrimSpace(c.PushName)
			}
			if n != "" && cjid.User != "" {
				nameByUser[cjid.User] = n
			}
		}
	}
	// Nossa própria identidade, pra saber se ESTE número é admin (define se conseguimos enviar
	// quando o grupo está em modo "só admins enviam" = IsAnnounce).
	myPhone, myLID := "", ""
	if sess.Client.Store.ID != nil {
		myPhone = sess.Client.Store.ID.User
	}
	if sess.Client.Store.LID.User != "" {
		myLID = sess.Client.Store.LID.User
	}
	meIsAdmin := false
	out := make([]map[string]any, 0, len(info.Participants))
	for _, p := range info.Participants {
		// Telefone: prefere PhoneNumber (quando o WA conhece o número real); senão, o JID
		// se ele for @s.whatsapp.net (grupo não-LID). Em grupo LID o JID vem @lid → sem telefone.
		phone := p.PhoneNumber.User
		if phone == "" && p.JID.Server == types.DefaultUserServer {
			phone = p.JID.User
		}
		name := strings.TrimSpace(p.DisplayName)
		if name == "" {
			for _, u := range []string{p.PhoneNumber.User, p.JID.User, p.LID.User} {
				if u != "" {
					if n, ok := nameByUser[u]; ok {
						name = n
						break
					}
				}
			}
		}
		isMe := (myPhone != "" && (p.PhoneNumber.User == myPhone || p.JID.User == myPhone)) ||
			(myLID != "" && p.LID.User == myLID)
		if isMe && (p.IsAdmin || p.IsSuperAdmin) {
			meIsAdmin = true
		}
		out = append(out, map[string]any{
			"jid":          p.JID.String(),
			"phone":        phone,
			"lid":          p.LID.User,
			"name":         name,
			"isAdmin":      p.IsAdmin,
			"isSuperAdmin": p.IsSuperAdmin,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success":      true,
		"count":        len(out),
		"participants": out,
		"isAnnounce":   info.IsAnnounce, // só admins podem enviar
		"isLocked":     info.IsLocked,   // só admins editam infos do grupo
		"meIsAdmin":    meIsAdmin,
	})
}
