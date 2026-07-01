package api

import "net/http"

// requireAdmin gates admin endpoints on the GW_ADMIN_TOKEN. Returns false (and writes the
// error) when the token is missing or wrong.
func (a *API) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	if a.AdminToken == "" || extractToken(r) != a.AdminToken {
		writeError(w, http.StatusUnauthorized, "invalid admin token")
		return false
	}
	return true
}

// handleAdminSessions lists paired whatsmeow devices (GET) or removes one (DELETE ?jid=).
// Removing logs out the live session if any (unpair) and deletes the device from the store —
// mata o "fantasma" de vez.
func (a *API) handleAdminSessions(w http.ResponseWriter, r *http.Request) {
	if !a.requireAdmin(w, r) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		devs, err := a.Mgr.ListDevices(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"success": true, "instances": devs})
	case http.MethodDelete:
		jid := r.URL.Query().Get("jid")
		if jid == "" {
			writeError(w, http.StatusBadRequest, "jid is required")
			return
		}
		if err := a.Mgr.RemoveByJID(r.Context(), jid); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"success": true})
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}
