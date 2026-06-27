package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/nextflow/whatsmeow-gateway/internal/media"
	"go.mau.fi/whatsmeow/types"
)

// decodeDataURI parses a "data:<mime>;base64,<payload>" string. If src is not a
// data URI it returns ("", nil) so the caller treats src as a plain URL.
func decodeDataURI(src string) (mime string, data []byte) {
	if !strings.HasPrefix(src, "data:") {
		return "", nil
	}
	rest := src[len("data:"):]
	comma := strings.IndexByte(rest, ',')
	if comma < 0 {
		return "", nil
	}
	meta, payload := rest[:comma], rest[comma+1:]
	isBase64 := false
	if i := strings.IndexByte(meta, ';'); i >= 0 {
		mime = meta[:i]
		if strings.Contains(meta[i:], "base64") {
			isBase64 = true
		}
	} else {
		mime = meta
	}
	if isBase64 {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(payload))
		if err != nil {
			return "", nil
		}
		return mime, decoded
	}
	return mime, []byte(payload)
}

// sendMediaRequest is the shared body for the media send endpoints. The source
// field (Image/Video/Document/Audio) carries either an http(s) URL or a
// data:...;base64,... URI.
type sendMediaRequest struct {
	ConnectionID string `json:"connectionId"`
	Phone        string `json:"Phone"`
	Image        string `json:"Image"`
	Video        string `json:"Video"`
	Document     string `json:"Document"`
	Audio        string `json:"Audio"`
	Caption      string `json:"Caption"`
	FileName     string `json:"FileName"`
}

type mediaKind int

const (
	kindImage mediaKind = iota
	kindVideo
	kindDocument
	kindAudio
)

// handleSendMedia is the shared handler for all media send endpoints.
func (a *API) handleSendMedia(kind mediaKind) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req sendMediaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		if req.Phone == "" {
			writeError(w, http.StatusBadRequest, "Phone is required")
			return
		}

		var src string
		switch kind {
		case kindImage:
			src = req.Image
		case kindVideo:
			src = req.Video
		case kindDocument:
			src = req.Document
		case kindAudio:
			src = req.Audio
		}
		if src == "" {
			writeError(w, http.StatusBadRequest, "media source (url or data uri) is required")
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

		dataMime, data := decodeDataURI(src)
		url := ""
		if data == nil {
			url = src
		}

		ctx := r.Context()
		var id string
		switch kind {
		case kindImage:
			id, err = media.SendImage(ctx, sess.Client, jid, url, data, dataMime, req.Caption)
		case kindVideo:
			id, err = media.SendVideo(ctx, sess.Client, jid, url, data, dataMime, req.Caption)
		case kindDocument:
			id, err = media.SendDocument(ctx, sess.Client, jid, url, data, dataMime, req.Caption, req.FileName)
		case kindAudio:
			id, err = media.SendAudio(ctx, sess.Client, jid, url, data, dataMime)
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"success": true, "id": id})
	}
}

// downloadRequest carries the inbound media metadata (as forwarded by the
// webhook payload) needed to reconstruct and decrypt a media message.
type downloadRequest struct {
	ConnectionID  string `json:"connectionId"`
	Kind          string `json:"kind"`     // image|video|audio|document|sticker
	URL           string `json:"url"`      // optional; directPath is preferred
	DirectPath    string `json:"directPath"`
	Mimetype      string `json:"mimetype"`
	MediaKey      []byte `json:"mediaKey"`      // raw bytes (JSON base64)
	FileSHA256    []byte `json:"fileSHA256"`    // raw bytes (JSON base64)
	FileEncSHA256 []byte `json:"fileEncSHA256"` // raw bytes (JSON base64)
	FileLength    uint64 `json:"fileLength"`
	AsBase64      bool   `json:"asBase64"` // if true, return data as base64 string
}

// handleDownload reconstructs an inbound media message from its metadata and
// downloads the decrypted bytes from WhatsApp.
func (a *API) handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req downloadRequest
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
	if len(req.MediaKey) == 0 || (req.DirectPath == "" && req.URL == "") {
		writeError(w, http.StatusBadRequest, "mediaKey and (directPath or url) are required")
		return
	}

	data, err := media.Download(r.Context(), sess.Client, media.DownloadParams{
		Kind:          media.DownloadKind(strings.ToLower(req.Kind)),
		URL:           req.URL,
		DirectPath:    req.DirectPath,
		Mimetype:      req.Mimetype,
		MediaKey:      req.MediaKey,
		FileSHA256:    req.FileSHA256,
		FileEncSHA256: req.FileEncSHA256,
		FileLength:    req.FileLength,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if req.AsBase64 {
		writeJSON(w, http.StatusOK, map[string]any{
			"success":  true,
			"mimetype": req.Mimetype,
			"data":     base64.StdEncoding.EncodeToString(data),
		})
		return
	}

	ct := req.Mimetype
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
