// Package media handles outbound media sending (upload + SendMessage) and
// inbound media downloading for the WhatsApp gateway.
package media

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/nextflow/whatsmeow-gateway/internal/audio"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

var httpClient = &http.Client{Timeout: 60 * time.Second}

// fetchURL downloads the bytes at url and returns them together with the
// reported content type.
func fetchURL(ctx context.Context, url string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", fmt.Errorf("media: build request: %w", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("media: fetch %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("media: fetch %s returned status %d", url, resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("media: read body: %w", err)
	}
	ct := resp.Header.Get("Content-Type")
	return data, ct, nil
}

// Source resolves the raw bytes + mime type for a media payload. If data is
// non-empty it is used directly (already-decoded data-URI bytes); otherwise the
// url is fetched. dataMime is the mime hint extracted from a data: URI, if any.
func resolve(ctx context.Context, url string, data []byte, dataMime string) ([]byte, string, error) {
	if len(data) > 0 {
		return data, dataMime, nil
	}
	if url == "" {
		return nil, "", fmt.Errorf("media: no url or data provided")
	}
	return fetchURL(ctx, url)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// SendImage uploads the given image (URL or raw data) and sends it.
func SendImage(ctx context.Context, cli *whatsmeow.Client, to types.JID, url string, data []byte, dataMime, caption string) (string, error) {
	raw, mime, err := resolve(ctx, url, data, dataMime)
	if err != nil {
		return "", err
	}
	mime = firstNonEmpty(mime, "image/jpeg")

	up, err := cli.Upload(ctx, raw, whatsmeow.MediaImage)
	if err != nil {
		return "", fmt.Errorf("media: upload image: %w", err)
	}
	msg := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Caption:       optString(caption),
		Mimetype:      proto.String(mime),
		URL:           proto.String(up.URL),
		DirectPath:    proto.String(up.DirectPath),
		MediaKey:      up.MediaKey,
		FileEncSHA256: up.FileEncSHA256,
		FileSHA256:    up.FileSHA256,
		FileLength:    proto.Uint64(up.FileLength),
	}}
	return send(ctx, cli, to, msg)
}

// BuildImageMessage uploads an image (URL or raw data) and returns its ImageMessage
// proto WITHOUT sending — usado como header/card de mensagens interativas (carrossel).
func BuildImageMessage(ctx context.Context, cli *whatsmeow.Client, url string, data []byte, dataMime string) (*waE2E.ImageMessage, error) {
	raw, mime, err := resolve(ctx, url, data, dataMime)
	if err != nil {
		return nil, err
	}
	mime = firstNonEmpty(mime, "image/jpeg")
	up, err := cli.Upload(ctx, raw, whatsmeow.MediaImage)
	if err != nil {
		return nil, fmt.Errorf("media: upload image: %w", err)
	}
	return &waE2E.ImageMessage{
		Mimetype:      proto.String(mime),
		URL:           proto.String(up.URL),
		DirectPath:    proto.String(up.DirectPath),
		MediaKey:      up.MediaKey,
		FileEncSHA256: up.FileEncSHA256,
		FileSHA256:    up.FileSHA256,
		FileLength:    proto.Uint64(up.FileLength),
	}, nil
}

// SendVideo uploads the given video (URL or raw data) and sends it.
func SendVideo(ctx context.Context, cli *whatsmeow.Client, to types.JID, url string, data []byte, dataMime, caption string) (string, error) {
	raw, mime, err := resolve(ctx, url, data, dataMime)
	if err != nil {
		return "", err
	}
	mime = firstNonEmpty(mime, "video/mp4")

	up, err := cli.Upload(ctx, raw, whatsmeow.MediaVideo)
	if err != nil {
		return "", fmt.Errorf("media: upload video: %w", err)
	}
	msg := &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
		Caption:       optString(caption),
		Mimetype:      proto.String(mime),
		URL:           proto.String(up.URL),
		DirectPath:    proto.String(up.DirectPath),
		MediaKey:      up.MediaKey,
		FileEncSHA256: up.FileEncSHA256,
		FileSHA256:    up.FileSHA256,
		FileLength:    proto.Uint64(up.FileLength),
	}}
	return send(ctx, cli, to, msg)
}

// SendDocument uploads the given document (URL or raw data) and sends it.
func SendDocument(ctx context.Context, cli *whatsmeow.Client, to types.JID, url string, data []byte, dataMime, caption, filename string) (string, error) {
	raw, mime, err := resolve(ctx, url, data, dataMime)
	if err != nil {
		return "", err
	}
	mime = firstNonEmpty(mime, "application/octet-stream")
	if filename == "" {
		filename = "documento"
	}

	up, err := cli.Upload(ctx, raw, whatsmeow.MediaDocument)
	if err != nil {
		return "", fmt.Errorf("media: upload document: %w", err)
	}
	msg := &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
		Caption:       optString(caption),
		Title:         proto.String(filename),
		FileName:      proto.String(filename),
		Mimetype:      proto.String(mime),
		URL:           proto.String(up.URL),
		DirectPath:    proto.String(up.DirectPath),
		MediaKey:      up.MediaKey,
		FileEncSHA256: up.FileEncSHA256,
		FileSHA256:    up.FileSHA256,
		FileLength:    proto.Uint64(up.FileLength),
	}}
	return send(ctx, cli, to, msg)
}

// SendAudio transcodes the given audio to Ogg/Opus and sends it as a PTT
// (voice) message.
func SendAudio(ctx context.Context, cli *whatsmeow.Client, to types.JID, url string, data []byte, dataMime string) (string, error) {
	raw, _, err := resolve(ctx, url, data, dataMime)
	if err != nil {
		return "", err
	}

	ogg, err := audio.ToOpusOgg(raw)
	if err != nil {
		return "", err
	}

	up, err := cli.Upload(ctx, ogg, whatsmeow.MediaAudio)
	if err != nil {
		return "", fmt.Errorf("media: upload audio: %w", err)
	}
	msg := &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
		Mimetype:      proto.String("audio/ogg; codecs=opus"),
		PTT:           proto.Bool(true),
		Seconds:       proto.Uint32(audio.DurationSeconds(ogg)), // senão WhatsApp mostra 0:00
		URL:           proto.String(up.URL),
		DirectPath:    proto.String(up.DirectPath),
		MediaKey:      up.MediaKey,
		FileEncSHA256: up.FileEncSHA256,
		FileSHA256:    up.FileSHA256,
		FileLength:    proto.Uint64(up.FileLength),
	}}
	return send(ctx, cli, to, msg)
}

// send dispatches the message and returns the resulting message ID.
func send(ctx context.Context, cli *whatsmeow.Client, to types.JID, msg *waE2E.Message) (string, error) {
	resp, err := cli.SendMessage(ctx, to, msg)
	if err != nil {
		return "", err
	}
	return resp.ID, nil
}

func optString(s string) *string {
	if s == "" {
		return nil
	}
	return proto.String(s)
}

// DownloadKind identifies which media sub-message to reconstruct for download.
type DownloadKind string

const (
	KindImage    DownloadKind = "image"
	KindVideo    DownloadKind = "video"
	KindAudio    DownloadKind = "audio"
	KindDocument DownloadKind = "document"
	KindSticker  DownloadKind = "sticker"
)

// DownloadParams carries the media metadata received from an inbound webhook,
// enough to reconstruct the downloadable proto message and decrypt it.
type DownloadParams struct {
	Kind          DownloadKind
	URL           string
	DirectPath    string
	Mimetype      string
	MediaKey      []byte
	FileSHA256    []byte
	FileEncSHA256 []byte
	FileLength    uint64
}

// Download reconstructs the media proto message from the supplied metadata and
// downloads/decrypts the original bytes from WhatsApp servers.
func Download(ctx context.Context, cli *whatsmeow.Client, p DownloadParams) ([]byte, error) {
	var dl whatsmeow.DownloadableMessage
	switch p.Kind {
	case KindImage:
		dl = &waE2E.ImageMessage{
			URL: optString(p.URL), DirectPath: optString(p.DirectPath), Mimetype: optString(p.Mimetype),
			MediaKey: p.MediaKey, FileSHA256: p.FileSHA256, FileEncSHA256: p.FileEncSHA256,
			FileLength: proto.Uint64(p.FileLength),
		}
	case KindVideo:
		dl = &waE2E.VideoMessage{
			URL: optString(p.URL), DirectPath: optString(p.DirectPath), Mimetype: optString(p.Mimetype),
			MediaKey: p.MediaKey, FileSHA256: p.FileSHA256, FileEncSHA256: p.FileEncSHA256,
			FileLength: proto.Uint64(p.FileLength),
		}
	case KindAudio:
		dl = &waE2E.AudioMessage{
			URL: optString(p.URL), DirectPath: optString(p.DirectPath), Mimetype: optString(p.Mimetype),
			MediaKey: p.MediaKey, FileSHA256: p.FileSHA256, FileEncSHA256: p.FileEncSHA256,
			FileLength: proto.Uint64(p.FileLength),
		}
	case KindDocument:
		dl = &waE2E.DocumentMessage{
			URL: optString(p.URL), DirectPath: optString(p.DirectPath), Mimetype: optString(p.Mimetype),
			MediaKey: p.MediaKey, FileSHA256: p.FileSHA256, FileEncSHA256: p.FileEncSHA256,
			FileLength: proto.Uint64(p.FileLength),
		}
	case KindSticker:
		dl = &waE2E.StickerMessage{
			URL: optString(p.URL), DirectPath: optString(p.DirectPath), Mimetype: optString(p.Mimetype),
			MediaKey: p.MediaKey, FileSHA256: p.FileSHA256, FileEncSHA256: p.FileEncSHA256,
			FileLength: proto.Uint64(p.FileLength),
		}
	default:
		return nil, fmt.Errorf("media: unknown download kind %q", p.Kind)
	}

	data, err := cli.Download(ctx, dl)
	if err != nil {
		return nil, fmt.Errorf("media: download: %w", err)
	}
	return data, nil
}
