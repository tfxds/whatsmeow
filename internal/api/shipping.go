package api

import (
	"bytes"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"time"
)

// shipRecording envia o RAW da gravação pro NextFlow (endpoint de ingest), que converte
// pra ogg e sobe no S3. A URL do ingest é derivada do webhookURL da conexão (mesma
// origem do NextFlow). Em sucesso, apaga o raw local pra não acumular disco no 225.
// Roda em goroutine — não bloqueia o encerramento da chamada.
func shipRecording(webhookURL, connID, token, callID, rawPath string) {
	u, err := url.Parse(webhookURL)
	if err != nil || u.Host == "" {
		fmt.Printf("[REC] ship %s: webhookURL invalido (%q)\n", callID, webhookURL)
		return
	}
	ingest := u.Scheme + "://" + u.Host + "/api/telephony/recording/wacall-ingest"

	raw, err := os.ReadFile(rawPath)
	if err != nil {
		fmt.Printf("[REC] ship %s: nao consegui ler raw: %v\n", callID, err)
		return
	}

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	_ = mw.WriteField("connectionId", connID)
	_ = mw.WriteField("callId", callID)
	_ = mw.WriteField("token", token)
	if fw, e := mw.CreateFormFile("audio", callID+".raw"); e == nil {
		_, _ = fw.Write(raw)
	}
	_ = mw.Close()

	req, err := http.NewRequest(http.MethodPost, ingest, &body)
	if err != nil {
		fmt.Printf("[REC] ship %s: request err: %v\n", callID, err)
		return
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("[REC] ship %s: POST falhou: %v (raw mantido)\n", callID, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_ = os.Remove(rawPath)
		fmt.Printf("[REC] ship %s: enviado OK (%d bytes) -> NextFlow/S3, raw local apagado\n", callID, len(raw))
	} else {
		fmt.Printf("[REC] ship %s: ingest retornou HTTP %d (raw mantido)\n", callID, resp.StatusCode)
	}
}
