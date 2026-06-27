package webhook

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	waLog "go.mau.fi/whatsmeow/util/log"
)

// Dispatcher delivers webhook payloads to tenant-configured URLs.
type Dispatcher struct {
	client *http.Client
	log    waLog.Logger
}

// New creates a Dispatcher with a 15s HTTP timeout.
func New() *Dispatcher {
	return &Dispatcher{
		client: &http.Client{Timeout: 15 * time.Second},
		log:    waLog.Stdout("Webhook", "INFO", true),
	}
}

// Send POSTs the payload as JSON to webhookURL in a background goroutine with
// up to 3 attempts and exponential backoff. It never blocks the caller.
func (d *Dispatcher) Send(webhookURL string, payload any) {
	if webhookURL == "" {
		return
	}

	body, err := json.Marshal(payload)
	if err != nil {
		d.log.Errorf("marshal webhook payload: %v", err)
		return
	}

	go func() {
		backoff := 500 * time.Millisecond
		for attempt := 1; attempt <= 3; attempt++ {
			req, err := http.NewRequest(http.MethodPost, webhookURL, bytes.NewReader(body))
			if err != nil {
				d.log.Errorf("build webhook request: %v", err)
				return
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := d.client.Do(req)
			if err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode < 300 {
					return
				}
				d.log.Warnf("webhook %s returned status %d (attempt %d/3)", webhookURL, resp.StatusCode, attempt)
			} else {
				d.log.Warnf("webhook %s failed: %v (attempt %d/3)", webhookURL, err, attempt)
			}

			if attempt < 3 {
				time.Sleep(backoff)
				backoff *= 2
			}
		}
		d.log.Errorf("webhook %s exhausted retries", webhookURL)
	}()
}
