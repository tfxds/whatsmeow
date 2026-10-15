package webhook

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	waLog "go.mau.fi/whatsmeow/util/log"
)

// Dispatcher delivers webhook payloads to tenant-configured URLs.
//
// Entrega durável (15/10/2026): antes eram só 3 tentativas em ~1,5 s; um restart do NextFlow
// (deploy) bastava pra mensagem do cliente sumir. Agora, se as 3 tentativas falharem, o
// payload vai pra tabela webhook_pendentes e um laço reenvia (backoff até 10 min) por até 24 h.
// Presença/digitando não entram na fila: velhos, não servem pra nada.
type Dispatcher struct {
	client *http.Client
	log    waLog.Logger
	db     *sql.DB
}

const createPendentes = `
CREATE TABLE IF NOT EXISTS webhook_pendentes (
	id BIGSERIAL PRIMARY KEY,
	url TEXT NOT NULL,
	body BYTEA NOT NULL,
	tentativas INT NOT NULL DEFAULT 0,
	proxima TIMESTAMPTZ NOT NULL DEFAULT now(),
	criado TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS webhook_pendentes_proxima_idx ON webhook_pendentes (proxima);`

// New creates a Dispatcher with a 15s HTTP timeout.
func New() *Dispatcher {
	return &Dispatcher{
		client: &http.Client{Timeout: 15 * time.Second},
		log:    waLog.Stdout("Webhook", "INFO", true),
	}
}

// EnablePersistence liga a fila durável no Postgres do gateway e começa o laço de reenvio.
func (d *Dispatcher) EnablePersistence(db *sql.DB) {
	if db == nil {
		return
	}
	if _, err := db.Exec(createPendentes); err != nil {
		d.log.Errorf("webhook_pendentes: não criou a tabela (%v); seguindo sem fila durável", err)
		return
	}
	d.db = db
	go d.loopReenvio()
}

func tipoDoPayload(body []byte) string {
	var t struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(body, &t)
	return t.Type
}

func (d *Dispatcher) entregar(url string, body []byte) (int, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return 0, err
	}
	_ = resp.Body.Close()
	return resp.StatusCode, nil
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
			status, err := d.entregar(webhookURL, body)
			if err == nil && status < 300 {
				return
			}
			if err != nil {
				d.log.Warnf("webhook %s failed: %v (attempt %d/3)", webhookURL, err, attempt)
			} else {
				d.log.Warnf("webhook %s returned status %d (attempt %d/3)", webhookURL, status, attempt)
			}
			if attempt < 3 {
				time.Sleep(backoff)
				backoff *= 2
			}
		}
		tipo := tipoDoPayload(body)
		if d.db != nil && tipo != "Presence" && tipo != "ChatPresence" {
			if _, err := d.db.Exec(`INSERT INTO webhook_pendentes (url, body, tentativas, proxima) VALUES ($1, $2, 3, now() + interval '20 seconds')`, webhookURL, body); err != nil {
				d.log.Errorf("webhook %s exhausted retries e NÃO foi pra fila: %v", webhookURL, err)
				return
			}
			d.log.Warnf("webhook %s exhausted retries: guardado na fila de reenvio (%s)", webhookURL, tipo)
			return
		}
		d.log.Errorf("webhook %s exhausted retries", webhookURL)
	}()
}

// loopReenvio tenta de novo o que ficou pendente, em ordem de chegada (a ordem importa:
// mensagem antes do recibo de leitura dela).
func (d *Dispatcher) loopReenvio() {
	for {
		time.Sleep(20 * time.Second)
		_, _ = d.db.Exec(`DELETE FROM webhook_pendentes WHERE criado < now() - interval '24 hours'`)
		rows, err := d.db.Query(`SELECT id, url, body, tentativas FROM webhook_pendentes WHERE proxima <= now() ORDER BY id LIMIT 100`)
		if err != nil {
			d.log.Warnf("webhook_pendentes: leitura falhou: %v", err)
			continue
		}
		type pend struct {
			id         int64
			url        string
			body       []byte
			tentativas int
		}
		var lista []pend
		for rows.Next() {
			var p pend
			if rows.Scan(&p.id, &p.url, &p.body, &p.tentativas) == nil {
				lista = append(lista, p)
			}
		}
		_ = rows.Close()
		falhou := map[string]bool{} // URL que falhou nesta volta: não insiste nas outras (servidor fora)
		for _, p := range lista {
			if falhou[p.url] {
				continue
			}
			status, err := d.entregar(p.url, p.body)
			if err == nil && status < 300 {
				_, _ = d.db.Exec(`DELETE FROM webhook_pendentes WHERE id = $1`, p.id)
				continue
			}
			falhou[p.url] = true
			espera := time.Duration(1<<uint(min(p.tentativas, 10))) * 5 * time.Second
			if espera > 10*time.Minute {
				espera = 10 * time.Minute
			}
			_, _ = d.db.Exec(`UPDATE webhook_pendentes SET tentativas = tentativas + 1, proxima = now() + $2::interval WHERE id = $1`, p.id, espera.String())
		}
		if len(lista) > 0 && len(falhou) == 0 {
			d.log.Infof("webhook_pendentes: %d reenviado(s) com sucesso", len(lista))
		}
	}
}
