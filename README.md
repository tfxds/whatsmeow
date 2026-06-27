# whatsmeow-gateway

Gateway WhatsApp próprio do NextFlow, em **Go** sobre a lib [whatsmeow](https://github.com/tulir/whatsmeow). Conecta números via QR, envia/recebe mensagem + mídia, e expõe uma API REST (estilo WuzAPI) + webhooks que o NextFlow consome como o provider `WhatsApp (whatsmeow)`.

**Fase 1 = mensageria** (este repo). Fase 2 = chamadas de voz/vídeo via [meowcaller](https://github.com/purpshell/meowcaller), na mesma sessão.

## Arquitetura

- 1 binário Go, **multi-sessão num processo** (N números, 1 `whatsmeow.Client` por número).
- **Device store em Postgres** (`sqlstore`) — as sessões/pareamento vivem aqui ⇒ sobrevivem a restart e **migram via `pg_dump` sem reparear**.
- REST + webhook dispatcher. Toda lógica de chat/CRM/bot fica no NextFlow.

## Requisitos

- Go **1.25+** (a versão atual do whatsmeow exige; o toolchain do Go auto-baixa).
- Postgres (dedicado, ver `deploy/docker-compose.postgres.yml`).
- ffmpeg (só pra converter áudio PTT no envio).

## Build

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o whatsmeow-gateway ./cmd/gateway
```
Binário estático (Go puro) — roda em qualquer Linux x64, sem dependência de runtime.

## Config (env)

| var | descrição |
|---|---|
| `GW_PORT` | porta HTTP (default 3020) |
| `GW_PG_DSN` | DSN do Postgres do device store |
| `GW_ADMIN_TOKEN` | token admin (reservado) |

## Deploy (resumo)

1. `WM_PG_PASSWORD=... docker compose -f deploy/docker-compose.postgres.yml up -d`
2. Copie o binário pra `/usr/local/bin/whatsmeow-gateway`.
3. Crie `/etc/whatsmeow-gateway.env` (ver `deploy/whatsmeow-gateway.env.example`).
4. `cp deploy/whatsmeow-gateway.service /etc/systemd/system/ && systemctl enable --now whatsmeow-gateway`
5. (Opcional) HTTPS: bloco do `deploy/Caddyfile.snippet` no Caddy.
6. `curl localhost:3020/health` → `{"status":"ok"}`.

## Migração pra outro servidor

O pareamento vive no Postgres. Pra migrar **sem reparear**:
1. `deploy/backup.sh` no origem (pg_dump do DB `whatsmeow`).
2. Suba o Postgres + restaure o dump no destino.
3. Copie o binário + env (DSN do novo Postgres) + systemd. Suba.
4. (DNS) aponte o subdomínio pro servidor novo.

## API REST (resumo)

- `POST /session/connect` `{connectionId, tenantId, webhookUrl, token}` → `{status:"qr"|"connected", qr}`
- `GET  /session/qr?connectionId=` (header `token`) → `{qr, connected}`
- `GET  /session/status?connectionId=` → `{connected, found}`
- `POST /chat/send/{text,image,video,audio,document}` (header `token`)
- `POST /chat/download` (metadados de mídia → bytes/base64)
- `POST /user/check` · `POST /chat/markread` · `POST /chat/presence`
- Webhook inbound: POST no `webhookUrl` (formato compatível WuzAPI: `Info` + `Message`).

## Estrutura

```
cmd/gateway/main.go        bootstrap (config, store, RestoreAll, HTTP)
internal/config            env
internal/store             sqlstore (Postgres) + tabela connections
internal/session           manager (connect/QR/status/reconnect) + events
internal/webhook           dispatcher (POST p/ NextFlow)
internal/api               handlers REST (session, send, media, util)
internal/media             upload/download de mídia
internal/audio             ffmpeg PTT (ogg/opus)
deploy/                    systemd, docker-compose PG, Caddy, backup
```
