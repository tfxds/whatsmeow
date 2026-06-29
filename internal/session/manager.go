package session

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nextflow/whatsmeow-gateway/internal/store"
	"github.com/nextflow/whatsmeow-gateway/internal/webhook"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Session is a single live WhatsApp client for one connection.
type Session struct {
	ConnectionID string
	TenantID     string
	Client       *whatsmeow.Client
	LastQR       string
	Connected    bool
	qrActive     bool // canal de QR vivo (goroutine consumeQR rodando) → NÃO recriar no Connect
}

// Manager owns all live sessions and their lifecycle.
type Manager struct {
	mu         sync.RWMutex
	sessions   map[string]*Session
	store      *store.Store
	dispatcher *webhook.Dispatcher
	log        waLog.Logger

	onConnected func(connID string, wa *whatsmeow.Client)
}

// SetOnConnected registra um hook chamado após cada sessão PAREADA conectar com
// sucesso (no Connect e no RestoreAll). Usado pra registrar o handler de chamadas.
func (m *Manager) SetOnConnected(fn func(connID string, wa *whatsmeow.Client)) { m.onConnected = fn }

// LookupConn expõe a busca da conexão armazenada (webhook URL, tenant, token).
func (m *Manager) LookupConn(connectionID string) *store.Conn { return m.lookupConn(connectionID) }

// NewManager builds a Manager backed by the given store and webhook dispatcher.
func NewManager(st *store.Store, d *webhook.Dispatcher) *Manager {
	return &Manager{
		sessions:   make(map[string]*Session),
		store:      st,
		dispatcher: d,
		log:        waLog.Stdout("Session", "INFO", true),
	}
}

// Connect returns the existing session for connectionID or creates and connects
// a new one. If the device is not yet paired it starts the QR flow.
func (m *Manager) Connect(ctx context.Context, connectionID, tenantID string) (*Session, error) {
	m.mu.RLock()
	existing, ok := m.sessions[connectionID]
	m.mu.RUnlock()
	if ok {
		// Já pareado/conectado OU com QR ativo → REAPROVEITA (não recriar!).
		// CRÍTICO: a UI chama /connect mais de uma vez (re-render/reabrir o modal). Se aqui
		// destruir+recriar o cliente a cada chamada, o websocket cai e sobe a cada poll =
		// churn de ~6s que o WhatsApp derruba e o QR nunca pareia. Só recria quando o canal
		// de QR realmente morreu (timeout) E o device não pareou.
		if existing.Client.IsLoggedIn() || existing.Connected || existing.qrActive {
			return existing, nil
		}
		existing.Client.Disconnect()
		m.remove(connectionID)
	}

	dev := m.store.Container.NewDevice()
	cli := whatsmeow.NewClient(dev, m.log)
	sess := &Session{ConnectionID: connectionID, TenantID: tenantID, Client: cli}
	m.attachHandlers(sess)

	// Register before connecting so event handlers can find the session in the map.
	m.mu.Lock()
	m.sessions[connectionID] = sess
	m.mu.Unlock()

	if cli.Store.ID == nil {
		// Not paired yet → start the QR pairing flow.
		// CRÍTICO: o canal de QR e o cliente vivem ALÉM do request HTTP — usar o contexto
		// do request (cancelado quando /connect retorna) fechava o canal em ~1s e o QR
		// nunca pareava. Sempre context.Background() pra esse fluxo de longa duração.
		qrChan, err := cli.GetQRChannel(context.Background())
		if err != nil {
			m.remove(connectionID)
			return nil, fmt.Errorf("get qr channel: %w", err)
		}
		if err := cli.Connect(); err != nil {
			m.remove(connectionID)
			return nil, fmt.Errorf("connect: %w", err)
		}
		m.mu.Lock()
		sess.qrActive = true
		m.mu.Unlock()
		go m.consumeQR(connectionID, qrChan)
	} else {
		// Already paired → just connect.
		if err := cli.Connect(); err != nil {
			m.remove(connectionID)
			return nil, fmt.Errorf("connect: %w", err)
		}
		m.setConnected(connectionID, true)
		if m.onConnected != nil {
			m.onConnected(connectionID, cli)
		}
	}

	return sess, nil
}

// consumeQR drains the QR channel, tracking the latest code and pairing result.
func (m *Manager) consumeQR(connectionID string, qrChan <-chan whatsmeow.QRChannelItem) {
	for evt := range qrChan {
		switch evt.Event {
		case "code":
			// whatsmeow (pair.go) emite o QR como URL "https://wa.me/settings/linked_devices#2@...".
			// O scanner "Conectar aparelho" do WhatsApp lê o código CRU "2@..." — tirar o prefixo
			// (igual WuzAPI faz), senão NENHUM WhatsApp lê o QR.
			raw := strings.TrimPrefix(evt.Code, "https://wa.me/settings/linked_devices#")
			m.mu.Lock()
			if s, ok := m.sessions[connectionID]; ok {
				s.LastQR = raw
				s.Connected = false
			}
			m.mu.Unlock()
			m.log.Infof("QR code emitido para %s (len=%d)", connectionID, len(raw))
		case "success":
			m.mu.Lock()
			if s, ok := m.sessions[connectionID]; ok {
				s.LastQR = ""
				s.Connected = true
				s.qrActive = false
			}
			m.mu.Unlock()
			m.log.Infof("QR SUCCESS — pareado %s", connectionID)
			return
		default:
			// "timeout", "error", etc. — o canal naturalmente encerra; o whatsmeow já
			// emitiu vários códigos ao longo de ~160s. NÃO recriar aqui (auto-refresh
			// causava churn de reconexão a cada ~5s que o WhatsApp derrubava). Pra um QR
			// novo após o timeout, o usuário clica "QR" de novo (Connect recria).
			m.log.Infof("QR channel de %s encerrou com evento %q", connectionID, evt.Event)
		}
	}
	// Canal fechou (timeout/erro). Marca qrActive=false → o próximo /connect recria pra QR novo.
	m.mu.Lock()
	if s, ok := m.sessions[connectionID]; ok {
		s.qrActive = false
	}
	m.mu.Unlock()
	m.log.Infof("QR channel de %s fechou", connectionID)
}

// Get returns the session for connectionID, if any.
// PairCode inicia o pareamento por CÓDIGO (PairPhone) — alternativa ao QR. Cria a sessão,
// conecta e retorna o código de 8 chars que o usuário digita em "Conectar com número de
// telefone". Também serve de diagnóstico: se a Meta/WhatsApp bloquear o número, PairPhone
// retorna erro (em vez de código). phone = só dígitos com DDI (ex 5548...).
func (m *Manager) PairCode(connectionID, tenantID, phone string) (string, error) {
	m.mu.RLock()
	existing, ok := m.sessions[connectionID]
	m.mu.RUnlock()
	if ok {
		if existing.Client.IsLoggedIn() {
			return "", fmt.Errorf("conexão já pareada")
		}
		existing.Client.Disconnect()
		m.remove(connectionID)
	}

	dev := m.store.Container.NewDevice()
	cli := whatsmeow.NewClient(dev, m.log)
	sess := &Session{ConnectionID: connectionID, TenantID: tenantID, Client: cli}
	m.attachHandlers(sess)
	m.mu.Lock()
	m.sessions[connectionID] = sess
	m.mu.Unlock()

	if err := cli.Connect(); err != nil {
		m.remove(connectionID)
		return "", fmt.Errorf("connect: %w", err)
	}
	time.Sleep(1500 * time.Millisecond) // doc do whatsmeow: aguardar o websocket estabelecer
	code, err := cli.PairPhone(context.Background(), phone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
	if err != nil {
		m.remove(connectionID)
		return "", fmt.Errorf("pair phone: %w", err)
	}
	m.log.Infof("PairCode gerado para %s (phone %s)", connectionID, phone)
	return code, nil
}

func (m *Manager) Get(connectionID string) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[connectionID]
	return s, ok
}

// Status reports the current QR code and live connection state.
func (m *Manager) Status(connectionID string) (qr string, connected bool, ok bool) {
	m.mu.RLock()
	s, found := m.sessions[connectionID]
	if !found {
		m.mu.RUnlock()
		return "", false, false
	}
	qr = s.LastQR
	cli := s.Client
	m.mu.RUnlock()

	connected = cli.IsConnected() && cli.IsLoggedIn()
	return qr, connected, true
}

// attachHandlers wires whatsmeow events to the webhook dispatcher and status.
func (m *Manager) attachHandlers(sess *Session) {
	connID := sess.ConnectionID
	tenantID := sess.TenantID
	sess.Client.AddEventHandler(func(evt any) {
		switch v := evt.(type) {
		case *events.Message:
			conn := m.lookupConn(connID)
			if conn == nil || conn.WebhookURL == "" {
				return
			}
			m.dispatcher.Send(conn.WebhookURL, normalizeMessage(connID, tenantID, v))
		case *events.Receipt:
			// Recibo de entrega/leitura das msgs ENVIADAS → ✓✓ e azul no painel.
			if payload := normalizeReceipt(connID, tenantID, v); payload != nil {
				if conn := m.lookupConn(connID); conn != nil && conn.WebhookURL != "" {
					m.dispatcher.Send(conn.WebhookURL, payload)
				}
			}
		case *events.Connected:
			m.setConnected(connID, true)
			m.persistJID(connID)
			// CRÍTICO p/ "digitando…" e tique azul (read receipt): o whatsmeow só ENTREGA
			// chat presence (composing) e recibos se o cliente tiver mandado SendPresence
			// (Available) ao menos uma vez após conectar. Sem isso, MarkRead/typing viram no-op.
			if err := sess.Client.SendPresence(context.Background(), types.PresenceAvailable); err != nil {
				m.log.Warnf("SendPresence(available) %s: %v", connID, err)
			}
		case *events.PairSuccess:
			m.persistJID(connID)
		case *events.LoggedOut:
			m.setConnected(connID, false)
		}
	})
}

// persistJID writes the paired device JID into the connections table so the
// session can be restored on the next boot without re-scanning the QR code.
func (m *Manager) persistJID(connectionID string) {
	m.mu.RLock()
	sess, ok := m.sessions[connectionID]
	m.mu.RUnlock()
	if !ok || sess.Client.Store == nil || sess.Client.Store.ID == nil {
		return
	}
	jid := sess.Client.Store.ID.String()

	conn := m.lookupConn(connectionID)
	if conn == nil {
		return
	}
	if conn.JID == jid {
		return // already persisted
	}
	conn.JID = jid
	if err := m.store.UpsertConn(context.Background(), *conn); err != nil {
		m.log.Warnf("persistJID %s: %v", connectionID, err)
		return
	}
	m.log.Infof("persisted JID %s for connection %s", jid, connectionID)
}

// RestoreAll reconnects every previously-paired connection on boot. Each
// connection is restored independently: a failure on one is logged and does not
// abort the others.
func (m *Manager) RestoreAll(ctx context.Context) error {
	conns, err := m.store.ListConns(ctx)
	if err != nil {
		return fmt.Errorf("restore: list conns: %w", err)
	}

	restored := 0
	for _, conn := range conns {
		if conn.JID == "" {
			continue // never paired → nothing to restore (waits for QR flow)
		}
		if _, ok := m.Get(conn.ConnectionID); ok {
			continue // already live
		}

		jid, err := types.ParseJID(conn.JID)
		if err != nil {
			m.log.Errorf("restore %s: bad stored JID %q: %v", conn.ConnectionID, conn.JID, err)
			continue
		}

		dev, err := m.store.Container.GetDevice(ctx, jid)
		if err != nil {
			m.log.Errorf("restore %s: get device: %v", conn.ConnectionID, err)
			continue
		}
		if dev == nil {
			m.log.Warnf("restore %s: no device for JID %s (skipping)", conn.ConnectionID, conn.JID)
			continue
		}

		cli := whatsmeow.NewClient(dev, m.log)
		sess := &Session{ConnectionID: conn.ConnectionID, TenantID: conn.TenantID, Client: cli}
		m.attachHandlers(sess)

		m.mu.Lock()
		m.sessions[conn.ConnectionID] = sess
		m.mu.Unlock()

		if err := cli.Connect(); err != nil {
			m.remove(conn.ConnectionID)
			m.log.Errorf("restore %s: connect: %v", conn.ConnectionID, err)
			continue
		}
		m.setConnected(conn.ConnectionID, true)
		if m.onConnected != nil {
			m.onConnected(conn.ConnectionID, cli)
		}
		restored++
		m.log.Infof("restored connection %s (JID %s)", conn.ConnectionID, conn.JID)
	}

	m.log.Infof("RestoreAll: restored %d/%d connection(s)", restored, len(conns))
	return nil
}

// lookupConn fetches the stored connection row (webhook URL, token, etc.).
func (m *Manager) lookupConn(connectionID string) *store.Conn {
	conns, err := m.store.ListConns(context.Background())
	if err != nil {
		m.log.Warnf("lookupConn %s: %v", connectionID, err)
		return nil
	}
	for i := range conns {
		if conns[i].ConnectionID == connectionID {
			return &conns[i]
		}
	}
	return nil
}

// setConnected updates the in-memory connection flag for a session.
func (m *Manager) setConnected(connectionID string, connected bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[connectionID]; ok {
		s.Connected = connected
		if connected {
			s.LastQR = ""
		}
	}
}

// remove deletes a session from the map (used to roll back failed connects).
func (m *Manager) remove(connectionID string) {
	m.mu.Lock()
	delete(m.sessions, connectionID)
	m.mu.Unlock()
}
