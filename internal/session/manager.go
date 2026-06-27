package session

import (
	"context"
	"fmt"
	"sync"

	"github.com/nextflow/whatsmeow-gateway/internal/store"
	"github.com/nextflow/whatsmeow-gateway/internal/webhook"
	"go.mau.fi/whatsmeow"
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
}

// Manager owns all live sessions and their lifecycle.
type Manager struct {
	mu         sync.RWMutex
	sessions   map[string]*Session
	store      *store.Store
	dispatcher *webhook.Dispatcher
	log        waLog.Logger
}

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
	if s, ok := m.sessions[connectionID]; ok {
		m.mu.RUnlock()
		return s, nil
	}
	m.mu.RUnlock()

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
		qrChan, err := cli.GetQRChannel(ctx)
		if err != nil {
			m.remove(connectionID)
			return nil, fmt.Errorf("get qr channel: %w", err)
		}
		if err := cli.Connect(); err != nil {
			m.remove(connectionID)
			return nil, fmt.Errorf("connect: %w", err)
		}
		go m.consumeQR(connectionID, qrChan)
	} else {
		// Already paired → just connect.
		if err := cli.Connect(); err != nil {
			m.remove(connectionID)
			return nil, fmt.Errorf("connect: %w", err)
		}
		m.setConnected(connectionID, true)
	}

	return sess, nil
}

// consumeQR drains the QR channel, tracking the latest code and pairing result.
func (m *Manager) consumeQR(connectionID string, qrChan <-chan whatsmeow.QRChannelItem) {
	for evt := range qrChan {
		switch evt.Event {
		case "code":
			m.mu.Lock()
			if s, ok := m.sessions[connectionID]; ok {
				s.LastQR = evt.Code
				s.Connected = false
			}
			m.mu.Unlock()
		case "success":
			m.mu.Lock()
			if s, ok := m.sessions[connectionID]; ok {
				s.LastQR = ""
				s.Connected = true
			}
			m.mu.Unlock()
		default:
			// "timeout", "error", etc. — leave LastQR as-is; status reflects reality.
			m.log.Infof("QR channel for %s ended with event %q", connectionID, evt.Event)
		}
	}
}

// Get returns the session for connectionID, if any.
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
		case *events.Connected:
			m.setConnected(connID, true)
		case *events.LoggedOut:
			m.setConnected(connID, false)
		}
	})
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
