package call

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Manager guarda 1 cliente meowcaller por conexão (connID) e a chamada ativa do PoC.
// meowcaller.NewClient instala handlers no *whatsmeow.Client, então é criado UMA vez
// por conexão e cacheado.
type Manager struct {
	mu      sync.Mutex
	clients map[string]*meowcaller.Client
	active  map[string]*meowcaller.Call
	log     waLog.Logger
}

func NewManager() *Manager {
	return &Manager{
		clients: make(map[string]*meowcaller.Client),
		active:  make(map[string]*meowcaller.Call),
		log:     waLog.Stdout("Call", "INFO", true),
	}
}

// clientFor devolve (criando e cacheando) o cliente meowcaller pra essa conexão.
func (m *Manager) clientFor(connID string, wa *whatsmeow.Client) *meowcaller.Client {
	if c, ok := m.clients[connID]; ok {
		return c
	}
	c := meowcaller.NewClient(wa)
	m.clients[connID] = c
	return c
}

// place coloca a chamada, registra callbacks/guards e marca como ativa. label só p/ log.
func (m *Manager) place(ctx context.Context, connID string, wa *whatsmeow.Client, phone, label string) (*meowcaller.Call, string, error) {
	m.mu.Lock()
	mc := m.clientFor(connID, wa)
	prev := m.active[connID]
	m.mu.Unlock()
	if prev != nil {
		_ = prev.Hangup()
	}

	call, err := mc.Call(ctx, phone)
	if err != nil {
		return nil, "", fmt.Errorf("place call: %w", err)
	}
	callID := call.ID()
	m.mu.Lock()
	m.active[connID] = call
	m.mu.Unlock()

	call.OnStateChange(func(p meowcaller.CallPhase) {
		m.log.Infof("call %s fase=%v", callID, p)
	})
	call.OnReady(func() {
		m.log.Infof("call %s READY — atendida (%s)", callID, label)
	})
	call.OnEnd(func(reason string) {
		m.log.Infof("call %s ENCERRADA (%s)", callID, reason)
		m.mu.Lock()
		if m.active[connID] == call {
			delete(m.active, connID)
		}
		m.mu.Unlock()
	})

	m.log.Infof("call %s INICIADA para %s (conn %s, %s)", callID, phone, connID, label)
	return call, callID, nil
}

// Start coloca uma chamada e anexa áudio conforme o mode: "loopback" ecoa a voz do
// cliente; qualquer outro toca o tom 440Hz + loga RMS do recebido.
func (m *Manager) Start(ctx context.Context, connID string, wa *whatsmeow.Client, phone, mode string) (string, error) {
	call, callID, err := m.place(ctx, connID, wa, phone, "mode="+mode)
	if err != nil {
		return "", err
	}
	if mode == "loopback" {
		pipe := newLoopbackPipe()
		call.Play(pipe)
		call.Receive(pipe)
		return callID, nil
	}
	call.Play(newToneSource(440))
	var frames int
	var sumsq float64
	call.Receive(meowcaller.SinkFunc(func(pcm []float32) {
		for _, s := range pcm {
			sumsq += float64(s) * float64(s)
		}
		frames++
		if frames >= 17 {
			rms := math.Sqrt(sumsq / float64(frames*meowcaller.FrameSamples))
			m.log.Infof("call %s áudio recebido: %d frames, RMS=%.4f", callID, frames, rms)
			frames = 0
			sumsq = 0
		}
	}))
	return callID, nil
}

// StartWithPipe coloca a chamada usando AudioSource/AudioSink externos (ex: WebSocket).
// Retorna a *Call pra o caller poder dar Hangup quando o WS fechar.
func (m *Manager) StartWithPipe(ctx context.Context, connID string, wa *whatsmeow.Client, phone string, src meowcaller.AudioSource, sink meowcaller.AudioSink) (*meowcaller.Call, string, error) {
	call, callID, err := m.place(ctx, connID, wa, phone, "ws")
	if err != nil {
		return nil, "", err
	}
	call.Play(src)
	call.Receive(sink)
	return call, callID, nil
}

// Hangup encerra a chamada ativa da conexão (se houver).
func (m *Manager) Hangup(connID string) error {
	m.mu.Lock()
	call, ok := m.active[connID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("nenhuma chamada ativa pra conexão %s", connID)
	}
	return call.Hangup()
}
