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

// Start coloca uma chamada outbound de áudio pra phone (só dígitos) e anexa o áudio
// conforme o mode: "loopback" ecoa a voz do cliente de volta (prova codec+latência),
// qualquer outro valor toca o tom de teste 440Hz + loga RMS do recebido. Retorna o ID.
func (m *Manager) Start(ctx context.Context, connID string, wa *whatsmeow.Client, phone, mode string) (string, error) {
	m.mu.Lock()
	mc := m.clientFor(connID, wa)
	prev := m.active[connID]
	m.mu.Unlock()

	// Se já houver uma chamada ativa nessa conexão, encerra antes (senão a antiga fica
	// órfã tocando o tom e o OnEnd dela apagaria a entrada da nova).
	if prev != nil {
		_ = prev.Hangup()
	}

	call, err := mc.Call(ctx, phone)
	if err != nil {
		return "", fmt.Errorf("place call: %w", err)
	}

	callID := call.ID()
	m.mu.Lock()
	m.active[connID] = call
	m.mu.Unlock()

	call.OnStateChange(func(p meowcaller.CallPhase) {
		m.log.Infof("call %s fase=%v", callID, p)
	})
	call.OnReady(func() {
		m.log.Infof("call %s READY — atendida (mode=%s)", callID, mode)
	})
	call.OnEnd(func(reason string) {
		m.log.Infof("call %s ENCERRADA (%s)", callID, reason)
		m.mu.Lock()
		// Só apaga se ainda for ESTA chamada (OnEnd pode disparar 2x, e uma nova
		// chamada pode ter assumido a entrada).
		if m.active[connID] == call {
			delete(m.active, connID)
		}
		m.mu.Unlock()
	})

	if mode == "loopback" {
		// Eco: o mesmo pipe é source e sink — a voz do cliente volta pra ele.
		pipe := newLoopbackPipe()
		call.Play(pipe)
		call.Receive(pipe)
	} else {
		// Tom de teste (uplink) + sink que loga RMS do recebido (downlink) ~1x/s.
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
	}

	m.log.Infof("call %s INICIADA para %s (conn %s, mode=%s)", callID, phone, connID, mode)
	return callID, nil
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
