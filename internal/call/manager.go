package call

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types/events"
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

	pending     map[string]*meowcaller.Call            // chamadas RECEBIDAS seguradas, por callID
	callerPhone map[string]string                      // callID → telefone REAL do chamador (CallCreatorAlt)
	onIncoming  func(connID, callID, fromPhone string) // dispara webhook (setado pela API/main)
}

func NewManager() *Manager {
	return &Manager{
		clients:     make(map[string]*meowcaller.Client),
		active:      make(map[string]*meowcaller.Call),
		pending:     make(map[string]*meowcaller.Call),
		callerPhone: make(map[string]string),
		log:         waLog.Stdout("Call", "INFO", true),
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
func (m *Manager) place(ctx context.Context, connID string, wa *whatsmeow.Client, phone, label string, onState func(string)) (*meowcaller.Call, string, error) {
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
		if onState != nil && int(p) == 3 {
			onState("ringing")
		}
	})
	call.OnReady(func() {
		m.log.Infof("call %s READY — atendida (%s)", callID, label)
		if onState != nil {
			onState("ready")
		}
	})
	call.OnEnd(func(reason string) {
		m.log.Infof("call %s ENCERRADA (%s)", callID, reason)
		if onState != nil {
			onState("ended")
		}
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
	call, callID, err := m.place(ctx, connID, wa, phone, "mode="+mode, nil)
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
func (m *Manager) StartWithPipe(ctx context.Context, connID string, wa *whatsmeow.Client, phone string, src meowcaller.AudioSource, sink meowcaller.AudioSink, onState func(string)) (*meowcaller.Call, string, error) {
	call, callID, err := m.place(ctx, connID, wa, phone, "ws", onState)
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

// EnsureClient cria (se preciso) o cliente meowcaller da conexão e registra o handler de
// chamada RECEBIDA. Chamado quando a sessão CONECTA (não lazy) pra capturar inbound.
func (m *Manager) EnsureClient(connID string, wa *whatsmeow.Client) {
	m.mu.Lock()
	_, exists := m.clients[connID]
	m.mu.Unlock()
	if exists {
		return
	}

	// Captura o telefone REAL do chamador (CallCreatorAlt) — o meowcaller só expõe o LID
	// via Peer(). Registrar ANTES do clientFor (handler do meowcaller) pra rodar primeiro
	// e a chave já estar pronta quando o OnIncomingCall disparar.
	wa.AddEventHandler(func(evt any) {
		if co, ok := evt.(*events.CallOffer); ok && co.CallCreatorAlt.User != "" {
			m.mu.Lock()
			m.callerPhone[co.CallID] = co.CallCreatorAlt.User
			m.mu.Unlock()
		}
	})

	m.mu.Lock()
	mc := m.clientFor(connID, wa)
	m.mu.Unlock()

	mc.OnIncomingCall(func(call *meowcaller.Call) {
		callID := call.ID()
		m.mu.Lock()
		from := m.callerPhone[callID]
		delete(m.callerPhone, callID)
		m.pending[callID] = call
		m.mu.Unlock()
		if from == "" {
			from = call.Peer().User // fallback: LID se não veio o telefone real
		}
		m.log.Infof("INBOUND call %s de %s (conn %s) — segurando, ring-all", callID, from, connID)

		call.OnEnd(func(reason string) {
			m.mu.Lock()
			delete(m.pending, callID)
			m.mu.Unlock()
			m.log.Infof("INBOUND call %s encerrada (%s)", callID, reason)
		})

		if m.onIncoming != nil {
			m.onIncoming(connID, callID, from)
		}
		go func() {
			time.Sleep(45 * time.Second)
			m.mu.Lock()
			c, still := m.pending[callID]
			if still {
				delete(m.pending, callID)
			}
			m.mu.Unlock()
			if still {
				_ = c.Reject()
				m.log.Infof("INBOUND call %s rejeitada por timeout", callID)
			}
		}()
	})
}

// AcceptIncoming atende uma chamada recebida segurada e liga o áudio.
func (m *Manager) AcceptIncoming(callID string, src meowcaller.AudioSource, sink meowcaller.AudioSink, onState func(string)) (*meowcaller.Call, error) {
	m.mu.Lock()
	call, ok := m.pending[callID]
	if ok {
		delete(m.pending, callID)
	}
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("chamada %s nao esta tocando (ja atendida/encerrada)", callID)
	}
	if onState != nil {
		call.OnStateChange(func(p meowcaller.CallPhase) { m.log.Infof("call %s fase=%v", callID, p) })
		call.OnReady(func() { onState("ready") })
		// Avisa o navegador quando a chamada encerra (cliente desligou) — senão o modal
		// não fecha. Substitui o OnEnd do EnsureClient (a chamada já saiu do pending).
		call.OnEnd(func(reason string) {
			m.log.Infof("INBOUND call %s ENCERRADA (%s)", callID, reason)
			onState("ended")
		})
	}
	// Anexa source/sink ANTES do Answer pra o media loop pegar o áudio desde o início
	// (anexar depois do Answer fazia o inbound começar sem áudio / com atraso).
	call.Play(src)
	call.Receive(sink)
	if err := call.Answer(); err != nil {
		return nil, fmt.Errorf("answer: %w", err)
	}
	if onState != nil {
		onState("ready")
	}
	return call, nil
}

// RejectIncoming recusa uma chamada recebida que está tocando.
func (m *Manager) RejectIncoming(callID string) error {
	m.mu.Lock()
	call, ok := m.pending[callID]
	if ok {
		delete(m.pending, callID)
	}
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("chamada %s nao esta tocando", callID)
	}
	return call.Reject()
}

// SetOnIncoming registra o callback disparado quando chega uma chamada (dispara o webhook).
func (m *Manager) SetOnIncoming(fn func(connID, callID, fromPhone string)) { m.onIncoming = fn }
