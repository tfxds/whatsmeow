package call

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"

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
	active  map[string]*meowcaller.Call // chamadas OUTBOUND ativas, por callID (várias simultâneas no mesmo número)
	log     waLog.Logger

	pending     map[string]*inboundCall                // chamadas RECEBIDAS (já atendidas no protocolo, tocando ringback), por callID
	callerPhone map[string]string                      // callID → telefone REAL do chamador (CallCreatorAlt)
	onIncoming  func(connID, callID, fromPhone string) // dispara webhook (setado pela API/main)
}

// inboundCall guarda uma chamada recebida que já foi atendida no protocolo (Answer
// imediato, tocando ringback pro chamador) e está esperando um atendente pegar. Quando
// um atendente atende, onState é setado (pra avisar o navegador) e accepted vira true
// (cancela o timeout de "ninguém atendeu").
type inboundCall struct {
	call     *meowcaller.Call
	onState  func(string)
	accepted bool
}

func NewManager() *Manager {
	return &Manager{
		clients:     make(map[string]*meowcaller.Client),
		active:      make(map[string]*meowcaller.Call),
		pending:     make(map[string]*inboundCall),
		callerPhone: make(map[string]string),
		log:         waLog.Stdout("Call", "INFO", true),
	}
}

// clientFor devolve (criando e cacheando) o cliente meowcaller pra essa conexão.
func (m *Manager) clientFor(connID string, wa *whatsmeow.Client) *meowcaller.Client {
	if c, ok := m.clients[connID]; ok {
		return c
	}
	// Logger interno do meowcaller em nível Info (default é Nop). Mostra os marcadores de
	// mídia ("connecting media", "inbound audio flowing", etc) sem o spam de trace
	// (per-frame "protected audio frame"/"sent relay packet").
	mcLog := zerolog.New(os.Stdout).Level(zerolog.InfoLevel).With().Timestamp().Logger()
	c := meowcaller.NewClient(wa, meowcaller.WithLogger(mcLog))
	m.clients[connID] = c
	return c
}

// place coloca a chamada, registra callbacks/guards e marca como ativa. label só p/ log.
func (m *Manager) place(ctx context.Context, connID string, wa *whatsmeow.Client, phone, label string, onState func(string)) (*meowcaller.Call, string, error) {
	m.mu.Lock()
	mc := m.clientFor(connID, wa)
	m.mu.Unlock()

	call, err := mc.Call(ctx, phone)
	if err != nil {
		return nil, "", fmt.Errorf("place call: %w", err)
	}
	callID := call.ID()
	// Indexa por callID (NÃO por connID): vários atendentes ligam pelo mesmo número
	// (mesmo connID) ao mesmo tempo, cada chamada independente. Não derruba as outras.
	m.mu.Lock()
	m.active[callID] = call
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
		delete(m.active, callID)
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

// Hangup encerra uma chamada ativa pelo callID (cada outbound é independente).
func (m *Manager) Hangup(callID string) error {
	m.mu.Lock()
	call, ok := m.active[callID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("nenhuma chamada ativa %s", callID)
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
		ic := &inboundCall{call: call}
		m.pending[callID] = ic
		m.mu.Unlock()
		if from == "" {
			from = call.Peer().User // fallback: LID se não veio o telefone real
		}
		m.log.Infof("INBOUND call %s de %s (conn %s) — atendendo no protocolo (ringback) + ring-all", callID, from, connID)

		// ATENDE NA HORA no protocolo: o WhatsApp exige o accept dentro de uma janela
		// curta, senão o relay para de bridar a mídia do chamador (RX). Tocamos ringback
		// pro chamador OUVIR "chamando" e descartamos a voz dele enquanto nenhum atendente
		// pegou. Quando um atendente atende, AcceptIncoming troca a fonte/sink ao vivo.
		call.OnEnd(func(reason string) {
			m.mu.Lock()
			delete(m.pending, callID)
			onState := ic.onState
			m.mu.Unlock()
			m.log.Infof("INBOUND call %s encerrada (%s)", callID, reason)
			if onState != nil {
				onState("ended") // avisa o navegador (atendente já estava na linha)
			}
		})
		call.Play(newRingbackSource())
		call.Receive(meowcaller.SinkFunc(func([]float32) {})) // descarta o áudio do chamador enquanto toca o ringback
		if err := call.Answer(); err != nil {
			m.log.Infof("INBOUND call %s answer ERRO: %v", callID, err)
			m.mu.Lock()
			delete(m.pending, callID)
			m.mu.Unlock()
			return
		}

		if m.onIncoming != nil {
			m.onIncoming(connID, callID, from)
		}
		// Timeout: ninguém atendeu em 30 s → desliga (só se ainda não foi aceita).
		go func() {
			time.Sleep(30 * time.Second)
			m.mu.Lock()
			c, still := m.pending[callID]
			if still && !c.accepted {
				delete(m.pending, callID)
			} else {
				still = false
			}
			m.mu.Unlock()
			if still {
				_ = c.call.Hangup()
				m.log.Infof("INBOUND call %s desligada por timeout (ninguém atendeu)", callID)
			}
		}()
	})
}

// AcceptIncoming liga um atendente a uma chamada recebida que JÁ está atendida no
// protocolo (tocando ringback). Não chama Answer — só troca ao vivo a fonte/sink do
// ringback/descarte pro áudio do navegador (mic do atendente ↔ voz do chamador). O
// meowcaller lê a fonte/sink a cada frame, então a troca é imediata.
func (m *Manager) AcceptIncoming(callID string, src meowcaller.AudioSource, sink meowcaller.AudioSink, onState func(string)) (*meowcaller.Call, error) {
	m.mu.Lock()
	ic, ok := m.pending[callID]
	if ok {
		ic.accepted = true     // cancela o timeout de "ninguém atendeu"
		ic.onState = onState   // o OnEnd (setado no EnsureClient) avisa o navegador ao encerrar
	}
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("chamada %s nao esta tocando (ja atendida/encerrada)", callID)
	}
	m.log.Infof("INBOUND call %s ACEITA por atendente — trocando ringback→navegador", callID)
	// Troca ao vivo: para o ringback/descarte e conecta o áudio do navegador.
	ic.call.Play(src)
	ic.call.Receive(sink)
	if onState != nil {
		onState("ready")
	}
	return ic.call, nil
}

// RejectIncoming recusa uma chamada recebida. Como ela já foi atendida no protocolo
// (ringback tocando), recusar = desligar.
func (m *Manager) RejectIncoming(callID string) error {
	m.mu.Lock()
	ic, ok := m.pending[callID]
	if ok {
		delete(m.pending, callID)
	}
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("chamada %s nao esta tocando", callID)
	}
	return ic.call.Hangup()
}

// SetOnIncoming registra o callback disparado quando chega uma chamada (dispara o webhook).
func (m *Manager) SetOnIncoming(fn func(connID, callID, fromPhone string)) { m.onIncoming = fn }
