package call

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Manager guarda 1 cliente meowcaller por conexão (connID) e a chamada ativa do PoC.
// meowcaller.NewClient instala handlers no *whatsmeow.Client, então é criado UMA vez
// por conexão e cacheado.
type Manager struct {
	mu       sync.Mutex
	clients  map[string]*meowcaller.Client
	clientWa map[string]*whatsmeow.Client // qual *whatsmeow.Client cada connID registrou — pra re-registrar handlers quando o wa TROCA (repareamento/reconexão)
	incomingWa map[string]*whatsmeow.Client // qual *whatsmeow.Client já teve o OnIncomingCall registrado — SEPARADO de clientWa (que o clientFor seta em ligação OUTBOUND sem registrar inbound). Sem isso, ligar (outbound) antes fazia o EnsureClient pular o registro do inbound → não tocava.
	active   map[string]*meowcaller.Call  // chamadas OUTBOUND ativas, por callID (várias simultâneas no mesmo número)
	log     waLog.Logger

	pending     map[string]*inboundCall                // chamadas RECEBIDAS (já atendidas no protocolo, tocando ringback), por callID
	held        map[string]bool                        // callID → em TRANSFERÊNCIA (hold): não derrubar a call quando o WS do atendente fechar
	callerPhone map[string]string                      // callID → telefone REAL do chamador (CallCreatorAlt)
	onIncoming  func(connID, callID, fromPhone string, isVideo bool) // dispara webhook IncomingCall (setado pela API/main)
	onCallEnded func(connID, callID string)            // dispara webhook CallEnded (para a UI de tocar)
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
		clientWa:    make(map[string]*whatsmeow.Client),
		incomingWa:  make(map[string]*whatsmeow.Client),
		active:      make(map[string]*meowcaller.Call),
		pending:     make(map[string]*inboundCall),
		held:        make(map[string]bool),
		callerPhone: make(map[string]string),
		log:         waLog.Stdout("Call", "INFO", true),
	}
}

// clientFor devolve (criando e cacheando) o cliente meowcaller pra essa conexão.
func (m *Manager) clientFor(connID string, wa *whatsmeow.Client) *meowcaller.Client {
	if c, ok := m.clients[connID]; ok && m.clientWa[connID] == wa {
		return c // mesmo wa → reusa o meowcaller cacheado
	}
	// wa novo (repareamento/reconexão) → cria um meowcaller NOVO ligado a ESTE wa (o antigo
	// ficou preso ao wa desconectado, e seus handlers não pegam os offers do cliente novo).
	// Logger interno do meowcaller em nível Info (default é Nop). Mostra os marcadores de
	// mídia ("connecting media", "inbound audio flowing", etc) sem o spam de trace
	// (per-frame "protected audio frame"/"sent relay packet").
	mcLog := zerolog.New(os.Stdout).Level(zerolog.InfoLevel).With().Timestamp().Logger()
	c := meowcaller.NewClient(wa, meowcaller.WithLogger(mcLog))
	m.clients[connID] = c
	m.clientWa[connID] = wa
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
		delete(m.held, callID)
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

// CallByID devolve a *Call ativa pelo callID (outbound em active, inbound em pending) —
// pra o WS de vídeo anexar o ReceiveVideo na MESMA chamada do áudio.
func (m *Manager) CallByID(callID string) *meowcaller.Call {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.active[callID]; ok {
		return c
	}
	if ic, ok := m.pending[callID]; ok {
		return ic.call
	}
	return nil
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
	// Só pula se ESTE MESMO wa já foi registrado. Ao repareaar/reconectar, o wa é um cliente
	// NOVO — precisa re-registrar o event handler + OnIncomingCall nele (senão a chamada
	// recebida decripta mas nunca dispara o "atendendo/ring" → não toca). Antes checava só o
	// connID e pulava, exigindo restart do serviço a cada repareamento.
	// Guard pelo incomingWa (NÃO pelo clientWa): o clientFor seta clientWa numa ligação
	// OUTBOUND sem registrar o OnIncomingCall. Se guardássemos por clientWa, ligar antes de
	// receber pulava o registro do inbound → decriptava mas não tocava. incomingWa só é setado
	// aqui, depois de registrar o handler de fato.
	m.mu.Lock()
	sameWa := m.incomingWa[connID] == wa
	m.mu.Unlock()
	if sameWa {
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
		// Captura da oferta crua pra alimentar o motor whatsapp.wasm offline (projeto da
		// ponte de chamada). SÓ grava arquivo — não altera o fluxo da chamada. Desligado por
		// padrão; ligar com WHATSMEOW_DUMP_CALL_STANZAS=1.
		if os.Getenv("WHATSMEOW_DUMP_CALL_STANZAS") == "1" {
			if co, ok := evt.(*events.CallOffer); ok {
				go dumpOfferStanza(m.log, co)
			}
		}
	})

	m.mu.Lock()
	mc := m.clientFor(connID, wa)
	m.incomingWa[connID] = wa // marca ANTES de registrar (síncrono) — evita registro duplicado
	m.mu.Unlock()

	mc.OnIncomingCall(func(call *meowcaller.Call) {
		callID := call.ID()
		// PoC vídeo inbound (env-gated): atende na hora, grava o vídeo do peer e manda um
		// clipe H.264 de teste em loop. Só pra validar a mídia de vídeo no relay. Gateado em
		// call.IsVideo() pra NÃO sequestrar as chamadas de áudio de produção durante o teste.
		if os.Getenv("WHATSMEOW_VIDEO_POC") == "1" && call.IsVideo() {
			m.log.Infof("[VIDEO-POC] INBOUND %s isVideo=%v — atendendo", callID, call.IsVideo())
			rec, err := meowcaller.AnnexBRecorder("/tmp/peer-video.h264")
			if err != nil {
				m.log.Infof("[VIDEO-POC] recorder erro: %v", err)
			} else {
				var n int
				call.ReceiveVideo(meowcaller.VideoSinkFunc(func(au []byte) {
					n++
					if n%30 == 1 {
						m.log.Infof("[VIDEO-POC] RX vídeo: %d access units (último %d bytes)", n, len(au))
					}
					_ = rec.WriteVideo(au)
				}))
			}
			call.OnVideoState(func(s meowcaller.VideoState) { m.log.Infof("[VIDEO-POC] videoState active=%v orient=%d", s.Active, s.Orientation) })
			call.Play(newRingbackSource())
			call.Receive(meowcaller.SinkFunc(func([]float32) {}))
			if err := call.Answer(); err != nil {
				m.log.Infof("[VIDEO-POC] answer erro: %v", err)
				return
			}
			go func() {
				clip, err := os.ReadFile("/tmp/testclip.h264")
				if err != nil {
					m.log.Infof("[VIDEO-POC] sem /tmp/testclip.h264: %v", err)
					return
				}
				aus := splitAnnexB(clip)
				m.log.Infof("[VIDEO-POC] clipe: %d access units", len(aus))
				time.Sleep(2 * time.Second)
				for i := 0; ; i++ {
					au := aus[i%len(aus)]
					if err := call.SendVideo(au); err != nil {
						m.log.Infof("[VIDEO-POC] SendVideo parou: %v", err)
						return
					}
					time.Sleep(66 * time.Millisecond)
				}
			}()
			return
		}
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
			delete(m.held, callID)
			onState := ic.onState
			m.mu.Unlock()
			m.log.Infof("INBOUND call %s encerrada (%s)", callID, reason)
			if onState != nil {
				onState("ended") // avisa o navegador (atendente já estava na linha)
			}
			// Avisa o NextFlow que a chamada acabou — pra parar de tocar na UI quando o
			// chamador cancela durante o ring (ninguém tinha atendido, onState é nil).
			if m.onCallEnded != nil {
				m.onCallEnded(connID, callID)
			}
		})
		// PREACCEPT + MÍDIA EAGER (estilo WaCalls): a mídia/relay tem que subir CEDO senão o
		// relay não brida a voz do chamador (RX) — confirmado: deferir pro pickup dava ZERO
		// pacote do peer. O <accept> continua deferido pro pickup (CommitAccept), então o
		// celular do chamador só "atende de verdade" quando o atendente pega. Tocamos ringback
		// e descartamos a voz do chamador até o pickup (AcceptIncoming troca o sink ao vivo).
		call.Play(newRingbackSource())
		call.Receive(meowcaller.SinkFunc(func([]float32) {}))
		if err := call.Answer(); err != nil {
			m.log.Infof("INBOUND call %s answer ERRO: %v", callID, err)
			m.mu.Lock()
			delete(m.pending, callID)
			m.mu.Unlock()
			return
		}
		if m.onIncoming != nil {
			m.onIncoming(connID, callID, from, call.IsVideo())
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
	m.log.Infof("INBOUND call %s ACEITA por atendente — troca ringback→navegador + <accept>", callID)
	// A mídia já subiu eager no OnIncomingCall (relay bridando desde o ring). Aqui só trocamos o
	// ringback/descarte pelo áudio do navegador (o loop lê fonte/sink a cada frame).
	ic.call.Play(src)
	ic.call.Receive(sink)
	// Pickup real: destrava o <accept> (dispara quando o mute_v2 chegar). Só AGORA o celular do
	// chamador para de tocar — o preaccept eager NÃO atende, só o <accept> atende de verdade.
	if err := ic.call.CommitAccept(); err != nil {
		m.log.Warnf("INBOUND call %s CommitAccept: %v", callID, err)
	}
	if onState != nil {
		onState("ready")
	}
	return ic.call, nil
}

// IsHeld diz se a chamada está em transferência (hold) — usado pelo WS de áudio pra NÃO
// derrubar a call quando o WS do atendente que está saindo fechar.
func (m *Manager) IsHeld(callID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.held[callID]
}

func (m *Manager) clearHeld(callID string) {
	m.mu.Lock()
	delete(m.held, callID)
	m.mu.Unlock()
}

// HoldForTransfer marca a chamada como em transferência: toca hold (ringback) pro cliente e
// impede que o próximo fechamento de WS a derrube. Guarda de 60s: se ninguém pegar, encerra.
func (m *Manager) HoldForTransfer(callID string) error {
	call := m.CallByID(callID)
	if call == nil {
		return fmt.Errorf("chamada %s nao esta ativa (transfer)", callID)
	}
	m.mu.Lock()
	m.held[callID] = true
	m.mu.Unlock()
	call.Play(newRingbackSource())
	call.Receive(meowcaller.SinkFunc(func([]float32) {}))
	m.log.Infof("TRANSFER call %s em HOLD (aguardando novo atendente)", callID)
	go func() {
		time.Sleep(60 * time.Second)
		if m.IsHeld(callID) {
			m.log.Infof("TRANSFER call %s — ninguem pegou em 60s, encerrando", callID)
			m.clearHeld(callID)
			_ = call.Hangup()
		}
	}()
	return nil
}

// AttachAudio re-liga um NOVO atendente (src/sink) a uma chamada JÁ ATIVA — é o coração da
// TRANSFERÊNCIA. Diferente do AcceptIncoming, NÃO re-atende (a call já foi aceita): só troca
// a fonte/sink ao vivo (o meowcaller lê a cada frame). Busca em active (outbound) e pending
// (inbound). Limpa o hold.
func (m *Manager) AttachAudio(callID string, src meowcaller.AudioSource, sink meowcaller.AudioSink, onState func(string)) (*meowcaller.Call, error) {
	m.mu.Lock()
	var call *meowcaller.Call
	if c, ok := m.active[callID]; ok {
		call = c
	} else if ic, ok := m.pending[callID]; ok {
		call = ic.call
		ic.onState = onState // o OnEnd (setado no EnsureClient) avisa o NOVO atendente ao encerrar
	}
	delete(m.held, callID) // novo atendente pegou → sai do hold
	m.mu.Unlock()
	if call == nil {
		return nil, fmt.Errorf("chamada %s nao esta ativa (transfer attach)", callID)
	}
	m.log.Infof("TRANSFER call %s — novo atendente plugado (attach)", callID)
	call.Play(src)
	call.Receive(sink)
	if onState != nil {
		onState("ready")
	}
	return call, nil
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
func (m *Manager) SetOnIncoming(fn func(connID, callID, fromPhone string, isVideo bool)) { m.onIncoming = fn }

// SetOnCallEnded registra o callback disparado quando uma chamada recebida encerra
// (chamador cancelou / desligou) — pra parar de tocar na UI.
func (m *Manager) SetOnCallEnded(fn func(connID, callID string)) { m.onCallEnded = fn }

// splitAnnexB quebra um stream H.264 Annex-B em access units pelo start code 00 00 00 01.
func splitAnnexB(b []byte) [][]byte {
	var out [][]byte
	start := -1
	for i := 0; i+3 < len(b); i++ {
		if b[i] == 0 && b[i+1] == 0 && b[i+2] == 0 && b[i+3] == 1 {
			if start >= 0 {
				out = append(out, b[start:i])
			}
			start = i
			i += 3
		}
	}
	if start >= 0 && start < len(b) {
		out = append(out, b[start:])
	}
	return out
}

// dumpOfferStanza grava a oferta crua no formato que o whatsapp.wasm consome
// (handleIncomingSignalingOffer): payload = base64 do nó binário + metadados do peer.
// Best-effort e fora do caminho da chamada — qualquer erro aqui só vira log.
func dumpOfferStanza(log waLog.Logger, co *events.CallOffer) {
	defer func() { _ = recover() }()
	dir := "/var/lib/whatsmeow-gateway/callstanzas"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	var payload string
	if co.Data != nil {
		if raw, err := waBinary.Marshal(*co.Data); err == nil {
			payload = base64.StdEncoding.EncodeToString(raw)
		}
	}
	rec := map[string]any{
		"callId":         co.CallID,
		"payload":        payload,
		"peerJid":        co.From.String(),
		"callCreator":    co.CallCreator.String(),
		"callCreatorAlt": co.CallCreatorAlt.String(),
		"peerPlatform":   co.RemotePlatform,
		"peerAppVersion": co.RemoteVersion,
		"timestamp":      co.Timestamp.UTC().Format(time.RFC3339),
		"capturadoEm":    time.Now().UTC().Format(time.RFC3339),
	}
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return
	}
	f := filepath.Join(dir, co.CallID+".json")
	if err := os.WriteFile(f, b, 0o644); err == nil {
		log.Infof("[DUMP] oferta gravada em %s (%d bytes de payload)", f, len(payload))
	}
}
