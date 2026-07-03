package call

import (
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/purpshell/meowcaller"
)

// WSPipe liga um WebSocket ao áudio de uma chamada: como AudioSource entrega os frames
// que CHEGAM do navegador (mic do atendente); como AudioSink recebe a voz do cliente e
// repassa via callback onClient (que escreve no WS) já em s16le.
type WSPipe struct {
	in       chan []float32
	onClient func([]byte)
	closed   chan struct{}
	once     sync.Once
	mu       sync.Mutex
	primed   bool // jitter buffer: só começa a drenar depois de acumular p.jitter frames
	jitter   int  // frames a acumular antes de drenar (env WSPIPE_JITTER; default 2). Cada ≈60ms
}

// Defaults do jitter buffer (browser do NextFlow, produção 225). Tunáveis por env — no NexCall
// (AudioSocket) dá pra baixar WSPIPE_JITTER/WSPIPE_BUFFER pra reduzir a latência atendente→cliente.
// Manter o áudio CONTÍNUO (sem silêncio no meio) evita o WhatsApp do celular crescer a jitter buffer.
const (
	jitterDefault = 2 // 120ms de folga
	bufferDefault = 4 // teto ~240ms (drop-oldest)
)

func wspipeEnvInt(name string, def, min int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= min {
			return n
		}
	}
	return def
}

// NewWSPipe cria o pipe. onClient recebe cada frame da voz do cliente já em s16le
// (1920 bytes); pode ser nil em testes.
func NewWSPipe(onClient func([]byte)) *WSPipe {
	return &WSPipe{
		in:       make(chan []float32, wspipeEnvInt("WSPIPE_BUFFER", bufferDefault, 1)),
		onClient: onClient,
		closed:   make(chan struct{}),
		jitter:   wspipeEnvInt("WSPIPE_JITTER", jitterDefault, 0),
	}
}

// PushMic recebe um frame s16le do browser (mic), converte e enfileira pra tocar na
// chamada. Se o buffer estiver cheio, descarta o frame mais VELHO e coloca o novo —
// assim a chamada fica sempre no áudio ATUAL (latência baixa) em vez de acumular atraso.
func (p *WSPipe) PushMic(s16 []byte) {
	f := s16leToFloat32(s16)
	if f == nil {
		return
	}
	select {
	case p.in <- f:
	default:
		select {
		case <-p.in: // descarta o mais velho
		default:
		}
		select {
		case p.in <- f:
		default:
		}
	}
}

// ReadFrame (AudioSource → cliente): alimenta o meowcaller com jitter buffer + prime.
// Enquanto não acumular jitterTarget frames, devolve silêncio (priming). Depois drena
// de forma contínua; se esvaziar (underrun), volta a primar — evita inserir silêncio
// no meio do áudio, que faria o WhatsApp do celular bufferizar mais (atraso crescente).
func (p *WSPipe) ReadFrame() ([]float32, error) {
	select {
	case <-p.closed:
		return nil, io.EOF
	default:
	}
	p.mu.Lock()
	if !p.primed {
		if len(p.in) >= p.jitter {
			p.primed = true
		} else {
			p.mu.Unlock()
			return make([]float32, meowcaller.FrameSamples), nil // priming: silêncio
		}
	}
	p.mu.Unlock()

	select {
	case <-p.closed:
		return nil, io.EOF
	case f := <-p.in:
		return f, nil
	default:
		// underrun → silêncio e re-prima (reabastece antes de drenar de novo)
		p.mu.Lock()
		p.primed = false
		p.mu.Unlock()
		return make([]float32, meowcaller.FrameSamples), nil
	}
}

// WriteFrame (AudioSink ← cliente): converte pra s16le e manda pro WS via onClient.
func (p *WSPipe) WriteFrame(frame []float32) error {
	select {
	case <-p.closed:
		return nil
	default:
	}
	if p.onClient != nil {
		p.onClient(float32ToS16le(frame))
	}
	return nil
}

func (p *WSPipe) Close() error {
	p.once.Do(func() { close(p.closed) })
	return nil
}

func s16leToFloat32(b []byte) []float32 {
	n := len(b) / 2
	if n == 0 {
		return nil
	}
	out := make([]float32, n)
	for i := 0; i < n; i++ {
		s := int16(binary.LittleEndian.Uint16(b[i*2:]))
		out[i] = float32(s) / 32768
	}
	return out
}

func float32ToS16le(f []float32) []byte {
	out := make([]byte, len(f)*2)
	for i, v := range f {
		if v > 1 {
			v = 1
		} else if v < -1 {
			v = -1
		}
		var s int16
		if v < 0 {
			s = int16(v * 0x8000)
		} else {
			s = int16(v * 0x7fff)
		}
		binary.LittleEndian.PutUint16(out[i*2:], uint16(s))
	}
	return out
}

var (
	_ meowcaller.AudioSource = (*WSPipe)(nil)
	_ meowcaller.AudioSink   = (*WSPipe)(nil)
)
