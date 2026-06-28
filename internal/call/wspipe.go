package call

import (
	"encoding/binary"
	"io"
	"sync"
	"time"

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
}

// NewWSPipe cria o pipe. onClient recebe cada frame da voz do cliente já em s16le
// (1920 bytes); pode ser nil em testes.
func NewWSPipe(onClient func([]byte)) *WSPipe {
	return &WSPipe{
		// Buffer raso (~480ms teto): sob clock-drift entre o mic do browser e o ritmo
		// do meowcaller, manter pouca folga evita o atraso crescer ao longo da chamada.
		in:       make(chan []float32, 8),
		onClient: onClient,
		closed:   make(chan struct{}),
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

// ReadFrame (AudioSource → cliente): próximo frame do mic, ou silêncio se vazio.
func (p *WSPipe) ReadFrame() ([]float32, error) {
	select {
	case <-p.closed:
		return nil, io.EOF
	case f := <-p.in:
		return f, nil
	case <-time.After(20 * time.Millisecond):
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
