package call

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"

	"github.com/purpshell/meowcaller"
)

// Recorder grava a conversa de uma chamada num único arquivo raw s16le 16kHz mono,
// mixando uplink (mic do atendente, o que ENVIAMOS) com downlink (voz do cliente, o
// que RECEBEMOS). A cadência de escrita é puxada pelo uplink: o meowcaller lê um frame
// da AudioSource a cada 60ms pra enviar, então a cada leitura mixamos com o último
// frame recebido do cliente e gravamos. Simples, em tempo real, sem depender do
// downlink ter cadência regular (silêncio do cliente = mixa com zero).
type Recorder struct {
	dir    string
	connID string

	mu       sync.Mutex
	f        *os.File
	path     string
	lastDown []float32
	closed   bool
}

// NewRecorder prepara o recorder mas NÃO abre arquivo ainda (o callID só é conhecido
// depois de colocar a chamada). Chame Begin(callID) antes do áudio começar a fluir.
func NewRecorder(dir, connID string) *Recorder {
	return &Recorder{dir: dir, connID: connID, lastDown: make([]float32, meowcaller.FrameSamples)}
}

// Begin abre o arquivo <dir>/<connID>/<callID>.s16le-16k.raw. Idempotente.
func (r *Recorder) Begin(callID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.f != nil || r.closed {
		return nil
	}
	d := filepath.Join(r.dir, r.connID)
	if err := os.MkdirAll(d, 0o755); err != nil {
		return err
	}
	p := filepath.Join(d, callID+".s16le-16k.raw")
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	r.f = f
	r.path = p
	return nil
}

// Path devolve o caminho do arquivo (vazio se Begin não rodou).
func (r *Recorder) Path() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.path
}

// Source embrulha o AudioSource (uplink): a cada frame lido pra enviar, mixa+grava.
func (r *Recorder) Source(src meowcaller.AudioSource) meowcaller.AudioSource {
	return &recSource{r: r, inner: src}
}

// Sink embrulha o AudioSink (downlink): guarda o último frame recebido do cliente.
func (r *Recorder) Sink(sink meowcaller.AudioSink) meowcaller.AudioSink {
	return &recSink{r: r, inner: sink}
}

func (r *Recorder) writeMixed(up []float32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.f == nil || r.closed {
		return
	}
	n := len(up)
	buf := make([]byte, n*2)
	for i := 0; i < n; i++ {
		var d float32
		if i < len(r.lastDown) {
			d = r.lastDown[i]
		}
		s := up[i] + d
		if s > 1 {
			s = 1
		} else if s < -1 {
			s = -1
		}
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(int16(s*32767)))
	}
	_, _ = r.f.Write(buf)
}

func (r *Recorder) setDown(frame []float32) {
	cp := make([]float32, len(frame))
	copy(cp, frame)
	r.mu.Lock()
	r.lastDown = cp
	r.mu.Unlock()
}

// Close fecha o arquivo (idempotente) e devolve o tamanho gravado em bytes.
func (r *Recorder) Close() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0
	}
	r.closed = true
	if r.f == nil {
		return 0
	}
	fi, _ := r.f.Stat()
	_ = r.f.Close()
	if fi != nil {
		return fi.Size()
	}
	return 0
}

type recSource struct {
	r     *Recorder
	inner meowcaller.AudioSource
}

func (s *recSource) ReadFrame() ([]float32, error) {
	f, err := s.inner.ReadFrame()
	if err == nil && len(f) > 0 {
		s.r.writeMixed(f)
	}
	return f, err
}

func (s *recSource) Close() error { return s.inner.Close() }

type recSink struct {
	r     *Recorder
	inner meowcaller.AudioSink
}

func (s *recSink) WriteFrame(frame []float32) error {
	s.r.setDown(frame)
	return s.inner.WriteFrame(frame)
}

func (s *recSink) Close() error { return s.inner.Close() }
