package call

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/purpshell/meowcaller"
)

// Recorder grava a conversa de uma chamada num único arquivo raw s16le 16kHz mono,
// mixando uplink (mic do atendente, o que ENVIAMOS) com downlink (voz do cliente, o
// que RECEBEMOS).
//
// ⚠️ A cadência de escrita é de RELÓGIO PRÓPRIO (um tick a cada 60 ms), não do uplink.
// Antes era puxada pela leitura do uplink ("a cada frame lido pra enviar, mixa e grava"),
// e isso rendeu dois defeitos que o Thiago ouviu como um "treeee", tipo fita rebobinando
// (24/09/2026):
//
//  1. DURAÇÃO ENCURTADA. Se o uplink atrasa ou para de ser lido (mic do navegador
//     engasgando, backpressure do websocket), nenhum frame era escrito naquele tempo. A
//     gravação saía MENOR que a ligação e, na hora de ouvir, a conversa salta pra frente.
//     Medido nas 31 gravações existentes: 10 delas (32%) com menos áudio que a ligação, e
//     uma de 100 s virou 66 s — 34 segundos que desapareceram.
//  2. FRAME REPETIDO EM LOOP. O último frame do cliente ficava guardado e era mixado de
//     novo a cada escrita. Cliente calado = o MESMO trecho de 60 ms repetindo, o que soa
//     como um zumbido/tom contínuo em vez de silêncio.
//
// Agora o tick grava sempre, no tempo certo, e cada frame é CONSUMIDO: sem frame novo, o
// lado fica em silêncio de verdade (zeros). Assim a duração do .ogg bate com a da ligação,
// que é o que também permite conferir depois se houve perda.
type Recorder struct {
	dir    string
	connID string

	mu        sync.Mutex
	f         *os.File
	path      string
	lastUp    []float32
	lastDown  []float32
	upFresh   bool
	downFresh bool
	closed    bool
	stop      chan struct{}
}

// frameDur = duração de um frame (60 ms para 960 samples a 16 kHz).
const frameDur = time.Duration(meowcaller.FrameSamples) * time.Second / meowcaller.SampleRate

// NewRecorder prepara o recorder mas NÃO abre arquivo ainda (o callID só é conhecido
// depois de colocar a chamada). Chame Begin(callID) antes do áudio começar a fluir.
func NewRecorder(dir, connID string) *Recorder {
	return &Recorder{
		dir:      dir,
		connID:   connID,
		lastUp:   make([]float32, meowcaller.FrameSamples),
		lastDown: make([]float32, meowcaller.FrameSamples),
		stop:     make(chan struct{}),
	}
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
	go r.loop()
	return nil
}

// loop grava um frame a cada 60 ms enquanto a chamada estiver de pé. É ELE que define a
// duração da gravação, e é por isso que o áudio passa a ter o mesmo tamanho da ligação.
func (r *Recorder) loop() {
	tk := time.NewTicker(frameDur)
	defer tk.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-tk.C:
			r.writeTick()
		}
	}
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

func (r *Recorder) writeTick() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.f == nil || r.closed {
		return
	}
	n := meowcaller.FrameSamples
	buf := make([]byte, n*2)
	for i := 0; i < n; i++ {
		var u, d float32
		// Frame só vale UMA vez. Sem frame novo naquele lado, entra silêncio — senão o
		// último trecho fica repetindo e vira zumbido.
		if r.upFresh && i < len(r.lastUp) {
			u = r.lastUp[i]
		}
		if r.downFresh && i < len(r.lastDown) {
			d = r.lastDown[i]
		}
		s := u + d
		if s > 1 {
			s = 1
		} else if s < -1 {
			s = -1
		}
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(int16(s*32767)))
	}
	r.upFresh = false
	r.downFresh = false
	_, _ = r.f.Write(buf)
}

func (r *Recorder) setUp(frame []float32) {
	cp := make([]float32, len(frame))
	copy(cp, frame)
	r.mu.Lock()
	r.lastUp = cp
	r.upFresh = true
	r.mu.Unlock()
}

func (r *Recorder) setDown(frame []float32) {
	cp := make([]float32, len(frame))
	copy(cp, frame)
	r.mu.Lock()
	r.lastDown = cp
	r.downFresh = true
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
	close(r.stop)
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
		s.r.setUp(f)
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
