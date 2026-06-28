package call

import (
	"io"
	"sync"
	"time"

	"github.com/purpshell/meowcaller"
)

// loopbackPipe implementa AudioSource E AudioSink ao mesmo tempo: o áudio recebido
// do cliente (sink) é ecoado de volta como áudio enviado (source). Serve pra PROVAR,
// com uma pessoa só, a qualidade do codec MLow + a latência de ida-e-volta — o
// usuário liga, fala, e ouve a própria voz com o atraso real do caminho.
//
// O canal é bufferizado e DESCARTA quando cheio: assim o eco não acumula atraso
// crescente (preferimos perder um frame velho a ir somando latência).
type loopbackPipe struct {
	frames chan []float32
	closed chan struct{}
	once   sync.Once
}

func newLoopbackPipe() *loopbackPipe {
	return &loopbackPipe{
		frames: make(chan []float32, 50), // ~3s de folga; cheio = descarta
		closed: make(chan struct{}),
	}
}

// WriteFrame (AudioSink): recebe um frame do cliente e enfileira pra ecoar. Copia o
// frame porque o buffer do meowcaller pode ser reusado depois do retorno. Não bloqueia:
// se o buffer estiver cheio, descarta este frame (mantém a latência baixa).
func (p *loopbackPipe) WriteFrame(frame []float32) error {
	cp := make([]float32, len(frame))
	copy(cp, frame)
	select {
	case p.frames <- cp:
	default:
	}
	return nil
}

// ReadFrame (AudioSource): devolve o próximo frame ecoado; se não houver nada pronto,
// devolve silêncio (frame de zeros) pra manter a cadência de 60 ms sem travar o loop
// de envio do meowcaller. io.EOF após Close.
func (p *loopbackPipe) ReadFrame() ([]float32, error) {
	select {
	case <-p.closed:
		return nil, io.EOF
	case f := <-p.frames:
		return f, nil
	case <-time.After(20 * time.Millisecond):
		return make([]float32, meowcaller.FrameSamples), nil
	}
}

func (p *loopbackPipe) Close() error {
	p.once.Do(func() { close(p.closed) })
	return nil
}

var (
	_ meowcaller.AudioSource = (*loopbackPipe)(nil)
	_ meowcaller.AudioSink   = (*loopbackPipe)(nil)
)
