package call

import (
	"io"
	"math"
	"sync"

	"github.com/purpshell/meowcaller"
)

// ringbackSource é um meowcaller.AudioSource que gera o tom de chamada brasileiro
// (425 Hz, cadência 1 s tocando / 4 s em silêncio) em PCM mono 16 kHz. É tocado pro
// CHAMADOR enquanto a chamada recebida fica tocando no ring-all (antes de um atendente
// pegar), pra ele OUVIR "chamando" em vez de silêncio — já que o accept do WhatsApp
// acontece na hora (senão o relay de RX morre). Toca até Close (nunca EOF antes disso).
type ringbackSource struct {
	mu     sync.Mutex
	phase  float64
	pos    int64 // posição absoluta em amostras, pra cadência
	closed bool
}

const (
	ringbackFreq    = 425.0                          // Hz (padrão BR)
	ringbackOnSamp  = 1 * meowcaller.SampleRate      // 1 s tocando
	ringbackPeriod  = 5 * meowcaller.SampleRate      // ciclo 1 s on + 4 s off
)

func newRingbackSource() *ringbackSource { return &ringbackSource{} }

// ReadFrame devolve o próximo frame de FrameSamples (60 ms). Dentro do trecho "on"
// gera a senóide de 425 Hz; no "off" devolve silêncio. io.EOF só após Close.
func (r *ringbackSource) ReadFrame() ([]float32, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, io.EOF
	}
	frame := make([]float32, meowcaller.FrameSamples)
	step := 2 * math.Pi * ringbackFreq / float64(meowcaller.SampleRate)
	for i := range frame {
		if r.pos%ringbackPeriod < ringbackOnSamp {
			frame[i] = float32(0.25 * math.Sin(r.phase))
			r.phase += step
			if r.phase > 2*math.Pi {
				r.phase -= 2 * math.Pi
			}
		} else {
			frame[i] = 0
			r.phase = 0
		}
		r.pos++
	}
	return frame, nil
}

func (r *ringbackSource) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

var _ meowcaller.AudioSource = (*ringbackSource)(nil)
