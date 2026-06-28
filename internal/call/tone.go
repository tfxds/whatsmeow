// Package call faz o PoC de chamada WhatsApp via meowcaller no gateway.
package call

import (
	"io"
	"math"
	"sync"

	"github.com/purpshell/meowcaller"
)

// toneSource é um meowcaller.AudioSource que gera uma senóide contínua (tom de teste)
// em PCM mono 16 kHz, frames de FrameSamples. Toca até Close (nunca EOF antes disso).
type toneSource struct {
	mu     sync.Mutex
	freq   float64
	phase  float64
	closed bool
}

func newToneSource(freqHz float64) *toneSource {
	return &toneSource{freq: freqHz}
}

// ReadFrame devolve o próximo frame de FrameSamples (60 ms) da senóide. Amplitude 0.3
// pra não estourar. io.EOF só após Close.
func (t *toneSource) ReadFrame() ([]float32, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, io.EOF
	}
	frame := make([]float32, meowcaller.FrameSamples)
	step := 2 * math.Pi * t.freq / float64(meowcaller.SampleRate)
	for i := range frame {
		frame[i] = float32(0.3 * math.Sin(t.phase))
		t.phase += step
		if t.phase > 2*math.Pi {
			t.phase -= 2 * math.Pi
		}
	}
	return frame, nil
}

func (t *toneSource) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}

// garante em tempo de compilação que toneSource implementa a interface.
var _ meowcaller.AudioSource = (*toneSource)(nil)
