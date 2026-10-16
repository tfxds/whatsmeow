package asterisk

import (
	"encoding/binary"
	"math"
	"testing"
)

// Tom de 1 kHz a 16 kHz passa pelo decimador sem perder volume (só o 0,85) e um tom de 6 kHz
// (acima do corte de 3,6 kHz) é cortado; um pico no teto não passa de 31.000.
func gerar(freq float64, amp float64, n int) []byte {
	b := make([]byte, n*2)
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint16(b[i*2:], uint16(int16(amp*math.Sin(2*math.Pi*freq*float64(i)/16000))))
	}
	return b
}
func rms(b []byte) float64 {
	s := 0.0
	n := len(b) / 2
	for i := n / 4; i < n; i++ { // ignora o começo (filtro enchendo)
		v := float64(int16(binary.LittleEndian.Uint16(b[i*2:])))
		s += v * v
	}
	return math.Sqrt(s / float64(n-n/4))
}
func pico(b []byte) int {
	m := 0
	for i := 0; i+1 < len(b); i += 2 {
		v := int(int16(binary.LittleEndian.Uint16(b[i:])))
		if v < 0 {
			v = -v
		}
		if v > m {
			m = v
		}
	}
	return m
}
func TestDecimador(t *testing.T) {
	d := &Decimador16to8{}
	baixo := d.Processar(gerar(1000, 10000, 3200))
	if r := rms(baixo) / (10000 / math.Sqrt2); r < 0.75 || r > 0.95 {
		t.Fatalf("1 kHz deveria passar com ~0,85, passou %.2f", r)
	}
	d2 := &Decimador16to8{}
	alto := d2.Processar(gerar(6000, 10000, 3200))
	if r := rms(alto) / (10000 / math.Sqrt2); r > 0.1 {
		t.Fatalf("6 kHz deveria ser cortado, passou %.2f", r)
	}
	d3 := &Decimador16to8{}
	if p := pico(d3.Processar(gerar(800, 32767, 3200))); p > 31000 {
		t.Fatalf("pico deveria ficar abaixo de 31.000, deu %d", p)
	}
}
