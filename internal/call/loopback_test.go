package call

import (
	"testing"

	"github.com/purpshell/meowcaller"
)

// O pipe ecoa: o que entra pelo sink sai pelo source.
func TestLoopbackEcho(t *testing.T) {
	p := newLoopbackPipe()
	in := make([]float32, meowcaller.FrameSamples)
	for i := range in {
		in[i] = 0.5
	}
	if err := p.WriteFrame(in); err != nil {
		t.Fatalf("WriteFrame erro: %v", err)
	}
	out, err := p.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame erro: %v", err)
	}
	if len(out) != meowcaller.FrameSamples || out[0] != 0.5 {
		t.Fatalf("eco errado: len=%d out[0]=%v", len(out), out[0])
	}
}

// Sem nada no buffer, o source devolve silêncio (mantém a cadência) — não bloqueia.
func TestLoopbackSilenceWhenEmpty(t *testing.T) {
	p := newLoopbackPipe()
	out, err := p.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame erro: %v", err)
	}
	if len(out) != meowcaller.FrameSamples {
		t.Fatalf("frame de silêncio com %d samples, esperado %d", len(out), meowcaller.FrameSamples)
	}
	for _, s := range out {
		if s != 0 {
			t.Fatalf("silêncio deveria ser zeros, achei %v", s)
		}
	}
}

// Após Close, ReadFrame retorna erro (EOF).
func TestLoopbackCloseEOF(t *testing.T) {
	p := newLoopbackPipe()
	_ = p.Close()
	if _, err := p.ReadFrame(); err == nil {
		t.Fatal("ReadFrame após Close deveria retornar EOF")
	}
}
