package call

import (
	"testing"

	"github.com/purpshell/meowcaller"
)

func TestWSPipeMicToReadFrame(t *testing.T) {
	p := NewWSPipe(nil)
	b := make([]byte, meowcaller.FrameSamples*2)
	for i := 0; i < meowcaller.FrameSamples; i++ {
		b[i*2] = 0xFF
		b[i*2+1] = 0x3F // 0x3FFF = 16383 ~ 0.5
	}
	// Prima o jitter buffer (jitterTarget frames) antes de drenar.
	for i := 0; i < jitterTarget; i++ {
		p.PushMic(b)
	}
	f, err := p.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame erro: %v", err)
	}
	if len(f) != meowcaller.FrameSamples {
		t.Fatalf("len=%d esperado %d", len(f), meowcaller.FrameSamples)
	}
	if f[0] < 0.49 || f[0] > 0.51 {
		t.Fatalf("conversao s16->f32 errada: %f (esperado ~0.5)", f[0])
	}
}

func TestWSPipeSilenceWhenEmpty(t *testing.T) {
	p := NewWSPipe(nil)
	f, err := p.ReadFrame()
	if err != nil || len(f) != meowcaller.FrameSamples {
		t.Fatalf("esperava silencio de %d samples, got len=%d err=%v", meowcaller.FrameSamples, len(f), err)
	}
	for _, s := range f {
		if s != 0 {
			t.Fatalf("silencio deveria ser zero, got %v", s)
		}
	}
}

func TestWSPipeClientToWS(t *testing.T) {
	var got []byte
	p := NewWSPipe(func(s16 []byte) { got = s16 })
	frame := make([]float32, meowcaller.FrameSamples)
	frame[0] = 0.5
	if err := p.WriteFrame(frame); err != nil {
		t.Fatalf("WriteFrame erro: %v", err)
	}
	if len(got) != meowcaller.FrameSamples*2 {
		t.Fatalf("onClient recebeu %d bytes, esperado %d", len(got), meowcaller.FrameSamples*2)
	}
	if got[0] != 0xFF || got[1] != 0x3F {
		t.Fatalf("conversao f32->s16 errada: %x %x", got[0], got[1])
	}
}

func TestWSPipeCloseEOF(t *testing.T) {
	p := NewWSPipe(nil)
	_ = p.Close()
	if _, err := p.ReadFrame(); err == nil {
		t.Fatal("ReadFrame apos Close deveria dar EOF")
	}
}
