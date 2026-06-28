package call

import (
	"math"
	"testing"

	"github.com/purpshell/meowcaller"
)

func TestToneSourceFrame(t *testing.T) {
	src := newToneSource(440)
	frame, err := src.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame erro inesperado: %v", err)
	}
	if len(frame) != meowcaller.FrameSamples {
		t.Fatalf("frame com %d samples, esperado %d", len(frame), meowcaller.FrameSamples)
	}
	var sumsq float64
	for _, s := range frame {
		if s < -1 || s > 1 {
			t.Fatalf("sample fora de [-1,1]: %f", s)
		}
		sumsq += float64(s) * float64(s)
	}
	rms := math.Sqrt(sumsq / float64(len(frame)))
	if rms < 0.05 {
		t.Fatalf("RMS muito baixo (%f), tom deveria ter energia", rms)
	}
}

func TestToneSourceCloseEOF(t *testing.T) {
	src := newToneSource(440)
	_ = src.Close()
	if _, err := src.ReadFrame(); err == nil {
		t.Fatal("ReadFrame apos Close deveria retornar erro (EOF)")
	}
}
