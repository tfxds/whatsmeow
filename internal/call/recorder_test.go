package call

import (
	"os"
	"testing"
	"time"

	"github.com/purpshell/meowcaller"
)

// bytesPorSegundo = 16000 samples/s * 2 bytes (s16le mono).
const bytesPorSegundo = meowcaller.SampleRate * 2

// TestGravaSemUplink prova o conserto principal: a gravação tem que crescer com o TEMPO,
// não com a leitura do uplink. Antes, sem ninguém falar (ou com o mic engasgado), nada era
// escrito e o áudio saía menor que a ligação — era o que fazia a conversa "saltar".
func TestGravaSemUplink(t *testing.T) {
	dir, err := os.MkdirTemp("", "rec")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	r := NewRecorder(dir, "conn-teste")
	if err := r.Begin("call-teste"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second) // 1s de chamada, ZERO frame de uplink
	r.Close()

	fi, err := os.Stat(r.Path())
	if err != nil {
		t.Fatal(err)
	}
	seg := float64(fi.Size()) / float64(bytesPorSegundo)
	t.Logf("1,0s de chamada sem áudio nenhum -> %d bytes = %.2fs de gravação", fi.Size(), seg)
	if seg < 0.8 || seg > 1.2 {
		t.Fatalf("gravação com %.2fs, esperado ~1,0s (antes do conserto dava 0,00s)", seg)
	}
}

// TestNaoRepeteFrame prova o segundo conserto: um frame recebido vale UMA vez. Antes o
// último frame do cliente ficava sendo mixado pra sempre, e cliente calado virava um
// zumbido de 60ms em loop em vez de silêncio.
func TestNaoRepeteFrame(t *testing.T) {
	dir, _ := os.MkdirTemp("", "rec")
	defer os.RemoveAll(dir)

	r := NewRecorder(dir, "conn-teste")
	if err := r.Begin("call-teste"); err != nil {
		t.Fatal(err)
	}
	// Um único frame com valor alto, e depois silêncio por meio segundo.
	frame := make([]float32, meowcaller.FrameSamples)
	for i := range frame {
		frame[i] = 0.9
	}
	r.setDown(frame)
	time.Sleep(500 * time.Millisecond)
	r.Close()

	b, err := os.ReadFile(r.Path())
	if err != nil {
		t.Fatal(err)
	}
	// Conta quantas amostras saíram altas. Se o frame repetisse, seria quase tudo.
	altas := 0
	for i := 0; i+1 < len(b); i += 2 {
		v := int16(uint16(b[i]) | uint16(b[i+1])<<8)
		if v > 20000 || v < -20000 {
			altas++
		}
	}
	total := len(b) / 2
	t.Logf("%d amostras altas de %d (%.1f%%) — 1 frame de %d samples deveria dar ~%.1f%%",
		altas, total, 100*float64(altas)/float64(total), meowcaller.FrameSamples,
		100*float64(meowcaller.FrameSamples)/float64(total))
	if altas > meowcaller.FrameSamples*3 {
		t.Fatalf("frame repetiu: %d amostras altas, esperado no máximo ~%d", altas, meowcaller.FrameSamples*3)
	}
	if altas == 0 {
		t.Fatalf("o frame nem foi gravado")
	}
}
