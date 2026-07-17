// Package audio provides helpers for converting audio into the formats
// WhatsApp expects for voice messages (PTT).
package audio

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// ToOpusOgg transcodes arbitrary input audio bytes into an Ogg/Opus stream,
// which is the format WhatsApp requires for push-to-talk (voice) messages.
//
// It pipes the input into ffmpeg via stdin and reads the resulting Ogg from
// stdout. ffmpeg must be available on the PATH.
func ToOpusOgg(in []byte) ([]byte, error) {
	if len(in) == 0 {
		return nil, fmt.Errorf("audio: empty input")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-loglevel", "error",
		"-i", "pipe:0",
		"-c:a", "libopus",
		"-b:a", "64k",
		"-ar", "48000",
		"-ac", "1",
		"-f", "ogg",
		"pipe:1",
	)

	var out, stderr bytes.Buffer
	cmd.Stdin = bytes.NewReader(in)
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("audio: ffmpeg failed: %w: %s", err, stderr.String())
	}
	if out.Len() == 0 {
		return nil, fmt.Errorf("audio: ffmpeg produced no output: %s", stderr.String())
	}
	return out.Bytes(), nil
}

// DurationSeconds mede a duração (em segundos, arredondada) de um áudio via ffprobe.
// Necessário pra setar AudioMessage.Seconds — senão o WhatsApp mostra 0:00 (áudio
// gravado no navegador via MediaRecorder costuma vir SEM duração no header). Escreve
// num arquivo temporário porque ffprobe precisa de seek pra ler a duração do Ogg.
// Retorna 0 se não conseguir medir.
func DurationSeconds(media []byte) uint32 {
	f, err := os.CreateTemp("", "wm-audio-*.ogg")
	if err != nil {
		return 0
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(media); err != nil {
		f.Close()
		return 0
	}
	f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		f.Name(),
	).Output()
	if err != nil {
		return 0
	}
	secs, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil || secs <= 0 {
		return 0
	}
	return uint32(math.Round(secs))
}

// Waveform gera as "ondinhas" da nota de voz que o WhatsApp desenha.
// Sem AudioMessage.Waveform ele mostra um player genérico em vez da barra de áudio
// nativa (o áudio toca igual, mas "não fica com cara de WhatsApp").
//
// Formato que o WhatsApp espera: 64 bytes, cada um de 0 a 127, sendo a amplitude média
// absoluta de 1/64 do áudio. Pipeline: ffmpeg -> PCM s16le mono 8kHz (64 barras não
// precisam de mais taxa) -> normaliza -> 64 fatias -> média |amp| * 127, normalizado
// pelo pico (voz é baixa e sairia uma linha reta).
//
// Best-effort: retorna nil se não conseguir — aí o áudio vai sem waveform (não trava).
func Waveform(media []byte) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-i", "pipe:0",
		"-vn",
		"-ac", "1", // mono
		"-ar", "8000", // 8kHz basta pra 64 barras
		"-f", "s16le",
		"-acodec", "pcm_s16le",
		"pipe:1",
	)
	cmd.Stdin = bytes.NewReader(media)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil
	}

	pcm := out.Bytes()
	total := len(pcm) / 2 // amostras de 16 bits
	if total < 64 {
		return nil
	}
	per := total / 64

	wave := make([]byte, 64)
	var peak float64
	raw := make([]float64, 64)
	for i := 0; i < 64; i++ {
		var sum float64
		for j := 0; j < per; j++ {
			off := ((i * per) + j) * 2
			s := int16(binary.LittleEndian.Uint16(pcm[off : off+2]))
			v := float64(s)
			if v < 0 {
				v = -v
			}
			sum += v / 32768 // normaliza 0..1
		}
		raw[i] = sum / float64(per)
		if raw[i] > peak {
			peak = raw[i]
		}
	}
	for i := 0; i < 64; i++ {
		v := raw[i]
		if peak > 0 {
			v = (v / peak) * 110 // normaliza pelo pico
		} else {
			v = v * 127
		}
		if v > 127 {
			v = 127
		}
		wave[i] = byte(math.Round(v))
	}
	return wave
}
