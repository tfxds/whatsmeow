// Package audio provides helpers for converting audio into the formats
// WhatsApp expects for voice messages (PTT).
package audio

import (
	"bytes"
	"context"
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
