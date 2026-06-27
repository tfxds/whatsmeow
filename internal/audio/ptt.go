// Package audio provides helpers for converting audio into the formats
// WhatsApp expects for voice messages (PTT).
package audio

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
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
