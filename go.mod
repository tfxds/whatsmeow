module github.com/nextflow/whatsmeow-gateway

go 1.25.0

require (
	github.com/coder/websocket v1.8.15
	github.com/lib/pq v1.12.3
	github.com/purpshell/meowcaller v0.0.0-20260626012300-0f1265d7ebee
	github.com/rs/zerolog v1.35.1
	go.mau.fi/whatsmeow v0.0.0-20260622185415-5f04eac6dbbb
	google.golang.org/protobuf v1.36.11
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/beeper/argo-go v1.1.2 // indirect
	github.com/elliotchance/orderedmap/v3 v3.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hajimehoshi/go-mp3 v0.3.4 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/petermattis/goid v0.0.0-20260330135022-df67b199bc81 // indirect
	github.com/pion/datachannel v1.6.0 // indirect
	github.com/pion/dtls/v3 v3.1.2 // indirect
	github.com/pion/logging v0.2.4 // indirect
	github.com/pion/opus v0.1.0 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/sctp v1.9.4 // indirect
	github.com/pion/transport/v4 v4.0.1 // indirect
	github.com/vektah/gqlparser/v2 v2.5.27 // indirect
	go.mau.fi/libsignal v0.2.2 // indirect
	go.mau.fi/util v0.9.10 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/exp v0.0.0-20260611194520-c48552f49976 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sync v0.21.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.38.0 // indirect
)

// Fork com o fix do ICE-consent do callee (inbound RX). Backup: github.com/tfxds/meowcallerr
// Noutra máquina: git clone git@github.com:tfxds/meowcallerr.git /root/meowcaller-fork
replace github.com/purpshell/meowcaller => /root/meowcaller-fork
