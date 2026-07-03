// Package asterisk implementa a ponte de áudio entre o whatsmeow (chamada meowcaller,
// PCM s16le 16k) e o Asterisk via protocolo AudioSocket (TCP). É a peça NOVA da migração
// NexCall→whatsmeow: substitui o bridge que o SheIITear fazia. O Asterisk conecta neste
// servidor (dialplan `AudioSocket(uuid,host:porta)`); cada conexão carrega um UUID que
// identifica a chamada. O CONTROLE (o que fazer com a chamada — aceitar inbound / discar
// outbound) fica no callback OnCall, igual o callws.go faz pro browser.
//
// Protocolo AudioSocket (cada frame): [1 byte tipo][2 bytes len BE][payload]
//   0x00 terminate · 0x01 UUID (16 bytes) · 0x10 áudio (PCM signed-linear)
// Áudio = slin16 (s16le 16 kHz) — casa com o WSPipe do gateway. Asterisk manda ~20ms
// (640 bytes) por frame; o meowcaller reagrupa via a AudioSource.
package asterisk

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	waLog "go.mau.fi/whatsmeow/util/log"
)

const (
	asKindTerminate = 0x00
	asKindUUID      = 0x01
	asKindError     = 0xff
	asKindAudio     = 0x10
)

// Conn é uma conexão AudioSocket ativa (uma chamada). Expõe o áudio cru s16le nos dois
// sentidos: ReadAudio (Asterisk→gateway, uplink do ramal) e WriteAudio (gateway→Asterisk,
// downlink pro ramal). É o transporte que o control-plane liga a um WSPipe/chamada.
type Conn struct {
	UUID string

	raw    net.Conn
	log    waLog.Logger
	inCh   chan []byte // frames de áudio que CHEGAM do Asterisk (uplink)
	closed chan struct{}
	once   sync.Once
	wmu    sync.Mutex
}

// ReadAudio devolve o próximo frame s16le vindo do Asterisk, ou nil quando a conexão fecha.
func (c *Conn) ReadAudio() []byte {
	select {
	case f, ok := <-c.inCh:
		if !ok {
			return nil
		}
		return f
	case <-c.closed:
		return nil
	}
}

// WriteAudio manda um frame s16le pro Asterisk (tocar no ramal). Thread-safe.
func (c *Conn) WriteAudio(s16 []byte) error {
	if len(s16) == 0 {
		return nil
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	var hdr [3]byte
	hdr[0] = asKindAudio
	binary.BigEndian.PutUint16(hdr[1:], uint16(len(s16)))
	if _, err := c.raw.Write(hdr[:]); err != nil {
		return err
	}
	_, err := c.raw.Write(s16)
	return err
}

// Close encerra a conexão AudioSocket.
func (c *Conn) Close() {
	c.once.Do(func() {
		close(c.closed)
		_ = c.raw.Close()
	})
}

// Server é o listener AudioSocket. OnCall é chamado (numa goroutine) assim que uma conexão
// manda seu UUID — é onde o control-plane amarra a chamada (aceitar inbound / discar outbound)
// e liga o áudio num WSPipe.
type Server struct {
	addr   string
	log    waLog.Logger
	onCall func(*Conn)
	ln     net.Listener
}

func NewServer(addr string, logger waLog.Logger) *Server {
	return &Server{addr: addr, log: logger}
}

// OnCall registra o handler disparado quando uma nova chamada AudioSocket chega (após o UUID).
func (s *Server) OnCall(fn func(*Conn)) { s.onCall = fn }

// Start sobe o listener TCP. Bloqueia até erro; rode em goroutine.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("audiosocket listen %s: %w", s.addr, err)
	}
	s.ln = ln
	s.log.Infof("AudioSocket ouvindo em %s", s.addr)
	for {
		raw, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handle(raw)
	}
}

func (s *Server) handle(raw net.Conn) {
	c := &Conn{
		raw:    raw,
		log:    s.log,
		inCh:   make(chan []byte, 32),
		closed: make(chan struct{}),
	}
	defer c.Close()

	fired := false
	hdr := make([]byte, 3)
	for {
		if _, err := io.ReadFull(raw, hdr); err != nil {
			return
		}
		kind := hdr[0]
		n := int(binary.BigEndian.Uint16(hdr[1:]))
		var payload []byte
		if n > 0 {
			payload = make([]byte, n)
			if _, err := io.ReadFull(raw, payload); err != nil {
				return
			}
		}
		switch kind {
		case asKindUUID:
			c.UUID = formatUUID(payload)
			if !fired && s.onCall != nil {
				fired = true
				go s.onCall(c) // control-plane amarra a chamada e o áudio
			}
		case asKindAudio:
			select {
			case c.inCh <- payload:
			default: // buffer cheio → descarta o mais velho (latência baixa)
				select {
				case <-c.inCh:
				default:
				}
				select {
				case c.inCh <- payload:
				default:
				}
			}
		case asKindTerminate, asKindError:
			return
		}
	}
}

// formatUUID converte os 16 bytes do frame UUID no formato canônico 8-4-4-4-12.
func formatUUID(b []byte) string {
	if len(b) != 16 {
		return fmt.Sprintf("%x", b)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
