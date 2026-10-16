package asterisk

import (
	"encoding/binary"
	"math"
)

// Qualidade do áudio que vai pro ramal (16/10/2026, NexCall). O Down16to8 só tirava a média de
// duas amostras (sem filtro) e nada controlava o volume: na gravação medimos pico de 31.857 de
// 32.767 e ~1 estalo por segundo — o "às vezes alto, às vezes um estouro" que o Thiago ouviu.
//
// Decimador: passa-baixa FIR de 31 coeficientes (corte 3,6 kHz, janela de Hamming) antes de
// descartar 1 a cada 2 amostras, com histórico entre frames (sem degrau na emenda).
// Limitador: ganho fixo de 0,85 e joelho suave acima de 22.000 → nunca encosta no teto.

const tapsFIR = 31

var coefFIR = func() [tapsFIR]float64 {
	var h [tapsFIR]float64
	const corte = 3600.0 / 16000.0 // frequência de corte normalizada pela taxa de 16 kHz
	soma := 0.0
	meio := float64(tapsFIR-1) / 2
	for n := 0; n < tapsFIR; n++ {
		x := float64(n) - meio
		s := 2 * corte
		if x != 0 {
			s = math.Sin(2*math.Pi*corte*x) / (math.Pi * x)
		}
		janela := 0.54 - 0.46*math.Cos(2*math.Pi*float64(n)/float64(tapsFIR-1))
		h[n] = s * janela
		soma += h[n]
	}
	for n := range h {
		h[n] /= soma // ganho 1 em baixa frequência
	}
	return h
}()

// Decimador16to8 converte 16 kHz → 8 kHz com filtro, guardando o fim do frame anterior.
type Decimador16to8 struct {
	hist [tapsFIR - 1]float64
}

func limitar(v float64) int16 {
	v *= 0.85
	const joelho = 22000.0
	if a := math.Abs(v); a > joelho {
		// acima do joelho a curva achata (tende a ~31.000 sem nunca bater em 32.767)
		comp := joelho + (31000-joelho)*math.Tanh((a-joelho)/(31000-joelho))
		v = math.Copysign(comp, v)
	}
	return int16(v)
}

// Processar recebe s16le 16 kHz e devolve s16le 8 kHz filtrado e limitado.
func (d *Decimador16to8) Processar(in []byte) []byte {
	n := len(in) / 2
	if n == 0 {
		return nil
	}
	buf := make([]float64, len(d.hist)+n)
	copy(buf, d.hist[:])
	for i := 0; i < n; i++ {
		buf[len(d.hist)+i] = float64(int16(binary.LittleEndian.Uint16(in[i*2:])))
	}
	out := make([]byte, (n/2)*2)
	for k := 0; k < n/2; k++ {
		fim := len(d.hist) + 2*k // amostra "atual" (a mais nova da janela)
		acc := 0.0
		for t := 0; t < tapsFIR; t++ {
			acc += coefFIR[t] * buf[fim-t]
		}
		binary.LittleEndian.PutUint16(out[k*2:], uint16(limitar(acc)))
	}
	copy(d.hist[:], buf[len(buf)-len(d.hist):])
	return out
}

// Atenuar devolve uma cópia do frame s16le com ganho g (usado pra disfarçar falha de pacote).
func Atenuar(frame []byte, g float64) []byte {
	out := make([]byte, len(frame))
	for i := 0; i+1 < len(frame); i += 2 {
		v := float64(int16(binary.LittleEndian.Uint16(frame[i:]))) * g
		binary.LittleEndian.PutUint16(out[i:], uint16(int16(v)))
	}
	return out
}

// RampaEntrada sobe o volume do começo do frame de 0 a 1 nas primeiras `amostras` (sem clique
// quando o áudio volta depois de um buraco).
func RampaEntrada(frame []byte, amostras int) []byte {
	out := make([]byte, len(frame))
	copy(out, frame)
	n := len(frame) / 2
	if amostras > n {
		amostras = n
	}
	for i := 0; i < amostras; i++ {
		v := float64(int16(binary.LittleEndian.Uint16(out[i*2:]))) * float64(i) / float64(amostras)
		binary.LittleEndian.PutUint16(out[i*2:], uint16(int16(v)))
	}
	return out
}

// LimitarFrame aplica só o joelho suave (sem baixar o ganho) num frame s16le — no sentido
// ramal → celular, pra grito/microfonia do lado do atendente não chegar estourado no cliente.
func LimitarFrame(frame []byte) []byte {
	out := make([]byte, len(frame))
	for i := 0; i+1 < len(frame); i += 2 {
		v := float64(int16(binary.LittleEndian.Uint16(frame[i:])))
		const joelho = 22000.0
		if a := math.Abs(v); a > joelho {
			v = math.Copysign(joelho+(31000-joelho)*math.Tanh((a-joelho)/(31000-joelho)), v)
		}
		binary.LittleEndian.PutUint16(out[i:], uint16(int16(v)))
	}
	return out
}
