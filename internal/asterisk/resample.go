package asterisk

import "encoding/binary"

// Resample PCM s16le entre 8kHz (o que o Asterisk entrega no AudioSocket p/ ramal ulaw/alaw)
// e 16kHz (o que o WSPipe/meowcaller usa). Interpolação/média linear simples — mesmo esquema
// do motor Node antigo (up8to16/down16to8). Suficiente pra voz de telefonia.

// Up8to16 dobra a taxa: cada amostra 8k vira 2 amostras 16k (original + interpolada com a próxima).
func Up8to16(in []byte) []byte {
	n := len(in) / 2
	if n == 0 {
		return nil
	}
	out := make([]byte, n*4)
	for i := 0; i < n; i++ {
		cur := int16(binary.LittleEndian.Uint16(in[i*2:]))
		nxt := cur
		if i+1 < n {
			nxt = int16(binary.LittleEndian.Uint16(in[(i+1)*2:]))
		}
		mid := int16((int32(cur) + int32(nxt)) / 2)
		binary.LittleEndian.PutUint16(out[i*4:], uint16(cur))
		binary.LittleEndian.PutUint16(out[i*4+2:], uint16(mid))
	}
	return out
}

// Down16to8 corta a taxa pela metade: média de cada par de amostras 16k vira 1 amostra 8k.
func Down16to8(in []byte) []byte {
	n := len(in) / 2
	if n < 2 {
		return nil
	}
	out := make([]byte, (n/2)*2)
	for i := 0; i+1 < n; i += 2 {
		a := int16(binary.LittleEndian.Uint16(in[i*2:]))
		b := int16(binary.LittleEndian.Uint16(in[(i+1)*2:]))
		avg := int16((int32(a) + int32(b)) / 2)
		binary.LittleEndian.PutUint16(out[(i/2)*2:], uint16(avg))
	}
	return out
}
