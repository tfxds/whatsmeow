package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/coder/websocket"
	"github.com/nextflow/whatsmeow-gateway/internal/call"
)

// handleCallWS: WebSocket de áudio de uma chamada. Query: connectionId, phone, token.
// Browser manda frames s16le 16kHz/960 (mic) em mensagens binárias; o gateway devolve
// a voz do cliente em binário. Texto "hangup" encerra. Fechar o WS encerra a chamada.
func (a *API) handleCallWS(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	connID := q.Get("connectionId")
	phone := callDigitsOnly(q.Get("phone"))
	token := q.Get("token")

	conn, err := a.findConn(r.Context(), connID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if conn == nil || token == "" || token != conn.Token {
		writeError(w, http.StatusUnauthorized, "invalid token or connection")
		return
	}
	if phone == "" {
		writeError(w, http.StatusBadRequest, "phone is required")
		return
	}
	sess, ok := a.Mgr.Get(connID)
	if !ok {
		writeError(w, http.StatusNotFound, "session not found")
		return
	}

	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer c.CloseNow()

	ctx := context.Background()
	pipe := call.NewWSPipe(func(s16 []byte) {
		_ = c.Write(ctx, websocket.MessageBinary, s16)
	})
	mcall, _, err := a.Calls.StartWithPipe(ctx, connID, sess.Client, phone, pipe, pipe)
	if err != nil {
		_ = c.Close(websocket.StatusInternalError, "place call failed")
		return
	}
	defer func() {
		_ = mcall.Hangup()
		_ = pipe.Close()
	}()

	for {
		typ, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		switch typ {
		case websocket.MessageBinary:
			pipe.PushMic(data)
		case websocket.MessageText:
			if string(data) == "hangup" {
				return
			}
		}
	}
}

// handleCallTestPage serve um HTML standalone que capta o mic e toca o áudio recebido
// via /call/ws — valida a chamada nos 2 sentidos com uma pessoa.
func (a *API) handleCallTestPage(w http.ResponseWriter, r *http.Request) {
	mic, _ := json.Marshal(micWorkletJS)
	play, _ := json.Marshal(playWorkletJS)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintf(w, callTestPageHTML, string(mic), string(play))
}

const micWorkletJS = `class Mic extends AudioWorkletProcessor{
 constructor(){super();this.step=sampleRate/16000;this.acc=0;this.batch=new Int16Array(960);this.pos=0}
 process(inp){const ch=inp[0]&&inp[0][0];if(!ch)return true;let peak=0;
  for(let i=0;i<ch.length;i++){this.acc++;if(this.acc>=this.step){this.acc-=this.step;
   let v=Math.max(-1,Math.min(1,ch[i]));if(Math.abs(v)>peak)peak=Math.abs(v);
   this.batch[this.pos++]=v<0?v*0x8000:v*0x7fff;
   if(this.pos>=960){const c=new Int16Array(this.batch);this.port.postMessage({buf:c.buffer,peak},[c.buffer]);this.pos=0;peak=0}}}
  return true}}
registerProcessor('mic',Mic)`

const playWorkletJS = `class Play extends AudioWorkletProcessor{
 constructor(){super();this.cap=32768;this.buf=new Float32Array(this.cap);this.r=0;this.w=0;this.size=0;this.hi=8000;
  this.port.onmessage=e=>{const s=e.data;const n=s.length;
   if(this.size+n>this.cap){const d=this.size+n-this.cap;this.r=(this.r+d)%this.cap;this.size-=d}
   for(let i=0;i<n;i++){this.buf[this.w]=s[i];this.w=(this.w+1)%this.cap}this.size+=n;
   if(this.size>this.hi){const d=this.size-this.hi;this.r=(this.r+d)%this.cap;this.size-=d}}}
 process(out){const o=out[0][0];for(let i=0;i<o.length;i++){if(this.size>0){o[i]=this.buf[this.r];this.r=(this.r+1)%this.cap;this.size--}else o[i]=0}return true}}
registerProcessor('play',Play)`

// callTestPageHTML tem 2 %s (micWorkletJS, playWorkletJS). Todo % literal está escapado como %%.
const callTestPageHTML = `<!doctype html>
<html lang="pt-br"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NextFlow - Teste de Chamada (whatsmeow)</title>
<style>
 body{font-family:system-ui,sans-serif;background:#0f172a;color:#e2e8f0;max-width:520px;margin:40px auto;padding:0 16px}
 h1{font-size:18px} label{display:block;font-size:12px;margin:12px 0 4px;color:#94a3b8}
 input{width:100%%;padding:10px;border-radius:8px;border:1px solid #334155;background:#1e293b;color:#fff}
 button{margin-top:16px;padding:12px 18px;border:0;border-radius:10px;font-weight:700;cursor:pointer}
 #dial{background:#10b981;color:#04231a} #hang{background:#ef4444;color:#fff;display:none}
 #status{margin-top:16px;padding:12px;border-radius:8px;background:#1e293b;font-size:13px;white-space:pre-wrap}
 .lvl{height:8px;background:#334155;border-radius:4px;margin-top:6px;overflow:hidden}
 .lvl > i{display:block;height:100%%;width:0;background:#10b981}
</style></head><body>
<h1>Teste de chamada - gateway whatsmeow</h1>
<label>connectionId</label><input id="conn" value="341e2d9f-039c-43fd-93e4-1677067e34f4">
<label>token da conexao</label><input id="token" placeholder="cole o token">
<label>numero (DDI+DDD+no)</label><input id="phone" placeholder="553491173950">
<button id="dial">Ligar</button> <button id="hang">Desligar</button>
<div id="status">pronto.</div>
<div>mic <div class="lvl"><i id="micLvl"></i></div></div>
<div>recebido <div class="lvl"><i id="rxLvl"></i></div></div>
<script>
const $=id=>document.getElementById(id);
let ws,ac,micNode,playNode,micStream;
function log(m){$('status').textContent=m}
const micJS=%s;
const playJS=%s;
async function dial(){
 const conn=$('conn').value.trim(),token=$('token').value.trim(),phone=$('phone').value.replace(/\D/g,'');
 if(!token||!phone){log('preencha token e numero');return}
 ac=new AudioContext({sampleRate:48000});
 await ac.audioWorklet.addModule(URL.createObjectURL(new Blob([micJS],{type:'text/javascript'})));
 await ac.audioWorklet.addModule(URL.createObjectURL(new Blob([playJS],{type:'text/javascript'})));
 const proto=location.protocol==='https:'?'wss':'ws';
 ws=new WebSocket(proto+'://'+location.host+'/call/ws?connectionId='+encodeURIComponent(conn)+'&phone='+phone+'&token='+encodeURIComponent(token));
 ws.binaryType='arraybuffer';
 ws.onopen=async()=>{log('conectado - chamando '+phone+'...');
  micStream=await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true}});
  const src=ac.createMediaStreamSource(micStream);
  micNode=new AudioWorkletNode(ac,'mic');
  micNode.port.onmessage=e=>{if(ws&&ws.readyState===1)ws.send(e.data.buf);$('micLvl').style.width=Math.min(100,e.data.peak*140)+'%%'};
  src.connect(micNode);
  playNode=new AudioWorkletNode(ac,'play');playNode.connect(ac.destination);
  $('dial').style.display='none';$('hang').style.display='inline-block'};
 ws.onmessage=e=>{const i16=new Int16Array(e.data);const f=new Float32Array(i16.length);let peak=0;
  for(let k=0;k<i16.length;k++){f[k]=i16[k]/32768;if(Math.abs(f[k])>peak)peak=Math.abs(f[k])}
  if(playNode)playNode.port.postMessage(f);$('rxLvl').style.width=Math.min(100,peak*140)+'%%'};
 ws.onclose=()=>{log('chamada encerrada');cleanup()};
 ws.onerror=()=>log('erro no websocket')}
function hang(){if(ws&&ws.readyState===1)ws.send('hangup');if(ws)ws.close();cleanup()}
function cleanup(){try{micStream&&micStream.getTracks().forEach(t=>t.stop())}catch(e){}
 try{ac&&ac.close()}catch(e){}$('dial').style.display='inline-block';$('hang').style.display='none'}
$('dial').onclick=dial;$('hang').onclick=hang;
</script></body></html>`
