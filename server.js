'use strict';
// CART3R relay v3: resumable rooms, bounded relays, action acknowledgements.
const http=require('http'),crypto=require('crypto');
const rooms=new Map(),PORT=Number(process.env.PORT||8080),GRACE=45000;
// V23: prioritize the freshest packets; disable TCP Nagle delay on sockets.
const METRICS={states:0,inputs:0,droppedStates:0,connections:0};
function send(s,o){if(!s||s.destroyed||s.writableLength>256000)return false;const b=Buffer.from(JSON.stringify(o));if(b.length>180000)return false;let h;if(b.length<126)h=Buffer.from([129,b.length]);else if(b.length<65536){h=Buffer.alloc(4);h[0]=129;h[1]=126;h.writeUInt16BE(b.length,2)}else{h=Buffer.alloc(10);h[0]=129;h[1]=127;h.writeBigUInt64BE(BigInt(b.length),2)}return s.write(Buffer.concat([h,b]));}
function random(n=16){return crypto.randomBytes(n).toString('hex')}
function active(r){return !!(r.host&&!r.host.destroyed&&r.guest&&!r.guest.destroyed)}
function notify(r){if(active(r)){send(r.host,{type:'ready'});send(r.guest,{type:'ready'})}else{for(const s of [r.host,r.guest])send(s,{type:'waiting',message:'Opponent disconnected; waiting up to 45 seconds for reconnection'})}}
function expire(code,r){if(r.timer)clearTimeout(r.timer);r.timer=setTimeout(()=>{if(rooms.get(code)!==r||active(r))return;rooms.delete(code);for(const s of [r.host,r.guest])send(s,{type:'ended',reason:'Reconnect window expired'})},GRACE);r.timer.unref?.()}
const server=http.createServer((req,res)=>{res.writeHead(200,{'Content-Type':'text/plain','Access-Control-Allow-Origin':'*'});res.end('CART3R multiplayer relay v23 running\n')});
server.on('upgrade',(req,s)=>{s.setNoDelay(true);s.setKeepAlive(true,15000);METRICS.connections++;if(req.headers.upgrade?.toLowerCase()!=='websocket'||!req.headers['sec-websocket-key'])return s.destroy();const key=req.headers['sec-websocket-key'];const accept=crypto.createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');s.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');let buf=Buffer.alloc(0),code='',role='',lastState=0,lastInput=0;
function handle(m){if(!m||typeof m!=='object')return;
 if(!code){if(m.type==='create'){let c;do{c=random(3).toUpperCase()}while(rooms.has(c));let r={host:s,guest:null,hostToken:random(),guestToken:random(),timer:null};rooms.set(c,r);code=c;role='host';send(s,{type:'created',code:c,role,token:r.hostToken});return}
 if(m.type==='join'){const c=String(m.code||'').trim().toUpperCase(),r=rooms.get(c);if(!r||r.guest&&!r.guest.destroyed){send(s,{type:'error',message:'Room not found or full'});return}r.guest=s;code=c;role='guest';send(s,{type:'joined',code:c,role,token:r.guestToken});if(r.timer){clearTimeout(r.timer);r.timer=null}notify(r);return}
 if(m.type==='resume'){const c=String(m.code||'').trim().toUpperCase(),r=rooms.get(c),which=m.role;if(!r||!['host','guest'].includes(which)||m.token!==r[which+'Token']){send(s,{type:'error',message:'Could not resume room'});return}const old=r[which];r[which]=s;code=c;role=which;send(s,{type:'resumed',code:c,role,token:r[which+'Token']});if(old&&old!==s)old.destroy();if(r.timer){clearTimeout(r.timer);r.timer=null}notify(r);return}return}
 const r=rooms.get(code);if(!r||r[role]!==s)return;
 if(m.type==='leave'){rooms.delete(code);if(r.timer)clearTimeout(r.timer);for(const peer of [r.host,r.guest])if(peer!==s)send(peer,{type:'ended',reason:'Opponent left the match'});code='';return}
 if(role==='host'&&m.type==='state'){const now=Date.now();if(now-lastState<12)return;lastState=now;if(r.guest){if(r.guest.writableLength>64000){METRICS.droppedStates++}else if(send(r.guest,{type:'state',state:m.state}))METRICS.states++}return}
 if(role==='guest'&&m.type==='input'){const now=Date.now();if(now-lastInput<4&&!m.input?.id)return;lastInput=now;if(r.host&&send(r.host,{type:'input',input:m.input}))METRICS.inputs++;return}
 if(role==='host'&&m.type==='ack'&&typeof m.id==='string'&&m.id.length<80){if(r.guest)send(r.guest,{type:'ack',id:m.id});return}
 }
 s.on('data',chunk=>{buf=Buffer.concat([buf,chunk]);if(buf.length>400000){s.destroy();return}while(buf.length>=2){const op=buf[0]&15,masked=!!(buf[1]&128);let len=buf[1]&127,off=2;if(len===126){if(buf.length<4)break;len=buf.readUInt16BE(2);off=4}else if(len===127){if(buf.length<10)break;const n=buf.readBigUInt64BE(2);if(n>200000n){s.destroy();return}len=Number(n);off=10}if(!masked||len>200000){s.destroy();return}if(buf.length<off+4+len)break;const mask=buf.subarray(off,off+4),body=Buffer.from(buf.subarray(off+4,off+4+len));buf=buf.subarray(off+4+len);for(let i=0;i<body.length;i++)body[i]^=mask[i%4];if(op===8){s.end();return}if(op===9){s.write(Buffer.from([0x8a,0]));continue}if(op!==1)continue;try{handle(JSON.parse(body.toString('utf8')))}catch{}}});
 function disconnect(){if(!code)return;const r=rooms.get(code);if(!r||r[role]!==s)return;r[role]=null;notify(r);expire(code,r)}s.on('close',disconnect);s.on('error',disconnect);
});
server.listen(PORT,'0.0.0.0',()=>console.log('CART3R relay v23 listening on '+PORT));
