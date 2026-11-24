import assert from "node:assert/strict";
import {readFileSync,existsSync} from "node:fs";
import {resolve,dirname} from "node:path";
import {fileURLToPath} from "node:url";

const root=resolve(dirname(fileURLToPath(import.meta.url)),"..");
const load=file=>readFileSync(resolve(root,file),"utf8");
const html=load("public/index.html"),css=load("public/style.css"),js=load("public/app.js");
const worker=load("src/worker.js"),sw=load("public/sw.js");
for(const file of ["public/index.html","public/style.css","public/app.js","src/worker.js",
 "public/sw.js","public/manifest.json","public/icon-192.png","public/icon-512.png"])
 assert(existsSync(resolve(root,file)),file+" missing");
assert.doesNotThrow(()=>new Function(js),"Client script syntax invalid");
assert.doesNotThrow(()=>new Function(sw),"Service Worker syntax invalid");
assert.doesNotThrow(()=>new Function(worker.replace("export default","const handler =")
 .replace("export class Room","class Room")),"Cloudflare worker syntax invalid");
let depth=0;
for(const char of css){
 if(char==="{")depth++;
 if(char==="}")depth--;
 assert(depth>=0,"Unexpected CSS closing brace");
}
assert.equal(depth,0,"Unbalanced CSS braces");
assert(html.includes('src="/app.js"')&&html.includes('href="/style.css"'));
const ids=[...html.matchAll(/\bid="([^"]+)"/g)].map(m=>m[1]);
assert.equal(ids.length,new Set(ids).size,"Duplicate HTML ids");
for(const match of js.matchAll(/\$\("#([^"]+)"\)/g))
 assert(ids.includes(match[1]),"Missing element #"+match[1]);
assert(!html.includes("fonts.googleapis.com"),"Core app must not need remote fonts");
assert(sw.includes('"/app.js"')&&sw.includes('"/style.css"'));
assert(js.includes("relayTransport"),"Relay fallback missing");
assert(js.includes("tryDirect"),"WebRTC direct connection missing");
assert(js.includes('t:"received"'),"Receiver acknowledgement missing");
assert(js.includes('const MAX_FILE = 256 * 1024 * 1024;'),"Transfer size guard missing");
assert(worker.includes("const HEX_ID = /^[a-f0-9]{12,24}$/;"),
 "DriveTunes must retain its 24-character peer identifiers");

const Room=new Function(worker.replace("export default","const handler =")
 .replace("export class Room","class Room")+"\nreturn Room;")();
class Socket{
 constructor(){this.frames=[];this.attachment=null;}
 deserializeAttachment(){return this.attachment;}
 serializeAttachment(info){this.attachment=info;}
 send(frame){this.frames.push(frame);}
 close(){}
}
const a=new Socket(),b=new Socket(),legacy=new Socket();
const room=new Room({getWebSockets:()=>[a,b,legacy]});
const idA="abcdef012345",idB="9876543210ab",idLegacy="0123456789abcdef01234567";
for(const [socket,id] of [[a,idA],[b,idB],[legacy,idLegacy]])
 room.webSocketMessage(socket,JSON.stringify({type:"hello",id,name:"Browser"}));
assert.equal(room.members().size,3);
const oldB=b.frames.length;
room.webSocketMessage(a,JSON.stringify({type:"signal",to:idB,data:{sdp:{type:"offer"}}}));
assert.equal(b.frames.length,oldB+1);
assert.equal(JSON.parse(b.frames.at(-1)).type,"signal");
const relayedText=JSON.stringify({t:"text",text:"Hello!"});
room.webSocketMessage(a,JSON.stringify({type:"relay",to:idB,data:relayedText}));
assert.equal(JSON.parse(b.frames.at(-1)).data,relayedText);
const frame=new Uint8Array(16);
frame.set(new TextEncoder().encode(idB),0);
frame.set([1,2,3,4],12);
room.webSocketMessage(a,frame.buffer);
const forwarded=b.frames.at(-1);
assert(forwarded instanceof Uint8Array);
assert.equal(new TextDecoder().decode(forwarded.subarray(0,12)),idA);
assert.deepEqual([...forwarded.subarray(12)],[1,2,3,4]);
const received=b.frames.length;
room.webSocketMessage(new Socket(),frame.buffer);
assert.equal(b.frames.length,received,"Unregistered sender must not relay data");
console.log("PASS: assets, syntax, room-first UI, 24-digit DriveTunes signaling, binary and text relay");
