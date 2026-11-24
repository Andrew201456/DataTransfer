/* DataTransfer — automatic network discovery, no manual rooms or links.
   Network-IP rendezvous plus optional STUN gateway matching (IPv4/IPv6 cases).
   WebRTC transfers first, WebSocket relay fallback where needed. */
(() => {
  "use strict";
  const $ = selector => document.querySelector(selector);
  const store = {
    get(key) { try { return localStorage.getItem(key); } catch { return null; } },
    set(key, value) { try { localStorage.setItem(key, value); } catch {} }
  };
  const MAX_FILE = 256 * 1024 * 1024;
  const CHUNK = 32 * 1024;
  const WINDOW = 512 * 1024;
  const ICE = {iceServers:[{urls:"stun:stun.cloudflare.com:3478"},{urls:"stun:stun.l.google.com:19302"}]};
  const myId = Array.from(crypto.getRandomValues(new Uint8Array(6)), v=>v.toString(16).padStart(2,"0")).join("");
  const mobile = /Android|iPhone|iPad/i.test(navigator.userAgent);
  let name = store.get("name") || (mobile ? "Κινητό " : "Υπολογιστής ") + myId.slice(0,4).toUpperCase();
  // No shared room code or user action is necessary to join a local network.
  // The default WebSocket is grouped by the origin IP at the Cloudflare edge.
  let ws=null, backupWs=null, extraRoom=null, reconnectAttempts={primary:0,extra:0};
  const reconnectTimers={primary:null,extra:null}, pingTimers={primary:null,extra:null};
  const generations={primary:0,extra:0};
  const sourcePeers=new Map();
  let selectedPeer=null, toastTimer=null;
  const peers=new Map(), transfers=[], awaiting=new Map();
  const el={
    dot:$("#dot"),connection:$("#connectionText"),name:$("#meName"),rename:$("#rename"),
    peers:$("#peers"),peerCount:$("#peerCount"),items:$("#items"),file:$("#file"),
    drop:$("#dropZone"),toast:$("#toast"),session:$("#sessionMode")
  };
  const fmt = n => {
    if (n < 1024) return n + " B";
    if (n < 1024*1024) return (n/1024).toFixed(0) + " KB";
    if (n < 1024*1024*1024) return (n/(1024*1024)).toFixed(1) + " MB";
    return (n/(1024*1024*1024)).toFixed(2) + " GB";
  };
  const icon = (type) => {
    const paths = {
      phone:'<rect x="7" y="2" width="10" height="20" rx="3"/><path d="M11 18.5h2"/>',
      laptop:'<rect x="3" y="4" width="18" height="13" rx="2"/><path d="M1 21h22"/>',
      upload:'<path d="M12 16V3m-5 5 5-5 5 5M4 14v6h16v-6"/>',
      download:'<path d="M12 3v13m-5-5 5 5 5-5M4 14v6h16v-6"/>',
      file:'<path d="M6 2h8l5 5v15H6zM14 2v6h5"/>',
      check:'<path d="m4 12 5 5L20 6"/>'
    };
    const s=document.createElement("span");
    s.className="svg-icon";
    s.innerHTML='<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'+(paths[type] || paths.file)+'</svg>';
    return s;
  };
  function node(tag, cls, value) {
    const n=document.createElement(tag);
    if(cls)n.className=cls;
    if(value!==undefined)n.textContent=value;
    return n;
  }
  function toast(message, bad=false) {
    clearTimeout(toastTimer);
    el.toast.textContent=message;
    el.toast.classList.toggle("bad",bad);
    el.toast.hidden=false;
    toastTimer=setTimeout(()=>{el.toast.hidden=true;},6500);
  }
  function refreshConnection() {
    const online=[ws,backupWs].some(socket=>socket?.readyState===WebSocket.OPEN);
    el.dot.classList.toggle("on",online);
    el.connection.textContent=!online?"Επανασύνδεση…":
      peers.size?"Συνδέθηκαν οι συσκευές":"Αναζήτηση συσκευών…";
    el.session.textContent=peers.size?"Συσκευές διαθέσιμες για αποστολή":
      online?"Ανοιχτό και έτοιμο":"Γίνεται σύνδεση…";
  }
  function renderSelf() {el.name.textContent=name;}
  function disconnectPeer(p) {
    if(p.pc){try{p.pc.close();}catch{}p.pc=null;}
    p.dc=null;p.ready=null;p.mode=null;p.rx=null;
  }
  // A peer may be discoverable through the Cloudflare IP session and/or
  // a matching STUN reflexive IPv4. Merge the two groups without flicker.
  function refreshPeers() {
    const merged=new Map();
    for(const [socket,entries] of sourcePeers){
      if(socket.readyState!==WebSocket.OPEN)continue;
      for(const entry of entries){
        if(!entry||!/^[0-9a-f]{12}$/.test(entry.id)||entry.id===myId)continue;
        const old=merged.get(entry.id);
        if(!old)merged.set(entry.id,{name:entry.name,socket});
      }
    }
    for(const [id,p] of peers)if(!merged.has(id)){disconnectPeer(p);peers.delete(id);}
    for(const [id,entry] of merged) {
      let p=peers.get(id);
      if(!p){
        p={id,name:"Συσκευή",pc:null,dc:null,ready:null,rx:null,mode:null,
          q:Promise.resolve(),earlyICE:[],socket:entry.socket};
        peers.set(id,p);
      }
      p.name=String(entry.name||"Συσκευή").slice(0,40);
      // Prefer an existing live rendezvous path for active transfers.
      if(p.socket?.readyState!==WebSocket.OPEN||!sourcePeers.get(p.socket)?.some(e=>e.id===id))
        p.socket=entry.socket;
    }
    renderPeers();refreshConnection();
  }
  function syncPeers(list,socket) {
    sourcePeers.set(socket,list);
    refreshPeers();
  }
  function signal(to,data) {
    const socket=peers.get(to)?.socket;
    if(socket?.readyState===WebSocket.OPEN)
      socket.send(JSON.stringify({type:"signal",to,data}));
  }
  function closeChannel(slot) {
    clearTimeout(reconnectTimers[slot]);
    clearInterval(pingTimers[slot]);
    generations[slot]++;
    const socket=slot==="primary"?ws:backupWs;
    if(socket){
      socket.onclose=null;
      sourcePeers.delete(socket);
      try{socket.close();}catch{}
    }
    if(slot==="primary")ws=null;
    else backupWs=null;
    refreshPeers();
  }
  function stopSocket() {
    closeChannel("primary");
    closeChannel("extra");
  }
  function connectSocket(slot="primary") {
    const group=slot==="extra"?extraRoom:null;
    if(slot==="extra"&&!group)return;
    clearTimeout(reconnectTimers[slot]);
    const generation=++generations[slot];
    const endpoint=new URL("/ws",location.href);
    endpoint.protocol=location.protocol==="https:"?"wss:":"ws:";
    if(group)endpoint.searchParams.set("room",group);
    let socket;
    try{socket=new WebSocket(endpoint.href);}
    catch{reconnectTimers[slot]=setTimeout(()=>connectSocket(slot),3000);return;}
    socket.binaryType="arraybuffer";
    if(slot==="primary")ws=socket;
    else backupWs=socket;
    socket.onopen=()=>{
      if(generation!==generations[slot])return;
      reconnectAttempts[slot]=0;
      socket.send(JSON.stringify({type:"hello",id:myId,name}));
      pingTimers[slot]=setInterval(()=>{
        if(socket.readyState===WebSocket.OPEN)socket.send("ping");
      },25000);
      refreshConnection();
    };
    socket.onmessage=event=>{
      if(generation!==generations[slot])return;
      if(typeof event.data!=="string"){
        const bytes=event.data;
        if(!(bytes instanceof ArrayBuffer)||bytes.byteLength<13)return;
        const sender=new TextDecoder().decode(new Uint8Array(bytes,0,12));
        if(sourcePeers.get(socket)?.some(p=>p.id===sender))
          onData(sender,bytes.slice(12),"relay");
        return;
      }
      let m;
      try{m=JSON.parse(event.data);}catch{return;}
      if(m.type==="peers"&&Array.isArray(m.peers))syncPeers(m.peers,socket);
      else if(m.type==="signal"){
        const peer=peers.get(m.from);
        if(peer){peer.socket=socket;onSignal(m.from,m.data,socket);}
      }else if(m.type==="relay"&&sourcePeers.get(socket)?.some(p=>p.id===m.from))
        onData(m.from,m.data,"relay");
    };
    socket.onerror=()=>refreshConnection();
    socket.onclose=()=>{
      if(generation!==generations[slot])return;
      clearInterval(pingTimers[slot]);
      sourcePeers.delete(socket);
      if(slot==="primary")ws=null;
      else backupWs=null;
      refreshPeers();
      reconnectTimers[slot]=setTimeout(()=>connectSocket(slot),
        Math.min(1000*2**reconnectAttempts[slot]++,12000));
    };
    refreshConnection();
  }
  // ICE "srflx" public IPv4 can recover dual-stack cases where one browser
  // reaches Cloudflare over IPv6, and the other over IPv4 on the same router.
  // Hash the public address before sending it as a short-lived group key.
  function publicIPv4(value) {
    if(!/^(?:\d{1,3}\.){3}\d{1,3}$/.test(value))return false;
    const x=value.split(".").map(Number);
    if(x.some(n=>n>255))return false;
    return x[0]!==0&&x[0]!==10&&x[0]!==127&&x[0]<224&&
      !(x[0]===169&&x[1]===254)&&!(x[0]===192&&x[1]===168)&&
      !(x[0]===172&&x[1]>=16&&x[1]<=31)&&
      !(x[0]===100&&x[1]>=64&&x[1]<=127);
  }
  async function discoverGateway() {
    if(typeof RTCPeerConnection!=="function"||!crypto?.subtle?.digest)return;
    let pc;
    try{pc=new RTCPeerConnection(ICE);}
    catch{return;}
    const ip=await new Promise(resolve=>{
      let settled=false;
      function complete(value){
        if(settled)return;
        settled=true;clearTimeout(timer);
        try{pc.close();}catch{}
        resolve(value);
      }
      const timer=setTimeout(()=>complete(null),5000);
      pc.onicecandidate=event=>{
        const fields=String(event.candidate?.candidate||"").split(/\s+/);
        const marker=fields.indexOf("typ");
        if(marker>=0&&fields[marker+1]==="srflx"&&publicIPv4(fields[4]))
          complete(fields[4]);
      };
      try{
        pc.createDataChannel("discovery");
        pc.createOffer().then(offer=>pc.setLocalDescription(offer)).catch(()=>complete(null));
      }catch{complete(null);}
    });
    if(!ip)return;
    try{
      const hash=new Uint8Array(await crypto.subtle.digest("SHA-256",new TextEncoder().encode("dt-auto-v1:"+ip)));
      const group="auto-"+Array.from(hash.slice(0,10),b=>b.toString(16).padStart(2,"0")).join("");
      if(extraRoom===group)return;
      closeChannel("extra");
      extraRoom=group;
      connectSocket("extra");
    }catch{ /* Default network discovery still works without STUN */ }
  }
  function makePC(id) {
    const p=peers.get(id);if(!p)return null;
    if(p.pc){try{p.pc.close();}catch{}}
    const pc=new RTCPeerConnection(ICE);
    p.pc=pc;p.dc=null;
    pc.onicecandidate=e=>{if(e.candidate&&p.pc===pc)signal(id,{ice:e.candidate.toJSON?.()||e.candidate});};
    pc.onconnectionstatechange=()=>{
      if(p.pc!==pc)return;
      if(pc.connectionState==="failed"){
        p.mode=null;
        renderPeers();
      }
    };
    return pc;
  }
  function setupDC(id,dc,onOpen) {
    const p=peers.get(id);if(!p)return;
    dc.binaryType="arraybuffer";dc.bufferedAmountLowThreshold=256*1024;
    dc.onopen=()=>{p.dc=dc;p.mode="direct";if(onOpen)onOpen();renderPeers();};
    dc.onmessage=event=>onData(id,event.data,"direct");
    dc.onclose=()=>{if(p.dc===dc){p.dc=null;p.mode=null;renderPeers();}};
    dc.onerror=()=>{};
  }
  async function onSignal(id,data,source) {
    const p=peers.get(id);
    if(!p||!data||typeof data!=="object"||typeof RTCPeerConnection!=="function")return;
    if(source)p.socket=source;
    try {
      if(data.sdp?.type==="offer") {
        if(p.pc?.signalingState==="have-local-offer"&&myId>id)return;
        const pc=makePC(id);
        pc.ondatachannel=e=>setupDC(id,e.channel);
        await pc.setRemoteDescription(data.sdp);
        await pc.setLocalDescription(await pc.createAnswer());
        signal(id,{sdp:pc.localDescription});
        for(const ice of p.earlyICE.splice(0))await pc.addIceCandidate(ice);
      } else if(data.sdp?.type==="answer"&&p.pc) {
        await p.pc.setRemoteDescription(data.sdp);
        for(const ice of p.earlyICE.splice(0))await p.pc.addIceCandidate(ice);
      } else if(data.ice) {
        if(p.pc?.remoteDescription)await p.pc.addIceCandidate(data.ice);
        else if(p.earlyICE.length<100)p.earlyICE.push(data.ice);
      }
    } catch(error) {console.warn("WebRTC negotiation failed",error);}
  }
  function relayTransport(id) {
    const peer=peers.get(id),socket=peer?.socket;
    if(socket?.readyState!==WebSocket.OPEN)
      throw new Error("Η άλλη συσκευή δεν είναι διαθέσιμη.");
    const encoded=new TextEncoder().encode(id);
    return {
      kind:"relay",
      get readyState(){return socket.readyState===WebSocket.OPEN?"open":"closed";},
      get bufferedAmount(){return socket.bufferedAmount||0;},
      send(data) {
        if(socket.readyState!==WebSocket.OPEN)throw new Error("Η σύνδεση διακόπηκε.");
        if(typeof data==="string")
          socket.send(JSON.stringify({type:"relay",to:id,data}));
        else{
          const bytes=new Uint8Array(data);
          const packet=new Uint8Array(12+bytes.byteLength);
          packet.set(encoded,0);packet.set(bytes,12);
          socket.send(packet);
        }
      }
    };
  }
  function directTransport(dc){
    return {kind:"direct",get readyState(){return dc.readyState;},
      get bufferedAmount(){return dc.bufferedAmount;},send(data){dc.send(data);},
      drain:()=>new Promise((resolve,reject)=>{
        if(dc.readyState!=="open")return reject(new Error("Η άμεση σύνδεση έκλεισε."));
        if(dc.bufferedAmount<=dc.bufferedAmountLowThreshold)return resolve();
        const timer=setTimeout(()=>{dc.removeEventListener("bufferedamountlow",done);reject(new Error("Η σύνδεση καθυστέρησε."));},20000);
        function done(){clearTimeout(timer);resolve();}
        dc.addEventListener("bufferedamountlow",done,{once:true});
      })
    };
  }
  function tryDirect(id) {
    return new Promise((resolve,reject)=>{
      const p=peers.get(id);
      if(!p)return reject(new Error("Η συσκευή αποσυνδέθηκε."));
      const pc=makePC(id);
      let settled=false;
      const timer=setTimeout(()=>finish(new Error("WebRTC timeout")),8500);
      function finish(err,dc){
        if(settled)return;settled=true;clearTimeout(timer);
        if(err)reject(err);else resolve(directTransport(dc));
      }
      const dc=pc.createDataChannel("data",{ordered:true});
      setupDC(id,dc,()=>finish(null,dc));
      pc.createOffer().then(offer=>pc.setLocalDescription(offer))
        .then(()=>{if(p.pc===pc)signal(id,{sdp:pc.localDescription});})
        .catch(e=>finish(e));
    });
  }
  async function connect(id) {
    const p=peers.get(id);
    if(!p)throw new Error("Η συσκευή δεν είναι διαθέσιμη.");
    if(p.dc?.readyState==="open")return directTransport(p.dc);
    if(p.ready)return p.ready;
    const promise=(async()=>{
      if(typeof RTCPeerConnection==="function") {
        try{return await tryDirect(id);}
        catch(error){if(p.pc){try{p.pc.close();}catch{}}p.pc=null;}
      }
      const transport=relayTransport(id);
      p.mode="relay";renderPeers();
      return transport;
    })();
    p.ready=promise;
    try{return await promise;}finally{if(p.ready===promise)p.ready=null;}
  }
  function sendControl(to,message,via) {
    const p=peers.get(to);
    const payload=JSON.stringify(message);
    try{
      if(via==="direct"&&p?.dc?.readyState==="open")p.dc.send(payload);
      else relayTransport(to).send(payload);
    }catch(error){console.warn("Could not reply to sender",error);}
  }
  let redraw=null;
  function drawSoon(){
    if(redraw)return;
    redraw=requestAnimationFrame(()=>{redraw=null;renderItems();});
  }
  function addTransfer(item){
    transfers.unshift(item);
    while(transfers.length>100){const old=transfers.pop();if(old.url)URL.revokeObjectURL(old.url);}
    drawSoon();return item;
  }
  function onData(id,payload,via) {
    const p=peers.get(id);if(!p)return;
    if(typeof payload==="string"){
      let m;
      try{m=JSON.parse(payload);}catch{return;}
      if(m.t==="text"&&typeof m.text==="string"){
        addTransfer({dir:"in",peer:p.name,text:m.text.slice(0,5000),done:true});
      }else if(m.t==="progress"&&awaiting.has(m.id)){
        awaiting.get(m.id).got=Math.max(awaiting.get(m.id).got,Number(m.got)||0);
      }else if(m.t==="received"&&awaiting.has(m.id)){
        awaiting.get(m.id).resolve();
      }else if(m.t==="meta"){
        const size=Number(m.size);
        const fileName=String(m.name||"").split(/[\\/]/).pop().slice(0,200);
        if(!fileName||!Number.isSafeInteger(size)||size<0||size>MAX_FILE||p.rx)return;
        const idCode=String(m.id||"");
        if(!/^[0-9a-f]{32}$/.test(idCode))return;
        const item=addTransfer({dir:"in",peer:p.name,name:fileName,size,got:0,done:false});
        p.rx={id:idCode,name:fileName,size,mime:String(m.type||"").slice(0,100),
          chunks:[],item,via,lastAck:0};
        if(size===0)finishIncoming(p);
      }
      return;
    }
    const rx=p.rx;if(!rx)return;
    const bytes=payload instanceof ArrayBuffer?payload:
      (ArrayBuffer.isView(payload)?payload.buffer.slice(payload.byteOffset,payload.byteOffset+payload.byteLength):null);
    if(!bytes||rx.item.got+bytes.byteLength>rx.size){
      rx.item.err="Λανθασμένο μέγεθος αρχείου.";p.rx=null;drawSoon();return;
    }
    rx.chunks.push(bytes);
    rx.item.got+=bytes.byteLength;
    if(rx.item.got-rx.lastAck>=256*1024){
      rx.lastAck=rx.item.got;
      sendControl(p.id,{t:"progress",id:rx.id,got:rx.item.got},rx.via);
    }
    drawSoon();
    if(rx.item.got===rx.size)finishIncoming(p);
  }
  function finishIncoming(p){
    const rx=p.rx;
    if(!rx)return;
    p.rx=null;
    rx.item.url=URL.createObjectURL(new Blob(rx.chunks,{type:rx.mime||"application/octet-stream"}));
    rx.item.done=true;drawSoon();
    sendControl(p.id,{t:"received",id:rx.id},rx.via);
    toast("Παραλήφθηκε: "+rx.name);
  }
  function enqueue(id,fn){
    const p=peers.get(id);if(!p)return;
    p.q=p.q.catch(()=>{}).then(fn);
  }
  function makeReceipt(id){
    let resolve,reject;
    const promise=new Promise((a,b)=>{resolve=a;reject=b;});
    const receipt={got:0,resolve,reject,promise};
    awaiting.set(id,receipt);
    return receipt;
  }
  async function waitForRoom(receipt,offset,transport,until){
    while(transport.readyState==="open"){
      if(transport.kind==="direct"&&transport.bufferedAmount<=WINDOW)return;
      if(transport.kind==="relay"&&offset-receipt.got<=WINDOW&&transport.bufferedAmount<=WINDOW)return;
      if(Date.now()>until)throw new Error("Η μεταφορά δεν προχωρά. Δοκίμασε ξανά.");
      if(transport.kind==="direct"&&transport.bufferedAmount>WINDOW&&transport.drain)
        await transport.drain();
      else await new Promise(resolve=>setTimeout(resolve,45));
    }
    throw new Error("Η σύνδεση διακόπηκε.");
  }
  async function sendFile(id,file,item) {
    if(file.size>MAX_FILE)throw new Error("Μέγιστο αρχείο 256 MB.");
    const transport=await connect(id);
    item.method=transport.kind;
    const tx=crypto.randomUUID().replace(/-/g,"");
    const receipt=makeReceipt(tx);
    let timeout=null;
    try {
      transport.send(JSON.stringify({t:"meta",id:tx,name:file.name,size:file.size,type:file.type}));
      for(let offset=0;offset<file.size;offset+=CHUNK){
        // Only stall a transfer after 30 seconds without network progress;
        // large files must not have a hard two-minute total duration limit.
        await waitForRoom(receipt,offset,transport,Date.now()+30000);
        const bytes=await file.slice(offset,offset+CHUNK).arrayBuffer();
        transport.send(bytes);
        item.got=Math.min(offset+bytes.byteLength,file.size);
        drawSoon();
      }
      await Promise.race([receipt.promise,new Promise((_,reject)=>{
        timeout=setTimeout(()=>reject(new Error("Δεν ήρθε επιβεβαίωση παραλαβής.")),120000);
      })]);
      item.got=file.size;item.done=true;
    } finally {clearTimeout(timeout);awaiting.delete(tx);drawSoon();}
  }
  function sendFiles(id,fileList) {
    const p=peers.get(id);if(!p)return toast("Δεν υπάρχει συνδεδεμένη συσκευή.",true);
    for(const file of [...fileList]){
      const item=addTransfer({dir:"out",peer:p.name,name:file.name,size:file.size,got:0,done:false});
      if(file.size>MAX_FILE){item.err="Το αρχείο είναι μεγαλύτερο από 256 MB.";continue;}
      enqueue(id,async()=>{
        try{await sendFile(id,file,item);toast("Στάλθηκε: "+file.name);}
        catch(error){item.err=error.message||"Σφάλμα μεταφοράς";toast(item.err,true);drawSoon();}
      });
    }
  }
  function sendText(id,text){
    const p=peers.get(id);if(!p||!text)return;
    const clean=text.slice(0,5000);
    const item=addTransfer({dir:"out",peer:p.name,text:clean,done:false});
    enqueue(id,async()=>{
      try {const transport=await connect(id);transport.send(JSON.stringify({t:"text",text:clean}));item.done=true;item.method=transport.kind;}
      catch(error){item.err=error.message||"Αποτυχία αποστολής";}
      drawSoon();
    });
  }
  function renderPeers() {
    el.peers.replaceChildren();
    el.peerCount.textContent=peers.size===1?"1 συσκευή":peers.size+" συσκευές";
    if(!peers.size){
      el.peers.append(node("div","peers-empty","Περιμένουμε τη δεύτερη συσκευή. Άνοιξε εκεί τον σύνδεσμο ή πληκτρολόγησε τον κωδικό."));
      return;
    }
    for(const [id,p] of peers){
      const card=node("article","peer-card");
      const avatar=node("div","peer-avatar");
      avatar.append(icon(/κινητ|iphone|ipad|android/i.test(p.name)?"phone":"laptop"));
      const info=node("div","peer-info");
      info.append(node("strong","peer-title",p.name));
      info.append(node("small","peer-sub",p.mode==="direct"?"Απευθείας σύνδεση":p.mode==="relay"?"Σύνδεση μέσω Cloudflare":"Διαθέσιμη για μεταφορά"));
      const actions=node("div","peer-buttons");
      const send=node("button","send-button","Αποστολή αρχείων");
      send.type="button";send.addEventListener("click",()=>{selectedPeer=id;el.file.click();});
      const textBtn=node("button","text-button","Κείμενο / link");
      textBtn.type="button";textBtn.addEventListener("click",()=>{
        const text=prompt("Γράψε κείμενο ή έναν σύνδεσμο για "+p.name+":");
        if(text?.trim())sendText(id,text.trim());
      });
      actions.append(send,textBtn);card.append(avatar,info,actions);
      card.addEventListener("dragover",e=>{e.preventDefault();card.classList.add("drag");});
      card.addEventListener("dragleave",()=>card.classList.remove("drag"));
      card.addEventListener("drop",e=>{
        e.preventDefault();card.classList.remove("drag");
        if(e.dataTransfer?.files?.length)sendFiles(id,e.dataTransfer.files);
      });
      el.peers.append(card);
    }
  }
  function renderItems(){
    el.items.replaceChildren();
    if(!transfers.length){
      el.items.append(node("div","items-empty","Οι αποστολές και οι παραλαβές θα εμφανίζονται εδώ."));
      return;
    }
    for(const t of transfers){
      const row=node("article","transfer"+(t.err?" errored":""));
      const graphic=node("div","transfer-icon");graphic.append(icon(t.dir==="in"?"download":"upload"));
      const body=node("div","transfer-body");
      if(t.text!==undefined){
        body.append(node("strong","transfer-title",t.dir==="in"?"Κείμενο που έλαβες":"Κείμενο που έστειλες"));
        body.append(node("div","transfer-note",t.text));
        if(t.dir==="in"){
          const copyButton=node("button","text-copy","Αντιγραφή");
          copyButton.type="button";copyButton.onclick=()=>copy(t.text,"Το κείμενο αντιγράφηκε.");
          body.append(copyButton);
        }
      }else{
        body.append(node("strong","transfer-title",t.name));
        const pct=t.size?Math.round(t.got/t.size*100):100;
        const status=t.err?"Σφάλμα: "+t.err:t.done?"Ολοκληρώθηκε":pct+"% · σε εξέλιξη";
        body.append(node("small","transfer-note",fmt(t.size)+" · "+(t.dir==="in"?"από ":"προς ")+t.peer+" · "+status+
          (t.method?" · "+(t.method==="direct"?"P2P":"Cloudflare"):"")));
        if(!t.done&&!t.err){
          const progress=node("div","transfer-progress");progress.setAttribute("role","progressbar");
          progress.setAttribute("aria-valuenow",String(pct));progress.setAttribute("aria-valuemin","0");progress.setAttribute("aria-valuemax","100");
          const fill=node("i","");fill.style.width=pct+"%";progress.append(fill);body.append(progress);
        }
        if(t.dir==="in"&&t.url){
          const link=node("a","download-button","Λήψη αρχείου");
          link.href=t.url;link.download=t.name;body.append(link);
        }
      }
      row.append(graphic,body);el.items.append(row);
    }
  }
  function onlyPeer(){
    if(peers.size===1)return peers.keys().next().value;
    toast(peers.size?"Επίλεξε πρώτα τη συσκευή από τη λίστα.":"Άνοιξε πρώτα το link στη δεύτερη συσκευή.",true);
    return null;
  }
  el.rename.onclick=()=>{
    const chosen=prompt("Όνομα συσκευής",name);
    if(chosen?.trim()){
      name=chosen.trim().slice(0,40);store.set("name",name);
      renderSelf();
      for(const socket of [ws,backupWs])
        if(socket?.readyState===WebSocket.OPEN)
          socket.send(JSON.stringify({type:"hello",id:myId,name}));
    }
  };
  el.file.addEventListener("change",event=>{
    if(selectedPeer&&event.target.files?.length)sendFiles(selectedPeer,event.target.files);
    event.target.value="";
  });
  el.drop.addEventListener("click",event=>{
    if(event.target.closest("button"))return;
    const id=onlyPeer();if(id){selectedPeer=id;el.file.click();}
  });
  el.drop.addEventListener("keydown",event=>{
    if(event.key==="Enter"||event.key===" "){event.preventDefault();const id=onlyPeer();if(id){selectedPeer=id;el.file.click();}}
  });
  el.drop.addEventListener("dragover",event=>{event.preventDefault();el.drop.classList.add("drag");});
  el.drop.addEventListener("dragleave",()=>el.drop.classList.remove("drag"));
  el.drop.addEventListener("drop",event=>{
    event.preventDefault();el.drop.classList.remove("drag");
    const id=onlyPeer();if(id&&event.dataTransfer?.files?.length)sendFiles(id,event.dataTransfer.files);
  });
  document.addEventListener("visibilitychange",()=>{
    if(document.hidden)return;
    if(!ws||ws.readyState>1)connectSocket("primary");
    if(extraRoom&&(!backupWs||backupWs.readyState>1))connectSocket("extra");
  });
  window.addEventListener("online",()=>{
    if(!ws||ws.readyState>1)connectSocket("primary");
    discoverGateway();
  });
  window.addEventListener("beforeunload",()=>{
    for(const t of transfers)if(t.url)URL.revokeObjectURL(t.url);
    stopSocket();
  });
  if("serviceWorker" in navigator&&window.isSecureContext)
    navigator.serviceWorker.register("/sw.js").catch(()=>{});
  // Ignore legacy invitations: matching is now automatic on both devices.
  if(location.search)history.replaceState(null,"",location.pathname);
  renderSelf();renderPeers();renderItems();connectSocket("primary");
  discoverGateway();
})();
