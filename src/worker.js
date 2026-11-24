// Cloudflare Durable Object: rendezvous, WebRTC signaling and optional streaming
// relay when a direct data channel cannot be established. No files are stored.
const HEX_ID = /^[a-f0-9]{12,24}$/;
const MAX_BINARY = 48 * 1024 + 12; // 12-byte recipient prefix + one bounded chunk
const MAX_TEXT = 12000;

// Both devices on an IPv6 LAN usually share a /64 even if their temporary
// privacy addresses differ. Normalize compressed IPv6 before grouping.
function networkKey(ip) {
  if(!ip.includes(":")) return "ip:" + ip;
  const parts=ip.toLowerCase().split("::");
  if(parts.length>2)return "isolated:" + ip;
  const left=parts[0]?parts[0].split(":"):[];
  const right=parts[1]?parts[1].split(":"):[];
  // IPv4-mapped addresses are treated as single endpoints, not /64 networks.
  if(parts.length===1&&left.length!==8||[...left,...right].some(x=>!/^[0-9a-f]{1,4}$/.test(x)))
    return "ip:" + ip;
  const gap=8-left.length-right.length;
  if(gap<0||(parts.length===1&&gap!==0))return "ip:" + ip;
  const full=[...left,...Array(gap).fill("0"),...right];
  return "ip6:" + full.slice(0,4).map(part=>parseInt(part,16).toString(16)).join(":");
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/ws") {
      if ((req.headers.get("Upgrade") || "").toLowerCase() !== "websocket")
        return new Response("Expected websocket", { status: 426 });
      const room = (url.searchParams.get("room") || "").trim().toLowerCase();
      // Existing DriveTunes signaling clients still use their dt- prefixed rooms.
      // Keep the legacy IP group for older clients, but the new DataTransfer
      // always creates a random explicit room instead of guessing public IP.
      const ip = req.headers.get("CF-Connecting-IP");
      const key = room && /^[a-z0-9-]{6,32}$/.test(room)
        ? "room:" + room
        : ip ? networkKey(ip) : "isolated:" + crypto.randomUUID();

      return env.ROOMS.get(env.ROOMS.idFromName(key)).fetch(req);
    }
    return env.ASSETS.fetch(req);
  }
};

export class Room {
  constructor(state) { this.state = state; }
  async fetch() {
    if (this.state.getWebSockets().length >= 12)
      return new Response("Room is full", { status: 429 });
    const [client, server] = Object.values(new WebSocketPair());
    this.state.acceptWebSocket(server);
    return new Response(null, { status: 101, webSocket: client });
  }
  members() {
    const members = new Map();
    for (const socket of this.state.getWebSockets()) {
      const info = socket.deserializeAttachment();
      if (info?.id && HEX_ID.test(info.id)) members.set(info.id, {socket, info});
    }
    return members;
  }
  broadcast() {
    const members = this.members();
    for (const {socket, info} of members.values()) {
      const peers = [...members.values()]
        .filter(entry => entry.info.id !== info.id)
        .map(entry => entry.info);
      try { socket.send(JSON.stringify({ type: "peers", peers })); } catch {}
    }
  }
  webSocketMessage(socket, raw) {
    const me = socket.deserializeAttachment();
    if (typeof raw !== "string") {
      if (!me || !(raw instanceof ArrayBuffer) || raw.byteLength < 13 || raw.byteLength > MAX_BINARY) return;
      const bytes = new Uint8Array(raw);
      const targetId = new TextDecoder().decode(bytes.subarray(0, 12));
      if (!/^[a-f0-9]{12}$/.test(targetId) || targetId === me.id) return;
      const target = this.members().get(targetId);
      if (!target || target.socket === socket) return;
      // Relay only one chunk at a time. Outbound frame begins with the sender ID.
      const prefix = new TextEncoder().encode(me.id.slice(0, 12));
      const packet = new Uint8Array(12 + bytes.byteLength - 12);
      packet.set(prefix, 0);
      packet.set(bytes.subarray(12), 12);
      try { target.socket.send(packet); } catch {}
      return;
    }
    if (raw.length > MAX_TEXT) return;
    let message;
    try { message = JSON.parse(raw); } catch { return; } // keepalive "ping"
    if (message.type === "hello") {
      const id = String(message.id || "").toLowerCase();
      if (!HEX_ID.test(id)) return;
      // Reject collisions rather than letting two sockets impersonate one peer.
      const existing = this.members().get(id);
      if (existing && existing.socket !== socket) return;
      socket.serializeAttachment({id, name: String(message.name || "Συσκευή").slice(0, 40)});
      this.broadcast();
      return;
    }
    if (!me || !HEX_ID.test(me.id)) return;
    const to = String(message.to || "").toLowerCase();
    const target = this.members().get(to);
    if (!target || to === me.id || target.socket === socket) return;
    if (message.type === "signal" && message.data && typeof message.data === "object") {
      try { target.socket.send(JSON.stringify({type:"signal",from:me.id,data:message.data})); } catch {}
    } else if (message.type === "relay" && typeof message.data === "string" && message.data.length <= 8000) {
      try { target.socket.send(JSON.stringify({type:"relay",from:me.id,data:message.data})); } catch {}
    }
  }
  webSocketClose(socket) {
    try { socket.close(); } catch {}
    this.broadcast();
  }
  webSocketError(socket) { this.broadcast(); }
}
