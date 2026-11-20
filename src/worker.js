// Signaling για WebRTC. Δεν περνάνε αρχεία από εδώ, μόνο μηνύματα σύνδεσης.
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/ws") {
      if (req.headers.get("Upgrade") !== "websocket")
        return new Response("Expected websocket", { status: 426 });
      const code = (url.searchParams.get("room") || "").trim().toLowerCase().slice(0, 32);
      let key;
      if (code) key = "room:" + code;
      else {
        // Ίδιο δίκτυο = ίδια δημόσια IP (για IPv6 κρατάμε το /64)
        const ip = req.headers.get("CF-Connecting-IP") || "unknown";
        key = "ip:" + (ip.includes(":") ? ip.split(":").slice(0, 4).join(":") : ip);
      }
      return env.ROOMS.get(env.ROOMS.idFromName(key)).fetch(req);
    }
    return env.ASSETS.fetch(req);
  },
};

export class Room {
  constructor(state) {
    this.state = state;
  }
  async fetch() {
    const [client, server] = Object.values(new WebSocketPair());
    this.state.acceptWebSocket(server);
    return new Response(null, { status: 101, webSocket: client });
  }
  broadcast(exclude) {
    const live = this.state.getWebSockets().filter((w) => w !== exclude);
    const byId = new Map();
    for (const w of live) {
      const a = w.deserializeAttachment();
      if (a) byId.set(a.id, a);
    }
    for (const w of live) {
      const me = w.deserializeAttachment();
      if (!me) continue;
      const peers = [...byId.values()].filter((p) => p.id !== me.id);
      try { w.send(JSON.stringify({ type: "peers", peers })); } catch {}
    }
  }
  webSocketMessage(ws, raw) {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (m.type === "hello") {
      ws.serializeAttachment({ id: String(m.id).slice(0, 16), name: String(m.name || "?").slice(0, 40) });
      this.broadcast();
    } else if (m.type === "signal") {
      const me = ws.deserializeAttachment();
      if (!me) return;
      for (const w of this.state.getWebSockets()) {
        const a = w.deserializeAttachment();
        if (a && a.id === m.to) {
          try { w.send(JSON.stringify({ type: "signal", from: me.id, data: m.data })); } catch {}
        }
      }
    }
  }
  webSocketClose(ws) {
    try { ws.close(); } catch {}
    this.broadcast(ws);
  }
  webSocketError(ws) {
    this.broadcast(ws);
  }
}
