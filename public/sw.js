/* Cache only the static app. Files, websocket data and discovery messages are never stored. */
const CACHE="datatransfer-shell-v8";
const SHELL=["/","/index.html","/style.css","/app.js","/manifest.json","/icon-192.png","/icon-512.png"];
self.addEventListener("install",event=>{
  event.waitUntil(caches.open(CACHE).then(c=>c.addAll(SHELL)).then(()=>self.skipWaiting()));
});
self.addEventListener("activate",event=>{
  event.waitUntil(caches.keys().then(keys=>Promise.all(
    keys.filter(key=>key!==CACHE).map(key=>caches.delete(key))
  )).then(()=>self.clients.claim()));
});
self.addEventListener("fetch",event=>{
  if(event.request.method!=="GET")return;
  const url=new URL(event.request.url);
  if(url.origin!==self.location.origin||url.pathname==="/ws")return;
  if(event.request.mode==="navigate"){
    event.respondWith(fetch(event.request).then(response=>{
      if(response.ok)event.waitUntil(caches.open(CACHE).then(c=>c.put("/index.html",response.clone())));
      return response;
    }).catch(()=>caches.match("/index.html")));
    return;
  }
  if(!SHELL.includes(url.pathname))return;
  event.respondWith(fetch(event.request).then(response=>{
    if(response.ok)event.waitUntil(caches.open(CACHE).then(c=>c.put(url.pathname,response.clone())));
    return response;
  }).catch(()=>caches.match(url.pathname)));
});
