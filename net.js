// 通信部分（Firebase Realtime Database）。
// ゲーム本体からは window.Net だけを使います。設定は config.js に書きます。
(function(){
  "use strict";
  if(window.NET_MOCK){ window.Net = window.NET_MOCK; return; }

  const SDK = "https://www.gstatic.com/firebasejs/10.12.2/";
  const ROOM_TTL = 12 * 3600 * 1000;   // 12時間たった部屋は作り直してよい
  let fb = null, db = null;

  const ready = (async () => {
    const cfg = window.FIREBASE_CONFIG || {};
    if(!cfg.apiKey || String(cfg.apiKey).includes("ここに")) throw new Error("config");
    const appMod = await import(SDK + "firebase-app.js");
    fb = await import(SDK + "firebase-database.js");
    const app = appMod.initializeApp(cfg);
    db = fb.getDatabase(app);
  })();

  const okCode = code => /^[0-9]{4}$/.test(code);
  const fresh = m => !!m && Date.now() - (m.created || 0) < ROOM_TTL;

  window.Net = {
    ready,
    // サイトのON/OFF（config/open）を見張る。cb(true/false, エラー)
    async onOpen(cb){
      try{ await ready; }catch(e){ cb(false, e.message === "config" ? "config" : "load"); return; }
      fb.onValue(fb.ref(db, "config/open"), s => cb(s.val() === true), () => cb(false, "denied"));
    },
    // 空いている4けたの部屋コードを作る
    async createRoom(hostName){
      await ready;
      for(let i = 0; i < 25; i++){
        const code = String(1000 + Math.floor(Math.random() * 9000));
        const s = await fb.get(fb.ref(db, `rooms/${code}/meta`));
        const v = s.val();
        if(!v || !fresh(v) || v.closed){
          await fb.set(fb.ref(db, `rooms/${code}`), { meta:{ created:Date.now(), host:String(hostName).slice(0,12), closed:false, locked:false } });
          return code;
        }
      }
      throw new Error("full");
    },
    // 部屋の様子（入る前の確認用）
    async roomInfo(code){
      await ready;
      if(!okCode(code)) return { exists:false };
      const s = await fb.get(fb.ref(db, `rooms/${code}/meta`));
      const m = s.val();
      if(!fresh(m)) return { exists:false };
      let count = 0;
      try{ const ps = await fb.get(fb.ref(db, `rooms/${code}/peers`)); count = Object.keys(ps.val() || {}).length; }catch(e){}
      return { exists:true, host:m.host || "", closed:!!m.closed, locked:!!m.locked, count };
    },
    // 改善要望を送る（読めるのは管理者だけ）
    async sendFeedback(item){
      await ready;
      await fb.set(fb.push(fb.ref(db, "feedback")), item);
    },
    // 部屋に入る
    async join(code){
      await ready;
      const base = `rooms/${code}`;
      const peersRef = fb.ref(db, `${base}/peers`);
      const meRef = fb.push(peersRef);
      const myId = meRef.key;
      const avRef = fb.ref(db, `${base}/av/${myId}`);
      let mine = {}, myAv = "";
      const peerH = [], connH = [], errH = [], mediaH = [], metaH = [];
      let rawPeers = {}, avs = {}, last = [], connected = false, left = false;
      const unsubs = [];
      const fail = err => { if(!left) errH.forEach(f => f({ code:"revoked", message:String(err) })); };

      const write = () => left ? Promise.resolve() : fb.set(meRef, { p:JSON.stringify(mine), t:Date.now() }).catch(fail);
      fb.onDisconnect(meRef).remove();
      fb.onDisconnect(avRef).remove();
      await write();

      const emit = () => {
        const arr = Object.entries(rawPeers).map(([k, v]) => {
          let pr = {};
          try{ pr = JSON.parse(v && v.p) || {}; }catch(e){}
          return { peer:k, presence:pr, avatar:avs[k] || "", isMe:k === myId, sameTab:k === myId, updatedAt:(v && v.t) || 0 };
        });
        if(!arr.some(x => x.isMe)) arr.push({ peer:myId, presence:mine, avatar:myAv, isMe:true, sameTab:true, updatedAt:Date.now() });
        last = arr;
        peerH.forEach(h => h({ peers:arr }));
      };
      unsubs.push(fb.onValue(peersRef, snap => { rawPeers = snap.val() || {}; emit(); }, fail));
      unsubs.push(fb.onValue(fb.ref(db, `${base}/av`), snap => { avs = snap.val() || {}; emit(); }, () => {}));
      const mediaRef = fb.ref(db, `${base}/media`);
      const onMediaSnap = s => mediaH.forEach(h => h(s.key, s.val()));
      unsubs.push(fb.onChildAdded(mediaRef, onMediaSnap, () => {}));
      unsubs.push(fb.onChildChanged(mediaRef, onMediaSnap, () => {}));
      unsubs.push(fb.onValue(fb.ref(db, `${base}/meta`), s => metaH.forEach(h => h(s.val() || { closed:true })), () => {}));
      unsubs.push(fb.onValue(fb.ref(db, ".info/connected"), s => {
        connected = !!s.val();
        if(connected && !left){
          fb.onDisconnect(meRef).remove(); fb.onDisconnect(avRef).remove();
          write(); if(myAv) fb.set(avRef, myAv).catch(() => {});
        }
        connH.forEach(h => h(connected));
      }));

      return {
        code, myId,
        presence(patch){
          for(const k in patch){ if(patch[k] === null) delete mine[k]; else mine[k] = patch[k]; }
          mine = JSON.parse(JSON.stringify(mine));
          return write();
        },
        setAvatar(str){ myAv = String(str || ""); return fb.set(avRef, myAv).catch(() => {}); },
        setMedia(key, str){ return fb.set(fb.ref(db, `${base}/media/${key}`), String(str)).catch(() => {}); },
        setMeta(patch){ return fb.update(fb.ref(db, `${base}/meta`), patch).catch(() => {}); },
        onPeers(h, onErr){ peerH.push(h); if(onErr) errH.push(onErr); setTimeout(() => h({ peers:last }), 0); },
        onConnection(h, onErr){ connH.push(h); if(onErr) errH.push(onErr); setTimeout(() => h(connected), 0); },
        onMedia(h){ mediaH.push(h); },
        onMeta(h){ metaH.push(h); },
        connected(){ return connected; },
        async leave(){
          left = true;
          unsubs.forEach(u => { try{ u(); }catch(e){} });
          try{ await fb.onDisconnect(meRef).cancel(); await fb.onDisconnect(avRef).cancel(); }catch(e){}
          try{ await fb.remove(meRef); await fb.remove(avRef); }catch(e){}
        }
      };
    }
  };
})();
