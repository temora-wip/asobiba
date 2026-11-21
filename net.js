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

  function okCode(code){ return /^[0-9]{4}$/.test(code); }

  window.Net = {
    ready,
    // サイトのON/OFF（config/open）を見張る。cb(true/false, エラー文字)
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
        if(!v || Date.now() - (v.created || 0) > ROOM_TTL){
          await fb.set(fb.ref(db, `rooms/${code}`), { meta:{ created:Date.now(), host:String(hostName).slice(0,12) } });
          return code;
        }
      }
      throw new Error("full");
    },
    async roomExists(code){
      await ready;
      if(!okCode(code)) return false;
      const s = await fb.get(fb.ref(db, `rooms/${code}/meta`));
      const v = s.val();
      return !!v && Date.now() - (v.created || 0) < ROOM_TTL;
    },
    // 部屋に入る。返り値は「参加者の状態を共有する」ための小さな道具箱
    async join(code){
      await ready;
      const peersRef = fb.ref(db, `rooms/${code}/peers`);
      const meRef = fb.push(peersRef);
      const myId = meRef.key;
      let mine = {};
      const peerH = [], connH = [], errH = [];
      let last = [];
      let connected = false;

      const write = () => fb.set(meRef, { p:JSON.stringify(mine), t:Date.now() }).catch(err => errH.forEach(f => f({ code:"revoked", message:String(err) })));
      fb.onDisconnect(meRef).remove();
      await write();

      fb.onValue(peersRef, snap => {
        const val = snap.val() || {};
        const arr = Object.entries(val).map(([k, v]) => {
          let pr = {};
          try{ pr = JSON.parse(v && v.p) || {}; }catch(e){}
          return { peer:k, presence:pr, isMe:k === myId, sameTab:k === myId, kind:"viewer", guest:false, by:null, updatedAt:(v && v.t) || 0 };
        });
        if(!arr.some(x => x.isMe)) arr.push({ peer:myId, presence:mine, isMe:true, sameTab:true, kind:"viewer", guest:false, by:null, updatedAt:Date.now() });
        last = arr;
        peerH.forEach(h => h({ peers:arr, joined:[], left:[], updated:[] }));
      }, err => errH.forEach(f => f({ code:"revoked", message:String(err) })));

      fb.onValue(fb.ref(db, ".info/connected"), s => {
        connected = !!s.val();
        if(connected){ fb.onDisconnect(meRef).remove(); write(); }
        connH.forEach(h => h(connected));
      });

      return {
        code, myId,
        presence(patch){
          for(const k in patch){ if(patch[k] === null) delete mine[k]; else mine[k] = patch[k]; }
          mine = JSON.parse(JSON.stringify(mine));
          return write();
        },
        peers(){ return last; },
        onPeers(h, onErr){ peerH.push(h); if(onErr) errH.push(onErr); setTimeout(() => h({ peers:last, joined:[], left:[], updated:[] }), 0); return () => {}; },
        onConnection(h, onErr){ connH.push(h); if(onErr) errH.push(onErr); setTimeout(() => h(connected), 0); return () => {}; },
        connected(){ return connected; }
      };
    }
  };
})();
