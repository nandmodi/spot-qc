// Metabase -> sample -> Supabase pool (daily)
const MB_URL = "https://metabase.spyne.ai";
const MB_USER = process.env.METABASE_USER;
const MB_PASS = process.env.METABASE_PASS;
const SB_URL  = process.env.SUPABASE_URL;
const SB_KEY  = process.env.SUPABASE_KEY;
const sbHead  = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, "Content-Type": "application/json" };

// QC-Pass = card 8351, Re-Edit = card 8346
const CARDS = {
  qc:      { card: 8351, userCol: "qc_user_name", actionEq: null,             actionNe: null,             l: "input_image_hres_url", m: "output_image_hres_url", r: "manual_image_hres_url" },
  edited:  { card: 8346, userCol: "last_qc_user", actionEq: null,             actionNe: "qc_editingtool", l: "input_image_hres_url", m: "ai_output",             r: "final_output" },
  qc_tool: { card: 8346, userCol: "last_qc_user", actionEq: "qc_editingtool", actionNe: null,             l: "input_image_hres_url", m: "ai_output",             r: "final_output" },
};
const IMG = "image_id", SKU = "sku_id", ENT = "enterprise_name", ACT = "latest_image_action";
const PCT = 0.25;          // per user×enterprise %
const MAX_POOL = 35000;    // safety cap

function yesterdayStr() {
  const d = new Date(); d.setDate(d.getDate() - 1);
  return d.toISOString().slice(0, 10);   // YYYY-MM-DD
}
function parseCSV(text) {
  const rows = []; let row = [], cur = "", q = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (q) { if (c === '"') { if (text[i+1] === '"') { cur += '"'; i++; } else q = false; } else cur += c; }
    else { if (c === '"') q = true; else if (c === ",") { row.push(cur); cur = ""; }
      else if (c === "\n") { row.push(cur); rows.push(row); row = []; cur = ""; }
      else if (c !== "\r") cur += c; }
  }
  if (cur.length || row.length) { row.push(cur); rows.push(row); }
  return rows;
}
function shuffle(a) { for (let i=a.length-1;i>0;i--){ const j=Math.floor(Math.random()*(i+1)); [a[i],a[j]]=[a[j],a[i]]; } return a; }

async function sb(method, path, body, extra) {
  const res = await fetch(SB_URL + path, {
    method, headers: { ...sbHead, ...(extra||{}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new Error(`Supabase ${method} ${path} -> ${res.status} ${(await res.text()).slice(0,200)}`);
  return res;
}

async function main() {
  const dataDate = yesterdayStr();

  // 1. SKIP check: pool already has today-1 data?
  const chk = await fetch(`${SB_URL}/rest/v1/rpc/pool_needs_load`, { method: "POST", headers: sbHead, body: "{}" });
  const needsLoad = await chk.json();
  if (needsLoad !== true) { console.log("Pool already fresh (today-1). Skip."); return; }
  console.log("Pool stale/empty -> loading for", dataDate);

  // 2. Metabase login
  const sess = await fetch(`${MB_URL}/api/session`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username: MB_USER, password: MB_PASS }),
  });
  if (!sess.ok) throw new Error("MB login " + sess.status);
  const token = (await sess.json()).id;

  // 3. fetch + sample per card
  const rows = [], seen = {}, csvCache = {};
  for (const mode of Object.keys(CARDS)) {
    const cfg = CARDS[mode];
    let csv = csvCache[cfg.card];
    if (!csv) {
      const r = await fetch(`${MB_URL}/api/card/${cfg.card}/query/csv`, { method: "POST", headers: { "X-Metabase-Session": token } });
      if (!r.ok) throw new Error("MB card " + cfg.card + " " + r.status);
      csv = parseCSV(await r.text()); csvCache[cfg.card] = csv;
    }
    if (!csv || csv.length < 2) continue;
    const H = csv[0].map(h => h.trim().toLowerCase());
    const ci = n => H.indexOf(n.toLowerCase());
    const cImg=ci(IMG), cSku=ci(SKU), cEnt=ci(ENT), cUsr=ci(cfg.userCol), cAct=ci(ACT), cL=ci(cfg.l), cM=ci(cfg.m), cR=ci(cfg.r);
    if (cImg < 0) { console.log(`${mode}: image col missing`); continue; }

    const groups = {};
    for (let i = 1; i < csv.length; i++) {
      const f = csv[i];
      const img = (f[cImg]||"").trim();
      if (!img || seen[img]) continue;
      const action = cAct>=0 ? (f[cAct]||"").trim() : "";
      if (cfg.actionEq && action !== cfg.actionEq) continue;
      if (cfg.actionNe && action === cfg.actionNe) continue;
      const user = (f[cUsr]||"").trim() || "N/A";
      const ent  = (f[cEnt]||"").trim() || "(Unknown)";
      (groups[user+"||"+ent] ||= []).push({
        image_id: img, mode,
        sku_id: cSku>=0 ? (f[cSku]||"").trim() : "",
        enterprise: ent, qc_user: user,
        input_img: cL>=0 ? (f[cL]||"") : "",
        ai_img:    cM>=0 ? (f[cM]||"") : "",
        final_img: cR>=0 ? (f[cR]||"") : "",
        data_date: dataDate,
      });
      seen[img] = true;
    }
    for (const k of Object.keys(groups)) {
      const arr = shuffle(groups[k]);
      const take = Math.max(1, Math.ceil(arr.length * PCT));
      for (let i = 0; i < Math.min(take, arr.length) && rows.length < MAX_POOL; i++) rows.push(arr[i]);
    }
  }
  console.log("Sampled rows:", rows.length);
  if (!rows.length) throw new Error("No rows sampled — aborting (pool untouched)");

  // 4. REPLACE pool (safe: only after we HAVE rows)
  await sb("DELETE", "/rest/v1/pool?image_id=not.is.null", null, { Prefer: "return=minimal" });
  for (let i = 0; i < rows.length; i += 1000) {
    await sb("POST", "/rest/v1/pool?on_conflict=image_id", rows.slice(i, i+1000), { Prefer: "resolution=ignore-duplicates,return=minimal" });
  }
  // 5. cleanup old reviews
  await fetch(`${SB_URL}/rest/v1/rpc/cleanup_reviews`, { method: "POST", headers: sbHead, body: "{}" });

  console.log("DONE. Loaded", rows.length, "rows for", dataDate);
}

main().catch(e => { console.error("FAILED:", e.message); process.exit(1); });
