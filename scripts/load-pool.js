// Metabase -> sample -> Supabase pool (daily) + BigQuery reviewed-sync
// Robust: each card fetched independently with retry; one card's failure
// does NOT abort the others; pool is only replaced if we actually got rows.
const MB_URL = "https://metabase.spyne.ai";
const MB_USER = process.env.METABASE_USER;
const MB_PASS = process.env.METABASE_PASS;
const SB_URL  = process.env.SUPABASE_URL;
const SB_KEY  = process.env.SUPABASE_KEY;
const GCP_SA  = process.env.GCP_SA_KEY;
const sbHead  = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, "Content-Type": "application/json" };

// QC-Pass = card 8351, Re-Edit = card 8346
const CARDS = {
  qc:      { card: 8351, userCol: "qc_user_name", actionEq: null,             actionNe: null,             l: "input_image_hres_url", m: "output_image_hres_url", r: "manual_image_hres_url" },
  edited:  { card: 8346, userCol: "last_qc_user", actionEq: null,             actionNe: "qc_editingtool", l: "input_image_hres_url", m: "ai_output",             r: "final_output" },
  qc_tool: { card: 8346, userCol: "last_qc_user", actionEq: "qc_editingtool", actionNe: null,             l: "input_image_hres_url", m: "ai_output",             r: "final_output" },
};
const IMG = "ai.image_id", SKU = "sku_id", ENT = "enterprise_name", ACT = "latest_image_action";
const SAMPLE_RATE = 0.25;
const MAX_POOL = 35000;

const sleep = ms => new Promise(r => setTimeout(r, ms));

function yesterdayStr() {
  const d = new Date(); d.setDate(d.getDate() - 1);
  return d.toISOString().slice(0, 10);
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

// Fetch one Metabase card's CSV with retry (handles 504 / transient errors).
async function fetchCardCsv(card, token, attempts = 3) {
  let lastErr = "";
  for (let a = 1; a <= attempts; a++) {
    try {
      const r = await fetch(`${MB_URL}/api/card/${card}/query/csv`, {
        method: "POST", headers: { "X-Metabase-Session": token },
      });
      if (r.ok) return parseCSV(await r.text());
      lastErr = "HTTP " + r.status;
    } catch (e) { lastErr = e.message; }
    console.log(`MB card ${card} attempt ${a} failed: ${lastErr}`);
    if (a < attempts) await sleep(10000); // 10s before retry
  }
  throw new Error(`MB card ${card} failed after ${attempts} attempts: ${lastErr}`);
}

// BigQuery reviewed image_ids -> Supabase reviews (already-reviewed never re-served)
async function syncReviewedFromBigQuery() {
  if (!GCP_SA) { console.log("No GCP_SA_KEY — skipping BQ reviewed sync"); return; }
  const { BigQuery } = await import("@google-cloud/bigquery");
  const creds = JSON.parse(GCP_SA);
  const bq = new BigQuery({ projectId: creds.project_id, credentials: creds });
  const [rows] = await bq.query({
    query: `SELECT DISTINCT Image_ID FROM \`spyne-reprocess.spot_qc.image_responses\`
            WHERE Image_ID IS NOT NULL AND Image_ID != ''
              AND Timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY)`,
    location: "asia-south1",
  });
  const ids = rows.map(r => r.Image_ID).filter(Boolean);
  console.log("BQ reviewed image_ids:", ids.length);
  for (let i = 0; i < ids.length; i += 1000) {
    const batch = ids.slice(i, i+1000).map(id => ({ image_id: id }));
    await sb("POST", "/rest/v1/reviews?on_conflict=image_id", batch, { Prefer: "resolution=ignore-duplicates,return=minimal" });
  }
}

// Sample one card's parsed CSV into rows (per user×enterprise 25%, cap MAX_POOL, dedup by seen)
function sampleCard(mode, csv, seen, dataDate, remainingCap) {
  const cfg = CARDS[mode];
  if (!csv || csv.length < 2) return [];
  const H = csv[0].map(h => h.trim().toLowerCase());
  const ci = n => H.indexOf(n.toLowerCase());
  const cImg=ci(IMG), cSku=ci(SKU), cEnt=ci(ENT), cUsr=ci(cfg.userCol), cAct=ci(ACT), cL=ci(cfg.l), cM=ci(cfg.m), cR=ci(cfg.r);
  if (cImg < 0) { console.log(`${mode}: image col missing`); return []; }

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
  const out = [];
  for (const k of Object.keys(groups)) {
    const arr = shuffle(groups[k]);
    const take = Math.max(1, Math.ceil(arr.length * SAMPLE_RATE));
    for (let i = 0; i < Math.min(take, arr.length) && out.length < remainingCap; i++) out.push(arr[i]);
    if (out.length >= remainingCap) break;
  }
  return out;
}

async function main() {
  const dataDate = yesterdayStr();

  // SKIP check: pool already has today-1 data?
  const chk = await fetch(`${SB_URL}/rest/v1/rpc/pool_needs_load`, { method: "POST", headers: sbHead, body: "{}" });
  const needsLoad = await chk.json();
  if (needsLoad !== true) { console.log("Pool already fresh (today-1). Skip."); return; }
  console.log("Pool stale/empty -> loading for", dataDate);

  // 1) BigQuery reviewed -> Supabase reviews (best-effort)
  try { await syncReviewedFromBigQuery(); } catch (e) { console.log("BQ sync failed (continuing):", e.message); }

  // 2) Metabase login
  const sess = await fetch(`${MB_URL}/api/session`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username: MB_USER, password: MB_PASS }),
  });
  if (!sess.ok) throw new Error("MB login " + sess.status);
  const token = (await sess.json()).id;

  // 3) Fetch + sample each card INDEPENDENTLY (retry per card; one failure skips only that card)
  const rows = [], seen = {}, csvCache = {};
  let anyOk = false, anyFail = false;
  for (const mode of Object.keys(CARDS)) {
    const cfg = CARDS[mode];
    try {
      let csv = csvCache[cfg.card];
      if (!csv) { csv = await fetchCardCsv(cfg.card, token); csvCache[cfg.card] = csv; }
      const got = sampleCard(mode, csv, seen, dataDate, MAX_POOL - rows.length);
      rows.push(...got);
      anyOk = true;
      console.log(`${mode}: sampled ${got.length} (total ${rows.length})`);
    } catch (e) {
      anyFail = true;
      console.log(`${mode}: SKIPPED — ${e.message}`);
    }
  }

  // 4) SAFETY: if nothing sampled (all cards failed) -> do NOT touch pool (keep yesterday's).
  if (!rows.length) {
    console.log("No rows from any card — pool left untouched (previous data kept).");
    process.exit(1); // non-zero so the run is marked failed and the schedule retries next slot
  }

  console.log("Total sampled rows:", rows.length, anyFail ? "(some cards skipped)" : "(all cards ok)");

  // 5) REPLACE pool (only now that we have rows)
  await sb("DELETE", "/rest/v1/pool?image_id=not.is.null", null, { Prefer: "return=minimal" });
  for (let i = 0; i < rows.length; i += 1000) {
    await sb("POST", "/rest/v1/pool?on_conflict=image_id", rows.slice(i, i+1000), { Prefer: "resolution=ignore-duplicates,return=minimal" });
  }

  // 6) exclude reviewed from pool + cleanup
  await fetch(`${SB_URL}/rest/v1/rpc/exclude_reviewed_from_pool`, { method: "POST", headers: sbHead, body: "{}" });
  await fetch(`${SB_URL}/rest/v1/rpc/cleanup_reviews`, { method: "POST", headers: sbHead, body: "{}" });

  console.log("DONE. Loaded", rows.length, "rows for", dataDate);
}

main().catch(e => { console.error("FAILED:", e.message); process.exit(1); });
