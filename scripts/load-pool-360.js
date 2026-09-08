// 360: Metabase (card 7610) -> Supabase pool_360 (full week) + BigQuery reviewed-sync
const MB_URL = "https://metabase.spyne.ai";
const MB_USER = process.env.METABASE_USER;
const MB_PASS = process.env.METABASE_PASS;
const SB_URL  = process.env.SUPABASE_URL;
const SB_KEY  = process.env.SUPABASE_KEY;
const GCP_SA  = process.env.GCP_SA_KEY;
const sbHead  = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, "Content-Type": "application/json" };

const CARD_360 = 7610;
const SPIN = "spin_id", SKU = "sku_id", ENT = "enterprise", USR = "user", URL = "spin_url", ISS = "issues";

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
async function sb(method, path, body, extra) {
  const res = await fetch(SB_URL + path, {
    method, headers: { ...sbHead, ...(extra||{}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new Error(`Supabase ${method} ${path} -> ${res.status} ${(await res.text()).slice(0,200)}`);
  return res;
}

// BigQuery reviewed spin_ids (responses_360) -> Supabase reviews_360
async function syncReviewedFromBigQuery360() {
  if (!GCP_SA) { console.log("No GCP_SA_KEY — skip BQ 360 sync"); return; }
  const { BigQuery } = await import("@google-cloud/bigquery");
  const creds = JSON.parse(GCP_SA);
  const bq = new BigQuery({ projectId: creds.project_id, credentials: creds });
  const [rows] = await bq.query({
    query: `SELECT DISTINCT Spin_ID FROM \`spyne-reprocess.spot_qc.responses_360\`
            WHERE Spin_ID IS NOT NULL AND Spin_ID != ''
              AND Timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)`,
    location: "asia-south1",
  });
  const ids = rows.map(r => r.Spin_ID).filter(Boolean);
  console.log("BQ reviewed spin_ids:", ids.length);
  for (let i = 0; i < ids.length; i += 1000) {
    const batch = ids.slice(i, i+1000).map(id => ({ spin_id: id }));
    await sb("POST", "/rest/v1/reviews_360?on_conflict=spin_id", batch, { Prefer: "resolution=ignore-duplicates,return=minimal" });
  }
}

async function main() {
  // SKIP check: pool_360 already fresh today?
  const chk = await fetch(`${SB_URL}/rest/v1/rpc/pool360_needs_load`, { method: "POST", headers: sbHead, body: "{}" });
  const needsLoad = await chk.json();
  if (needsLoad !== true) { console.log("pool_360 already fresh today. Skip."); return; }
  console.log("pool_360 stale/empty -> loading");

  // 1) BQ reviewed -> reviews_360
  try { await syncReviewedFromBigQuery360(); } catch (e) { console.log("BQ 360 sync failed (continuing):", e.message); }

  // 2) Metabase login + fetch card 7610
  const sess = await fetch(`${MB_URL}/api/session`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username: MB_USER, password: MB_PASS }),
  });
  if (!sess.ok) throw new Error("MB login " + sess.status);
  const token = (await sess.json()).id;
  const r = await fetch(`${MB_URL}/api/card/${CARD_360}/query/csv`, { method: "POST", headers: { "X-Metabase-Session": token } });
  if (!r.ok) throw new Error("MB card " + CARD_360 + " " + r.status);
  const csv = parseCSV(await r.text());
  if (!csv || csv.length < 2) throw new Error("360 CSV empty");

  const H = csv[0].map(h => h.trim().toLowerCase());
  const ci = n => H.indexOf(n.toLowerCase());
  const cSpin=ci(SPIN), cSku=ci(SKU), cEnt=ci(ENT), cUsr=ci(USR), cUrl=ci(URL), cIss=ci(ISS), cDate=ci("date");
  if (cSpin < 0) throw new Error("spin_id col missing. Headers: " + JSON.stringify(H));

  const today = new Date().toISOString().slice(0,10);
  const rows = [], seen = {};
  for (let i = 1; i < csv.length; i++) {
    const f = csv[i];
    const spin = (f[cSpin]||"").trim();
    if (!spin || seen[spin]) continue;   // FULL data, no sampling; just dedup spin_id
    seen[spin] = true;
    rows.push({
      spin_id: spin,
      sku_id: cSku>=0 ? (f[cSku]||"").trim() : "",
      enterprise: cEnt>=0 ? ((f[cEnt]||"").trim() || "(Unknown)") : "(Unknown)",
      qc_user: cUsr>=0 ? ((f[cUsr]||"").trim() || "N/A") : "N/A",
      spin_url: cUrl>=0 ? (f[cUrl]||"") : "",
      issues: cIss>=0 ? (f[cIss]||"") : "",
      data_date: today,
    });
  }
  console.log("360 rows:", rows.length);
  if (!rows.length) throw new Error("No 360 rows — aborting (pool untouched)");

  // 3) REPLACE pool_360
  await sb("DELETE", "/rest/v1/pool_360?spin_id=not.is.null", null, { Prefer: "return=minimal" });
  for (let i = 0; i < rows.length; i += 1000) {
    await sb("POST", "/rest/v1/pool_360?on_conflict=spin_id", rows.slice(i, i+1000), { Prefer: "resolution=ignore-duplicates,return=minimal" });
  }

  // 4) exclude reviewed + cleanup
  await fetch(`${SB_URL}/rest/v1/rpc/exclude_reviewed_from_pool_360`, { method: "POST", headers: sbHead, body: "{}" });
  await fetch(`${SB_URL}/rest/v1/rpc/cleanup_reviews_360`, { method: "POST", headers: sbHead, body: "{}" });

  console.log("DONE. Loaded", rows.length, "360 rows");
}

main().catch(e => { console.error("FAILED:", e.message); process.exit(1); });
