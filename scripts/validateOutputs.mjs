/**
 * Validação pontual da estrutura de outputs (latest/lean/debug/metrics).
 * Uso: node scripts/validateOutputs.mjs
 */
import fs from 'node:fs';
import path from 'node:path';

const out = path.resolve('output');
const latest = [
  'catalogo_ml.json',
  'catalogo_ml_lean.json',
  'pdp_all.json',
  'pdp_all_lean.json',
  'pdp_debug_lean.json',
  'metrics.json',
];

function size(p) {
  try {
    return fs.statSync(p).size;
  } catch {
    return null;
  }
}

const report = { files: {}, catalog: {}, pdp: {}, debug: {}, metrics: {}, history: {}, issues: [] };

for (const f of latest) {
  const p = path.join(out, f);
  const sz = size(p);
  report.files[f] = sz == null ? 'MISSING' : sz;
  if (sz == null && f !== 'catalogo_ml_lean.json') {
    report.issues.push(`Falta ficheiro: ${f}`);
  }
}

const histRoot = path.join(out, 'history');
if (fs.existsSync(histRoot)) {
  const subs = ['catalog', 'pdp', 'debug', 'metrics'];
  report.history.subdirs = {};
  for (const s of subs) {
    const d = path.join(histRoot, s);
    if (!fs.existsSync(d)) {
      report.history.subdirs[s] = 0;
      continue;
    }
    report.history.subdirs[s] = fs.readdirSync(d).filter((x) => x.endsWith('.json')).length;
  }
} else {
  report.history.subdirs = null;
}

function sampleKeys(obj, n = 200) {
  const k = Object.keys(obj.items || {});
  return k.slice(0, n);
}

function forbiddenCatalogLean(p) {
  const bad = ['score', 'completeness_score', 'normalized_sales', 'rank_position', '_source_conflicts', '_provenance'];
  return bad.filter((k) => k in p);
}

function forbiddenPdpLean(p) {
  const bad = [
    'score',
    'completeness_score',
    'normalized_sales',
    '_source_conflicts',
    '_field_rejections',
    '_provenance',
    '_field_sources',
    'validation',
    'issues',
  ];
  return bad.filter((k) => k in p);
}

function essentialCatalog(p) {
  const need = ['product_id', 'name', 'price_current', 'url', 'collected_at'];
  const img = Array.isArray(p.images) && p.images.length > 0;
  const im = p.image_main && String(p.image_main).trim();
  const miss = need.filter((k) => p[k] == null || p[k] === '');
  return { ok: miss.length === 0 && (img || im), missing: miss, hasShipping: p.shipping != null };
}

function essentialPdp(p) {
  const checks = {
    product_id: Boolean(String(p.product_id || '').trim()),
    item_id: Boolean(String(p.item_id || '').trim()),
    name: Boolean(String(p.name || '').trim()),
    price_current: typeof p.price_current === 'number' && Number.isFinite(p.price_current),
    url: Boolean(String(p.url || p.url_primary || '').trim()),
    collected_at: Boolean(String(p.collected_at || '').trim()),
  };
  return { checks };
}

const catPath = path.join(out, 'catalogo_ml.json');
const catLeanPath = path.join(out, 'catalogo_ml_lean.json');
if (report.files['catalogo_ml.json'] !== 'MISSING') {
  const cat = JSON.parse(fs.readFileSync(catPath, 'utf8'));
  const keys = sampleKeys(cat, 200);
  report.catalog.total_items = Object.keys(cat.items || {}).length;
  let essOk = 0;
  const forb = new Set();
  for (const id of keys) {
    const p = cat.items[id];
    if (essentialCatalog(p).ok) essOk++;
    forbiddenCatalogLean(p).forEach((x) => forb.add(x));
  }
  report.catalog.raw_sample_essential_ok = `${essOk}/${keys.length}`;
  report.catalog.raw_forbidden_keys_in_sample = [...forb];

  if (report.files['catalogo_ml_lean.json'] !== 'MISSING') {
    const lean = JSON.parse(fs.readFileSync(catLeanPath, 'utf8'));
    report.catalog.lean_total = Object.keys(lean.items || {}).length;
    report.catalog.lean_smaller_bytes = size(catLeanPath) < size(catPath);
    let leOk = 0;
    const lforb = new Set();
    let keyMismatches = 0;
    for (const id of keys) {
      if (!lean.items[id]) {
        keyMismatches++;
        continue;
      }
      const p = lean.items[id];
      if (essentialCatalog(p).ok) leOk++;
      forbiddenCatalogLean(p).forEach((x) => lforb.add(x));
    }
    report.catalog.lean_sample_essential_ok = `${leOk}/${keys.length}`;
    report.catalog.lean_forbidden_keys_in_sample = [...lforb];
    report.catalog.lean_key_mismatches_in_sample = keyMismatches;
  } else {
    report.catalog.lean_note = 'catalogo_ml_lean.json ausente neste workspace';
  }
}

const pdpPath = path.join(out, 'pdp_all.json');
const pdpLeanPath = path.join(out, 'pdp_all_lean.json');
if (report.files['pdp_all.json'] !== 'MISSING' && report.files['pdp_all_lean.json'] !== 'MISSING') {
  const full = JSON.parse(fs.readFileSync(pdpPath, 'utf8'));
  const lean = JSON.parse(fs.readFileSync(pdpLeanPath, 'utf8'));
  const keys = Object.keys(full.items || {});
  report.pdp.total_full = keys.length;
  report.pdp.total_lean = Object.keys(lean.items || {}).length;
  report.pdp.lean_smaller_bytes = size(pdpLeanPath) < size(pdpPath);
  report.pdp.key_sets_match = report.pdp.total_full === report.pdp.total_lean;

  const sample = keys.slice(0, Math.min(150, keys.length));
  let essScore = 0;
  const forbLean = new Set();
  for (const id of sample) {
    const p = lean.items[id];
    if (!p) continue;
    const e = essentialPdp(p);
    if (Object.values(e.checks).every(Boolean)) essScore++;
    forbiddenPdpLean(p).forEach((x) => forbLean.add(x));
  }
  report.pdp.sample_essential_core_ok = `${essScore}/${sample.length}`;
  report.pdp.lean_forbidden_present_in_sample = [...forbLean];

  const one = keys[0];
  if (one) {
    const L = lean.items[one];
    const F = full.items[one];
    report.pdp.spot_check_id = one;
    report.pdp.spot_lean_has = {
      seller_id: Boolean(L?.seller_id),
      categories: Array.isArray(L?.categories) && L.categories.length > 0,
      taxonomy_path: Boolean(L?.taxonomy_path),
      images: Array.isArray(L?.images) && L.images.length > 0,
      description: Boolean(L?.description),
      shop_name: Boolean(L?.shop_name),
      shop_link: Boolean(L?.shop_link),
      shipping: L?.shipping != null,
      pdp_attributes_table:
        L?.pdp_attributes_table != null &&
        Object.keys(typeof L.pdp_attributes_table === 'object' ? L.pdp_attributes_table : {}).length > 0,
      sales_count: typeof L?.sales_count === 'number',
      rating: typeof L?.rating === 'number' && L.rating > 0,
      rating_count: typeof L?.rating_count === 'number' && L.rating_count > 0,
      price_original_when_full:
        F?.price_original != null && Number(F.price_original) > 0
          ? typeof L?.price_original === 'number' && L.price_original > 0
          : 'n/a_or_zero_in_full',
    };
  }
}

const dbgPath = path.join(out, 'pdp_debug_lean.json');
if (report.files['pdp_debug_lean.json'] !== 'MISSING') {
  const dbg = JSON.parse(fs.readFileSync(dbgPath, 'utf8'));
  const ids = Object.keys(dbg.items || {});
  report.debug.item_count = ids.length;
  const allowedTop = new Set(['product_id', 'item_id', 'name', 'url', '_source_conflicts', '_field_rejections']);
  let badShape = 0;
  let urlSanOk = true;
  let longStrings = 0;
  for (const id of ids.slice(0, 400)) {
    const it = dbg.items[id];
    const top = Object.keys(it);
    if (!top.every((k) => allowedTop.has(k))) badShape++;
    const conf = it._source_conflicts || [];
    const rej = it._field_rejections || [];
    if (conf.length === 0 && rej.length === 0) badShape++;
    for (const c of conf) {
      if (c.field === 'url' || c.field === 'url_primary') {
        if (c.rejected_value !== '[tracking_or_raw_url]' || c.kept_value !== '[derived_or_raw_url]') urlSanOk = false;
      }
      for (const fld of ['rejected_value', 'kept_value']) {
        if (typeof c[fld] === 'string' && c[fld].length > 200) longStrings++;
      }
    }
    for (const r of rej) {
      if (typeof r.value === 'string' && r.value.length > 200) longStrings++;
    }
  }
  report.debug.sample_bad_shape_or_empty_audit = badShape;
  report.debug.url_conflict_sanitized_ok = urlSanOk;
  report.debug.oversized_strings_in_sample = longStrings;

  const full = report.files['pdp_all.json'] !== 'MISSING' ? JSON.parse(fs.readFileSync(pdpPath, 'utf8')) : null;
  if (full) {
    let withAudit = 0;
    for (const rec of Object.values(full.items || {})) {
      const c = Array.isArray(rec._source_conflicts) ? rec._source_conflicts.length : 0;
      const r = Array.isArray(rec._field_rejections) ? rec._field_rejections.length : 0;
      if (c > 0 || r > 0) withAudit++;
    }
    report.debug.items_with_audit_in_full = withAudit;
    report.debug.debug_item_count_eq_full_audit = withAudit === ids.length;
  }
}

const mPath = path.join(out, 'metrics.json');
if (report.files['metrics.json'] !== 'MISSING') {
  const m = JSON.parse(fs.readFileSync(mPath, 'utf8'));
  const sum = (m.success_items || 0) + (m.failed_items || 0);
  report.metrics.processed_eq_success_plus_failed = sum === (m.processed_items ?? -1);
  report.metrics.unique_eq_success = m.unique_items_stored === m.success_items;
  report.metrics.fields_filled_present = typeof m.fields_filled === 'object' && m.fields_filled != null;
  report.metrics.snapshot = {
    processed_items: m.processed_items,
    success_items: m.success_items,
    failed_items: m.failed_items,
    unique_items_stored: m.unique_items_stored,
  };
  if (report.pdp.total_full != null) {
    report.metrics.pdp_json_items_match_unique = m.unique_items_stored === report.pdp.total_full;
  }
}

console.log(JSON.stringify(report, null, 2));
