/**
 * Snapshot de debug/auditoria enxuto: só itens com conflitos ou rejeições, valores sanitizados.
 * Não altera scraping, merge nem validação — só o ficheiro derivado.
 */

import path from 'node:path';
import { config } from '../config.js';
import { writeSnapshot, writeSnapshotSync } from '../io/writeSnapshot.js';
import { LEAN_SCHEMA_DEBUG } from './leanSchemaVersions.js';

/** @param {unknown} v */
function str(v) {
  return v == null ? '' : String(v).trim();
}

const STR_MAX = 160;

/**
 * @param {unknown} v
 * @param {number} [max]
 */
function truncateDebugValue(v, max = STR_MAX) {
  if (v == null) return v;
  if (typeof v === 'string') {
    return v.length > max ? `${v.slice(0, max)}...` : v;
  }
  if (typeof v === 'number' || typeof v === 'boolean') return v;
  try {
    const s = JSON.stringify(v);
    if (s.length <= max) return /** @type {unknown} */ (JSON.parse(s));
    return `${s.slice(0, max)}...`;
  } catch {
    const s = String(v);
    return s.length > max ? `${s.slice(0, max)}...` : s;
  }
}

/**
 * @param {unknown} conflict
 * @returns {Record<string, unknown>}
 */
export function sanitizeConflict(conflict) {
  if (!conflict || typeof conflict !== 'object') {
    return {
      field: '',
      rejected_source: '',
      kept_source: '',
      rejected_value: conflict,
      kept_value: conflict,
    };
  }
  const c = /** @type {Record<string, unknown>} */ (conflict);
  const out = {
    field: c.field,
    rejected_source: c.rejected_source,
    kept_source: c.kept_source,
  };

  const field = str(c.field);
  if (field === 'url' || field === 'url_primary') {
    return {
      ...out,
      rejected_value: '[tracking_or_raw_url]',
      kept_value: '[derived_or_raw_url]',
    };
  }

  if (typeof c.rejected_value === 'string') {
    out.rejected_value =
      c.rejected_value.length > STR_MAX ? `${c.rejected_value.slice(0, STR_MAX)}...` : c.rejected_value;
  } else {
    out.rejected_value = truncateDebugValue(c.rejected_value);
  }

  if (typeof c.kept_value === 'string') {
    out.kept_value =
      c.kept_value.length > STR_MAX ? `${c.kept_value.slice(0, STR_MAX)}...` : c.kept_value;
  } else {
    out.kept_value = truncateDebugValue(c.kept_value);
  }

  return out;
}

/**
 * @param {unknown} rejection
 * @returns {Record<string, unknown>}
 */
export function sanitizeRejection(rejection) {
  if (!rejection || typeof rejection !== 'object') {
    return { field: '', source: '', reason: '', value: rejection };
  }
  const r = /** @type {Record<string, unknown>} */ (rejection);
  const out = {
    field: r.field,
    source: r.source,
    reason: r.reason,
  };

  if (typeof r.value === 'string') {
    out.value = r.value.length > STR_MAX ? `${r.value.slice(0, STR_MAX)}...` : r.value;
  } else {
    out.value = truncateDebugValue(r.value);
  }

  return out;
}

/** @param {unknown} v @param {number} max */
function shortLabel(v, max = STR_MAX) {
  const s = str(v);
  if (!s) return '';
  return s.length > max ? `${s.slice(0, max)}...` : s;
}

/**
 * @param {Record<string, unknown>} product
 */
function buildDebugLeanItem(product) {
  const conflicts = Array.isArray(product._source_conflicts) ? product._source_conflicts : [];
  const rejections = Array.isArray(product._field_rejections) ? product._field_rejections : [];
  return {
    product_id: str(product.product_id) || undefined,
    listing_product_id: str(product.listing_product_id) || undefined,
    item_id: str(product.item_id) || undefined,
    canonical_id: str(product.canonical_id) || undefined,
    name: shortLabel(product.name, 200),
    url: shortLabel(product.url),
    _source_conflicts: conflicts.map((c) => sanitizeConflict(c)),
    _field_rejections: rejections.map((r) => sanitizeRejection(r)),
  };
}

/**
 * @param {{ meta?: Record<string, unknown>; items?: Record<string, Record<string, unknown>> }} fullPayload
 */
export function buildPdpDebugLeanSnapshot(fullPayload) {
  const meta = fullPayload.meta && typeof fullPayload.meta === 'object' ? { ...fullPayload.meta } : {};
  const items = fullPayload.items && typeof fullPayload.items === 'object' ? fullPayload.items : {};
  /** @type {Record<string, Record<string, unknown>>} */
  const outItems = {};
  for (const [k, rec] of Object.entries(items)) {
    const conflicts = Array.isArray(rec._source_conflicts) ? rec._source_conflicts : [];
    const rejections = Array.isArray(rec._field_rejections) ? rec._field_rejections : [];
    if (conflicts.length === 0 && rejections.length === 0) continue;
    outItems[k] = buildDebugLeanItem(rec);
  }
  return {
    schema_version: LEAN_SCHEMA_DEBUG,
    meta,
    items: outItems,
  };
}

/**
 * @param {{ meta?: Record<string, unknown>; items?: Record<string, Record<string, unknown>> }} fullPayload
 */
export async function writePdpDebugLeanFromPayload(fullPayload) {
  const out = str(config.mlPdpDebugLeanOutput);
  if (!out || out === '-' || out.toLowerCase() === 'none') return;
  const debugPath = path.resolve(out);
  const debugPayload = buildPdpDebugLeanSnapshot(fullPayload);
  const pretty = config.mlBulkPretty;
  const json = pretty ? JSON.stringify(debugPayload, null, 2) : JSON.stringify(debugPayload);
  await writeSnapshot({
    latestPath: debugPath,
    historySubdir: 'debug',
    historyBaseName: 'pdp_debug_lean',
    content: json,
  });
  console.info(
    `[ml-pdp-debug-lean] gravado ${Object.keys(debugPayload.items).length} itens com conflitos/rejeições → ${debugPath}`
  );
}

/**
 * @param {{ meta?: Record<string, unknown>; items?: Record<string, Record<string, unknown>> }} fullPayload
 */
export function writePdpDebugLeanFromPayloadSync(fullPayload) {
  const out = str(config.mlPdpDebugLeanOutput);
  if (!out || out === '-' || out.toLowerCase() === 'none') return;
  const debugPath = path.resolve(out);
  const debugPayload = buildPdpDebugLeanSnapshot(fullPayload);
  const pretty = config.mlBulkPretty;
  const json = pretty ? JSON.stringify(debugPayload, null, 2) : JSON.stringify(debugPayload);
  writeSnapshotSync({
    latestPath: debugPath,
    historySubdir: 'debug',
    historyBaseName: 'pdp_debug_lean',
    content: json,
  });
  console.info(
    `[ml-pdp-debug-lean] gravado ${Object.keys(debugPayload.items).length} itens com conflitos/rejeições → ${debugPath}`
  );
}
