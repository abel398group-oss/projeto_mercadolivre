/**
 * Catálogo “lean” para descoberta: só campos úteis, sem metadados de PDP / merge.
 * Não altera scraping nem merge — só o ficheiro derivado.
 */

import path from 'node:path';
import { config } from '../config.js';
import { writeSnapshot, writeSnapshotSync } from '../io/writeSnapshot.js';
import { LEAN_SCHEMA_CATALOG } from './leanSchemaVersions.js';
import { computeCanonicalProductId } from './mlProductFinalize.js';

/** @param {unknown} v */
function str(v) {
  return v == null ? '' : String(v).trim();
}

/**
 * @param {Record<string, unknown>} product
 * @returns {Record<string, unknown>}
 */
export function buildCatalogLean(product) {
  const p = product && typeof product === 'object' ? product : {};
  /** @type {Record<string, unknown>} */
  const lean = {};

  lean.product_id = str(p.product_id);
  const lip = str(p.listing_product_id) || str(p.product_id);
  if (lip) lean.listing_product_id = lip;
  const canon = str(p.canonical_id) || computeCanonicalProductId(/** @type {Record<string, unknown>} */ (p));
  if (canon) lean.canonical_id = canon;

  lean.name = str(p.name);
  lean.price_current = Number(p.price_current);
  if (!Number.isFinite(lean.price_current)) lean.price_current = 0;

  const po = Number(p.price_original);
  if (Number.isFinite(po) && po > 0) lean.price_original = po;

  const disc = Number(p.discount);
  if (Number.isFinite(disc) && disc > 0) lean.discount = disc;

  const rating = Number(p.rating);
  if (Number.isFinite(rating) && rating > 0) lean.rating = rating;

  const rc = Number(p.rating_count);
  if (Number.isFinite(rc) && rc > 0) lean.rating_count = rc;

  if (Array.isArray(p.images) && p.images.length > 0) {
    lean.images = [...p.images];
  }

  const im = str(p.image_main);
  if (im) lean.image_main = im;

  const u = str(p.url);
  if (u) lean.url = u;

  if (p.shipping && typeof p.shipping === 'object') {
    lean.shipping = p.shipping;
  }

  const ca = str(p.collected_at);
  if (ca) lean.collected_at = ca;

  const cs = str(p.category_source_id);
  if (cs) lean.category_source_id = cs;

  const pc = str(p.price_currency);
  if (pc) lean.price_currency = pc;

  return lean;
}

/**
 * @param {{ meta?: Record<string, unknown>; items?: Record<string, Record<string, unknown>> }} fullPayload
 */
export function buildLeanCatalogPayload(fullPayload) {
  const meta = fullPayload.meta && typeof fullPayload.meta === 'object' ? { ...fullPayload.meta } : {};
  const items = fullPayload.items && typeof fullPayload.items === 'object' ? fullPayload.items : {};
  /** @type {Record<string, Record<string, unknown>>} */
  const leanItems = {};
  for (const [k, rec] of Object.entries(items)) {
    leanItems[k] = buildCatalogLean(rec);
  }
  return {
    schema_version: LEAN_SCHEMA_CATALOG,
    meta,
    items: leanItems,
  };
}

/**
 * @param {{ meta?: Record<string, unknown>; items?: Record<string, Record<string, unknown>> }} fullPayload
 */
export async function writeCatalogLeanFile(fullPayload) {
  const out = str(config.mlCatalogLeanOutput);
  if (!out || out === '-' || out.toLowerCase() === 'none') return;
  const leanPath = path.resolve(out);
  const leanPayload = buildLeanCatalogPayload(fullPayload);
  const json = config.mlCatalogPretty ? JSON.stringify(leanPayload, null, 2) : JSON.stringify(leanPayload);
  await writeSnapshot({
    latestPath: leanPath,
    historySubdir: 'catalog',
    historyBaseName: 'catalogo_ml_lean',
    content: json,
  });
  console.info(`[ml-catalog-lean] gravado ${Object.keys(leanPayload.items).length} itens → ${leanPath}`);
}

/**
 * @param {{ meta?: Record<string, unknown>; items?: Record<string, Record<string, unknown>> }} fullPayload
 */
export function writeCatalogLeanFileSync(fullPayload) {
  const out = str(config.mlCatalogLeanOutput);
  if (!out || out === '-' || out.toLowerCase() === 'none') return;
  const leanPath = path.resolve(out);
  const leanPayload = buildLeanCatalogPayload(fullPayload);
  const json = config.mlCatalogPretty ? JSON.stringify(leanPayload, null, 2) : JSON.stringify(leanPayload);
  writeSnapshotSync({
    latestPath: leanPath,
    historySubdir: 'catalog',
    historyBaseName: 'catalogo_ml_lean',
    content: json,
  });
  console.info(`[ml-catalog-lean] gravado ${Object.keys(leanPayload.items).length} itens → ${leanPath}`);
}
