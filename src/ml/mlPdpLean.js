/**
 * Snapshot PDP “lean”: só campos úteis no dia a dia, sem proveniência pesada / placeholders.
 * Não altera scraping, merge nem validação — só o ficheiro derivado.
 */

import path from 'node:path';
import { config } from '../config.js';
import { writeSnapshot, writeSnapshotSync } from '../io/writeSnapshot.js';
import { LEAN_SCHEMA_PDP } from './leanSchemaVersions.js';

/** @param {unknown} v */
function str(v) {
  return v == null ? '' : String(v).trim();
}

/**
 * @param {Record<string, unknown>} product
 * @returns {Record<string, unknown>}
 */
export function buildPdpLean(product) {
  const p = product && typeof product === 'object' ? product : {};
  /** @type {Record<string, unknown>} */
  const lean = {};

  if (str(p.product_id)) lean.product_id = str(p.product_id);
  if (str(p.listing_product_id)) lean.listing_product_id = str(p.listing_product_id);
  if (str(p.item_id)) lean.item_id = str(p.item_id);
  if (str(p.canonical_id)) lean.canonical_id = str(p.canonical_id);
  if (str(p.variation_id)) lean.variation_id = str(p.variation_id);
  if (str(p.category_id)) lean.category_id = str(p.category_id);
  if (str(p.domain_id)) lean.domain_id = str(p.domain_id);

  if (str(p.name)) lean.name = str(p.name);
  if (typeof p.price_current === 'number') lean.price_current = p.price_current;
  if (typeof p.price_original === 'number' && p.price_original > 0) lean.price_original = p.price_original;
  if (typeof p.discount === 'number' && p.discount > 0) lean.discount = p.discount;
  if (str(p.price_currency)) lean.price_currency = str(p.price_currency);

  if (typeof p.sales_count === 'number') lean.sales_count = p.sales_count;
  if (p.sales_count_precision && str(p.sales_count_precision) !== 'unknown') {
    lean.sales_count_precision = p.sales_count_precision;
  }
  if (typeof p.rating === 'number' && p.rating > 0) lean.rating = p.rating;
  if (typeof p.rating_count === 'number' && p.rating_count > 0) lean.rating_count = p.rating_count;
  if (str(p.seller_reputation_snippet)) lean.seller_reputation_snippet = str(p.seller_reputation_snippet);

  if (str(p.shop_name)) lean.shop_name = str(p.shop_name);
  if (str(p.seller_id)) lean.seller_id = str(p.seller_id);
  if (str(p.shop_link)) lean.shop_link = str(p.shop_link);

  if (Array.isArray(p.images) && p.images.length > 0) lean.images = [...p.images];
  if (str(p.image_main)) lean.image_main = str(p.image_main);

  if (str(p.description)) lean.description = str(p.description);
  if (Array.isArray(p.categories) && p.categories.length > 0) lean.categories = [...p.categories];
  if (str(p.taxonomy_path)) lean.taxonomy_path = str(p.taxonomy_path);
  if (str(p.product_category_from_breadcrumb)) {
    lean.product_category_from_breadcrumb = str(p.product_category_from_breadcrumb);
  }

  if (p.shipping) lean.shipping = p.shipping;
  if (p.shipping_precision && str(p.shipping_precision) !== 'unknown') {
    lean.shipping_precision = p.shipping_precision;
  }
  if (str(p.stock_hint)) lean.stock_hint = str(p.stock_hint);
  if (str(p.stock_status)) lean.stock_status = str(p.stock_status);
  if (typeof p.available_quantity_embedded === 'number') {
    lean.available_quantity_embedded = p.available_quantity_embedded;
  }

  const pat = p.pdp_attributes_table;
  if (pat && typeof pat === 'object' && Object.keys(/** @type {Record<string, unknown>} */ (pat)).length > 0) {
    lean.pdp_attributes_table = pat;
  }

  if (str(p.pdp_subtitle)) lean.pdp_subtitle = str(p.pdp_subtitle);
  if (p.pdp_installments) lean.pdp_installments = p.pdp_installments;
  if (str(p.pdp_shipping_snippet)) lean.pdp_shipping_snippet = str(p.pdp_shipping_snippet);
  if (p.review_summary_ai) lean.review_summary_ai = p.review_summary_ai;

  if (str(p.url)) lean.url = str(p.url);
  if (str(p.url_primary)) lean.url_primary = str(p.url_primary);
  if (str(p.collected_at)) lean.collected_at = str(p.collected_at);

  return lean;
}

/**
 * @param {{ meta?: Record<string, unknown>; items?: Record<string, Record<string, unknown>> }} fullPayload
 */
export function buildLeanPdpSnapshot(fullPayload) {
  const meta = fullPayload.meta && typeof fullPayload.meta === 'object' ? { ...fullPayload.meta } : {};
  const items = fullPayload.items && typeof fullPayload.items === 'object' ? fullPayload.items : {};
  /** @type {Record<string, Record<string, unknown>>} */
  const leanItems = {};
  for (const [k, rec] of Object.entries(items)) {
    leanItems[k] = buildPdpLean(rec);
  }
  return {
    schema_version: LEAN_SCHEMA_PDP,
    meta,
    items: leanItems,
  };
}

/**
 * @param {{ meta?: Record<string, unknown>; items?: Record<string, Record<string, unknown>> }} fullPayload
 */
export async function writePdpLeanFromPayload(fullPayload) {
  const out = str(config.mlPdpLeanOutput);
  if (!out || out === '-' || out.toLowerCase() === 'none') return;
  const leanPath = path.resolve(out);
  const leanPayload = buildLeanPdpSnapshot(fullPayload);
  const pretty = config.mlBulkPretty;
  const json = pretty ? JSON.stringify(leanPayload, null, 2) : JSON.stringify(leanPayload);
  await writeSnapshot({
    latestPath: leanPath,
    historySubdir: 'pdp',
    historyBaseName: 'pdp_all_lean',
    content: json,
  });
  console.info(`[ml-pdp-lean] gravado ${Object.keys(leanPayload.items).length} itens → ${leanPath}`);
}

/**
 * @param {{ meta?: Record<string, unknown>; items?: Record<string, Record<string, unknown>> }} fullPayload
 */
export function writePdpLeanFromPayloadSync(fullPayload) {
  const out = str(config.mlPdpLeanOutput);
  if (!out || out === '-' || out.toLowerCase() === 'none') return;
  const leanPath = path.resolve(out);
  const leanPayload = buildLeanPdpSnapshot(fullPayload);
  const pretty = config.mlBulkPretty;
  const json = pretty ? JSON.stringify(leanPayload, null, 2) : JSON.stringify(leanPayload);
  writeSnapshotSync({
    latestPath: leanPath,
    historySubdir: 'pdp',
    historyBaseName: 'pdp_all_lean',
    content: json,
  });
  console.info(`[ml-pdp-lean] gravado ${Object.keys(leanPayload.items).length} itens → ${leanPath}`);
}
