/**
 * Snapshot PDP “core”: camada mínima operacional (identidade, comercial, loja, URLs, mídia limitada).
 * Não altera scraping, merge nem validação — só export derivado.
 * O ficheiro “rico” continua em `mlPdpLean.js` → pdp_all_lean.json.
 */

import path from 'node:path';
import { config } from '../config.js';
import { writeSnapshot, writeSnapshotSync } from '../io/writeSnapshot.js';
import { LEAN_SCHEMA_CORE } from './leanSchemaVersions.js';

/** Máx. de URLs em `images` no core (primeiras N). */
const MAX_CORE_IMAGES = 8;

/** @param {unknown} v */
function str(v) {
  return v == null ? '' : String(v).trim();
}

/**
 * Inclui `shipping` só se houver sinal útil (evita objeto vazio / só placeholder).
 * @param {unknown} sh
 */
function shippingForCore(sh) {
  if (!sh || typeof sh !== 'object' || Array.isArray(sh)) return null;
  const o = /** @type {Record<string, unknown>} */ (sh);
  if (o.is_free === true) return sh;
  const text = str(o.text);
  if (text && text !== 'unknown') return sh;
  const price = o.price;
  if (typeof price === 'number' && Number.isFinite(price) && price > 0) return sh;
  if (str(o.mode)) return sh;
  return null;
}

/**
 * @param {Record<string, unknown>} product
 * @returns {Record<string, unknown>}
 */
export function buildPdpCoreProduct(product) {
  const p = product && typeof product === 'object' ? product : {};
  /** @type {Record<string, unknown>} */
  const core = {};

  if (str(p.product_id)) core.product_id = str(p.product_id);
  if (str(p.listing_product_id)) core.listing_product_id = str(p.listing_product_id);
  if (str(p.item_id)) core.item_id = str(p.item_id);
  if (str(p.canonical_id)) core.canonical_id = str(p.canonical_id);
  if (str(p.variation_id)) core.variation_id = str(p.variation_id);
  if (str(p.category_id)) core.category_id = str(p.category_id);
  if (str(p.domain_id)) core.domain_id = str(p.domain_id);

  if (str(p.name)) core.name = str(p.name);
  if (typeof p.price_current === 'number' && Number.isFinite(p.price_current)) {
    core.price_current = p.price_current;
  }
  if (typeof p.price_original === 'number' && Number.isFinite(p.price_original) && p.price_original > 0) {
    core.price_original = p.price_original;
  }
  if (typeof p.discount === 'number' && Number.isFinite(p.discount) && p.discount > 0) {
    core.discount = p.discount;
  }
  if (str(p.price_currency)) core.price_currency = str(p.price_currency);

  if (typeof p.sales_count === 'number' && Number.isFinite(p.sales_count)) {
    core.sales_count = p.sales_count;
  }
  if (p.sales_count_precision && str(p.sales_count_precision) && str(p.sales_count_precision) !== 'unknown') {
    core.sales_count_precision = p.sales_count_precision;
  }
  if (typeof p.rating === 'number' && Number.isFinite(p.rating) && p.rating > 0) {
    core.rating = p.rating;
  }
  if (typeof p.rating_count === 'number' && Number.isFinite(p.rating_count) && p.rating_count > 0) {
    core.rating_count = p.rating_count;
  }
  if (str(p.shop_name)) core.shop_name = str(p.shop_name);
  if (str(p.seller_id)) core.seller_id = str(p.seller_id);
  if (str(p.shop_link)) core.shop_link = str(p.shop_link);

  if (Array.isArray(p.categories) && p.categories.length > 0) {
    core.categories = [...p.categories];
  }
  if (str(p.taxonomy_path)) core.taxonomy_path = str(p.taxonomy_path);
  if (str(p.product_category_from_breadcrumb)) {
    core.product_category_from_breadcrumb = str(p.product_category_from_breadcrumb);
  }

  if (str(p.image_main)) core.image_main = str(p.image_main);
  if (Array.isArray(p.images) && p.images.length > 0) {
    const imgs = p.images.slice(0, MAX_CORE_IMAGES).map((x) => str(x)).filter(Boolean);
    if (imgs.length > 0) core.images = imgs;
  }

  const shCore = shippingForCore(p.shipping);
  if (shCore) core.shipping = shCore;
  if (p.shipping_precision && str(p.shipping_precision) && str(p.shipping_precision) !== 'unknown') {
    core.shipping_precision = p.shipping_precision;
  }
  if (str(p.stock_status)) core.stock_status = str(p.stock_status);
  if (str(p.stock_hint)) core.stock_hint = str(p.stock_hint);
  if (typeof p.available_quantity_embedded === 'number' && Number.isFinite(p.available_quantity_embedded)) {
    core.available_quantity_embedded = p.available_quantity_embedded;
  }

  if (str(p.url)) core.url = str(p.url);
  if (str(p.url_primary)) core.url_primary = str(p.url_primary);
  if (str(p.collected_at)) core.collected_at = str(p.collected_at);

  return core;
}

/**
 * @param {{ meta?: Record<string, unknown>; items?: Record<string, Record<string, unknown>> }} fullPayload
 */
export function buildPdpCoreSnapshot(fullPayload) {
  const meta = fullPayload.meta && typeof fullPayload.meta === 'object' ? { ...fullPayload.meta } : {};
  const items = fullPayload.items && typeof fullPayload.items === 'object' ? fullPayload.items : {};
  /** @type {Record<string, Record<string, unknown>>} */
  const coreItems = {};
  for (const [k, rec] of Object.entries(items)) {
    coreItems[k] = buildPdpCoreProduct(rec);
  }
  return {
    schema_version: LEAN_SCHEMA_CORE,
    meta,
    items: coreItems,
  };
}

/**
 * @param {{ meta?: Record<string, unknown>; items?: Record<string, Record<string, unknown>> }} fullPayload
 */
export async function writePdpCoreFromPayload(fullPayload) {
  const out = str(config.mlPdpCoreOutput);
  if (!out || out === '-' || out.toLowerCase() === 'none') return;
  const corePath = path.resolve(out);
  const corePayload = buildPdpCoreSnapshot(fullPayload);
  const pretty = config.mlBulkPretty;
  const json = pretty ? JSON.stringify(corePayload, null, 2) : JSON.stringify(corePayload);
  await writeSnapshot({
    latestPath: corePath,
    historySubdir: 'pdp',
    historyBaseName: 'pdp_all_core',
    content: json,
  });
  console.info(`[ml-pdp-core] gravado ${Object.keys(corePayload.items).length} itens → ${corePath}`);
}

/**
 * @param {{ meta?: Record<string, unknown>; items?: Record<string, Record<string, unknown>> }} fullPayload
 */
export function writePdpCoreFromPayloadSync(fullPayload) {
  const out = str(config.mlPdpCoreOutput);
  if (!out || out === '-' || out.toLowerCase() === 'none') return;
  const corePath = path.resolve(out);
  const corePayload = buildPdpCoreSnapshot(fullPayload);
  const pretty = config.mlBulkPretty;
  const json = pretty ? JSON.stringify(corePayload, null, 2) : JSON.stringify(corePayload);
  writeSnapshotSync({
    latestPath: corePath,
    historySubdir: 'pdp',
    historyBaseName: 'pdp_all_core',
    content: json,
  });
  console.info(`[ml-pdp-core] gravado ${Object.keys(corePayload.items).length} itens → ${corePath}`);
}
