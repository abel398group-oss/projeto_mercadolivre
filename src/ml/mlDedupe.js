/**
 * Chaves de deduplicação com prioridade explícita (coleta massiva).
 */

import crypto from 'node:crypto';

/** @param {unknown} v */
function str(v) {
  return v == null ? '' : String(v).trim();
}

/**
 * @param {Record<string, unknown>} p
 * @returns {string}
 */
export function dedupePrimaryKey(p) {
  const item = str(p.item_id);
  if (item) return `item:${item}`;
  const cat = str(p.catalog_product_id);
  const seller = str(p.seller_id);
  if (cat && seller) return `cat_seller:${cat}:${seller}`;
  const canon = str(p.url_primary || p.url);
  if (canon) {
    try {
      const u = new URL(canon);
      u.search = '';
      u.hash = '';
      return `url:${u.toString()}`;
    } catch {
      return `url:${canon}`;
    }
  }
  const name = str(p.name);
  const shop = str(p.shop_name);
  const price = str(p.price_current);
  const img = str(p.image_main);
  const h = crypto.createHash('sha256').update(`${name}|${shop}|${price}|${img}`).digest('hex').slice(0, 24);
  return `hash:${h}`;
}

/**
 * @param {Record<string, unknown>} p
 * @returns {string[]}
 */
export function dedupeKeyCandidates(p) {
  const keys = new Set();
  keys.add(dedupePrimaryKey(p));
  const item = str(p.item_id);
  if (item) keys.add(`item:${item}`);
  const cat = str(p.catalog_product_id);
  const seller = str(p.seller_id);
  if (cat && seller) keys.add(`cat_seller:${cat}:${seller}`);
  const u = str(p.url_primary || p.url);
  if (u) keys.add(`url:${u}`);
  return [...keys];
}
