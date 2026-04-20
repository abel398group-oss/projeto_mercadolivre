/**
 * Identidade ML: catálogo (MLBU) vs item (MLB) vs variação.
 * Funções puras (sem I/O).
 */

/**
 * @param {string} urlStr
 * @returns {{
 *   catalog_product_id: string;
 *   item_id: string;
 *   variation_id: string;
 * }}
 */
export function parseIdentityFromUrl(urlStr) {
  const out = {
    catalog_product_id: '',
    item_id: '',
    variation_id: '',
  };
  if (!urlStr || typeof urlStr !== 'string') return out;
  try {
    const u = new URL(urlStr);
    const path = u.pathname + (u.search || '');

    const mlbu = path.match(/\/(MLBU[0-9]+)(?:\/|$|[?#])/i);
    if (mlbu) out.catalog_product_id = mlbu[1];

    const mlb = path.match(/\/(MLB)-?(\d{6,})(?:\/|$|[?#])/i);
    if (mlb) out.item_id = `MLB${mlb[2]}`;

    const varM = u.searchParams.get('variation') || u.searchParams.get('variation_id');
    if (varM && String(varM).trim()) out.variation_id = String(varM).trim();
  } catch {
    /* ignore */
  }
  return out;
}

/**
 * @param {string} id
 * @returns {boolean}
 */
export function isCatalogProductId(id) {
  return /^MLBU[0-9]+$/i.test(String(id || '').trim());
}

/**
 * @param {string} id
 * @returns {boolean}
 */
export function isItemId(id) {
  return /^MLB\d{6,}$/i.test(String(id || '').trim());
}
