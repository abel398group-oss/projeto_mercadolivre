/**
 * API pública Mercado Libre — items e products (catálogo MLBU).
 * Sem Puppeteer; fetch nativo.
 */

const API = 'https://api.mercadolibre.com';
const HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  Accept: 'application/json',
};

/**
 * @param {string} url
 * @returns {Promise<unknown>}
 */
async function getJson(url) {
  const r = await fetch(url, { headers: HEADERS });
  if (!r.ok) {
    const t = await r.text().catch(() => '');
    throw new Error(`[ml-api-item] ${r.status} ${url.slice(0, 80)} ${t.slice(0, 120)}`);
  }
  return r.json();
}

/**
 * @param {string} itemId ex. MLB1234567890
 * @returns {Promise<Record<string, unknown> | null>}
 */
export async function fetchMlItem(itemId) {
  const id = String(itemId || '').trim();
  if (!id) return null;
  try {
    const data = await getJson(`${API}/items/${encodeURIComponent(id)}`);
    return /** @type {Record<string, unknown>} */ (data);
  } catch {
    return null;
  }
}

/**
 * Produto de catálogo (MLBU…).
 * @param {string} productId ex. MLBU3460353119
 * @returns {Promise<Record<string, unknown> | null>}
 */
export async function fetchMlCatalogProduct(productId) {
  const id = String(productId || '').trim();
  if (!id || !/^MLBU/i.test(id)) return null;
  try {
    const data = await getJson(`${API}/products/${encodeURIComponent(id)}`);
    return /** @type {Record<string, unknown>} */ (data);
  } catch {
    return null;
  }
}

/**
 * Normaliza resposta GET /items/{id} para partial canónico + meta API.
 * @param {Record<string, unknown>} j
 * @param {string} [pageUrl]
 * @returns {Record<string, unknown>}
 */
export function normalizeItemApiToPartial(j, pageUrl = '') {
  if (!j || typeof j !== 'object') return {};

  const id = String(j.id ?? '');
  const title = String(j.title ?? '').trim();
  const price = typeof j.price === 'number' ? j.price : Number(j.price);
  const currency = String(j.currency_id ?? 'BRL');
  const permalink = String(j.permalink ?? '').trim();
  const thumb = j.thumbnail != null ? String(j.thumbnail) : '';
  const pics = Array.isArray(j.pictures)
    ? j.pictures.map((p) => (p && typeof p === 'object' && 'secure_url' in p ? String(/** @type {{secure_url?: string}} */ (p).secure_url || '') : '')).filter(Boolean)
    : thumb
      ? [thumb]
      : [];

  const seller = j.seller && typeof j.seller === 'object' ? /** @type {Record<string, unknown>} */ (j.seller) : null;
  const sellerId = seller?.id != null ? String(seller.id) : '';

  const catId = j.category_id != null ? String(j.category_id) : '';
  const domainId = j.domain_id != null ? String(j.domain_id) : '';

  const sold = j.sold_quantity != null ? Number(j.sold_quantity) : 0;
  const available = j.available_quantity != null ? Number(j.available_quantity) : NaN;

  /** @type {Record<string, unknown>} */
  const shipping = {};
  const sh = j.shipping && typeof j.shipping === 'object' ? /** @type {Record<string, unknown>} */ (j.shipping) : null;
  if (sh) {
    if (sh.free_shipping === true) {
      shipping.is_free = true;
      shipping.price = 0;
      shipping.text = 'Frete grátis (API)';
    }
  }

  /** @type {Record<string, unknown>} */
  const partial = {
    item_id: id && /^MLB/i.test(id) ? id.replace(/^(MLB)-?/i, 'MLB') : id,
    seller_id: sellerId,
    category_id: catId,
    domain_id: domainId,
    name: title,
    price_current: Number.isFinite(price) && price > 0 ? price : 0,
    url: permalink || pageUrl,
    url_primary: permalink || pageUrl,
    image_main: thumb || pics[0] || '',
    images: pics.length ? pics : thumb ? [thumb] : [],
    sales_count: Number.isFinite(sold) ? sold : 0,
    collected_at: new Date().toISOString(),
    price_currency_api: currency,
  };

  if (Number.isFinite(available) && available >= 0) {
    partial.stock_hint = `${available} unidades (API)`;
    partial.available_quantity_api = available;
  }

  if (Object.keys(shipping).length) partial.shipping = shipping;

  const vars = j.variations;
  if (Array.isArray(vars) && vars.length === 1 && vars[0] && typeof vars[0] === 'object') {
    const vid = /** @type {Record<string, unknown>} */ (vars[0]).id;
    if (vid != null) partial.variation_id = String(vid);
  }

  return partial;
}

/**
 * Extrai candidatos a item MLB a partir da resposta GET /products/{MLBU}.
 * @param {Record<string, unknown>} j
 * @returns {{ item_id: string; seller_id: string; permalink: string }[]}
 */
export function extractBuyBoxWinnersFromProductApi(j) {
  if (!j || typeof j !== 'object') return [];
  /** @type {{ item_id: string; seller_id: string; permalink: string }[]} */
  const out = [];

  const bbw = /** @type {Record<string, unknown> | undefined} */ (j.buy_box_winner);
  if (bbw && bbw.item_id) {
    out.push({
      item_id: String(bbw.item_id),
      seller_id: bbw.seller_id != null ? String(bbw.seller_id) : '',
      permalink: bbw.permalink != null ? String(bbw.permalink) : '',
    });
  }

  const results = /** @type {unknown[]} */ (j.buy_box_winner_price_details?.results || j.results || []);
  for (const r of results) {
    if (!r || typeof r !== 'object') continue;
    const o = /** @type {Record<string, unknown>} */ (r);
    const itemId = o.item_id != null ? String(o.item_id) : '';
    const sid = o.seller_id != null ? String(o.seller_id) : '';
    const link = o.permalink != null ? String(o.permalink) : '';
    if (itemId && /^MLB/i.test(itemId)) out.push({ item_id: itemId, seller_id: sid, permalink: link });
  }
  const pick = /** @type {Record<string, unknown> | undefined} */ (j.pick);
  if (pick && pick.item_id) {
    out.push({
      item_id: String(pick.item_id),
      seller_id: pick.seller_id != null ? String(pick.seller_id) : '',
      permalink: pick.permalink != null ? String(pick.permalink) : '',
    });
  }
  return out;
}

/**
 * Partial a partir de GET /products/{MLBU} (campos agregados + primeiro winner).
 * @param {Record<string, unknown>} j
 * @param {string} catalogProductId
 * @param {string} [pageUrl]
 */
export function normalizeProductApiToPartial(j, catalogProductId, pageUrl = '') {
  if (!j || typeof j !== 'object') return {};

  const name = String(j.name ?? '').trim();
  const winners = extractBuyBoxWinnersFromProductApi(j);
  const first = winners[0];

  /** @type {Record<string, unknown>} */
  const partial = {
    catalog_product_id: catalogProductId,
    collected_at: new Date().toISOString(),
  };
  if (j.domain_id != null) partial.domain_id = String(j.domain_id);
  if (j.category_id != null) partial.category_id = String(j.category_id);
  if (name) partial.name = name;
  if (first?.item_id) partial.item_id = first.item_id.replace(/^(MLB)-?/i, 'MLB');
  if (first?.seller_id) partial.seller_id = first.seller_id;
  if (first?.permalink) {
    partial.url = first.permalink;
    partial.url_primary = first.permalink;
  } else if (pageUrl) {
    partial.url = pageUrl;
    partial.url_primary = pageUrl;
  }

  const mainPic = j.pictures;
  if (Array.isArray(mainPic) && mainPic[0] && typeof mainPic[0] === 'object') {
    const url = /** @type {{ url?: string; secure_url?: string }} */ (mainPic[0]).secure_url || /** @type {{ url?: string }} */ (mainPic[0]).url;
    if (url) {
      partial.image_main = String(url);
      partial.images = [String(url)];
    }
  }

  return partial;
}
