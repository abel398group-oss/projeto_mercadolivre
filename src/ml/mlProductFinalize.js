/**
 * Consolidação final do registo de coleta (identidade legada, precisões, validação).
 */

import { emptyProduct } from '../productSchema.js';
import { isValidImageUrl, normalizeImageList, trimStr } from './fieldValidators.js';
import { validateCollectionProduct } from './mlProductValidate.js';

function str(v) {
  return v == null ? '' : String(v).trim();
}

/**
 * URLs de cliques / tracking que não devem ser persistidas como canónicas.
 * @param {string} urlStr
 */
function isTrackingMercadoLivreUrl(urlStr) {
  const s = str(urlStr).toLowerCase();
  if (!s) return false;
  if (s.includes('click1.mercadolivre.com.br')) return true;
  if (s.includes('mlclics')) return true;
  if (s.includes('/mclics/')) return true;
  return false;
}

/**
 * Remove query/hash; rejeita hosts de tracking e caminhos mclics.
 * @param {string} candidate
 * @returns {string}
 */
function stripToCleanMercadoLivreProductUrl(candidate) {
  const raw = str(candidate);
  if (!raw || isTrackingMercadoLivreUrl(raw)) return '';
  try {
    const u = new URL(raw.startsWith('//') ? `https:${raw}` : raw);
    const host = u.hostname.toLowerCase();
    if (!host.endsWith('mercadolivre.com.br')) return '';
    if (/^click\d/i.test(host)) return '';
    if (u.pathname.toLowerCase().includes('mclics')) return '';
    u.search = '';
    u.hash = '';
    return u.toString();
  } catch {
    return '';
  }
}

/**
 * URL canónica do PDP (sem tracking). Prioridade: item_id → catalog_product_id → URL já limpa.
 * @param {import('../productSchema.js').CanonicalProduct | Record<string, unknown>} product
 * @returns {string | null}
 */
export function buildCanonicalUrl(product) {
  const itemId = str(product?.item_id);
  if (itemId && itemId.toUpperCase().startsWith('MLB')) {
    return `https://www.mercadolivre.com.br/p/${itemId.toUpperCase()}`;
  }
  const catId = str(product?.catalog_product_id);
  if (catId && catId.toUpperCase().startsWith('MLBU')) {
    return `https://www.mercadolivre.com.br/p/${catId.toUpperCase()}`;
  }
  const fallback = str(product?.url) || str(product?.url_primary);
  if (!fallback) return null;
  const cleaned = stripToCleanMercadoLivreProductUrl(fallback);
  return cleaned || null;
}

/**
 * `product_id` legado = item_id || catalog_product_id (único campo agregado para CSVs antigos).
 * @param {import('../productSchema.js').CanonicalProduct} p
 * @returns {import('../productSchema.js').CanonicalProduct}
 */
export function finalizeCollectionRecord(p) {
  const out = { ...emptyProduct(), ...p };
  const item = str(out.item_id);
  const cat = str(out.catalog_product_id);
  out.product_id = item || cat || str(out.product_id);
  return /** @type {import('../productSchema.js').CanonicalProduct} */ (out);
}

/**
 * Preenche precisões quando ainda `unknown` e há sinais nas fontes.
 * @param {import('../productSchema.js').CanonicalProduct} p
 */
/**
 * Garante lista de imagens só com URLs válidas e `image_main` coerente com a galeria.
 * @param {import('../productSchema.js').CanonicalProduct} p
 */
export function coalesceCanonicalImages(p) {
  const out = /** @type {import('../productSchema.js').CanonicalProduct} */ (p);
  const imgs = normalizeImageList(out.images || []);
  let main = trimStr(out.image_main);
  if (main.startsWith('//')) main = `https:${main}`;
  const mainOk = isValidImageUrl(main);
  if (mainOk && !imgs.includes(main)) {
    out.images = [main, ...imgs];
  } else {
    out.images = imgs;
    if (!mainOk) out.image_main = imgs[0] || '';
    else out.image_main = main;
  }
  return out;
}

export function applyPrecisionDefaults(p) {
  const fs = p._field_sources && typeof p._field_sources === 'object' ? p._field_sources : {};
  const src = /** @type {Record<string, string>} */ (fs);

  if (!p.sales_count_precision || p.sales_count_precision === 'unknown') {
    const s = src.sales_count;
    if (s === 'api_item' || s === 'api_product' || s === 'embedded_json') p.sales_count_precision = 'exact';
    else if (s === 'regex_text' || s === 'heuristic') p.sales_count_precision = 'approximate';
    else if (Number(p.sales_count) > 0) p.sales_count_precision = 'approximate';
  }

  if (!p.shipping_precision || p.shipping_precision === 'unknown') {
    const sh = p.shipping;
    const text = sh && typeof sh === 'object' ? str(/** @type {Record<string, unknown>} */ (sh).text) : '';
    if (!sh || text === 'unknown') p.shipping_precision = 'unknown';
    else if (/primeira\s+compra|1[ªa]\s+compra/i.test(text)) p.shipping_precision = 'conditional';
    else if (/** @type {Record<string, unknown>} */ (sh).is_free === true) p.shipping_precision = 'exact';
    else if (Number(/** @type {Record<string, unknown>} */ (sh).price) > 0) p.shipping_precision = 'exact';
  }

  return p;
}

/**
 * @param {import('../productSchema.js').CanonicalProduct} p
 * @param {string} [priceCurrency]
 */
export function finalizeAndValidate(p, priceCurrency = '') {
  let out = finalizeCollectionRecord(p);
  const canonicalUrl = buildCanonicalUrl(out);
  if (canonicalUrl) {
    out.url = canonicalUrl;
    out.url_primary = canonicalUrl;
    if (!out._field_sources || typeof out._field_sources !== 'object') out._field_sources = {};
    /** @type {Record<string, string>} */ (out._field_sources).url = 'derived';
    /** @type {Record<string, string>} */ (out._field_sources).url_primary = 'derived';
  } else {
    out.url = '';
    out.url_primary = '';
  }
  out = coalesceCanonicalImages(out);
  out = applyPrecisionDefaults(out);
  const { validation, issues } = validateCollectionProduct(out);
  out.validation = validation;
  out.issues = issues;
  if (priceCurrency && typeof out === 'object') {
    /** @type {Record<string, unknown>} */ (out).price_currency = priceCurrency;
  }
  return /** @type {import('../productSchema.js').CanonicalProduct & { price_currency?: string }} */ (out);
}
