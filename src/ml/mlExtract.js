import { emptyProduct } from '../productSchema.js';
import { mergeWithFieldSources } from './mlCanonicalMerge.js';
import { parseIdentityFromUrl } from './mlIdentity.js';

/**
 * Extrai identificador estável a partir da URL do anúncio (MLB… ou MLBU…).
 * @param {string} urlStr
 * @returns {string}
 */
/**
 * URL de PDP a partir de um item do `catalogo_ml.json` (permalink ou `/p/MLB…`).
 * @param {Record<string, unknown>} item
 * @returns {string}
 */
export function resolveCatalogItemToPdpUrl(item) {
  if (!item || typeof item !== 'object') return '';
  const u = String(item.url_primary || item.url || '').trim();
  if (u && /mercadoliv(ere|ibre)/i.test(u)) {
    try {
      const parsed = new URL(u);
      if (parsed.hostname.includes('mercadoliv') || parsed.hostname.includes('mercadolibre')) return u;
    } catch {
      /* ignorar */
    }
  }
  const id = String(item.product_id || '').trim();
  if (/^MLB\d+$/i.test(id)) {
    const n = id.replace(/^MLB/i, '').replace(/\D/g, '');
    return `https://www.mercadolivre.com.br/p/MLB${n}`;
  }
  return '';
}

export function extractMlProductIdFromUrl(urlStr) {
  try {
    const u = new URL(urlStr);
    const path = `${u.pathname}${u.search || ''}`;
    const mlb = path.match(/\/(MLB)-?(\d{6,})(?:\/|$|[?#])/i);
    if (mlb) return `MLB${mlb[2]}`;
    const mlbu = path.match(/\/(MLBU[0-9]+)(?:\/|$|[?#])/i);
    if (mlbu) return mlbu[1];
  } catch {
    /* ignore */
  }
  return '';
}

/**
 * schema.org Product: @type pode ser "Product", "http://schema.org/Product", etc.
 * @param {unknown} typeField
 * @returns {boolean}
 */
export function jsonLdIsProductType(typeField) {
  if (typeField == null) return false;
  const list = Array.isArray(typeField) ? typeField : [typeField];
  return list.some((raw) => {
    const s = String(raw).toLowerCase().trim();
    if (!s) return false;
    if (s === 'product') return true;
    if (s.endsWith('/product')) return true;
    if (s.endsWith('#product')) return true;
    return false;
  });
}

/**
 * Rejeita URLs de exemplo / placeholder antes de abrir o browser.
 * @param {string} urlStr
 */
export function assertUsableMlPdpUrl(urlStr) {
  const trimmed = String(urlStr || '').trim();
  if (!trimmed) {
    throw new Error('[ml-pdp] URL vazia.');
  }
  let u;
  try {
    u = new URL(trimmed);
  } catch {
    throw new Error('[ml-pdp] URL inválida (não é um endereço http válido).');
  }
  const host = u.hostname.toLowerCase();
  if (!host.includes('mercadolivre') && !host.includes('mercadolibre')) {
    throw new Error(
      '[ml-pdp] O host não parece ser Mercado Livre. Copia o link direto do anúncio no site.'
    );
  }
  const full = trimmed.toLowerCase();
  if (
    full.includes('seu-produto') ||
    full.includes('seu_produto') ||
    full === 'https://...' ||
    full === 'http://...' ||
    /^https?:\/\/\.{2,3}\/?$/i.test(trimmed)
  ) {
    throw new Error(
      '[ml-pdp] Isso é um placeholder de tutorial, não um anúncio real. Exemplo válido: https://www.mercadolivre.com.br/…/up/MLBU1234567890'
    );
  }
}

/**
 * @param {unknown} v
 * @returns {number}
 */
function numPrice(v) {
  if (v == null || v === '') return 0;
  const n = Number(v);
  return Number.isFinite(n) && n >= 0 ? n : 0;
}

/**
 * @param {unknown} offers
 * @returns {Record<string, unknown> | null}
 */
function firstOfferShape(offers) {
  if (offers == null) return null;
  if (Array.isArray(offers)) {
    for (const o of offers) {
      if (o && typeof o === 'object') return /** @type {Record<string, unknown>} */ (o);
    }
    return null;
  }
  if (typeof offers === 'object') return /** @type {Record<string, unknown>} */ (offers);
  return null;
}

/**
 * @param {Record<string, unknown> | null} offer
 */
function pickPriceFromOffer(offer) {
  if (!offer) return 0;
  const t = String(offer['@type'] || '').toLowerCase();
  if (t.includes('aggregateoffer')) {
    const low = offer.lowPrice ?? offer.highPrice;
    return numPrice(low);
  }
  if (offer.price != null) return numPrice(offer.price);
  return 0;
}

/**
 * Preço tachado / de lista (ex.: highPrice em AggregateOffer).
 * @param {Record<string, unknown> | null} offer
 * @returns {number | null}
 */
function pickOriginalPriceFromOffer(offer) {
  if (!offer) return null;
  const t = String(offer['@type'] || '').toLowerCase();
  if (t.includes('aggregateoffer')) {
    const high = offer.highPrice;
    const n = numPrice(high);
    return n > 0 ? n : null;
  }
  const high = offer.highPrice;
  if (high != null) {
    const n = numPrice(high);
    return n > 0 ? n : null;
  }
  return null;
}

/**
 * @param {Record<string, unknown> | null} offer
 */
function pickCurrencyFromOffer(offer) {
  if (!offer) return '';
  const c = offer.priceCurrency;
  return c == null ? '' : String(c).trim();
}

/**
 * @param {Record<string, unknown> | null} offer
 * @param {string} fallbackUrl
 */
function pickUrlFromOffer(offer, fallbackUrl) {
  if (!offer) return fallbackUrl;
  const u = offer.url;
  if (typeof u === 'string' && u.trim()) return u.trim();
  return fallbackUrl;
}

/**
 * @param {unknown} image
 * @returns {string}
 */
export function pickPrimaryImage(image) {
  if (image == null) return '';
  if (typeof image === 'string') return image.trim();
  if (Array.isArray(image)) {
    for (const it of image) {
      const s = pickPrimaryImage(it);
      if (s) return s;
    }
    return '';
  }
  if (typeof image === 'object' && image !== null) {
    const o = /** @type {Record<string, unknown>} */ (image);
    if (typeof o.url === 'string') return o.url.trim();
  }
  return '';
}

/**
 * @param {unknown} node
 * @returns {boolean}
 */
function isProductNode(node) {
  if (!node || typeof node !== 'object') return false;
  const t = /** @type {Record<string, unknown>} */ (node)['@type'];
  return jsonLdIsProductType(t);
}

/**
 * Achata @graph / arrays para lista de nós.
 * @param {unknown} data
 * @returns {unknown[]}
 */
export function flattenJsonLdNodes(data) {
  if (data == null) return [];
  if (Array.isArray(data)) return data.flatMap((x) => flattenJsonLdNodes(x));
  if (typeof data === 'object' && data !== null) {
    const g = /** @type {Record<string, unknown>} */ (data)['@graph'];
    if (g != null) return flattenJsonLdNodes(g);
    return [data];
  }
  return [];
}

/**
 * Escolhe o primeiro nó schema.org Product no payload JSON-LD.
 * @param {unknown} parsedJson
 * @returns {Record<string, unknown> | null}
 */
export function findProductNode(parsedJson) {
  for (const node of flattenJsonLdNodes(parsedJson)) {
    if (isProductNode(node)) return /** @type {Record<string, unknown>} */ (node);
  }
  return null;
}

/**
 * Partial a partir do nó JSON-LD (sem merge) + `price_currency`.
 * @param {{
 *   productNode: Record<string, unknown>;
 *   pageUrl: string;
 *   productIdFromUrl: string;
 * }} args
 * @returns {{ partial: Record<string, unknown>; price_currency: string }}
 */
export function buildJsonLdPartial(args) {
  const { productNode, pageUrl, productIdFromUrl } = args;
  const name = String(productNode.name ?? '').trim();
  const offer = firstOfferShape(productNode.offers);
  const price = pickPriceFromOffer(offer);
  const priceOriginal = pickOriginalPriceFromOffer(offer);
  const price_currency = pickCurrencyFromOffer(offer);
  const url = pickUrlFromOffer(offer, pageUrl);
  const image = pickPrimaryImage(productNode.image);

  const identity = parseIdentityFromUrl(pageUrl);
  let item_id = identity.item_id;
  if (!item_id && /^MLB\d+$/i.test(productIdFromUrl)) item_id = productIdFromUrl.replace(/^(MLB)-?/i, 'MLB');
  let catalog_product_id = identity.catalog_product_id;
  if (!catalog_product_id && /^MLBU/i.test(productIdFromUrl)) catalog_product_id = productIdFromUrl;

  let categoryHint = '';
  const catRaw = productNode.category;
  if (typeof catRaw === 'string' && catRaw.trim()) {
    const s = catRaw.trim();
    try {
      const u = new URL(s, 'https://www.mercadolivre.com.br');
      const last = u.pathname.split('/').filter(Boolean).pop();
      categoryHint = last ? decodeURIComponent(last.replace(/-/g, ' ')) : s;
    } catch {
      categoryHint = s;
    }
  } else if (catRaw && typeof catRaw === 'object') {
    const c = /** @type {Record<string, unknown>} */ (catRaw);
    const n = c.name;
    if (typeof n === 'string' && n.trim()) categoryHint = n.trim();
  }

  /** @type {Record<string, unknown>} */
  const partial = {
    catalog_product_id,
    item_id,
    variation_id: identity.variation_id || '',
    name,
    price_current: price,
    url,
    url_primary: url,
    image_main: image,
    images: image ? [image] : [],
    collected_at: new Date().toISOString(),
  };
  if (categoryHint) {
    partial.product_category_from_breadcrumb = categoryHint;
    partial.taxonomy_path = categoryHint;
  }
  if (priceOriginal != null && priceOriginal > 0 && price > 0 && priceOriginal > price) {
    partial.price_original = priceOriginal;
    partial.discount = Math.round(100 * (1 - price / priceOriginal));
  }

  return { partial, price_currency };
}

/**
 * Nó mínimo schema.org/Product quando não há JSON-LD mas há estado embutido suficiente.
 * @param {Record<string, unknown>} partial
 * @param {string} pageUrl
 * @returns {Record<string, unknown>}
 */
export function buildSyntheticJsonLdProductNode(partial, pageUrl) {
  const name = String(partial.name || '').trim();
  const price = Number(partial.price_current);
  const url = String(partial.url || partial.url_primary || pageUrl || '').trim();
  const cur = String(partial.price_currency || 'BRL').trim();
  const img = String(partial.image_main || (Array.isArray(partial.images) ? partial.images[0] : '') || '').trim();

  /** @type {Record<string, unknown>} */
  const offers = {
    '@type': 'Offer',
    priceCurrency: cur || 'BRL',
    url: url || pageUrl,
  };
  if (Number.isFinite(price) && price > 0) offers.price = price;

  /** @type {Record<string, unknown>} */
  const node = {
    '@type': 'Product',
    name: name || 'Produto (embedded)',
    offers,
  };
  if (img) node.image = img;
  return node;
}

/**
 * Monta registo canónico (productSchema) a partir do nó Product + URL da página.
 * Acrescenta `price_currency` fora do schema base (campo útil do JSON-LD).
 *
 * @param {{
 *   productNode: Record<string, unknown>;
 *   pageUrl: string;
 *   productIdFromUrl: string;
 * }} args
 * @returns {import('../productSchema.js').CanonicalProduct & { price_currency: string }}
 */
export function normalizeMlPdpFromJsonLd(args) {
  const { partial, price_currency } = buildJsonLdPartial(args);
  const canonical = mergeWithFieldSources(emptyProduct(), partial, 'json_ld');
  return { ...canonical, price_currency };
}
