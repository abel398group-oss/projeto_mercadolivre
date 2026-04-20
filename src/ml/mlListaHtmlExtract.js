import { extractMlProductIdFromUrl, jsonLdIsProductType } from './mlExtract.js';
import { emptyShipping, shippingLabelLooksFree } from '../shippingExtract.js';
import { isValidImageUrl, normalizeImageList, trimStr } from './fieldValidators.js';
import { challengeHtmlLooksBlocked } from './mlAccountChallenge.js';

/** Página de verificação / tráfego suspeito (sem resultados reais). */
export function isListaSuspiciousTrafficHtml(html) {
  if (!html || typeof html !== 'string') return true;
  return challengeHtmlLooksBlocked(html);
}

/**
 * @param {string} html
 * @returns {boolean}
 */
export function htmlLooksLikeSearchListing(html) {
  if (!html) return false;
  return (
    html.includes('ui-search-layout') ||
    html.includes('poly-card') ||
    html.includes('ui-search-result') ||
    html.includes('andes-money-amount__fraction')
  );
}

/**
 * @param {string} s
 * @returns {number}
 */
function brFractionToInt(s) {
  const t = String(s || '').trim();
  if (!t) return 0;
  return Number(t.replace(/\./g, '').replace(/,/g, '')) || 0;
}

/**
 * @param {string} chunk
 * @returns {{ price_current: number; price_original: number | null; discount: number }}
 */
function parseMoneyBlockFromChunk(chunk) {
  let price_current = 0;
  let price_original = null;
  let discount = 0;

  const fracM = chunk.match(/andes-money-amount__fraction[^>]*>([\d\.\s]+)</);
  if (fracM) {
    const centsM = chunk.match(/andes-money-amount__cents[^>]*>(\d{1,2})</);
    const whole = brFractionToInt(fracM[1]);
    const cents = centsM ? Number(centsM[1]) / 100 : 0;
    price_current = whole + cents;
  }

  const prevBlock = chunk.match(
    /andes-money-amount--previous[^]*?andes-money-amount__fraction[^>]*>([\d\.\s]+)</
  );
  if (prevBlock) {
    const pWhole = brFractionToInt(prevBlock[1]);
    const pCentsM = chunk.match(
      /andes-money-amount--previous[^]*?andes-money-amount__cents[^>]*>(\d{1,2})</
    );
    const pCents = pCentsM ? Number(pCentsM[1]) / 100 : 0;
    const po = pWhole + pCents;
    if (po > 0) price_original = po;
  }

  if (price_original != null && price_original > 0 && price_current > 0 && price_original > price_current) {
    discount = Math.round(100 * (1 - price_current / price_original));
  }

  const pctM = chunk.match(/(\d+)\s*%\s*OFF/i);
  if (pctM && !discount) discount = Number(pctM[1]) || 0;

  return { price_current, price_original, discount };
}

/**
 * @param {string} chunk
 * @returns {string}
 */
function parseTitleFromChunk(chunk) {
  const patterns = [
    /<h[23][^>]*class="[^"]*poly-component__title[^"]*"[^>]*>([^<]+)</i,
    /<[^>]*class="[^"]*ui-search-item__title[^"]*"[^>]*>([^<]+)</i,
    /<a[^>]*class="[^"]*ui-search-link__title[^"]*"[^>]*>\s*([^<]+?)\s*</i,
    /aria-label="([^"]{8,400})"[^>]*>[\s\S]{0,800}?\/p\/MLB\d+/i,
  ];
  for (const re of patterns) {
    const m = chunk.match(re);
    if (m) {
      const t = m[1].replace(/\s+/g, ' ').trim();
      if (t.length >= 3 && !/^R\$\s*\d/.test(t)) return t;
    }
  }
  const alt = chunk.match(
    /https:\/\/http2\.mlstatic\.com\/[^"']+["'][^>]*alt="([^"]{8,400})"/i
  );
  if (alt) return alt[1].replace(/\s+/g, ' ').trim();
  return '';
}

/**
 * @param {string} chunk
 * @returns {string}
 */
function parseMainImageFromChunk(chunk) {
  const m = chunk.match(/(https:\/\/http2\.mlstatic\.com\/[^"'\s>]+?\.(?:webp|jpg|jpeg|png))/i);
  return m ? m[1] : '';
}

/**
 * @param {string} chunk
 * @returns {{ rating: number; rating_count: number | null }}
 */
function parseReviewsFromChunk(chunk) {
  const rM = chunk.match(/ui-search-reviews__rating[^>]*>([\d,\.]+)</i);
  const rating = rM ? Number(String(rM[1]).replace(',', '.')) : 0;
  const cM = chunk.match(/\((\d[\d\.]*)\)/);
  let rating_count = null;
  if (cM) {
    const n = Number(String(cM[1]).replace(/\./g, ''));
    if (Number.isFinite(n)) rating_count = n;
  }
  return { rating: Number.isFinite(rating) ? rating : 0, rating_count };
}

/**
 * @param {string} chunk
 * @returns {number}
 */
function parseSoldFromChunk(chunk) {
  const m = chunk.match(/(\d[\d\.]*)\s*\+?\s*vendidos/i);
  if (!m) return 0;
  return Number(String(m[1]).replace(/\./g, '')) || 0;
}

/**
 * @param {string} chunk
 * @returns {string}
 */
function parseInstallmentsHint(chunk) {
  const m = chunk.match(/(\d+)\s*x\s*R\$\s*[\d\.,]+(?:\s*sem\s*juros)?/i);
  return m ? m[0].replace(/\s+/g, ' ').trim() : '';
}

/**
 * @param {string} chunk
 * @returns {string[]}
 */
function parseListingBadges(chunk) {
  const badges = [];
  if (/\bOFF\b|\d+\s*%/i.test(chunk) && /andes-money-amount--previous|price-tag--old/i.test(chunk)) {
    badges.push('discount');
  }
  if (/mais vendido/i.test(chunk)) badges.push('best_seller');
  if (/loja oficial|Loja Oficial/i.test(chunk)) badges.push('official_store');
  if (/Melhor preço|melhor\s+pre[cç]o/i.test(chunk)) badges.push('best_price');
  if (/Chega hoje|chega\s+hoje/i.test(chunk)) badges.push('fast_delivery');
  return badges;
}

/** @returns {ReturnType<typeof emptyShipping> | null} */
function parseShippingFromChunk(chunk) {
  const lower = chunk.toLowerCase();
  if (
    shippingLabelLooksFree(chunk) ||
    lower.includes('frete grátis') ||
    lower.includes('frete gratis') ||
    /ui-search-item__shipping[^>]*free/i.test(chunk)
  ) {
    return { ...emptyShipping(), price: 0, is_free: true, text: 'Frete grátis' };
  }
  return null;
}

/**
 * @param {unknown} offers
 * @returns {{ price: number; currency: string; original: number | null }}
 */
function offersToPrice(offers) {
  if (!offers) return { price: 0, currency: 'BRL', original: null };
  const o = Array.isArray(offers) ? offers[0] : offers;
  if (!o || typeof o !== 'object') return { price: 0, currency: 'BRL', original: null };
  const raw = /** @type {Record<string, unknown>} */ (o);
  const p = raw.price ?? raw.lowPrice ?? raw.highPrice;
  let price = 0;
  if (typeof p === 'number') price = p;
  else if (typeof p === 'string') price = Number(p.replace(',', '.')) || 0;
  const currency = String(raw.priceCurrency || 'BRL');
  let original = null;
  const po = raw.priceSpecification;
  if (po && typeof po === 'object' && 'price' in /** @type {Record<string, unknown>} */ (po)) {
    const op = /** @type {{ price?: unknown }} */ (po).price;
    if (typeof op === 'number' && op > price) original = op;
  }
  return { price, currency, original };
}

/**
 * @param {unknown} node
 * @param {string} categoryPath
 * @param {Map<string, Record<string, unknown>>} out
 */
function collectProductLikeFromJsonLd(node, categoryPath, out) {
  if (!node || typeof node !== 'object') return;
  const o = /** @type {Record<string, unknown>} */ (node);
  const t = o['@type'];
  const types = Array.isArray(t) ? t : t != null ? [t] : [];
  const isItemList = types.some((x) => String(x).toLowerCase().includes('itemlist'));

  if (isItemList) {
    const els = o.itemListElement;
    if (!Array.isArray(els)) return;
    for (const el of els) {
      if (!el || typeof el !== 'object') continue;
      const e = /** @type {Record<string, unknown>} */ (el);
      const item = e.item || e;
      const pos = typeof e.position === 'number' ? e.position : null;
      if (item && typeof item === 'object') {
        collectProductLikeFromJsonLd(item, categoryPath, out);
        const url = typeof /** @type {Record<string, unknown>} */ (item).url === 'string'
          ? String(/** @type {Record<string, unknown>} */ (item).url)
          : '';
        const rawPid = extractMlProductIdFromUrl(url);
        const pid = rawPid && /^MLB\d+$/i.test(rawPid)
          ? `MLB${rawPid.replace(/^MLB/i, '').replace(/\D/g, '')}`
          : '';
        if (pid && pos != null && out.has(pid)) {
          const cur = out.get(pid);
          if (cur) cur.rank_position = pos;
        }
      } else if (typeof item === 'string') {
        const rawPid = extractMlProductIdFromUrl(item);
        if (rawPid && /^MLB\d+$/i.test(rawPid)) {
          mergeListingRecord(out, {
            product_id: rawPid,
            url: item,
            url_primary: item,
            category_source_id: categoryPath,
            rank_position: pos ?? 0,
            listing_json_ld: true,
          });
        }
      }
    }
    return;
  }

  if (!jsonLdIsProductType(o['@type'])) return;

  const name = String(o.name || '').trim();
  const imageRaw = o.image;
  const images = [];
  if (typeof imageRaw === 'string') images.push(imageRaw);
  else if (Array.isArray(imageRaw)) {
    for (const im of imageRaw) {
      if (typeof im === 'string') images.push(im);
      else if (im && typeof im === 'object' && 'url' in im) {
        images.push(String(/** @type {{ url: unknown }} */ (im).url));
      }
    }
  }
  let url = '';
  if (typeof o.url === 'string') url = o.url;
  else if (o.offers && typeof o.offers === 'object') {
    const of = /** @type {Record<string, unknown>} */ (o.offers);
    if (typeof of.url === 'string') url = of.url;
  }
  const rawPid = extractMlProductIdFromUrl(url);
  if (!rawPid || !/^MLB\d+$/i.test(rawPid)) return;
  const productId = `MLB${rawPid.replace(/^MLB/i, '').replace(/\D/g, '')}`;

  const { price, currency, original } = offersToPrice(o.offers);
  /** @type {Record<string, unknown>} */
  const rec = {
    product_id: productId,
    name,
    price_current: price,
    price_currency: currency,
    price_original: original,
    discount: 0,
    url: url || `https://www.mercadolivre.com.br/p/${productId}`,
    url_primary: url || `https://www.mercadolivre.com.br/p/${productId}`,
    image_main: images[0] || '',
    images,
    category_source_id: categoryPath,
    listing_json_ld: true,
  };
  if (original != null && original > 0 && price > 0 && original > price) {
    rec.discount = Math.round(100 * (1 - price / original));
  }
  mergeListingRecord(out, rec);
}

/**
 * @param {string} html
 * @returns {string[]}
 */
function extractJsonLdRawBlocks(html) {
  const re = /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  const blocks = [];
  let m;
  while ((m = re.exec(html)) !== null) {
    const raw = m[1].trim();
    if (raw) blocks.push(raw);
  }
  return blocks;
}

/**
 * @param {unknown} node
 * @returns {string}
 */
function breadcrumbFromJsonLd(node) {
  if (!node || typeof node !== 'object') return '';
  const o = /** @type {Record<string, unknown>} */ (node);
  const t = o['@type'];
  const types = Array.isArray(t) ? t : t != null ? [t] : [];
  if (!types.some((x) => String(x).toLowerCase().includes('breadcrumb'))) return '';

  const items = o.itemListElement;
  if (!Array.isArray(items)) return '';
  const names = [];
  for (const it of items) {
    if (it && typeof it === 'object' && 'name' in it) {
      const n = String(/** @type {Record<string, unknown>} */ (it).name || '').trim();
      if (n) names.push(n);
    }
  }
  return names.join(' > ');
}

/**
 * @param {string} html
 * @returns {string}
 */
export function extractTaxonomyFromJsonLd(html) {
  let best = '';
  for (const raw of extractJsonLdRawBlocks(html)) {
    try {
      const data = JSON.parse(raw);
      const candidates = Array.isArray(data) ? data : [data];
      for (const c of candidates) {
        const b = breadcrumbFromJsonLd(c);
        if (b.length > best.length) best = b;
        if (c && typeof c === 'object' && '@graph' in c) {
          const g = /** @type {Record<string, unknown>} */ (c)['@graph'];
          if (Array.isArray(g)) {
            for (const x of g) {
              const bb = breadcrumbFromJsonLd(x);
              if (bb.length > best.length) best = bb;
            }
          }
        }
      }
    } catch {
      /* JSON inválido em LD+JSON */
    }
  }
  return best;
}

/**
 * @param {Map<string, Record<string, unknown>>} out
 * @param {Record<string, unknown>} partial
 */
function mergeListingRecord(out, partial) {
  const pid = String(partial.product_id || '');
  if (!pid || !/^MLB\d+$/i.test(pid)) return;
  const id = `MLB${pid.replace(/^MLB/i, '').replace(/\D/g, '')}`;
  const prev = out.get(id) || {};
  const next = { ...prev };
  for (const [k, v] of Object.entries(partial)) {
    if (v === undefined || v === null) continue;
    if (typeof v === 'string' && !v.trim()) continue;
    if (k === 'images' && Array.isArray(v)) {
      const merged = normalizeImageList(v);
      const base = normalizeImageList(Array.isArray(next.images) ? /** @type {string[]} */ (next.images) : []);
      const seen = new Set(base);
      for (const u of merged) {
        if (!seen.has(u)) {
          seen.add(u);
          base.push(u);
        }
      }
      next.images = base;
      continue;
    }
    if (k === 'image_main' && typeof v === 'string') {
      let s = trimStr(v);
      if (s.startsWith('//')) s = `https:${s}`;
      if (!isValidImageUrl(s)) continue;
      const cur = trimStr(String(next.image_main || ''));
      let curH = cur.startsWith('//') ? `https:${cur}` : cur;
      if (!cur || !isValidImageUrl(curH)) {
        next.image_main = s;
        continue;
      }
      if (s.length > cur.length) next.image_main = s;
      continue;
    }
    if ((k === 'name' || k === 'url' || k === 'url_primary') && next[k] && v) {
      const cur = String(next[k]).length;
      const inc = String(v).length;
      if (inc > cur) next[k] = v;
      continue;
    }
    if ((k === 'price_current' || k === 'sales_count') && Number(next[k]) > 0 && Number(v) === 0) continue;
    next[k] = v;
  }
  next.product_id = id;
  out.set(id, next);
}

/**
 * Extrai mapa MLB → campos de listagem a partir do HTML (JSON-LD + heurística por card).
 * @param {string} html
 * @param {string} categoryPath
 * @returns {Map<string, Record<string, unknown>>}
 */
export function extractListingEnrichmentFromHtml(html, categoryPath) {
  const out = new Map();

  for (const raw of extractJsonLdRawBlocks(html)) {
    try {
      const data = JSON.parse(raw);
      const candidates = Array.isArray(data) ? data : [data];
      for (const c of candidates) {
        collectProductLikeFromJsonLd(c, categoryPath, out);
        if (c && typeof c === 'object' && '@graph' in c) {
          const g = /** @type {Record<string, unknown>} */ (c)['@graph'];
          if (Array.isArray(g)) {
            for (const x of g) collectProductLikeFromJsonLd(x, categoryPath, out);
          }
        }
      }
    } catch {
      /* ignorar bloco */
    }
  }

  const taxonomy = extractTaxonomyFromJsonLd(html);
  /** Só DOM visível: URLs dentro de LD+JSON não são cards de listagem. */
  const htmlForCardChunks = html.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, '');
  const re = /\/p\/(MLB\d+)/gi;
  let m;
  while ((m = re.exec(htmlForCardChunks)) !== null) {
    const id = m[1];
    const idx = m.index;
    const chunk = htmlForCardChunks.slice(Math.max(0, idx - 3200), Math.min(htmlForCardChunks.length, idx + 2200));
    const title = parseTitleFromChunk(chunk);
    const money = parseMoneyBlockFromChunk(chunk);
    const img = parseMainImageFromChunk(chunk);
    const rev = parseReviewsFromChunk(chunk);
    const sold = parseSoldFromChunk(chunk);
    const inst = parseInstallmentsHint(chunk);
    const badges = parseListingBadges(chunk);
    const ship = parseShippingFromChunk(chunk);

    /** @type {Record<string, unknown>} */
    const partial = {
      product_id: id,
      category_source_id: categoryPath,
      listing_html_chunk: true,
    };
    if (title) partial.name = title;
    if (money.price_current > 0) partial.price_current = money.price_current;
    if (money.price_original != null && money.price_original > 0) partial.price_original = money.price_original;
    if (money.discount > 0) partial.discount = money.discount;
    if (img) {
      partial.image_main = img;
      partial.images = [img];
    }
    if (rev.rating > 0) partial.rating = rev.rating;
    if (rev.rating_count != null) partial.rating_count = rev.rating_count;
    if (sold > 0) partial.sales_count = sold;
    if (inst) partial.listing_installments_hint = inst;
    if (badges.length) partial.listing_badges = badges;
    if (taxonomy) partial.taxonomy_path = taxonomy;
    if (ship) partial.shipping = ship;

    mergeListingRecord(out, partial);
  }

  return out;
}
