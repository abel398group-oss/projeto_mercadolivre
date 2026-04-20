/**
 * Validação estrutural e semântica por campo (PDP Mercado Livre).
 * Sem scores, analytics ou classificação comercial — só integridade do dado.
 */

/** @param {unknown} v */
export function trimStr(v) {
  if (v == null) return '';
  return String(v).trim();
}

const OBJECT_HINT = /^\[object\s+\w+\]/i;

/** @param {string} s */
function nameLooksLikeLogisticsOrWidget(s) {
  const t = s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  if (OBJECT_HINT.test(s) || s.includes('[object Object]')) return true;
  if (/^\s*[\[{]/.test(s)) return true;
  if (t.length < 12) {
    if (/\b(frete|envio|retirada|agencia|prazo|entrega|chegar|gratis|grátis|dias?\s+uteis|corridos)\b/.test(t)) {
      return true;
    }
  }
  const logisticHints = [
    /^\s*frete\b/i,
    /^retirada\b/i,
    /\bagencia\b/i,
    /\bprazo\s+de\s+entrega\b/i,
    /\bcalcular\s+o\s+frete\b/i,
    /\bchegar[aá]?\s+gr[aá]tis\b/i,
    /^\(?\d+\)?\s*\d*\s*dias?\s+(uteis|corridos|úteis)/i,
    /^at[eé]\s+\d+\s*dias?\s+corridos/i,
    /^envio\s+(muito\s+)?r[aá]pido\s*$/i,
    /^\d+\s*\+\s*vendidos?\s*$/i,
    /^compre\s+agora\b/i,
    /^parcelas?\b/i,
    /^sem\s+juros\b/i,
    /^\$\s|^r\$\s*\d+[,.\d]*\s*$/i,
  ];
  if (logisticHints.some((re) => re.test(s))) return true;
  const wordish = (t.match(/\b[a-záàãâéêíóôõúç]{3,}\b/g) || []).length;
  if (t.length > 25 && wordish <= 2 && /\d/.test(t) && /\b(frete|dia|entrega|retirada)\b/.test(t)) return true;
  return false;
}

/**
 * @param {unknown} value
 * @returns {boolean}
 */
export function isValidProductName(value) {
  const s = trimStr(value);
  if (s.length < 3 || s.length > 500) return false;
  if (OBJECT_HINT.test(s) || s.includes('[object Object]')) return false;
  return !nameLooksLikeLogisticsOrWidget(s);
}

/**
 * @param {unknown} value
 */
export function isValidShopName(value) {
  const s = trimStr(value);
  if (!s || s.length > 220) return false;
  if (OBJECT_HINT.test(s) || s.includes('[object Object]')) return false;
  if (s.length >= 4 && nameLooksLikeLogisticsOrWidget(s)) return false;
  return true;
}

/**
 * @param {unknown} value
 */
export function isValidImageUrl(value) {
  let s = trimStr(value);
  if (!s || s.length > 8192) return false;
  if (s === '[object Object]' || OBJECT_HINT.test(s)) return false;
  if (s.startsWith('//')) s = `https:${s}`;
  if (s.startsWith('http://')) s = `https://${s.slice(7)}`;
  if (!/^https?:\/\//i.test(s)) return false;
  try {
    const u = new URL(s);
    if (!u.hostname || u.hostname.length < 3) return false;
  } catch {
    return false;
  }
  return true;
}

/**
 * Extrai URLs de strings ou objetos conhecidos (pictures API, etc.).
 * @param {unknown} value
 * @returns {string[]}
 */
export function normalizeImageList(value) {
  if (!Array.isArray(value)) return [];
  const out = [];
  const seen = new Set();
  for (const raw of value) {
    let u = '';
    if (typeof raw === 'string') u = raw.trim();
    else if (raw && typeof raw === 'object') {
      const o = /** @type {Record<string, unknown>} */ (raw);
      u = trimStr(o.secure_url || o.url || o.src || o.picture || o.image || o.secureUrl);
    }
    if (u.startsWith('//')) u = `https:${u}`;
    if (u.startsWith('http://')) u = `https://${u.slice(7)}`;
    if (!isValidImageUrl(u)) continue;
    if (seen.has(u)) continue;
    seen.add(u);
    out.push(u);
  }
  return out;
}

/**
 * @param {unknown} value
 */
export function isValidSellerId(value) {
  const s = trimStr(value);
  if (!/^\d+$/.test(s)) return false;
  return s.length >= 4 && s.length <= 15;
}

/**
 * @param {unknown} value
 */
export function isValidPrice(value) {
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) && n > 0 && n < 1e10;
}

/**
 * Listagem: permite 0 (preço ausente no card) mas rejeita NaN / não finitos.
 * @param {unknown} value
 */
export function isValidListingPrice(value) {
  if (value === null || value === undefined || value === '') return false;
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) && n >= 0 && n < 1e10;
}

/**
 * @param {unknown} value
 * @param {unknown} priceCurrent — preço atual já conhecido no merge (opcional)
 */
export function isValidOriginalPrice(value, priceCurrent) {
  if (value == null || value === '') return true;
  const n = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(n) || n <= 0 || n >= 1e10) return false;
  const c = typeof priceCurrent === 'number' ? priceCurrent : Number(priceCurrent);
  if (Number.isFinite(c) && c > 0 && n + 1e-6 < c) return false;
  return true;
}

/**
 * @param {unknown} value
 */
export function isValidRating(value) {
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) && n > 0 && n <= 5;
}

/**
 * @param {unknown} value
 * @param {unknown} [ratingHint] avaliação já escolhida no registo (contexto)
 */
export function isValidRatingCount(value, ratingHint) {
  if (value == null || value === '') return true;
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n < 0 || n > 50_000_000) return false;
  const r = typeof ratingHint === 'number' ? ratingHint : Number(ratingHint);
  if (Number.isFinite(r) && r > 0 && n === 0) return false;
  return true;
}

/**
 * @param {unknown} value
 */
export function isValidStockValue(value) {
  const s = trimStr(value);
  if (!s || s.length > 600) return false;
  if (OBJECT_HINT.test(s) || s.includes('[object Object]')) return false;
  if (!/\d/.test(s) && !/único|dispon[ií]vel|estoque|unid|stock|restam|últim|disponivel/i.test(s)) return false;
  return true;
}

/**
 * @param {unknown} value
 */
export function isValidShippingPartial(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const o = /** @type {Record<string, unknown>} */ (value);
  const text = trimStr(o.text);
  const rawP = o.price;
  const priceN = rawP == null || rawP === '' ? NaN : Number(rawP);
  if (o.is_free === true) {
    return text.length > 0 || (Number.isFinite(priceN) && priceN >= 0);
  }
  if (Number.isFinite(priceN) && priceN >= 0) {
    return text.length > 0 || priceN > 0;
  }
  if (text && text !== 'unknown' && text.length > 1) {
    if (/^[\[{]/.test(text)) return false;
    return true;
  }
  return false;
}

/**
 * @param {unknown} value
 * @returns {string}
 */
export function normalizeItemId(value) {
  const raw = trimStr(value).replace(/^(MLB)-?/i, 'MLB');
  if (!/^MLB\d{6,}$/i.test(raw)) return '';
  return raw;
}

/**
 * @param {unknown} value
 */
export function isValidItemId(value) {
  return Boolean(normalizeItemId(value));
}

/**
 * @param {unknown} value
 */
export function isValidCatalogProductId(value) {
  const s = trimStr(value);
  return /^MLBU\d{4,24}$/i.test(s);
}

/**
 * @param {unknown} value
 */
export function isValidCategoryId(value) {
  return /^MLB\d+$/i.test(trimStr(value));
}

/**
 * @param {unknown} value
 */
export function isValidDomainId(value) {
  return /^MLB-[A-Z0-9_-]+$/i.test(trimStr(value));
}

/**
 * @param {unknown} value
 */
export function isLikelyMlProductUrl(value) {
  const s = trimStr(value);
  if (s.length < 24 || s.length > 4000) return false;
  if (!/^https?:\/\//i.test(s)) return false;
  return /mercadolivre|mercadolibre|mercadolivre\.com|meli\.bz/i.test(s);
}

/**
 * URL de catálogo / listagem (permalink mais curto que o PDP completo).
 * @param {unknown} value
 */
export function isLikelyMlListingUrl(value) {
  const s = trimStr(value);
  if (s.length < 10 || s.length > 4000) return false;
  if (!/^https?:\/\//i.test(s)) return false;
  return /mercadolivre|mercadolibre|meli\.bz/i.test(s);
}

/**
 * Valida um candidato antes de promover no merge por campo.
 * @param {string} field
 * @param {unknown} value
 * @param {{ mode?: 'listing'; price_current?: number; rating?: number }} [ctx]
 * @returns {{ ok: true, normalized?: unknown } | { ok: false, reason: string }}
 */
export function validateFieldCandidate(field, value, ctx = {}) {
  const listing = ctx.mode === 'listing';
  switch (field) {
    case 'product_id': {
      const s = trimStr(value);
      if (!s || OBJECT_HINT.test(s) || s.includes('[object Object]')) return { ok: false, reason: 'invalid_product_id' };
      if (/^MLB\d/i.test(s)) {
        const id = normalizeItemId(value);
        if (!id) return { ok: false, reason: 'invalid_product_id' };
        return { ok: true, normalized: id };
      }
      if (listing) return { ok: true, normalized: s };
      return { ok: false, reason: 'invalid_product_id' };
    }
    case 'name': {
      const s = trimStr(value);
      const minLen = listing ? 2 : 3;
      if (s.length < minLen || s.length > 500) {
        return { ok: false, reason: listing ? 'invalid_name_format' : 'name_length_out_of_range' };
      }
      if (OBJECT_HINT.test(s) || s.includes('[object Object]')) return { ok: false, reason: 'invalid_product_name' };
      if (nameLooksLikeLogisticsOrWidget(s)) return { ok: false, reason: 'looks_like_logistics_text' };
      return { ok: true, normalized: s };
    }
    case 'shop_name': {
      if (!isValidShopName(value)) return { ok: false, reason: 'invalid_shop_name' };
      return { ok: true, normalized: trimStr(value) };
    }
    case 'image_main': {
      let s = trimStr(value);
      if (s.startsWith('//')) s = `https:${s}`;
      if (!isValidImageUrl(s)) return { ok: false, reason: 'invalid_image_url' };
      return { ok: true, normalized: s };
    }
    case 'price_current': {
      if (listing) {
        if (!isValidListingPrice(value)) return { ok: false, reason: 'invalid_price' };
      } else if (!isValidPrice(value)) {
        return { ok: false, reason: 'invalid_price' };
      }
      const n = typeof value === 'number' ? value : Number(value);
      return { ok: true, normalized: n };
    }
    case 'price_original': {
      if (!isValidOriginalPrice(value, ctx.price_current)) return { ok: false, reason: 'invalid_original_price' };
      if (value == null || value === '') return { ok: true, normalized: null };
      const n = typeof value === 'number' ? value : Number(value);
      return { ok: true, normalized: n };
    }
    case 'seller_id': {
      if (!isValidSellerId(value)) return { ok: false, reason: 'invalid_seller_id' };
      return { ok: true, normalized: trimStr(value) };
    }
    case 'stock_hint': {
      if (!isValidStockValue(value)) return { ok: false, reason: 'invalid_stock_hint' };
      return { ok: true, normalized: trimStr(value) };
    }
    case 'rating': {
      if (!isValidRating(value)) return { ok: false, reason: 'invalid_rating' };
      const n = typeof value === 'number' ? value : Number(value);
      return { ok: true, normalized: n };
    }
    case 'rating_count': {
      if (!isValidRatingCount(value, ctx.rating)) return { ok: false, reason: 'invalid_rating_count' };
      if (value == null || value === '') return { ok: true, normalized: null };
      const n = Math.floor(Number(value));
      return { ok: true, normalized: n };
    }
    case 'item_id': {
      const id = normalizeItemId(value);
      if (!id) return { ok: false, reason: 'invalid_item_id' };
      return { ok: true, normalized: id };
    }
    case 'catalog_product_id': {
      if (!isValidCatalogProductId(value)) return { ok: false, reason: 'invalid_catalog_product_id' };
      return { ok: true, normalized: trimStr(value).toUpperCase() };
    }
    case 'category_id': {
      if (!isValidCategoryId(value)) return { ok: false, reason: 'invalid_category_id' };
      return { ok: true, normalized: trimStr(value).toUpperCase() };
    }
    case 'domain_id': {
      if (!isValidDomainId(value)) return { ok: false, reason: 'invalid_domain_id' };
      return { ok: true, normalized: trimStr(value).toUpperCase() };
    }
    case 'url':
    case 'url_primary': {
      const s = trimStr(value);
      if (listing) {
        if (!isLikelyMlListingUrl(s)) return { ok: false, reason: 'invalid_product_url' };
      } else if (!isLikelyMlProductUrl(s)) {
        return { ok: false, reason: 'invalid_product_url' };
      }
      return { ok: true, normalized: s };
    }
    case 'variation_id': {
      const s = trimStr(value);
      if (!/^\d{1,20}$/.test(s)) return { ok: false, reason: 'invalid_variation_id' };
      return { ok: true, normalized: s };
    }
    default:
      return { ok: true, normalized: value };
  }
}
