/**
 * Modelo canónico alinhado ao produtos.json de referência (sem lifecycle worker).
 */

import {
  emptyShipping,
  mergeShippingPreferComplete,
  normalizeShippingEntry,
} from './shippingExtract.js';
import { normalizeImageList, validateFieldCandidate } from './ml/fieldValidators.js';
import {
  applyListingCanonicalUrlsToRecord,
  isCatalogListingMergeProvenance,
} from './ml/mlListingCanonicalUrl.js';
import { sourceRank as mlSourceRank } from './ml/mlSourceRank.js';

function num(v) {
  if (v === null || v === undefined || v === '') return 0;
  const n = Number(String(v).replace(',', '.'));
  return Number.isFinite(n) ? n : 0;
}

function str(v) {
  if (v == null) return '';
  return String(v).trim();
}

/** Proveniências de descoberta / listagem (validação mais permissiva em `validateFieldCandidate`). */
function isListingProvenance(provenance) {
  const p = str(provenance);
  if (p === '') return true;
  return (
    p === 'listing_network' ||
    p === 'listing_html' ||
    p === 'ml_search_api' ||
    p === 'category_ssr' ||
    p.includes('listing')
  );
}

/**
 * @param {import('./productSchema.js').CanonicalProduct} out
 * @param {string} provenance
 */
function validationCtx(out, provenance) {
  return {
    mode: isListingProvenance(provenance) ? /** @type {'listing'} */ ('listing') : undefined,
    price_current: Number(out.price_current) || 0,
    rating: Number(out.rating) || 0,
  };
}

/** @param {unknown} v */
function incomingScalarEmpty(v) {
  if (v === undefined || v === null) return true;
  if (typeof v === 'string' && !str(v)) return true;
  return false;
}

/**
 * @param {import('./productSchema.js').CanonicalProduct} out
 * @param {string} field
 * @param {string} source
 * @param {unknown} value
 * @param {string} reason
 */
function rejectionSnapshot(value) {
  /** @type {unknown} */
  let snap = value;
  if (value != null && typeof value === 'string') snap = value.length > 240 ? `${value.slice(0, 240)}…` : value;
  else if (typeof value === 'number' || typeof value === 'boolean') snap = value;
  else if (value != null && typeof value === 'object') {
    try {
      const j = JSON.stringify(value);
      snap = j.length > 400 ? `${j.slice(0, 400)}…` : JSON.parse(j);
    } catch {
      snap = String(value).slice(0, 200);
    }
  }
  return snap;
}

function rejectionValueEquals(a, b) {
  if (a === b) return true;
  if (typeof a === 'number' && typeof b === 'number' && Number.isFinite(a) && Number.isFinite(b)) {
    return Math.abs(a - b) < 1e-9;
  }
  try {
    return JSON.stringify(a) === JSON.stringify(b);
  } catch {
    return String(a) === String(b);
  }
}

function pushFieldRejection(out, field, source, value, reason) {
  if (!Array.isArray(out._field_rejections)) out._field_rejections = [];
  const snap = rejectionSnapshot(value);
  const list = /** @type {{ field: string; source: string; value: unknown; reason: string }[]} */ (out._field_rejections);
  for (const ex of list) {
    if (ex.field === field && ex.reason === reason && rejectionValueEquals(ex.value, snap)) return;
  }
  list.push({ field, source, value: snap, reason });
}

/** Lista única de fontes na ordem de primeira ocorrência (sem repetir `a|a|a`). */
function mergeProvenanceUnique(prevChain, incomingProv) {
  const parts = [...String(prevChain || '').split('|'), ...String(incomingProv || '').split('|')]
    .map((s) => s.trim())
    .filter(Boolean);
  const seen = new Set();
  const unique = [];
  for (const p of parts) {
    if (seen.has(p)) continue;
    seen.add(p);
    unique.push(p);
  }
  return unique.join('|');
}

/**
 * @param {import('./productSchema.js').CanonicalProduct} out
 * @param {string} fieldKey
 * @param {string} provenance
 */
function recordListingFieldSource(out, fieldKey, provenance) {
  const p = str(provenance);
  if (!p) return;
  if (!isListingProvenance(provenance) && p !== 'catalog_discovery') return;
  if (!out._field_sources || typeof out._field_sources !== 'object') out._field_sources = {};
  /** @type {Record<string, string>} */ (out._field_sources)[fieldKey] = p;
}

/** Listagem HTML/rede: ausência ou ≤0 não substitui vendas já conhecidas; sem dado prévio → null. */
function isSoftListingSalesProvenance(provenance) {
  const p = str(provenance);
  if (p === 'ml_search_api') return false;
  return (
    p === 'listing_html' ||
    p === 'listing_network' ||
    p === 'category_ssr' ||
    p.includes('listing')
  );
}

function ensureHttpsImagesOnProduct(out) {
  if (Array.isArray(out.images)) {
    out.images = out.images.map((u) => {
      const s = str(u);
      return s.startsWith('http://') ? `https://${s.slice(7)}` : s;
    });
  }
  const m = str(out.image_main);
  if (m.startsWith('http://')) out.image_main = `https://${m.slice(7)}`;
}

/** Ordem de confiança: alinhada a `mlSourceRank.js` (API > JSON-LD > DOM > listagem …). */
function provenanceRank(p) {
  const k = str(p);
  const r = mlSourceRank(k);
  if (r > 0) return r;
  return 0;
}

function maxRankFromChain(chain) {
  let m = 0;
  for (const part of String(chain || '').split('|')) {
    const r = provenanceRank(part.trim());
    if (r > m) m = r;
  }
  return m;
}

function incomingWinsConflict(prevChain, incomingProv) {
  const ir = provenanceRank(incomingProv);
  const pr = maxRankFromChain(prevChain);
  /** SSR nunca ganha de listing_network ou PDP já presentes na cadeia (ordem de execução irrelevante). */
  if (str(incomingProv) === 'category_ssr' && pr >= provenanceRank('listing_network')) {
    return false;
  }
  if (ir > pr) return true;
  if (ir < pr) return false;
  return true;
}

function unionDedupedStrings(prevArr, incArr) {
  const out = [];
  const seen = new Set();
  for (const arr of [prevArr, incArr]) {
    if (!Array.isArray(arr)) continue;
    for (const raw of arr) {
      const s = str(raw);
      if (!s || seen.has(s)) continue;
      seen.add(s);
      out.push(s);
    }
  }
  return out;
}

function onlyUncategorized(arr) {
  return Array.isArray(arr) && arr.length === 1 && str(arr[0]) === 'uncategorized';
}

function mergeCategoriesField(prevArr, incArr, prevChain, incomingProv) {
  const p = Array.isArray(prevArr) ? [...prevArr] : [];
  const i = Array.isArray(incArr) ? [...incArr] : [];
  if (!i.length) return p;
  if (!p.length) return i;
  if (onlyUncategorized(i) && !onlyUncategorized(p)) return p;
  if (onlyUncategorized(p) && !onlyUncategorized(i)) return i;
  if (!incomingWinsConflict(prevChain, incomingProv)) return p;
  return unionDedupedStrings(p, i);
}

function mergeNumericNoZeroDowngrade(prev, inc, prevChain, incomingProv) {
  const p = Number(prev);
  const i = Number(inc);
  const pOk = Number.isFinite(p) && p !== 0;
  const iOk = Number.isFinite(i) && i !== 0;
  if (!iOk) return Number.isFinite(p) ? p : 0;
  if (!pOk) return i;
  if (Math.abs(i - p) < 1e-9) return p;
  return incomingWinsConflict(prevChain, incomingProv) ? i : p;
}

function mergeSalesCount(prev, inc, prevChain, incomingProv) {
  const p = prev == null || prev === '' ? NaN : Number(prev);
  const i = inc == null || inc === '' ? NaN : Number(inc);
  const pOk = Number.isFinite(p);
  const iOk = Number.isFinite(i);
  if (!iOk) return pOk ? p : null;
  if (!pOk) return Math.max(0, i);
  if (i === 0 && p > 0) return p;
  if (Math.abs(i - p) < 1e-9) return p;
  return incomingWinsConflict(prevChain, incomingProv) ? i : p;
}

/**
 * Preço atual: listing_network válido substitui sempre; SSR não substitui após network/PDP na cadeia.
 */
function mergePriceCurrent(prev, inc, prevChain, incomingProv) {
  const p = Number(prev);
  const i = Number(inc);
  const pOk = Number.isFinite(p) && p > 0;
  const iOk = Number.isFinite(i) && i > 0;
  if (!iOk) return pOk ? p : 0;
  if (str(incomingProv) === 'listing_network') return i;
  if (!pOk) return i;
  if (Math.abs(i - p) < 1e-9) return p;
  return incomingWinsConflict(prevChain, incomingProv) ? i : p;
}

function mergePriceOriginal(prev, inc, prevChain, incomingProv) {
  const pN = prev == null || prev === '' ? 0 : num(prev);
  const iN = inc == null || inc === '' ? 0 : num(inc);
  const pOk = Number.isFinite(pN) && pN > 0;
  const iOk = Number.isFinite(iN) && iN > 0;
  if (!iOk) return pOk ? pN : null;
  if (!pOk) return iN;
  if (Math.abs(iN - pN) < 1e-9) return pN;
  return incomingWinsConflict(prevChain, incomingProv) ? iN : pN;
}

function mergeDiscount(prev, inc, prevChain, incomingProv) {
  const p = Number(prev);
  const i = Number(inc);
  const pOk = Number.isFinite(p) && p > 0;
  const iOk = Number.isFinite(i) && i > 0;
  if (!iOk) {
    if (i === 0 && pOk) return p;
    return pOk ? p : 0;
  }
  if (!pOk) return Math.max(0, i);
  if (Math.abs(i - p) < 1e-9) return p;
  return incomingWinsConflict(prevChain, incomingProv) ? i : p;
}

function mergeStringScalar(prevVal, incVal, prevChain, incomingProv) {
  const p = str(prevVal);
  const i = str(incVal);
  if (!i) return p;
  if (!p) return i;
  if (i === p) return p;
  return incomingWinsConflict(prevChain, incomingProv) ? i : p;
}

function mergeRatingCount(prev, inc, prevChain, incomingProv) {
  if (inc == null || inc === '') {
    return prev == null || prev === '' ? null : num(prev);
  }
  if (prev == null || prev === '') return num(inc);
  const pN = num(prev);
  const iN = num(inc);
  if (iN === pN) return pN;
  if (iN === 0 && pN > 0) return pN;
  return incomingWinsConflict(prevChain, incomingProv) ? iN : pN;
}

function mergeRankPosition(prev, inc, prevChain, incomingProv) {
  const p = num(prev);
  const i = num(inc);
  if (!i || i <= 0) return p > 0 ? p : 0;
  if (!p || p <= 0) return i;
  if (i === p) return p;
  return incomingWinsConflict(prevChain, incomingProv) ? i : p;
}

/** @returns {import('./productSchema.js').CanonicalProduct} */
export function emptyProduct() {
  return {
    /** @deprecated Legado: usar item_id / catalog_product_id / canonical_id. Preenchido em finalize. */
    product_id: '',
    /**
     * ID do anúncio tal como descoberto na listagem ou na fila (chave inicial).
     * Não substituir pelo item_id do PDP — ver canonical_id.
     */
    listing_product_id: '',
    /**
     * ID oficial recomendado para downstream (item_id → catalog_product_id → listing_product_id → product_id legado).
     * Preenchido em finalizeCollectionRecord.
     */
    canonical_id: '',
    /** Catálogo ML (URL /up/MLBU…). */
    catalog_product_id: '',
    /** Anúncio MLB (API items). */
    item_id: '',
    variation_id: '',
    category_id: '',
    domain_id: '',
    sku_id: '',
    name: '',
    price_current: 0,
    price_original: null,
    price_history: [],
    discount: 0,
    sales_count: null,
    normalized_sales: 0,
    score: 0,
    completeness_score: 0,
    rating: 0,
    rating_count: null,
    rating_distribution: null,
    rank_position: 0,
    categories: [],
    images: [],
    image_main: '',
    variants: [],
    description: '',
    shop_name: '',
    shop_logo: '',
    seller_id: '',
    shop_link: '',
    shop_product_count: null,
    shop_review_count: null,
    shop_sold_count: null,
    url: '',
    url_primary: '',
    url_type: 'static',
    canonical_url_hash: '',
    product_category_from_breadcrumb: '',
    collected_at: '',
    taxonomy_path: '',
    suspect: false,
    incomplete: false,
    missing_fields: [],
    _provenance: '',
    /** Origem declarada por campo (ex.: api_item, json_ld, dom, regex_text). */
    _field_sources: /** @type {Record<string, string>} */ ({}),
    /** Conflitos entre fontes (mantida a de maior rank). */
    _source_conflicts: /** @type {unknown[]} */ ([]),
    /** Candidatos rejeitados no merge (rastreabilidade; não analytics). */
    _field_rejections: /** @type {unknown[]} */ ([]),
    sales_count_precision: /** @type {'exact' | 'approximate' | 'unknown'} */ ('unknown'),
    shipping_precision: /** @type {'exact' | 'conditional' | 'unknown'} */ ('unknown'),
    validation: /** @type {Record<string, boolean> | null} */ (null),
    issues: /** @type {string[]} */ ([]),
    shipping: emptyShipping(),
  };
}

/**
 * @param {Partial<CanonicalProduct>} incoming
 * @param {Partial<CanonicalProduct>} prev
 */
export function mergeProduct(prev, incoming, provenance = '') {
  const incomingKeys = Object.keys(incoming || {});
  const out = { ...emptyProduct(), ...prev };
  out.variants = Array.isArray(prev.variants) ? [...prev.variants] : [];
  out.images = Array.isArray(prev.images) ? [...prev.images] : [];
  out.categories = Array.isArray(prev.categories) ? [...prev.categories] : [];
  out.price_history = Array.isArray(prev.price_history) ? [...prev.price_history] : [];
  out.shipping = normalizeShippingEntry(prev.shipping !== undefined ? prev.shipping : out.shipping);
  out._field_sources =
    prev._field_sources && typeof prev._field_sources === 'object'
      ? { .../** @type {Record<string, string>} */ (prev._field_sources) }
      : {};
  out._source_conflicts = Array.isArray(prev._source_conflicts) ? [...prev._source_conflicts] : [];
  out._field_rejections = Array.isArray(prev._field_rejections) ? [...prev._field_rejections] : [];

  const prevChain = str(prev._provenance);

  for (const k of incomingKeys) {
    if (k === '_provenance') continue;
    if (k === 'price_history') continue;
    if (k === '_field_rejections') continue;
    if (k === 'shipping') {
      if (incoming.shipping && typeof incoming.shipping === 'object') {
        out.shipping = mergeShippingPreferComplete(out.shipping, incoming.shipping);
        recordListingFieldSource(out, 'shipping', provenance);
      }
      continue;
    }
    if (k === 'variants') {
      const incV = incoming.variants;
      if (!Array.isArray(incV) || !incV.length) continue;
      if (!out.variants.length || incomingWinsConflict(prevChain, provenance)) {
        out.variants = [...incV];
        recordListingFieldSource(out, 'variants', provenance);
      }
      continue;
    }
    if (k === 'images') {
      if (!Array.isArray(incoming.images)) {
        pushFieldRejection(out, 'images', provenance, incoming.images, 'invalid_images_array');
        continue;
      }
      if (incoming.images.length === 0) continue;
      const norm = normalizeImageList(incoming.images);
      if (!norm.length) {
        pushFieldRejection(out, 'images', provenance, incoming.images, 'no_valid_image_urls');
        continue;
      }
      out.images = unionDedupedStrings(normalizeImageList(out.images), norm);
      recordListingFieldSource(out, 'images', provenance);
      continue;
    }
    if (k === 'categories') {
      if (Array.isArray(incoming.categories)) {
        out.categories = mergeCategoriesField(out.categories, incoming.categories, prevChain, provenance);
        recordListingFieldSource(out, 'categories', provenance);
      }
      continue;
    }
    if (k === 'canonical_id') {
      continue;
    }
    if (k === 'listing_product_id') {
      if (incomingScalarEmpty(incoming.listing_product_id)) continue;
      const v = validateFieldCandidate('listing_product_id', incoming.listing_product_id, validationCtx(out, provenance));
      if (!v.ok) {
        pushFieldRejection(out, 'listing_product_id', provenance, incoming.listing_product_id, v.reason);
        continue;
      }
      if (!str(out.listing_product_id)) {
        out.listing_product_id = /** @type {string} */ (v.normalized);
        recordListingFieldSource(out, 'listing_product_id', provenance);
      }
      continue;
    }
    if (k === 'product_id') {
      if (incomingScalarEmpty(incoming.product_id)) continue;
      const v = validateFieldCandidate('product_id', incoming.product_id, validationCtx(out, provenance));
      if (!v.ok) {
        pushFieldRejection(out, 'product_id', provenance, incoming.product_id, v.reason);
        continue;
      }
      out.product_id = mergeStringScalar(out.product_id, v.normalized, prevChain, provenance);
      recordListingFieldSource(out, 'product_id', provenance);
      continue;
    }
    if (k === 'catalog_product_id') {
      if (incomingScalarEmpty(incoming.catalog_product_id)) continue;
      const v = validateFieldCandidate('catalog_product_id', incoming.catalog_product_id, validationCtx(out, provenance));
      if (!v.ok) {
        pushFieldRejection(out, 'catalog_product_id', provenance, incoming.catalog_product_id, v.reason);
        continue;
      }
      out.catalog_product_id = mergeStringScalar(
        out.catalog_product_id,
        v.normalized,
        prevChain,
        provenance
      );
      recordListingFieldSource(out, 'catalog_product_id', provenance);
      continue;
    }
    if (k === 'item_id') {
      if (incomingScalarEmpty(incoming.item_id)) continue;
      const v = validateFieldCandidate('item_id', incoming.item_id, validationCtx(out, provenance));
      if (!v.ok) {
        pushFieldRejection(out, 'item_id', provenance, incoming.item_id, v.reason);
        continue;
      }
      out.item_id = mergeStringScalar(out.item_id, v.normalized, prevChain, provenance);
      recordListingFieldSource(out, 'item_id', provenance);
      continue;
    }
    if (k === 'variation_id') {
      out.variation_id = mergeStringScalar(out.variation_id, incoming.variation_id, prevChain, provenance);
      recordListingFieldSource(out, 'variation_id', provenance);
      continue;
    }
    if (k === 'category_id') {
      out.category_id = mergeStringScalar(out.category_id, incoming.category_id, prevChain, provenance);
      recordListingFieldSource(out, 'category_id', provenance);
      continue;
    }
    if (k === 'domain_id') {
      out.domain_id = mergeStringScalar(out.domain_id, incoming.domain_id, prevChain, provenance);
      recordListingFieldSource(out, 'domain_id', provenance);
      continue;
    }
    if (k === '_field_sources' && incoming._field_sources && typeof incoming._field_sources === 'object') {
      out._field_sources = { ...out._field_sources, .../** @type {Record<string, string>} */ (incoming._field_sources) };
      continue;
    }
    if (k === '_source_conflicts' && Array.isArray(incoming._source_conflicts)) {
      out._source_conflicts = [...out._source_conflicts, ...incoming._source_conflicts];
      continue;
    }
    if (k === 'sku_id') {
      out.sku_id = mergeStringScalar(out.sku_id, incoming.sku_id, prevChain, provenance);
      recordListingFieldSource(out, 'sku_id', provenance);
      continue;
    }
    if (k === 'name') {
      if (incomingScalarEmpty(incoming.name)) continue;
      const v = validateFieldCandidate('name', incoming.name, validationCtx(out, provenance));
      if (!v.ok) {
        pushFieldRejection(out, 'name', provenance, incoming.name, v.reason);
        continue;
      }
      out.name = mergeStringScalar(out.name, v.normalized, prevChain, provenance);
      recordListingFieldSource(out, 'name', provenance);
      continue;
    }
    if (k === 'price_current') {
      if (incoming.price_current === undefined) continue;
      if (incoming.price_current === null || incoming.price_current === '') continue;
      const v = validateFieldCandidate('price_current', incoming.price_current, validationCtx(out, provenance));
      if (!v.ok) {
        pushFieldRejection(out, 'price_current', provenance, incoming.price_current, v.reason);
        continue;
      }
      out.price_current = mergePriceCurrent(
        out.price_current,
        /** @type {number} */ (v.normalized),
        prevChain,
        provenance
      );
      recordListingFieldSource(out, 'price_current', provenance);
      continue;
    }
    if (k === 'price_original') {
      if (incoming.price_original === undefined) continue;
      if (incoming.price_original === null || incoming.price_original === '') continue;
      const v = validateFieldCandidate('price_original', incoming.price_original, validationCtx(out, provenance));
      if (!v.ok) {
        pushFieldRejection(out, 'price_original', provenance, incoming.price_original, v.reason);
        if (Number(out.discount) > 0) out.discount = 0;
        continue;
      }
      out.price_original = mergePriceOriginal(
        out.price_original,
        v.normalized,
        prevChain,
        provenance
      );
      recordListingFieldSource(out, 'price_original', provenance);
      continue;
    }
    if (k === 'discount') {
      out.discount = mergeDiscount(out.discount, incoming.discount, prevChain, provenance);
      recordListingFieldSource(out, 'discount', provenance);
      continue;
    }
    if (k === 'sales_count') {
      if (incoming.sales_count === undefined) continue;
      if (isSoftListingSalesProvenance(provenance)) {
        const raw = incoming.sales_count;
        const n = raw == null || raw === '' ? NaN : Number(raw);
        if (!Number.isFinite(n) || n <= 0) {
          const prevN = out.sales_count == null || out.sales_count === '' ? NaN : Number(out.sales_count);
          if (Number.isFinite(prevN)) {
            recordListingFieldSource(out, 'sales_count', provenance);
            continue;
          }
          out.sales_count = null;
          out.sales_count_precision = 'unknown';
          recordListingFieldSource(out, 'sales_count', provenance);
          continue;
        }
        out.sales_count = mergeSalesCount(out.sales_count, n, prevChain, provenance);
        recordListingFieldSource(out, 'sales_count', provenance);
        continue;
      }
      out.sales_count = mergeSalesCount(out.sales_count, incoming.sales_count, prevChain, provenance);
      recordListingFieldSource(out, 'sales_count', provenance);
      continue;
    }
    if (k === 'rating') {
      out.rating = mergeNumericNoZeroDowngrade(out.rating, incoming.rating, prevChain, provenance);
      recordListingFieldSource(out, 'rating', provenance);
      continue;
    }
    if (k === 'rating_count') {
      out.rating_count = mergeRatingCount(out.rating_count, incoming.rating_count, prevChain, provenance);
      recordListingFieldSource(out, 'rating_count', provenance);
      continue;
    }
    if (k === 'rating_distribution') {
      const inc = incoming.rating_distribution;
      if (inc == null || typeof inc !== 'object') continue;
      const empty =
        Array.isArray(inc) ? inc.length === 0 : Object.keys(/** @type {Record<string, unknown>} */ (inc)).length === 0;
      if (empty) continue;
      const prevD = out.rating_distribution;
      const hasPrev =
        prevD != null &&
        typeof prevD === 'object' &&
        (Array.isArray(prevD) ? prevD.length > 0 : Object.keys(/** @type {Record<string, unknown>} */ (prevD)).length > 0);
      if (!hasPrev || incomingWinsConflict(prevChain, provenance)) {
        out.rating_distribution = /** @type {typeof out.rating_distribution} */ (
          JSON.parse(JSON.stringify(inc))
        );
        recordListingFieldSource(out, 'rating_distribution', provenance);
      }
      continue;
    }
    if (k === 'taxonomy_path') {
      out.taxonomy_path = mergeStringScalar(out.taxonomy_path, incoming.taxonomy_path, prevChain, provenance);
      recordListingFieldSource(out, 'taxonomy_path', provenance);
      continue;
    }
    if (k === 'url') {
      if (incomingScalarEmpty(incoming.url)) continue;
      const v = validateFieldCandidate('url', incoming.url, validationCtx(out, provenance));
      if (!v.ok) {
        pushFieldRejection(out, 'url', provenance, incoming.url, v.reason);
        continue;
      }
      out.url = mergeStringScalar(out.url, v.normalized, prevChain, provenance);
      recordListingFieldSource(out, 'url', provenance);
      continue;
    }
    if (k === 'url_primary') {
      if (incomingScalarEmpty(incoming.url_primary)) continue;
      const v = validateFieldCandidate('url_primary', incoming.url_primary, validationCtx(out, provenance));
      if (!v.ok) {
        pushFieldRejection(out, 'url_primary', provenance, incoming.url_primary, v.reason);
        continue;
      }
      out.url_primary = mergeStringScalar(out.url_primary, v.normalized, prevChain, provenance);
      recordListingFieldSource(out, 'url_primary', provenance);
      continue;
    }
    if (k === 'image_main') {
      if (incomingScalarEmpty(incoming.image_main)) continue;
      const v = validateFieldCandidate('image_main', incoming.image_main, validationCtx(out, provenance));
      if (!v.ok) {
        pushFieldRejection(out, 'image_main', provenance, incoming.image_main, v.reason);
        continue;
      }
      out.image_main = mergeStringScalar(out.image_main, v.normalized, prevChain, provenance);
      recordListingFieldSource(out, 'image_main', provenance);
      continue;
    }
    if (k === 'description') {
      out.description = mergeStringScalar(out.description, incoming.description, prevChain, provenance);
      recordListingFieldSource(out, 'description', provenance);
      continue;
    }
    if (k === 'shop_name') {
      if (incomingScalarEmpty(incoming.shop_name)) continue;
      const v = validateFieldCandidate('shop_name', incoming.shop_name, validationCtx(out, provenance));
      if (!v.ok) {
        pushFieldRejection(out, 'shop_name', provenance, incoming.shop_name, v.reason);
        continue;
      }
      out.shop_name = mergeStringScalar(out.shop_name, v.normalized, prevChain, provenance);
      recordListingFieldSource(out, 'shop_name', provenance);
      continue;
    }
    if (k === 'shop_logo') {
      out.shop_logo = mergeStringScalar(out.shop_logo, incoming.shop_logo, prevChain, provenance);
      recordListingFieldSource(out, 'shop_logo', provenance);
      continue;
    }
    if (k === 'seller_id') {
      if (incomingScalarEmpty(incoming.seller_id)) continue;
      const v = validateFieldCandidate('seller_id', incoming.seller_id, validationCtx(out, provenance));
      if (!v.ok) {
        pushFieldRejection(out, 'seller_id', provenance, incoming.seller_id, v.reason);
        continue;
      }
      out.seller_id = mergeStringScalar(out.seller_id, v.normalized, prevChain, provenance);
      recordListingFieldSource(out, 'seller_id', provenance);
      continue;
    }
    if (k === 'shop_link') {
      out.shop_link = mergeStringScalar(out.shop_link, incoming.shop_link, prevChain, provenance);
      recordListingFieldSource(out, 'shop_link', provenance);
      continue;
    }
    if (k === 'shop_product_count' || k === 'shop_review_count' || k === 'shop_sold_count') {
      out[k] = mergeRatingCount(out[k], incoming[k], prevChain, provenance);
      recordListingFieldSource(out, k, provenance);
      continue;
    }
    if (k === 'rank_position') {
      out.rank_position = mergeRankPosition(out.rank_position, incoming.rank_position, prevChain, provenance);
      recordListingFieldSource(out, 'rank_position', provenance);
      continue;
    }
    if (k === 'collected_at') {
      const p = str(out.collected_at);
      const i = str(incoming.collected_at);
      if (!i) continue;
      if (!p) {
        out.collected_at = i;
        recordListingFieldSource(out, 'collected_at', provenance);
        continue;
      }
      out.collected_at = incomingWinsConflict(prevChain, provenance) ? i : p;
      recordListingFieldSource(out, 'collected_at', provenance);
      continue;
    }
    if (k === 'sales_count_precision' || k === 'shipping_precision') {
      const i = incoming[k];
      if (typeof i === 'string' && i.trim()) {
        if (!out[k] || incomingWinsConflict(prevChain, provenance)) {
          out[k] = i;
          recordListingFieldSource(out, k, provenance);
        }
      }
      continue;
    }
    if (k === 'validation' && incoming.validation && typeof incoming.validation === 'object') {
      out.validation = /** @type {typeof out.validation} */ ({
        ...(out.validation && typeof out.validation === 'object' ? out.validation : {}),
        .../** @type {Record<string, boolean>} */ (incoming.validation),
      });
      continue;
    }
    if (k === 'issues' && Array.isArray(incoming.issues)) {
      out.issues = [...(Array.isArray(out.issues) ? out.issues : []), ...incoming.issues];
      continue;
    }

    const v = incoming[k];
    if (v === undefined) continue;
    if (v === null) continue;
    if (typeof v === 'string' && !v.trim()) continue;
    if (typeof v === 'number' && !Number.isFinite(v)) continue;
    if (typeof v === 'number' && v === 0 && typeof out[k] === 'number' && out[k] !== 0) continue;
    out[k] = v;
  }

  const po = out.price_original == null || out.price_original === '' ? 0 : num(out.price_original);
  if (!Number.isFinite(po) || po <= 0) {
    if (Number(out.discount) > 0) out.discount = 0;
  }
  ensureHttpsImagesOnProduct(out);

  if (provenance) {
    out._provenance = mergeProvenanceUnique(prevChain, provenance);
  }

  if (isCatalogListingMergeProvenance(provenance)) {
    applyListingCanonicalUrlsToRecord(/** @type {Record<string, unknown>} */ (out));
  }

  return out;
}

/**
 * @param {Record<string, unknown>} row
 */
export function fromLegacyRow(row) {
  const price = num(String(row.preco_atual || '').replace(/[^\d.,]/g, '').replace(',', '.'));
  const priceO = num(String(row.preco_original || '').replace(/[^\d.,]/g, '').replace(',', '.'));
  const sku = str(row.sku);
  const link = str(row.link_do_produto);
  const img = str(row.link_imagem);
  const imgs = [];
  if (img) imgs.push(img);
  if (Array.isArray(row.images)) {
    for (const u of row.images) {
      const s = str(u);
      if (s && !imgs.includes(s)) imgs.push(s);
    }
  }

  let discount = num(row.discount);
  if (price > 0 && priceO > price) {
    discount = Math.round(100 * (1 - price / priceO));
  }

  /** @type {Record<string, unknown>} */
  const payload = {
    product_id: sku,
    sku_id: str(row.sku_id) || sku,
    name: str(row.nome),
    price_current: price,
    price_original: priceO > 0 ? priceO : null,
    discount,
    rating: num(row.nota_avaliacao),
    sales_count: num(String(row.total_vendas || '').replace(/[^\d.]/g, '')),
    taxonomy_path: str(row.taxonomia),
    url: link,
    images: imgs,
    image_main: img || imgs[0] || '',
    collected_at: str(row.data_coleta) || new Date().toISOString(),
  };
  if (row.rating_count != null && row.rating_count !== '') {
    payload.rating_count = num(row.rating_count);
  }
  if (row.rating_distribution != null && typeof row.rating_distribution === 'object') {
    try {
      const cloned = JSON.parse(JSON.stringify(row.rating_distribution));
      const empty =
        Array.isArray(cloned) ? cloned.length === 0 : Object.keys(cloned).length === 0;
      if (!empty) payload.rating_distribution = cloned;
    } catch {
      /* ignorar */
    }
  }
  if (row.rank_position != null) payload.rank_position = num(row.rank_position);
  if (row.shipping && typeof row.shipping === 'object') {
    payload.shipping = normalizeShippingEntry(row.shipping);
  }

  const shn = str(row.shop_name);
  if (shn) payload.shop_name = shn;
  const shl = str(row.shop_logo);
  if (shl) payload.shop_logo = shl;
  const sid = str(row.seller_id);
  if (sid) payload.seller_id = sid;
  const slk = str(row.shop_link);
  if (slk) payload.shop_link = slk;
  for (const key of ['shop_product_count', 'shop_review_count', 'shop_sold_count']) {
    const raw = row[key];
    if (raw != null && raw !== '') {
      const n = num(raw);
      if (Number.isFinite(n)) payload[key] = n;
    }
  }

  if (Array.isArray(row.variants) && row.variants.length) {
    /** @type {{ name: string; value: string }[]} */
    const cleaned = [];
    for (const it of row.variants) {
      if (!it || typeof it !== 'object') continue;
      const o = /** @type {Record<string, unknown>} */ (it);
      const n = str(o.name);
      const v = str(o.value);
      if (n && v) cleaned.push({ name: n, value: v });
    }
    if (cleaned.length) payload.variants = cleaned;
  }

  return mergeProduct(emptyProduct(), payload, '');
}

/**
 * @param {CanonicalProduct} p
 */
export function toLegacyRow(p) {
  return {
    sku: p.product_id,
    nome: p.name,
    preco_atual: p.price_current ? String(p.price_current) : '',
    preco_original:
      p.price_original != null && num(p.price_original) > 0 ? String(p.price_original) : '',
    nota_avaliacao: p.rating ? String(p.rating) : '',
    total_vendas: p.sales_count != null && p.sales_count !== '' ? String(p.sales_count) : '',
    taxonomia: p.taxonomy_path || (Array.isArray(p.categories) ? p.categories.join(' > ') : ''),
    link_do_produto: p.url,
    link_imagem: str(p.image_main) || (Array.isArray(p.images) ? p.images[0] || '' : ''),
    data_coleta: p.collected_at,
  };
}

/** @typedef {ReturnType<typeof emptyProduct>} CanonicalProduct */
