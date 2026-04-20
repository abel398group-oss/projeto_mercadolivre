/**
 * Merge canónico com proveniência por campo, validação antes de promover e registo de conflitos.
 */

import { emptyProduct, mergeProduct } from '../productSchema.js';
import { mergeShippingPreferComplete, normalizeShippingEntry } from '../shippingExtract.js';
import { validateFieldCandidate, isValidShippingPartial, normalizeImageList } from './fieldValidators.js';
import { incomingSourceWins, sourceRank } from './mlSourceRank.js';

const META = new Set([
  '_provenance',
  '_field_sources',
  '_source_conflicts',
  '_field_rejections',
  'validation',
  'issues',
  'price_currency',
  'price_currency_api',
  'available_quantity_api',
]);

/** Objetos complexos permitidos no merge (evita promover blobs arbitrários). */
const MERGE_OBJECT_KEYS = new Set(['rating_distribution', 'pdp_attributes_table']);

/**
 * @param {Record<string, unknown>} out
 * @param {string} field
 * @param {string} incomingSource
 * @param {unknown} newVal
 * @param {unknown} prevVal
 */
function recordConflict(out, field, incomingSource, newVal, prevVal) {
  const conflicts = Array.isArray(out._source_conflicts) ? /** @type {unknown[]} */ (out._source_conflicts) : [];
  conflicts.push({
    field,
    rejected_source: incomingSource,
    rejected_value: newVal,
    kept_value: prevVal,
    kept_source: /** @type {Record<string, string>} */ (out._field_sources || {})[field] || '',
  });
  out._source_conflicts = conflicts;
}

/**
 * @param {unknown} v
 */
function snapshotValue(v) {
  if (v == null) return v;
  if (typeof v === 'string') return v.length > 240 ? `${v.slice(0, 240)}…` : v;
  if (typeof v === 'number' || typeof v === 'boolean') return v;
  try {
    const j = JSON.stringify(v);
    if (j.length > 400) return `${j.slice(0, 400)}…`;
    return /** @type {unknown} */ (JSON.parse(j));
  } catch {
    return String(v).slice(0, 200);
  }
}

/**
 * @param {Record<string, unknown>} out
 * @param {string} field
 * @param {string} source
 * @param {unknown} value
 * @param {string} reason
 */
function pushRejection(out, field, source, value, reason) {
  if (!Array.isArray(out._field_rejections)) out._field_rejections = [];
  /** @type {unknown[]} */
  const list = out._field_rejections;
  list.push({
    field,
    source,
    value: snapshotValue(value),
    reason,
  });
}

/**
 * @param {unknown} v
 */
function isEmptyMergeValue(v) {
  if (v == null) return true;
  if (typeof v === 'string') return !v.trim();
  if (typeof v === 'number') return !Number.isFinite(v);
  return false;
}

/**
 * Merge incoming partial aplicando hierarquia de fontes por campo.
 * @param {import('../productSchema.js').CanonicalProduct} prev
 * @param {Record<string, unknown>} incoming
 * @param {string} sourceId
 * @returns {import('../productSchema.js').CanonicalProduct}
 */
export function mergeWithFieldSources(prev, incoming, sourceId) {
  const base = { ...emptyProduct(), ...prev };
  if (!base._field_sources || typeof base._field_sources !== 'object') base._field_sources = {};
  if (!Array.isArray(base._source_conflicts)) base._source_conflicts = [];
  if (!Array.isArray(base._field_rejections)) base._field_rejections = [];
  else base._field_rejections = [...base._field_rejections];

  const fs = /** @type {Record<string, string>} */ (base._field_sources);
  const src = String(sourceId || '').trim() || 'unknown';

  for (const [key, incVal] of Object.entries(incoming)) {
    if (META.has(key)) continue;
    if (incVal === undefined) continue;

    const ctx = {
      price_current: Number(base.price_current) || 0,
      rating: Number(base.rating) || 0,
    };

    if (key === 'shipping' && incVal && typeof incVal === 'object') {
      if (!isValidShippingPartial(incVal)) {
        pushRejection(base, 'shipping', src, incVal, 'invalid_shipping_partial');
        continue;
      }
      const prevShipSource = fs.shipping || '';
      const prevRank = sourceRank(prevShipSource);
      const incRank = sourceRank(src);
      if (!base.shipping || typeof base.shipping !== 'object') {
        base.shipping = normalizeShippingEntry(incVal);
        fs.shipping = src;
        continue;
      }
      if (incRank > prevRank || prevRank === 0) {
        base.shipping = mergeShippingPreferComplete(base.shipping, incVal);
        fs.shipping = src;
      } else {
        recordConflict(base, 'shipping', src, incVal, base.shipping);
      }
      continue;
    }

    if (key === 'images' && Array.isArray(incVal)) {
      const normalizedIncoming = normalizeImageList(incVal);
      if (!normalizedIncoming.length) {
        pushRejection(base, 'images', src, incVal, 'no_valid_image_urls');
        continue;
      }
      const prevSrc = fs.images || '';
      const prevList = normalizeImageList(base.images);
      if (!prevList.length || incomingSourceWins(prevSrc, src)) {
        base.images = normalizedIncoming;
        fs.images = src;
      } else {
        recordConflict(base, 'images', src, normalizedIncoming, prevList);
      }
      continue;
    }

    if (key === 'categories' && Array.isArray(incVal) && incVal.length) {
      const prevCat = Array.isArray(base.categories) ? [...base.categories] : [];
      const cleaned = [...incVal.map((x) => String(x).trim())].filter(Boolean);
      if (!cleaned.length) {
        pushRejection(base, 'categories', src, incVal, 'empty_category_labels');
        continue;
      }
      if (!prevCat.length || incomingSourceWins(fs.categories || '', src)) {
        base.categories = cleaned;
        fs.categories = src;
      } else {
        const merged = [...new Set([...prevCat, ...cleaned])];
        base.categories = merged;
      }
      continue;
    }

    if (key === 'variants' && Array.isArray(incVal) && incVal.length) {
      if (!base.variants?.length || incomingSourceWins(fs.variants || '', src)) {
        base.variants = /** @type {typeof base.variants} */ (JSON.parse(JSON.stringify(incVal)));
        fs.variants = src;
      }
      continue;
    }

    if (typeof incVal === 'object' && incVal !== null && !Array.isArray(incVal)) {
      if (!MERGE_OBJECT_KEYS.has(key)) {
        pushRejection(base, key, src, incVal, 'object_field_not_whitelisted');
        continue;
      }
      const prevVal = /** @type {Record<string, unknown>} */ (base)[key];
      const prevFieldSrc = fs[key] || '';
      const emptyPrev =
        prevVal == null ||
        (typeof prevVal === 'object' &&
          !Array.isArray(prevVal) &&
          Object.keys(/** @type {Record<string, unknown>} */ (prevVal)).length === 0);

      if (emptyPrev || incomingSourceWins(prevFieldSrc, src)) {
        /** @type {Record<string, unknown>} */ (base)[key] = JSON.parse(JSON.stringify(incVal));
        fs[key] = src;
      } else {
        recordConflict(base, key, src, incVal, prevVal);
      }
      continue;
    }

    const prevVal = /** @type {Record<string, unknown>} */ (base)[key];
    const prevFieldSrc = fs[key] || '';
    const emptyPrev =
      prevVal == null || prevVal === '' || (typeof prevVal === 'number' && !Number.isFinite(prevVal));

    if (isEmptyMergeValue(incVal) && typeof incVal !== 'boolean') continue;

    const validated = validateFieldCandidate(key, incVal, ctx);
    if (!validated.ok) {
      pushRejection(base, key, src, incVal, validated.reason);
      continue;
    }

    const norm = validated.normalized !== undefined ? validated.normalized : incVal;

    if (emptyPrev) {
      /** @type {Record<string, unknown>} */ (base)[key] = norm;
      fs[key] = src;
      continue;
    }

    if (norm === prevVal || String(norm) === String(prevVal)) {
      if (!fs[key]) fs[key] = src;
      continue;
    }

    if (incomingSourceWins(prevFieldSrc, src)) {
      /** @type {Record<string, unknown>} */ (base)[key] = norm;
      fs[key] = src;
    } else {
      recordConflict(base, key, src, norm, prevVal);
    }
  }

  base._provenance = [String(prev._provenance || '').trim(), src].filter(Boolean).join('|');
  return /** @type {import('../productSchema.js').CanonicalProduct} */ (base);
}

/**
 * Legado: merge por string de proveniência do productSchema (listagens).
 * @param {import('../productSchema.js').CanonicalProduct} prev
 * @param {Record<string, unknown>} incoming
 * @param {string} provenance
 */
export function mergeListingLayer(prev, incoming, provenance) {
  return mergeProduct(prev, /** @type {Partial<import('../productSchema.js').CanonicalProduct>} */ (incoming), provenance);
}
