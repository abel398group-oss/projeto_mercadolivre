/** Frete em listagem / PDP (mínimo compatível com produtos.json de referência). */

export function emptyShipping() {
  return {
    price: 0,
    is_free: false,
    text: 'unknown',
    original_price: null,
    delivery_name: '',
    shipping_type: '',
  };
}

/**
 * Listagem BR: "Frete grátis", "grátis", "free shipping", etc.
 * @param {unknown} rawLabel
 */
export function shippingLabelLooksFree(rawLabel) {
  const s = String(rawLabel ?? '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');
  return (
    s.includes('free') ||
    s.includes('gratis') ||
    s.includes('sem frete') ||
    s.includes('envio gratis')
  );
}

/**
 * @param {unknown} labels
 */
export function shippingLabelsIndicateFree(labels) {
  if (!Array.isArray(labels)) return false;
  return labels.some((x) => shippingLabelLooksFree(x));
}

/**
 * @param {unknown} s
 */
export function shippingHasData(s) {
  return !!(
    s &&
    typeof s === 'object' &&
    (s.text ||
      s.is_free === true ||
      Number(s.price) > 0 ||
      (s.delivery_name && String(s.delivery_name).trim()) ||
      (s.shipping_type && String(s.shipping_type).trim()) ||
      (s.original_price != null && Number(s.original_price) > 0))
  );
}

/**
 * Dados de frete já resolvidos (não só placeholder de listagem vazia / texto unknown).
 * @param {{ text?: string; is_free?: boolean; price?: number | null; delivery_name?: string; shipping_type?: string; original_price?: number | null }} a
 */
function shippingHasMeaningfulData(a) {
  return !!(
    (a.text && String(a.text) !== 'unknown') ||
    a.is_free === true ||
    (typeof a.price === 'number' && a.price > 0) ||
    (a.delivery_name && String(a.delivery_name).trim()) ||
    (a.shipping_type && String(a.shipping_type).trim()) ||
    (a.original_price != null && Number(a.original_price) > 0)
  );
}

/**
 * @param {Record<string, unknown>} productNode
 */
export function extractFreeShippingFromListingLabels(productNode) {
  if (!productNode || typeof productNode !== 'object') return null;
  const labels = productNode.product_marketing_info?.shipping_labels;
  if (!shippingLabelsIndicateFree(labels)) return null;
  return { price: 0, is_free: true, text: 'Frete grátis' };
}

/**
 * @param {unknown} s
 */
export function normalizeShippingEntry(s) {
  if (!s || typeof s !== 'object') return emptyShipping();
  const o = /** @type {Record<string, unknown>} */ (s);
  const rawPrice = o.price;
  /** @type {number | null} */
  let price;
  if (rawPrice === null) price = null;
  else {
    const n = Number(rawPrice);
    price = Number.isFinite(n) ? n : 0;
  }
  const out = {
    price,
    is_free: Boolean(o.is_free),
    text: String(o.text ?? 'unknown'),
    original_price: /** @type {number | null} */ (null),
    delivery_name: '',
    shipping_type: '',
  };
  const op = o.original_price ?? o.originalPrice;
  if (op != null && op !== '') {
    const n = Number(op);
    if (Number.isFinite(n)) out.original_price = n;
  }
  const dn = o.delivery_name ?? o.deliveryName;
  if (dn != null && String(dn).trim()) out.delivery_name = String(dn).trim();
  const st = o.shipping_type ?? o.shippingType;
  if (st != null && String(st).trim()) out.shipping_type = String(st).trim();
  return out;
}

/**
 * @param {unknown} prev
 * @param {unknown} inc
 */
export function mergeShippingPreferComplete(prev, inc) {
  const a = normalizeShippingEntry(prev);
  const b = normalizeShippingEntry(inc);
  if (b.price === null && String(b.text) === 'unknown') {
    if (shippingHasMeaningfulData(a)) return a;
    return { ...a, ...b };
  }
  if (b.text && b.text !== 'unknown') return { ...a, ...b };
  if (b.is_free) return { ...a, ...b, is_free: true };
  if (b.delivery_name || b.shipping_type || b.original_price != null) {
    return { ...a, ...b };
  }
  return a;
}
