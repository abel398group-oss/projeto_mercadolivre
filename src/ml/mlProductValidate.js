/**
 * Validação auditável do produto consolidado (sem analytics).
 */

/** @param {unknown} v */
function str(v) {
  return v == null ? '' : String(v).trim();
}

/**
 * @param {unknown} shipping
 */
function normalizeShippingForAudit(shipping) {
  const base = {
    is_free: false,
    free_condition: /** @type {'first_purchase' | 'full' | null} */ (null),
    display_price: /** @type {number | null} */ (null),
    effective_price: 0,
  };

  if (!shipping || typeof shipping !== 'object') return base;

  const o = /** @type {Record<string, unknown>} */ (shipping);
  const text = String(o.text ?? '');
  const rawPrice = o.price;
  const priceNum = rawPrice == null || rawPrice === '' ? NaN : Number(rawPrice);
  const hasPrice = Number.isFinite(priceNum) && priceNum > 0;

  if (hasPrice) base.display_price = priceNum;

  const firstPurchase = /primeira\s+compra|1[ªa]\s+compra|sua\s+primeira/i.test(text);
  const grátis =
    Boolean(o.is_free) ||
    /frete\s+gr[áa]tis|chegar[áa]\s+gr[áa]tis|gr[áa]tis\s+(segunda|ter|na|hoje)|\bgr[áa]tis\b/i.test(text);

  if (grátis && firstPurchase) {
    base.is_free = true;
    base.free_condition = 'first_purchase';
    base.effective_price = hasPrice ? priceNum : 0;
  } else if (grátis && !firstPurchase) {
    base.is_free = true;
    base.free_condition = 'full';
    base.effective_price = 0;
  } else if (hasPrice) {
    base.is_free = false;
    base.free_condition = null;
    base.effective_price = priceNum;
  } else {
    base.is_free = false;
    base.effective_price = 0;
  }

  return base;
}

/**
 * @param {unknown} product
 * @returns {boolean}
 */
function isShippingInconsistent(product) {
  if (!product || typeof product !== 'object') return false;
  const sh = /** @type {Record<string, unknown>} */ (product).shipping;
  if (!sh || typeof sh !== 'object') return false;
  const text = String(/** @type {Record<string, unknown>} */ (sh).text ?? '');
  const price = Number(/** @type {Record<string, unknown>} */ (sh).price);
  const flaggedFree = Boolean(/** @type {Record<string, unknown>} */ (sh).is_free);

  if (!flaggedFree || !Number.isFinite(price) || price <= 0) return false;
  const explained =
    /primeira\s+compra|1[ªa]\s+compra|gr[áa]tis|chegar[áa]\s+gr[áa]tis|frete\s+gr[áa]tis/i.test(text);
  if (explained) return false;

  const norm = normalizeShippingForAudit(sh);
  if (norm.free_condition === 'full' && price > 0) return true;
  return flaggedFree && price > 0;
}

/**
 * @param {import('../productSchema.js').CanonicalProduct} p
 * @returns {{ validation: Record<string, boolean>; issues: string[] }}
 */
export function validateCollectionProduct(p) {
  /** @type {string[]} */
  const issues = [];

  const itemId = str(p.item_id);
  const catalogId = str(p.catalog_product_id);
  const sellerId = str(p.seller_id);
  const url = str(p.url || p.url_primary);
  const price = Number(p.price_current);
  const sh = p.shipping && typeof p.shipping === 'object' ? /** @type {Record<string, unknown>} */ (p.shipping) : null;
  const shipText = sh ? str(sh.text) : '';
  const hasImage = Boolean(str(p.image_main)) || (Array.isArray(p.images) && p.images.length > 0);
  const catOk =
    Boolean(str(p.category_id)) ||
    Boolean(str(p.domain_id)) ||
    (Array.isArray(p.categories) && p.categories.length > 0 && str(p.categories[0]) !== 'uncategorized') ||
    Boolean(str(p.taxonomy_path));

  if (!itemId) issues.push('missing_item_id');
  if (!sellerId) issues.push('missing_seller_id');
  if (isShippingInconsistent(p)) issues.push('shipping_conflict');
  if (!hasImage) issues.push('missing_images');

  const valid_identity = Boolean((itemId || catalogId) && url);
  const valid_pricing = Number.isFinite(price) && price > 0;
  const valid_seller = Boolean(sellerId);
  const valid_shipping = Boolean(sh) && !isShippingInconsistent(p) && shipText !== '' && shipText !== 'unknown';
  const valid_category = catOk;
  const valid_media = hasImage;

  const validation = {
    valid_identity,
    valid_pricing,
    valid_seller,
    valid_shipping,
    valid_category,
    valid_media,
  };

  return { validation, issues };
}
