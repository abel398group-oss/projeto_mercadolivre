/**
 * Camada analítica sobre produtos já normalizados (productSchema).
 * Funções puras — sem I/O nem Puppeteer.
 */

/** @param {number} x @param {number} lo @param {number} hi */
function clamp(x, lo, hi) {
  return Math.max(lo, Math.min(hi, x));
}

/**
 * Extrai número de vendas do texto de reputação (ex.: "+1 M vendas", "+10 mil vendas").
 * @param {unknown} snippet
 * @returns {number | null}
 */
export function parseSellerReputation(snippet) {
  const raw = String(snippet ?? '').trim();
  if (!raw) return null;

  // +1 M vendas / +2M vendas (M = milhão, não confundir com "mil")
  let m = raw.match(/\+\s*([\d\.,]+)\s*M\b/i);
  if (m && !/\bmil\b/i.test(raw)) {
    const n = parseFloat(String(m[1]).replace(/\./g, '').replace(',', '.'));
    if (Number.isFinite(n) && n > 0) return Math.round(n * 1_000_000);
  }

  // +10 mil vendas
  m = raw.match(/\+\s*([\d\.,]+)\s*mil\b/i);
  if (m) {
    const n = parseFloat(String(m[1]).replace(/\./g, '').replace(',', '.'));
    if (Number.isFinite(n) && n > 0) return Math.round(n * 1000);
  }

  // milhões por extenso
  if (/milh(ão|ões|oeis)/i.test(raw)) {
    m = raw.match(/\+\s*([\d\.,]+)/);
    if (m) {
      const n = parseFloat(String(m[1]).replace(/\./g, '').replace(',', '.'));
      if (Number.isFinite(n) && n > 0) return Math.round(n * 1_000_000);
    }
  }

  // +500 vendidos / 1.200 vendas
  m = raw.match(/\+\s*([\d\.,]+)\s*vend/i);
  if (m) {
    const n = parseInt(String(m[1]).replace(/\./g, '').replace(/,/g, ''), 10);
    if (Number.isFinite(n) && n > 0) return n;
  }

  m = raw.match(/([\d\.,]+)\s*vend(?:as|idos)?/i);
  if (m) {
    const n = parseInt(String(m[1]).replace(/\./g, '').replace(/,/g, ''), 10);
    if (Number.isFinite(n) && n > 0) return n;
  }

  return null;
}

/**
 * @param {unknown} text
 * @returns {number | null}
 */
export function parseStockHint(text) {
  const s = String(text ?? '');
  let m = s.match(/\+\s*(\d+)\s*dispon/i);
  if (m) return parseInt(m[1], 10);

  m = s.match(/quantidade:\s*(\d+)/i);
  if (m) return parseInt(m[1], 10);

  m = s.match(/(\d+)\s*unidades?/i);
  if (m) return parseInt(m[1], 10);

  m = s.match(/estoque[:\s]+(\d+)/i);
  if (m) return parseInt(m[1], 10);

  return null;
}

/**
 * @typedef {{
 *   is_free: boolean;
 *   free_condition: 'first_purchase' | 'full' | null;
 *   display_price: number | null;
 *   effective_price: number;
 * }} NormalizedShipping
 */

/**
 * @param {unknown} shipping
 * @returns {NormalizedShipping}
 */
export function normalizeShipping(shipping) {
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
  const priceNum =
    rawPrice == null || rawPrice === ''
      ? NaN
      : Number(rawPrice);
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
export function isShippingInconsistent(product) {
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

  const norm = normalizeShipping(sh);
  if (norm.free_condition === 'full' && price > 0) return true;
  return flaggedFree && price > 0;
}

/**
 * @param {Record<string, number>} parts
 * @param {number} cap
 */
function sumCap(parts, cap) {
  const s = Object.values(parts).reduce((a, b) => a + b, 0);
  return clamp(s, 0, cap);
}

/**
 * Componentes do score (0–100 cada bloco interno antes do clamp final).
 * @param {unknown} product
 * @returns {Record<string, number>}
 */
export function getMlScoreSignals(product) {
  const p = product && typeof product === 'object' ? /** @type {Record<string, unknown>} */ (product) : {};

  const discount = Number(p.discount);
  const discOk = Number.isFinite(discount) && discount > 0;
  let discountPts = 0;
  if (discOk) {
    if (discount >= 50) discountPts = 25;
    else if (discount >= 30) discountPts = 18;
    else if (discount >= 15) discountPts = 12;
    else discountPts = 6;
  }

  const rating = Number(p.rating);
  let ratingPts = 0;
  if (Number.isFinite(rating) && rating > 0) {
    if (rating >= 4.7) ratingPts = 20;
    else if (rating >= 4.3) ratingPts = 15;
    else if (rating >= 4.0) ratingPts = 10;
    else ratingPts = 5;
  }

  const sales = Number(p.sales_count);
  let salesPts = 0;
  if (Number.isFinite(sales) && sales > 0) {
    if (sales > 500) salesPts = 15;
    else if (sales > 100) salesPts = 10;
    else if (sales > 20) salesPts = 6;
    else salesPts = 3;
  }

  const repRaw = String(p.seller_reputation_snippet ?? '');
  const repParsed = parseSellerReputation(repRaw);
  let sellerPts = 0;
  if (repParsed != null && repParsed > 0) {
    if (repParsed >= 1_000_000) sellerPts = 20;
    else if (repParsed >= 100_000) sellerPts = 16;
    else if (repParsed >= 10_000) sellerPts = 12;
    else if (repParsed >= 1000) sellerPts = 8;
    else sellerPts = 4;
  }

  const normShip = normalizeShipping(p.shipping);
  let shipPts = 0;
  if (normShip.free_condition === 'full') shipPts = 10;
  else if (normShip.free_condition === 'first_purchase') shipPts = 6;
  else if (normShip.effective_price > 0 && normShip.effective_price < 30) shipPts = 4;
  else if (normShip.effective_price > 0) shipPts = 2;

  const pc = Number(p.price_current);
  const po = p.price_original != null ? Number(p.price_original) : NaN;
  let pricePts = 0;
  if (Number.isFinite(pc) && pc > 0 && Number.isFinite(po) && po > pc) {
    const ratio = pc / po;
    if (ratio <= 0.4) pricePts = 10;
    else if (ratio <= 0.55) pricePts = 7;
    else if (ratio <= 0.75) pricePts = 4;
    else pricePts = 2;
  } else if (Number.isFinite(pc) && pc > 0) {
    pricePts = 3;
  }

  const stockHint = parseStockHint(String(p.stock_hint ?? ''));
  let stockPts = 0;
  if (stockHint != null && stockHint > 0) {
    if (stockHint >= 25) stockPts = 10;
    else if (stockHint >= 5) stockPts = 7;
    else stockPts = 4;
  } else {
    const st = String(p.stock_status ?? '');
    if (/available|dispon|em estoque/i.test(st)) stockPts = 5;
  }

  return {
    discount: discountPts,
    rating: ratingPts,
    sales: salesPts,
    seller_reputation: sellerPts,
    shipping: shipPts,
    price_competitive: pricePts,
    stock: stockPts,
  };
}

/**
 * Score comercial 0–100.
 * @param {unknown} product
 * @returns {number}
 */
export function calculateMlScore(product) {
  const sig = getMlScoreSignals(product);
  return Math.round(sumCap(sig, 100));
}

const COMPLETENESS_WEIGHTS = {
  name: 9,
  price_current: 12,
  price_original: 8,
  discount: 6,
  rating: 8,
  sales_count: 8,
  images: 7,
  category: 8,
  seller: 8,
  shipping: 8,
  stock: 8,
  description: 10,
};

/**
 * Completude dos campos 0–100.
 * @param {unknown} product
 * @returns {number}
 */
export function calculateCompletenessScore(product) {
  if (!product || typeof product !== 'object') return 0;
  const p = /** @type {Record<string, unknown>} */ (product);

  let earned = 0;
  let max = 0;

  const check = (key, weight, ok) => {
    max += weight;
    if (ok) earned += weight;
  };

  check('name', COMPLETENESS_WEIGHTS.name, Boolean(String(p.name ?? '').trim()));
  check(
    'price_current',
    COMPLETENESS_WEIGHTS.price_current,
    Number.isFinite(Number(p.price_current)) && Number(p.price_current) > 0
  );
  const po = p.price_original;
  check(
    'price_original',
    COMPLETENESS_WEIGHTS.price_original,
    po != null && Number.isFinite(Number(po)) && Number(po) > 0
  );
  check('discount', COMPLETENESS_WEIGHTS.discount, Number.isFinite(Number(p.discount)) && Number(p.discount) > 0);
  check('rating', COMPLETENESS_WEIGHTS.rating, Number.isFinite(Number(p.rating)) && Number(p.rating) > 0);
  check(
    'sales_count',
    COMPLETENESS_WEIGHTS.sales_count,
    p.sales_count != null && p.sales_count !== '' && Number.isFinite(Number(p.sales_count))
  );

  const imgs = p.images;
  check(
    'images',
    COMPLETENESS_WEIGHTS.images,
    Array.isArray(imgs) ? imgs.length > 0 : Boolean(String(p.image_main ?? '').trim())
  );

  const catOk =
    (Array.isArray(p.categories) && p.categories.length > 0) ||
    Boolean(String(p.taxonomy_path ?? '').trim()) ||
    Boolean(String(p.product_category_from_breadcrumb ?? '').trim());
  check('category', COMPLETENESS_WEIGHTS.category, catOk);

  const sellerOk =
    Boolean(String(p.shop_name ?? '').trim()) ||
    Boolean(String(p.seller_id ?? '').trim()) ||
    Boolean(String(p.seller_reputation_snippet ?? '').trim());
  check('seller', COMPLETENESS_WEIGHTS.seller, sellerOk);

  const sh = p.shipping;
  check(
    'shipping',
    COMPLETENESS_WEIGHTS.shipping,
    sh &&
      typeof sh === 'object' &&
      (String(/** @type {Record<string, unknown>} */ (sh).text ?? '') !== 'unknown' ||
        /** @type {Record<string, unknown>} */ (sh).is_free === true ||
        Number(/** @type {Record<string, unknown>} */ (sh).price) > 0)
  );

  const stockOk =
    Boolean(String(p.stock_hint ?? '').trim()) ||
    Boolean(String(p.stock_status ?? '').trim()) ||
    parseStockHint(String(p.stock_hint ?? '')) != null;
  check('stock', COMPLETENESS_WEIGHTS.stock, stockOk);

  check('description', COMPLETENESS_WEIGHTS.description, Boolean(String(p.description ?? '').trim()));

  if (max <= 0) return 0;
  return Math.round((earned / max) * 100);
}
